package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

func unmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }

func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return ownsql.Connect(cctx, dsn) // the command's statements are LokaDB's own (check skips them)
}

type lokadbTable struct{ name, tenant string }

// DiffSchemas compares every moved organization's copy of the LokaDB tables with the cloud's.
func DiffSchemas(ctx context.Context, o Options) ([]*Org, error) { return run(ctx, o, false) }

// Sync brings every moved organization's copy up to the cloud's: one transaction per
// organization, all of its fixes or none. What it cannot fix stays in Diffs.
func Sync(ctx context.Context, o Options) ([]*Org, error) { return run(ctx, o, true) }

func run(ctx context.Context, o Options, sync bool) ([]*Org, error) {
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return nil, err
	}
	defer cloud.Close(ctx)
	// The cloud's schema holds still while it is compared and copied: a migration (ALTER
	// TABLE, CREATE INDEX: through the proxy, which replays it) waits, writes go on.
	tx, err := cloud.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := rowsOf(ctx, tx, `SELECT table_name, tenant_column FROM lokadb.tables ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("the LokaDB tables (lokadb.tables; is the database enabled?): %w", err)
	}
	var tables []lokadbTable
	for _, r := range rows {
		tables = append(tables, lokadbTable{r[0], r[1]})
	}
	if sync {
		for _, t := range tables {
			if _, err := tx.Exec(ctx, "LOCK TABLE "+admin.QR(t.name)+" IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
				return nil, err
			}
		}
	}
	cl := map[string]*table{}
	for _, t := range tables {
		if cl[t.name], err = cloudTable(ctx, tx, t.name, t.tenant); err != nil {
			return nil, err
		}
	}
	orgs, err := targets(ctx, tx, o)
	if err != nil {
		return nil, err
	}
	for _, org := range orgs {
		if org.dsn == "" {
			org.Error = "no credentials for its database: no user mapping of its FDW server, no -customer " + org.Org + "=DSN"
			continue
		}
		if err := org.compare(ctx, tables, cl, sync, o.DropExtra); err != nil {
			org.Error = err.Error()
		}
	}
	return orgs, nil
}

func (org *Org) compare(ctx context.Context, tables []lokadbTable, cl map[string]*table, sync, dropExtra bool) error {
	c, err := connect(ctx, org.dsn)
	if err != nil {
		return fmt.Errorf("its database: %w", err)
	}
	defer c.Close(ctx)
	diff := func(q querier) ([]Diff, error) {
		var out []Diff
		for _, t := range tables {
			ot, err := orgTable(ctx, q, admin.CustSchemaOf(t.name, org.Schema), admin.NameOf(t.name), t.tenant)
			if err != nil {
				return nil, err
			}
			out = append(out, compare(org.Org, org.Schema, t.name, t.tenant, cl[t.name], ot)...)
		}
		return out, nil
	}
	if !sync {
		org.Diffs, err = diff(c)
		return err
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	ds, err := diff(tx)
	if err != nil {
		return err
	}
	var fixed []Diff
	for _, d := range ds {
		if d.Fix == "" || d.Drop && !dropExtra {
			org.Diffs = append(org.Diffs, d)
			continue
		}
		if _, err := tx.Exec(ctx, d.Fix); err != nil {
			org.Diffs = ds
			return fmt.Errorf("%s: %w (nothing changed in this organization's database)", d.Fix, err)
		}
		fixed = append(fixed, d)
	}
	// what is left must be what was not tried: the fixes are checked by comparing again
	after, err := diff(tx)
	if err != nil {
		return err
	}
	if len(after) != len(org.Diffs) {
		org.Diffs = after
		return fmt.Errorf("the fixes did not converge (%d differences left, %d expected): nothing changed in this organization's database", len(after), len(org.Diffs))
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	org.Synced = fixed
	return nil
}

// Text: the report as an operator reads it.
func Text(orgs []*Org, sync bool) string {
	var b strings.Builder
	if len(orgs) == 0 {
		b.WriteString("no organization is moved\n")
	}
	for _, org := range orgs {
		fmt.Fprintf(&b, "organization %s (schema %s): ", org.Org, org.Schema)
		switch {
		case org.Error != "":
			fmt.Fprintf(&b, "ERROR %s\n", org.Error)
		case len(org.Diffs) == 0 && len(org.Synced) == 0:
			b.WriteString("same as the cloud\n")
		case len(org.Diffs) == 0:
			fmt.Fprintf(&b, "synced, %d fix(es)\n", len(org.Synced))
		default:
			fmt.Fprintf(&b, "%d difference(s)\n", len(org.Diffs))
		}
		for _, d := range org.Synced {
			fmt.Fprintf(&b, "  fixed  %s.%s %s: %s\n", d.Table, d.Object, d.Kind, d.Fix)
		}
		for _, d := range org.Diffs {
			fmt.Fprintf(&b, "  %-6s %s.%s %s: cloud %q, organization %q", "diff", d.Table, d.Object, d.Kind, d.Cloud, d.Here)
			switch {
			case d.Fix == "":
				b.WriteString("; fix by hand (or move-org again)")
			case d.Drop && sync:
				b.WriteString("; schema-sync -drop-extra drops it: " + d.Fix)
			default:
				b.WriteString("; schema-sync: " + d.Fix)
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

// Clean: nothing differs and nothing failed.
func Clean(orgs []*Org) bool {
	for _, org := range orgs {
		if org.Error != "" || len(org.Diffs) > 0 {
			return false
		}
	}
	return true
}
