package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Statements without a customer (the fanout review). The proxy counts, per statement shape, what ran
// or was refused for naming no customer; the runner adds that up across proxy restarts (state/unrouted.json),
// reports how many are still open ("unrouted_open"), and answers the panel's review and decisions.

type unroutedStore struct {
	mu       sync.Mutex
	loaded   bool
	items    map[string]*proxy.Unrouted
	last     map[string]int64 // the proxy's own count at the last look: what is new is the difference
	overflow int64
}

func (c *Config) unroutedPath() string { return filepath.Join(c.State, "unrouted.json") }

// loadUnrouted reads the saved list once; the caller holds c.unr.mu.
func (c *Config) loadUnrouted() {
	u := &c.unr
	if u.loaded {
		return
	}
	u.loaded, u.items, u.last = true, map[string]*proxy.Unrouted{}, map[string]int64{}
	if b, err := os.ReadFile(c.unroutedPath()); err == nil {
		var l []proxy.Unrouted
		if json.Unmarshal(b, &l) == nil {
			for i := range l {
				u.items[l[i].Fingerprint] = &l[i]
			}
		}
	}
}

// saveUnrouted writes the list; the caller holds c.unr.mu.
func (c *Config) saveUnrouted() {
	l := make([]proxy.Unrouted, 0, len(c.unr.items))
	for _, x := range c.unr.items {
		l = append(l, *x)
	}
	sort.Slice(l, func(i, j int) bool { return l[i].Fingerprint < l[j].Fingerprint })
	b, _ := json.Marshal(l)
	if os.MkdirAll(c.State, 0o755) == nil {
		_ = os.WriteFile(c.unroutedPath()+".tmp", b, 0o600)
		_ = os.Rename(c.unroutedPath()+".tmp", c.unroutedPath())
	}
}

// collectUnrouted takes what the running proxy has counted since the last look.
func (c *Config) collectUnrouted(list []proxy.Unrouted, overflow int64) {
	u := &c.unr
	u.mu.Lock()
	defer u.mu.Unlock()
	c.loadUnrouted()
	changed := false
	for _, x := range list {
		d := x.Count - u.last[x.Fingerprint]
		if d < 0 { // the proxy restarted: its count began again
			d = x.Count
		}
		u.last[x.Fingerprint] = x.Count
		if d == 0 {
			continue
		}
		changed = true
		if have := u.items[x.Fingerprint]; have != nil {
			have.Count += d
			have.Refused = x.Refused
			continue
		}
		x.Count = d
		u.items[x.Fingerprint] = &x
	}
	u.overflow = max(u.overflow, overflow)
	if changed {
		c.saveUnrouted()
	}
	c.setInfo("unrouted_open", strconv.Itoa(len(u.items)))
}

// reasonOf says in words why a statement names no customer.
func reasonOf(gaps []proxy.Gap) (reason, table, column string) {
	var parts []string
	for _, g := range gaps {
		if table == "" {
			table, column = g.Table, g.Column
		}
		switch g.Kind {
		case proxy.GapNoCondition:
			parts = append(parts, fmt.Sprintf("reads or changes %s without any condition on a customer", g.Table))
		case proxy.GapUnknownColumn:
			parts = append(parts, fmt.Sprintf("looks %s up by %s, which does not say which customer it belongs to", g.Table, g.Column))
		case proxy.GapUnsupportedForm:
			parts = append(parts, fmt.Sprintf("uses a form LokaDB cannot read for %s (%s)", g.Table, g.Form))
		case proxy.GapUnreadableValue:
			parts = append(parts, fmt.Sprintf("compares %s.%s with an expression instead of a value", g.Table, g.Column))
		default:
			parts = append(parts, g.String())
		}
	}
	if len(parts) == 0 {
		return "names no customer", "", ""
	}
	return strings.Join(parts, "; "), table, column
}

// Statements answers the panel's review: the open statement shapes, with what can be done about each.
func (o *Ops) Statements(ctx context.Context, _ cloudproto.StatementsArgs) (cloudproto.StatementsResult, error) {
	c := o.Cfg
	res := cloudproto.StatementsResult{Policy: c.fanoutMode, Items: []cloudproto.Statement{}}
	c.unr.mu.Lock()
	c.loadUnrouted()
	items := make([]proxy.Unrouted, 0, len(c.unr.items))
	for _, x := range c.unr.items {
		items = append(items, *x)
	}
	res.Overflow = c.unr.overflow
	c.unr.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].Count > items[j].Count || items[i].Count == items[j].Count && items[i].Fingerprint < items[j].Fingerprint
	})
	conn, err := ownsql.Connect(ctx, c.DB)
	if err != nil {
		conn = nil // the list still shows; the choices that need the database are left out
	} else {
		defer conn.Close(context.Background())
	}
	for _, x := range items {
		reason, table, col := reasonOf(x.Gaps)
		st := cloudproto.Statement{Fingerprint: x.Fingerprint, SQL: x.SQL, Table: table, Column: col, Reason: reason, Write: x.Write, Refused: x.Refused, Count: x.Count, Options: []string{}}
		if conn != nil {
			if o.keyColumn(ctx, conn, x.Gaps) != nil {
				st.Options = append(st.Options, "key")
			}
			if o.cloudTable(ctx, conn, x.Gaps) != nil {
				st.Options = append(st.Options, "cloud")
			}
		}
		if x.Full != "" {
			st.Options = append(st.Options, "allow")
		}
		res.Items = append(res.Items, st)
	}
	return res, nil
}

func splitTable(t string) (schema, name string) {
	if i := strings.IndexByte(t, '.'); i > 0 {
		return t[:i], t[i+1:]
	}
	return "public", t
}

// keyColumn: the table.column that could be a routing key: a column compared by value that is
// unique in its table by itself (a slug, a public token), and is neither the customer column nor an id.
func (o *Ops) keyColumn(ctx context.Context, conn *pgx.Conn, gaps []proxy.Gap) *admin.Key {
	for _, g := range gaps {
		if g.Kind != proxy.GapUnknownColumn || g.Cloud || g.Column == "" || g.Column == "id" || g.Form == "INSERT" {
			continue
		}
		if o.isTenantColumn(g.Column) {
			continue
		}
		schema, name := splitTable(g.Table)
		var ok bool
		err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i
			JOIN pg_class t ON t.oid = i.indrelid
			LEFT JOIN pg_inherits h ON h.inhrelid = t.oid
			LEFT JOIN pg_class p ON p.oid = h.inhparent
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
			WHERE i.indisunique AND i.indnkeyatts = 1 AND i.indpred IS NULL AND a.attname = $3
			  AND ((t.relname = $1 AND t.relnamespace = $2::regnamespace) OR (p.relname = $1 AND p.relnamespace = $2::regnamespace)))`, name, schema, g.Column).Scan(&ok)
		if err == nil && ok {
			return &admin.Key{Table: g.Table, Column: g.Column}
		}
	}
	return nil
}

func (o *Ops) isTenantColumn(col string) bool {
	for _, t := range o.Cfg.spec.Tables {
		if t.TenantColumn == col {
			return true
		}
	}
	return false
}

// cloudTable: a table of the statement that is not per customer (it has no customer column in the
// spec) but has a column that is the customer's number: it can be declared a cloud table.
func (o *Ops) cloudTable(ctx context.Context, conn *pgx.Conn, gaps []proxy.Gap) *admin.CloudTable {
	for _, g := range gaps {
		if g.Kind != proxy.GapUnknownColumn || !g.Cloud {
			continue
		}
		if o.isTenantColumn(g.Column) {
			return &admin.CloudTable{Table: g.Table, TenantColumn: g.Column}
		}
		if g.Column != "id" {
			continue
		}
		schema, name := splitTable(g.Table)
		for _, t := range o.Cfg.spec.Tables {
			if t.TenantColumn == "" {
				continue
			}
			var ok bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 AND data_type IN ('integer', 'bigint', 'smallint'))`,
				schema, name, t.TenantColumn).Scan(&ok); err == nil && ok {
				return &admin.CloudTable{Table: g.Table, TenantColumn: t.TenantColumn}
			}
		}
	}
	return nil
}

// Decide applies the panel's choices: routing declarations go into the spec (kept in spec.json) and to
// the database when it is prepared; an allowed statement goes into proxy.json's allow list of the
// application's database. What is decided leaves the open list.
func (o *Ops) Decide(ctx context.Context, a cloudproto.DecideArgs) (cloudproto.DecideResult, error) {
	c := o.Cfg
	res := cloudproto.DecideResult{Applied: []string{}, Failed: map[string]string{}}
	conn, err := ownsql.Connect(ctx, c.DB)
	if err != nil {
		return res, err
	}
	defer conn.Close(context.Background())
	spec := c.spec
	var allow []string
	routed := false
	c.unr.mu.Lock()
	c.loadUnrouted()
	for _, d := range a.Decisions {
		x := c.unr.items[d.Fingerprint]
		if x == nil {
			res.Failed[d.Fingerprint] = "this statement is not in the list any more"
			continue
		}
		switch d.Choice {
		case "allow":
			if x.Full == "" {
				res.Failed[d.Fingerprint] = "the statement is too long to be allowed by name"
				continue
			}
			allow = append(allow, x.Full)
		case "key":
			k := o.keyColumn(ctx, conn, x.Gaps)
			if k == nil {
				res.Failed[d.Fingerprint] = "no column of this statement is unique by itself, so it cannot name the customer"
				continue
			}
			spec.Merge(&admin.Routing{Keys: []admin.Key{*k}})
			routed = true
		case "cloud":
			t := o.cloudTable(ctx, conn, x.Gaps)
			if t == nil {
				res.Failed[d.Fingerprint] = "the table has no customer column to declare it a cloud table with"
				continue
			}
			spec.Merge(&admin.Routing{CloudTables: []admin.CloudTable{*t}})
			routed = true
		default:
			res.Failed[d.Fingerprint] = "unknown choice " + d.Choice
			continue
		}
		res.Applied = append(res.Applied, d.Fingerprint)
	}
	c.unr.mu.Unlock()
	if routed {
		b, _ := json.Marshal(spec)
		if err := os.WriteFile(filepath.Join(Dir, "spec.json"), append(b, '\n'), 0o644); err != nil {
			return res, fmt.Errorf("spec.json: %w", err)
		}
		c.spec = spec
		res.Spec = b
		if c.preparedTables(ctx) {
			if err := admin.ApplyRouting(ctx, c.DB, spec, c.Log); err != nil {
				return res, fmt.Errorf("the routing declarations could not be applied to your database: %w", err)
			}
		}
	}
	if len(allow) > 0 {
		if err := c.addFanoutAllow(allow); err != nil {
			return res, err
		}
	}
	c.unr.mu.Lock()
	for _, fp := range res.Applied {
		delete(c.unr.items, fp)
	}
	c.saveUnrouted()
	c.setInfo("unrouted_open", strconv.Itoa(len(c.unr.items)))
	c.unr.mu.Unlock()
	return res, nil
}

// preparedTables: LokaDB's tables are prepared in this database (routing is applied only then).
func (c *Config) preparedTables(ctx context.Context) bool {
	conn, err := ownsql.Connect(ctx, c.DB)
	if err != nil {
		return false
	}
	defer conn.Close(context.Background())
	var ok bool
	return conn.QueryRow(ctx, `SELECT to_regclass('lokadb.tables') IS NOT NULL`).Scan(&ok) == nil && ok
}

// addFanoutAllow writes statements into the allow list of the application's database in proxy.json
// (everything else in the file stays) and asks for a restart when the proxy runs.
func (c *Config) addFanoutAllow(sql []string) error {
	if c.found == nil || c.found.DBName == "" {
		return fmt.Errorf("the application's database is not known")
	}
	path := filepath.Join(Dir, "proxy.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return fmt.Errorf("proxy.json: %w", err)
	}
	fan := map[string]json.RawMessage{}
	if m["fanout"] != nil {
		if err := json.Unmarshal(m["fanout"], &fan); err != nil {
			return fmt.Errorf("proxy.json: fanout is not an object, so LokaDB leaves it alone")
		}
	}
	entry := map[string]json.RawMessage{}
	if e := fan[c.found.DBName]; e != nil {
		_ = json.Unmarshal(e, &entry)
	} else if star := fan["*"]; star != nil {
		_ = json.Unmarshal(star, &entry) // an entry of its own replaces "*" for this database: it starts from it
	}
	var have []string
	if entry["allow"] != nil {
		_ = json.Unmarshal(entry["allow"], &have)
	}
	seen := map[string]bool{}
	for _, s := range have {
		seen[s] = true
	}
	for _, s := range sql {
		if !seen[s] {
			have = append(have, s)
			seen[s] = true
		}
	}
	entry["allow"], _ = json.Marshal(have)
	fan[c.found.DBName], _ = json.Marshal(entry)
	m["fanout"], _ = json.Marshal(fan)
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(out, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if embeddedStarted() {
		c.restart = true
	}
	return nil
}
