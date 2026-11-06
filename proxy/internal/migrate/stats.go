package migrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// StatsRel is what stats-sync did with one relation.
type StatsRel struct {
	Relation string `json:"relation"`
	Org      string `json:"org,omitempty"`
	// Numbers: columns that got the numbers without values of the organization's statistics.
	Numbers    int    `json:"numbers,omitempty"`
	ValuesLeft bool   `json:"values_left,omitempty"`
	Note       string `json:"note,omitempty"`
}

// StatsSync brings the cloud's statistics to what a move of this version leaves: no value of a
// moved organization in them, a row count of each foreign partition, and on Postgres 18 the numbers
// without values. For a database whose organizations were moved by an older version, and to refresh
// those numbers as the organizations grow. It only changes statistics: no lock stops a read or a
// write, and an organization that cannot be reached is reported and left for the next run.
func StatsSync(ctx context.Context, o Options) ([]StatsRel, error) {
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return nil, err
	}
	defer cloud.Close(ctx)
	rows, err := rowsOf(ctx, cloud, `SELECT table_name FROM lokadb.tables ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("the LokaDB tables (lokadb.tables; is the database enabled?): %w", err)
	}
	orgs, err := targets(ctx, cloud, o)
	if err != nil {
		return nil, err
	}
	if left, err := admin.StatsNoValues(ctx, cloud); err != nil {
		return nil, err
	} else if left > 0 {
		return nil, fmt.Errorf("%d LokaDB tables are held by another transaction: run stats-sync again when it ends", left)
	}
	if _, err := cloud.Exec(ctx, "SET lock_timeout = '5s'"); err != nil {
		return nil, err
	}
	conns := map[string]*pgx.Conn{}
	defer func() {
		for _, c := range conns {
			c.Close(ctx)
		}
	}()
	var out []StatsRel
	// done looks at what is left: statistics the role cannot clear may hold nothing
	done := func(r StatsRel, cleared bool, err error) {
		if err != nil {
			r.Note = strings.TrimSpace(r.Note + " " + firstLine(err))
		}
		if holds, herr := admin.HoldsStatValues(ctx, cloud, r.Relation); herr != nil {
			r.ValuesLeft, r.Note = !cleared, strings.TrimSpace(r.Note+" "+firstLine(herr))
		} else if r.ValuesLeft = holds; holds {
			r.Note = strings.TrimSpace(r.Note + " holds values: removing them needs Postgres 18 or a superuser")
		}
		out = append(out, r)
	}
	for _, row := range rows {
		table := row[0]
		cleared, err := admin.ClearStats(ctx, cloud, table, true)
		done(StatsRel{Relation: table}, cleared, err)
		for _, org := range orgs {
			foreign := admin.Helper(table, "__org"+org.Org)
			var there bool
			if err := cloud.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, admin.QR(foreign)).Scan(&there); err != nil {
				return out, err
			}
			if !there { // a table added since this organization's move
				continue
			}
			r := StatsRel{Relation: foreign, Org: org.Org}
			cleared, err := admin.ClearStats(ctx, cloud, foreign, false)
			if err != nil {
				done(r, cleared, err)
				continue
			}
			c, cerr := orgConn(ctx, org, conns)
			schema, name := admin.CustSchemaOf(table, org.Schema), admin.NameOf(table)
			if cerr != nil {
				r.Note = "its database: " + firstLine(cerr)
			} else if _, err := c.Exec(ctx, "ANALYZE "+pgx.Identifier{schema, name}.Sanitize()); err != nil {
				r.Note = "ANALYZE in its database: " + firstLine(err) // what statistics it has will do
			}
			if _, err := cloud.Exec(ctx, "ANALYZE "+admin.QR(foreign)); err != nil {
				r.Note = strings.TrimSpace(r.Note + " ANALYZE of the partition: " + firstLine(err))
			}
			if cerr == nil {
				if r.Numbers, err = admin.StatNumbers(ctx, cloud, c, foreign, schema, name); err != nil {
					r.Note = strings.TrimSpace(r.Note + " numbers: " + firstLine(err))
				}
			}
			done(r, cleared, nil)
		}
		part, cleaned, err := admin.CleanCloudPartition(ctx, cloud, table)
		switch {
		case part == "" && err != nil:
			return out, err
		case cleaned: // analyzed: what it holds now is of the organizations still in the cloud
			out = append(out, StatsRel{Relation: part})
		default:
			done(StatsRel{Relation: part}, false, err)
		}
	}
	return out, nil
}

func orgConn(ctx context.Context, org *Org, conns map[string]*pgx.Conn) (*pgx.Conn, error) {
	if c := conns[org.Org]; c != nil {
		return c, nil
	}
	if org.dsn == "" {
		return nil, fmt.Errorf("no credentials: no user mapping of its FDW server, no -customer %s=DSN", org.Org)
	}
	c, err := connect(ctx, org.dsn)
	if err != nil {
		return nil, err
	}
	conns[org.Org] = c
	return c, nil
}

func firstLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}

// StatsClean: no statistics hold values any more.
func StatsClean(rels []StatsRel) bool {
	for _, r := range rels {
		if r.ValuesLeft {
			return false
		}
	}
	return true
}

// StatsText: the report of stats-sync for people.
func StatsText(rels []StatsRel) string {
	var b strings.Builder
	for _, r := range rels {
		fmt.Fprintf(&b, "%s", r.Relation)
		if r.Org != "" {
			fmt.Fprintf(&b, " (organization %s", r.Org)
			if r.Numbers > 0 {
				fmt.Fprintf(&b, ", numbers of %d columns", r.Numbers)
			}
			b.WriteString(")")
		}
		if r.ValuesLeft {
			b.WriteString(": VALUES LEFT")
		} else {
			b.WriteString(": no values of moved organizations")
		}
		if r.Note != "" {
			b.WriteString("; " + r.Note)
		}
		b.WriteString("\n")
	}
	return b.String()
}
