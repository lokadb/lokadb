package proxy

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Third code review (R4-20), on a real Postgres (LOKADB_STAMP_DSN, a superuser of a throwaway
// database): a table with a rule of the application is like one with a statement trigger, and
// a table without either is not.
func TestRuleCountsAsStatementTrigger(t *testing.T) {
	dsn := os.Getenv("LOKADB_STAMP_DSN")
	if dsn == "" {
		t.Skip("LOKADB_STAMP_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for _, q := range []string{
		`DROP TABLE IF EXISTS r4_soft, r4_plain, r4_audit CASCADE`,
		`CREATE TABLE r4_soft (id bigint PRIMARY KEY, org bigint NOT NULL, deleted boolean NOT NULL DEFAULT false)`,
		`CREATE TABLE r4_plain (id bigint PRIMARY KEY, org bigint NOT NULL)`,
		`CREATE TABLE r4_audit (id bigint, at timestamptz DEFAULT now())`,
		`CREATE RULE r4_soft_delete AS ON DELETE TO r4_soft DO INSTEAD UPDATE r4_soft SET deleted = true WHERE id = OLD.id`,
		`CREATE VIEW r4_view AS SELECT * FROM r4_plain`, // a view's _RETURN rule is not one of the application's
	} {
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	m := &Meta{Tables: map[string]*Table{"r4_soft": {Name: "r4_soft"}, "r4_plain": {Name: "r4_plain"}}}
	q := func(sql string) ([][]string, error) {
		rows, err := c.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out [][]string
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return nil, err
			}
			row := make([]string, len(vals))
			for i, v := range vals {
				if v != nil {
					row[i] = fmt.Sprint(v)
				}
			}
			out = append(out, row)
		}
		return out, rows.Err()
	}
	if err := CatalogFacts(q, m); err != nil {
		t.Fatal(err)
	}
	if !m.StmtTriggers["r4_soft"] {
		t.Errorf("a table with ON DELETE … DO INSTEAD is not StmtTriggers: a write aimed at a moved organization's partition would skip the rule")
	}
	if m.StmtTriggers["r4_plain"] {
		t.Errorf("a table with no trigger and no rule is StmtTriggers")
	}
}
