package check

// The DDL an organization's older PostgreSQL cannot create (admin.CustomerSyntaxProblems,
// customer_syntax), and check on a cloud of each version, on live servers of 13 to 18 (skipped
// without them; pg_stat_statements preloaded):
//   LOKADB_PG_VERSIONS=13=postgres://postgres:pw@host:5413,14=...,18=postgres://postgres:pw@host:5418 \
//     go test ./check -run LiveVersion

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// versionServers: the servers of LOKADB_PG_VERSIONS by major version.
func versionServers(t *testing.T) map[int]string {
	t.Helper()
	env := os.Getenv("LOKADB_PG_VERSIONS")
	if env == "" {
		t.Skip("LOKADB_PG_VERSIONS not set")
	}
	out := map[int]string{}
	for _, kv := range strings.Split(env, ",") {
		k, v, _ := strings.Cut(kv, "=")
		n, err := strconv.Atoi(k)
		if err != nil {
			t.Fatalf("LOKADB_PG_VERSIONS: %q", kv)
		}
		out[n] = v
	}
	return out
}

// freshDB makes database name on the server of base anew and runs stmts in it.
func freshDB(t *testing.T, base, name string, stmts ...string) string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := c.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	d, err := pgx.Connect(ctx, base+"/"+name)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	for _, s := range stmts {
		if _, err := d.Exec(ctx, s); err != nil {
			t.Fatalf("%v\n  in: %s", err, s)
		}
	}
	return base + "/" + name
}

// checkAgainst: the report of a check of the spec against the organization's database customer.
func checkAgainst(t *testing.T, cloud, customer string, spec admin.Spec) *Report {
	t.Helper()
	rep, err := Run(context.Background(), Options{DSN: cloud, Spec: &spec, CountLimit: -1, CustomerDSN: customer})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range rep.Findings {
		if f.Code == "customer_unreachable" {
			t.Fatalf("customer_unreachable: %s", f.Message)
		}
	}
	return rep
}

// syntaxOf: the messages of the customer_syntax findings, joined.
func syntaxOf(rep *Report) string {
	var out []string
	for _, f := range rep.Findings {
		if f.Code == "customer_syntax" {
			if f.Severity != Blocker {
				out = append(out, "NOT A BLOCKER: ")
			}
			out = append(out, f.Message)
		}
	}
	return strings.Join(out, "\n")
}

// A cloud on 18 whose tables use every form, checked against an organization's database of
// each version: each form is a blocker where that server is older than the form, and only there.
func TestLiveVersionSyntaxAgainstEach(t *testing.T) {
	srv := versionServers(t)
	cloud, ok := srv[18]
	if !ok {
		t.Skip("no PostgreSQL 18 in LOKADB_PG_VERSIONS")
	}
	dsn := freshDB(t, cloud, "b11_ver_cloud",
		`CREATE EXTENSION btree_gist`,
		`CREATE FUNCTION app_positive(x int) RETURNS boolean LANGUAGE sql IMMUTABLE RETURN x > 0`,
		`CREATE TABLE orders (id bigint PRIMARY KEY, org_id bigint NOT NULL, code text, qty int CHECK (app_positive(qty)),
			busy int4multirange, total int GENERATED ALWAYS AS (qty * 2) VIRTUAL,
			CONSTRAINT orders_code UNIQUE NULLS NOT DISTINCT (org_id, code),
			CONSTRAINT orders_small CHECK (qty < 1000) NOT ENFORCED)`,
		`CREATE TABLE bookings (id bigint PRIMARY KEY, org_id bigint NOT NULL, room int NOT NULL, during tsrange NOT NULL,
			CONSTRAINT bookings_room UNIQUE (org_id, room, during WITHOUT OVERLAPS))`,
	)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "orders", TenantColumn: "org_id"}, {Name: "bookings", TenantColumn: "org_id"}}}
	// what the message names, and the first version that has it
	forms := map[string]int{"multirange columns: orders.busy": 14, "SQL-standard body": 14, "app_positive(integer)": 14,
		"NULLS NOT DISTINCT: orders.orders_code": 15, "virtual generated columns: orders.total": 18,
		"orders.orders_small": 18, "bookings.bookings_room": 18}
	for v := 13; v <= 18; v++ {
		base, ok := srv[v]
		if !ok {
			t.Errorf("no PostgreSQL %d in LOKADB_PG_VERSIONS", v)
			continue
		}
		t.Run(strconv.Itoa(v), func(t *testing.T) {
			got := syntaxOf(checkAgainst(t, dsn, freshDB(t, base, "b11_ver_org", `CREATE EXTENSION btree_gist`), spec))
			if strings.Contains(got, "NOT A BLOCKER") {
				t.Errorf("customer_syntax is not a blocker: %s", got)
			}
			for what, min := range forms {
				if strings.Contains(got, what) != (v < min) {
					t.Errorf("PostgreSQL %d (%s needs %d): customer_syntax %q", v, what, min, got)
				}
			}
		})
	}
}

// A cloud and an organization of the same version, each of 13 to 18, with an FK of ON DELETE
// SET NULL (move-org writes it without a column list before 15): check reads the cloud (the
// workload of pg_stat_statements too, whose toplevel is new in 14) and finds nothing the
// organization's server cannot create.
func TestLiveVersionSameVersion(t *testing.T) {
	srv := versionServers(t)
	for v := 13; v <= 18; v++ {
		base, ok := srv[v]
		if !ok {
			t.Errorf("no PostgreSQL %d in LOKADB_PG_VERSIONS", v)
			continue
		}
		t.Run(strconv.Itoa(v), func(t *testing.T) {
			dsn := freshDB(t, base, "b11_ver_same",
				`CREATE EXTENSION pg_stat_statements`,
				`CREATE TABLE orders (id bigint PRIMARY KEY, org_id bigint NOT NULL, code text UNIQUE)`,
				`CREATE TABLE order_lines (id bigint PRIMARY KEY, order_id bigint REFERENCES orders ON DELETE SET NULL)`,
				`SELECT count(*) FROM orders WHERE org_id = 1`)
			spec := admin.Spec{Tables: []admin.SpecTable{{Name: "orders", TenantColumn: "org_id"},
				{Name: "order_lines", ParentTable: "orders", ParentColumn: "order_id"}}}
			rep := checkAgainst(t, dsn, freshDB(t, base, "b11_ver_same_org"), spec)
			if s := syntaxOf(rep); s != "" {
				t.Errorf("PostgreSQL %d with %d: %s", v, v, s)
			}
			for _, f := range rep.Findings {
				if f.Code == "pg_stat_statements_unreadable" || f.Code == "no_pg_stat_statements" {
					t.Errorf("PostgreSQL %d: %s: %s", v, f.Code, f.Message)
				}
			}
		})
	}
}
