package admin_test

// review3 R3-10: when repoint's COMMIT reports an error, it reads on a new connection whether
// the transaction is in the cloud before it puts the control plane's credentials back. Needs
// LOKADB_REVIEW_DSN.

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestRepointTellsWhetherItsCommitIsInTheCloud(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := rev3DB(t, base, "rev3c_cloud")
	cust := rev3DB(t, base, "rev3c_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_app') THEN CREATE ROLE rev3_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs7 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs7 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs7", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("rev3_app", "apppw")
	if err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 7,
		Owner: "rev3_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log}); err != nil {
		t.Fatal(err)
	}
	id := rev3One(t, cloud, `SELECT coalesce(db_id, '') FROM lokadb.orgs WHERE org = 7`)
	app, err := pgconn.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	other := *app
	other.Database = "rev3c_elsewhere"
	for _, c := range []struct {
		name, id string
		app      *pgconn.Config
		want     bool
	}{
		{"the database id it wrote", id, app, true},
		{"another database id", "0123456789abcdef", app, false},
		{"no id, the server at its address", "", app, true},
		{"no id, the server elsewhere", "", &other, false},
	} {
		if got := admin.RepointCommitted(cloud, 7, c.id, "lokadb_org7", c.app, log); got != c.want {
			t.Errorf("%s: committed %v, want %v", c.name, got, c.want)
		}
	}
	if id == "" {
		t.Error("move-org recorded no database id")
	}
}

// review 3, R3-5: repoint gives the FDW server the new DSN's TLS settings and drops those it does
// not set (the old database's would stay otherwise).
func TestRepointCarriesTheTLSSettings(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := rev3DB(t, base, "rev3s_cloud")
	cust := rev3DB(t, base, "rev3s_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_app') THEN CREATE ROLE rev3_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs8 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs8 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs8", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("rev3_app", "apppw")
	if err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 7,
		Owner: "rev3_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log}); err != nil {
		t.Fatal(err)
	}
	// the old database's settings: a CRL the new one does not name
	rev3Exec(t, cloud, `ALTER SERVER lokadb_org7 OPTIONS (ADD sslcrl '/old/crl.pem')`)
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("sslsni", "0")
	u.RawQuery = q.Encode()
	if err := admin.Repoint(ctx, admin.RepointOptions{CloudDSN: cloud, CustomerAppDSN: u.String(), Org: 7,
		LockTimeout: 5 * time.Second, Attempts: 3, Log: log}); err != nil {
		t.Fatal(err)
	}
	got := rev3One(t, cloud, `SELECT array_to_string(ARRAY(SELECT o FROM pg_foreign_server, unnest(srvoptions) o
		WHERE srvname = 'lokadb_org7' AND o ~ '^ssl' ORDER BY o), ' ')`)
	if got != "sslmode=disable sslsni=0" {
		t.Fatalf("the server's TLS options after repoint: %q, want sslmode=disable sslsni=0 (the old sslcrl dropped)", got)
	}
}
