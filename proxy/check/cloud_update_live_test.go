package check

// Review 3 of check, L1, on a live Postgres (skipped without one):
//   LOKADB_LIVE_DSN=postgres://postgres:pw@host:5432 go test ./check -run Live

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// An FK onto a cloud table with ON DELETE RESTRICT ON UPDATE CASCADE under reject-writes: a
// change of the cloud row's key writes the organizations' rows, which the policy refuses. check
// said "lokadb does that" (info): it read ON DELETE alone.
func TestLiveCloudFKOnUpdate(t *testing.T) {
	base := os.Getenv("LOKADB_LIVE_DSN")
	if base == "" {
		t.Skip("LOKADB_LIVE_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS b4_fk_update WITH (FORCE)")
	if _, err := c.Exec(ctx, "CREATE DATABASE b4_fk_update"); err != nil {
		t.Fatal(err)
	}
	c.Close(ctx)
	dsn := base + "/b4_fk_update"
	d, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		`CREATE TABLE accounts (code text PRIMARY KEY)`,
		`CREATE TABLE docs (id bigint PRIMARY KEY, org_id bigint NOT NULL, account text REFERENCES accounts ON DELETE RESTRICT ON UPDATE CASCADE)`,
	} {
		if _, err := d.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	d.Close(ctx)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs", TenantColumn: "org_id"}}}
	rep, err := Run(ctx, Options{DSN: dsn, Spec: &spec, CountLimit: -1,
		Proxy: &proxy.Config{Fanout: map[string]proxy.FanoutPolicy{"*": {Mode: proxy.FanoutRejectWrites}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range rep.Findings {
		if f.Code == "fk_to_cloud_delete" {
			if f.Severity != Warning {
				t.Errorf("fk_to_cloud_delete is %s, want warning: %s", f.Severity, f.Message)
			}
			return
		}
	}
	t.Errorf("no fk_to_cloud_delete: %s", codes(rep.Findings))
}
