package admin_test

// Review 4, R4-24. A child table (parent_table) with a routing key (routing.keys): disable made
// the parent's lokadb_key_<col> again on the plain table, and DROP COLUMN of the organization
// column it watches failed ("cannot drop column org_id ... other objects depend on it"); undoing
// enable -prepare left lokadb_prep_key_<col> on it, with the same failure after its steps had
// already dropped the other triggers (they ran outside a transaction). Needs LOKADB_REVIEW_DSN.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestDisableChildWithRoutingKey(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	spec := admin.Spec{
		Tables: []admin.SpecTable{
			{Name: "docs", TenantColumn: "org_id"},
			{Name: "comments", ParentTable: "docs", ParentColumn: "doc_id"},
		},
		Routing: &admin.Routing{Keys: []admin.Key{{Table: "docs", Column: "slug"}, {Table: "comments", Column: "code"}}},
	}
	for _, prepareOnly := range []bool{false, true} {
		name := "r4k_enable"
		if prepareOnly {
			name = "r4k_prepare"
		}
		t.Run(name, func(t *testing.T) {
			cloud := rev3DB(t, base, name)
			rev3Exec(t, cloud,
				`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'r4_owner') THEN CREATE ROLE r4_owner; END IF; END $$`,
				`CREATE TABLE orgs (id bigint PRIMARY KEY, name text)`,
				`CREATE TABLE docs (id bigserial PRIMARY KEY, org_id bigint NOT NULL REFERENCES orgs(id), slug text UNIQUE, title text)`,
				`CREATE TABLE comments (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, doc_id bigint REFERENCES docs(id) ON DELETE CASCADE, code text UNIQUE, body text)`,
				`INSERT INTO orgs VALUES (1, 'a'), (2, 'b')`,
				`INSERT INTO docs (org_id, slug, title) SELECT 1 + g % 2, 's' || g, 't' || g FROM generate_series(1, 20) g`,
				`INSERT INTO comments (doc_id, code, body) SELECT 1 + g % 20, 'c' || g, 'b' || g FROM generate_series(1, 40) g`)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			o := admin.EnableOptions{DSN: cloud, Spec: spec, Owner: "r4_owner", Log: log, AllowDowntime: true}
			var err error
			if prepareOnly {
				err = admin.Prepare(ctx, o)
			} else {
				err = admin.EnableWith(ctx, o)
			}
			if err != nil {
				t.Fatal(err)
			}
			derr := admin.Disable(ctx, admin.DisableOptions{CloudDSN: cloud, Spec: spec, Log: log})
			state := rev3One(t, cloud, `SELECT (SELECT relkind::text FROM pg_class WHERE oid = 'comments'::regclass) || '|' ||
				(SELECT count(*) FROM pg_attribute WHERE attrelid = 'comments'::regclass AND attname = 'org_id' AND NOT attisdropped) || '|' ||
				(SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('comments'::regclass, 'docs'::regclass) AND tgname LIKE 'lokadb%') || '|' ||
				(SELECT count(*) FROM comments) || '|' || (SELECT count(*) FROM docs)`)
			t.Logf("disable: %v; comments relkind|org_id columns|lokadb triggers on docs, comments|comments|docs rows: %s", derr, state)
			if derr != nil {
				t.Errorf("FINDING: disable of a child with a routing key: %v", derr)
			}
			if state != "r|0|0|40|20" {
				t.Errorf("after disable: %s, want r|0|0|40|20 (plain table, no organization column, no lokadb trigger, every row)", state)
			}
		})
	}
}
