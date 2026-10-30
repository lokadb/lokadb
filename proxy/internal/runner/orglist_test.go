package runner

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

// The column that names a customer in the list: the name, else the e-mail, else a title, slug or username.
func TestPickLabel(t *testing.T) {
	have := func(c ...string) map[string]bool {
		m := map[string]bool{}
		for _, x := range c {
			m[x] = true
		}
		return m
	}
	for _, c := range []struct {
		cols map[string]bool
		want string
	}{
		{have("id", "name", "email", "slug"), "name"},
		{have("id", "email", "slug", "username"), "email"},
		{have("id", "title", "slug"), "title"},
		{have("id", "slug", "username"), "slug"},
		{have("id", "username"), "username"},
		{have("id", "plan"), ""},
	} {
		want := c.want
		if got := pickLabel(c.cols); got != want {
			t.Errorf("%v: %q, want %q", c.cols, got, want)
		}
	}
}

// TestOrgListNullName (INIT_TEST_DSN): a customer whose name is NULL or empty is shown by its
// e-mail, one with neither by nothing; the list does not fail on them, and search finds by e-mail.
func TestOrgListNullName(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS orglist_users;
		CREATE TABLE orglist_users (id text PRIMARY KEY, name varchar, email varchar);
		INSERT INTO orglist_users VALUES ('a', 'Acme', 'a@x.test'), ('b', NULL, 'b@x.test'), ('c', '', 'c@x.test'), ('d', NULL, NULL)`); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `DROP TABLE orglist_users`)
	o := &Ops{Cfg: &Config{DB: dsn, settings: &cloudproto.Settings{OrgTable: "orglist_users", OrgKey: "id"}}}
	res, err := o.OrgList(ctx, cloudproto.OrgListArgs{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, it := range res.Orgs {
		got[it.Key] = it.Label
	}
	want := map[string]string{"a": "Acme", "b": "b@x.test", "c": "c@x.test", "d": ""}
	if res.LabelColumn != "name" || len(got) != 4 {
		t.Fatalf("label %q, %v", res.LabelColumn, got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q", k, got[k], v)
		}
	}
	if res, err = o.OrgList(ctx, cloudproto.OrgListArgs{Q: "b@x"}); err != nil || len(res.Orgs) != 1 || res.Orgs[0].Key != "b" {
		t.Errorf("search by e-mail: %v, %v", res.Orgs, err)
	}
}
