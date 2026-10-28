package main

// A10. A publication of a table (CDC such as Debezium, a replica, an outbox reader) holds it by
// OID: after enable it held <table>__cloud, the DEFAULT partition, and the new parent, the
// partition of rows of no organization and every organization's partition were not published.
// Subscribers saw the rows an organization's move took out of DEFAULT as deleted.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "A10", name: "a publication of a table publishes the table after enable, under its own name", run: checkPublication},
	)
}

func checkPublication(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents ALTER COLUMN organization_id DROP NOT NULL",
		"CREATE PUBLICATION cdc FOR TABLE users, documents",
		"CREATE PUBLICATION cdc_all FOR ALL TABLES",
		// a column list and a row filter: Postgres takes them on a partitioned table only
		// with publish_via_partition_root (block 6 found enable failing on it); the filter on
		// a replica identity column, else Postgres refuses DELETE from the table, enable or not
		"CREATE PUBLICATION cdc_cols FOR TABLE documents (id, organization_id, title) WHERE (id > 0)"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	expect := func(what, sql, want string) {
		got, err := one(e.ctx, e.cloud(), sql)
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		s := fmt.Sprintf("%s: %s", what, got)
		if got != want {
			ok = false
			s = "WRONG " + s + " (want " + want + ")"
		}
		notes = append(notes, s)
	}
	pub := func(name string) string {
		return `SELECT coalesce(string_agg(tablename, ',' ORDER BY tablename), '') FROM pg_publication_tables
			WHERE pubname = '` + name + `' AND schemaname = 'public' AND tablename LIKE ANY ('{documents%,users}') AND tablename NOT LIKE '%__refs'`
	}
	expect("cdc publishes", pub("cdc"), "documents,users")
	expect("cdc via the partitioned table", "SELECT pubviaroot::text FROM pg_publication WHERE pubname = 'cdc'", "true")
	expect("cdc_all publishes", pub("cdc_all"), "documents,users")
	expect("cdc_cols publishes", `SELECT string_agg(tablename || ' ' || attnames::text || ' ' || coalesce(rowfilter, ''), ',') FROM pg_publication_tables WHERE pubname = 'cdc_cols'`,
		"documents {id,organization_id,title} (id > 0)")
	expect("the values before enable, for disable", "SELECT string_agg(pubname || '=' || viaroot_before, ',' ORDER BY pubname) FROM lokadb.publications", "cdc=false,cdc_all=false,cdc_cols=false")
	// a row of no organization goes to documents__none: published through the parent
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (NULL, 2, 'a10-none')"); err != nil {
		return false, "", err
	}
	expect("the row of no organization is in", "SELECT tableoid::regclass::text FROM documents WHERE title = 'a10-none'", "documents__none")
	if _, err := e.move(2); err != nil {
		notes = append(notes, "WRONG move-org 2 failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	expect("after move-org cdc publishes", pub("cdc"), "documents,users")
	return ok, strings.Join(notes, "; "), nil
}
