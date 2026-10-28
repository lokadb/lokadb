package main

// Block 10 (cooperative mode): the application names the organization itself, and the proxy
// no longer looks a child row's parent up. A child row inserted under one organization with a
// parent of another was stored under the first (DEFAULT lokadb.org_default()); lokadb_parent
// checked only UPDATE. Now the database refuses it on INSERT too.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "PG1", name: "a child row inserted under another organization than its parent's is refused", run: checkParentInsert},
	)
}

func checkParentInsert(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc1, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	insert := func(org string) error {
		if _, err := c.Exec(e.ctx, "SELECT set_config('lokadb.org', $1, false)", org); err != nil {
			return err
		}
		_, err := c.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ("+doc1+", 'pg1-'||current_setting('lokadb.org'))")
		return err
	}
	wrong, right := insert("2"), insert("1")
	stored, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(content || ':' || organization_id, ',' ORDER BY content), '') FROM document_versions WHERE content LIKE 'pg1-%'")
	if err != nil {
		return false, "", err
	}
	w := "accepted"
	if wrong != nil {
		w = "refused: " + firstErrLine(wrong)
	}
	r := "accepted"
	if right != nil {
		r = "refused: " + firstErrLine(right)
	}
	ok := wrong != nil && strings.Contains(wrong.Error(), "the parent belongs to organization 1") && right == nil && stored == "pg1-1:1"
	return ok, fmt.Sprintf("a version of organization 1's document under organization 2: %s; under organization 1: %s; stored: %s", w, r, stored), nil
}
