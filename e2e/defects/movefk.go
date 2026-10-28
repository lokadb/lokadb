package main

// Block 5 (moving an organization), A6: two tables of their own organization column with an
// FK between them, listed child first.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV6", name: "trigger mode: two root tables with an FK between them, the referencing one listed first", run: checkTwoRoots},
	)
}

const twoRootsSpec = `{"tables": [
  {"name": "comments", "tenant_column": "organization_id"},
  {"name": "posts", "tenant_column": "organization_id"}
]}`

// MV6. The trigger mode copied the tables in the spec's order: the comments arrived before
// the posts they reference, and the FK the organization's database has between them refused
// them; every attempt failed the same way.
func checkTwoRoots(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE posts (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, title text)",
		"CREATE TABLE comments (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, post_id bigint REFERENCES posts ON DELETE CASCADE, body text)",
		"ALTER TABLE posts OWNER TO app", "ALTER TABLE comments OWNER TO app",
		"INSERT INTO posts (organization_id, title) SELECT 1 + g % 2, 'p' || g FROM generate_series(1, 100) g",
		"INSERT INTO comments (organization_id, post_id, body) SELECT organization_id, id, 'c' || id FROM posts"); err != nil {
		return false, "", err
	}
	e.spec = twoRootsSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	out, err := e.move(1, "-mode", "trigger")
	if err != nil {
		return false, "move-org failed: " + firstErrLine(err) + " / " + lastLine(strings.TrimSpace(out)), nil
	}
	posts, err := one(e.ctx, e.customer(), "SELECT count(*) FROM posts")
	if err != nil {
		return false, "", err
	}
	comments, err := one(e.ctx, e.customer(), "SELECT count(*) FROM comments c JOIN posts p ON p.id = c.post_id")
	if err != nil {
		return false, "", err
	}
	fk, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_constraint WHERE conrelid = 'comments'::regclass AND confrelid = 'posts'::regclass AND contype = 'f'")
	if err != nil {
		return false, "", err
	}
	ok := posts == "50" && comments == "50" && fk == "1"
	return ok, fmt.Sprintf("at organization 1: posts %s, comments with their post %s, FK comments -> posts %s", posts, comments, fk), nil
}
