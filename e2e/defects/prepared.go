package main

// 1.1 of research/REALISTIC_REVIEW.md: an application's pool outlives enable. Statements it
// prepared before enable ran afterwards with no analysis: under the organization of the
// statement before them (a version of organization 1's document stored under 2), or hidden
// from their row (UPDATE 0).

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "B1", name: "statements prepared through the proxy before enable run under their own organization after it", run: checkPreparedBeforeEnable},
	)
}

func checkPreparedBeforeEnable(e *env) (bool, string, error) {
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for name, sql := range map[string]string{
		"ins": "INSERT INTO document_versions (document_id, content) VALUES ($1, $2) RETURNING id",
		"upd": "UPDATE documents SET title = $2 WHERE id = $1",
	} {
		if _, err := c.Prepare(e.ctx, name, sql, nil); err != nil {
			return false, "", err
		}
	}
	doc1, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	time.Sleep(31 * time.Second) // the proxy reloads its metadata every 30 s (metaTTL)
	// a statement of organization 2 first: lokadb.org = 2 is left on the connection
	if err := c.ExecParams(e.ctx, "SELECT id FROM documents WHERE organization_id = $1", [][]byte{[]byte("2")}, nil, nil, nil).Read().Err; err != nil {
		return false, "", err
	}
	ins := c.ExecPrepared(e.ctx, "ins", [][]byte{[]byte(doc1), []byte("b1")}, nil, nil).Read()
	upd := c.ExecPrepared(e.ctx, "upd", [][]byte{[]byte(doc1), []byte("b1-title")}, nil, nil).Read()
	org, _ := one(e.ctx, e.cloud(), "SELECT organization_id FROM document_versions WHERE content = 'b1'")
	title, _ := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE id = "+doc1)
	mismatched, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM document_versions v JOIN documents d ON d.id = v.document_id
		WHERE v.organization_id IS DISTINCT FROM d.organization_id`)
	errs := []string{}
	for _, r := range []error{ins.Err, upd.Err} {
		if r != nil {
			errs = append(errs, firstErrLine(r))
		}
	}
	tag := upd.CommandTag.String()
	return org == "1" && tag == "UPDATE 1" && title == "b1-title" && mismatched == "0" && len(errs) == 0,
		fmt.Sprintf("the version of organization 1's document stored under organization %s; UPDATE by id: %q, title %q; versions under another organization than their document: %s; errors: %s",
			org, tag, title, mismatched, strings.Join(errs, ", ")), nil
}

func init() {
	checks = append(checks,
		check{id: "B2", name: "an upsert prepared before ALTER COLUMN TYPE (binary parameters) keeps working and keeps its values", run: checkPreparedTypeChange},
	)
}

// B2. pgx sends integer parameters in binary, for the types the server gave its statement. A
// rewritten upsert is rewritten again after the column type changes (P8): the new text must
// still take the client's encoding (int4 into bigint, bigint into numeric).
func checkPreparedTypeChange(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), "ALTER TABLE documents ADD COLUMN hits integer"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	dsnProxy := fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db)
	app, err := pgx.Connect(e.ctx, dsnProxy)
	if err != nil {
		return false, "", err
	}
	defer app.Close(e.ctx)
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	id, _ := strconv.ParseInt(doc, 10, 64)
	ups := "INSERT INTO documents (id, organization_id, user_id, title, hits) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET hits = excluded.hits"
	run := func(hits int64) string {
		if _, err := app.Exec(e.ctx, ups, id, int64(1), int64(2), "b2", hits); err != nil {
			return "error: " + firstErrLine(err)
		}
		v, err := one(e.ctx, e.cloud(), "SELECT hits::text FROM documents WHERE id = "+doc)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	ddl := func(sql string) error {
		c, err := pgx.Connect(e.ctx, dsnProxy)
		if err != nil {
			return err
		}
		defer c.Close(e.ctx)
		_, err = c.Exec(e.ctx, sql)
		return err
	}
	first := run(5)
	if err := ddl("ALTER TABLE documents ALTER COLUMN hits TYPE bigint"); err != nil {
		return false, "", err
	}
	big := run(7)
	if err := ddl("ALTER TABLE documents ALTER COLUMN hits TYPE numeric(12,2)"); err != nil {
		return false, "", err
	}
	num := run(500)
	return first == "5" && big == "7" && num == "500.00",
		fmt.Sprintf("before: %s; after integer -> bigint: %s; after bigint -> numeric(12,2): %s", first, big, num), nil
}
