package main

// Block 5, the admin review of 25.10 (tasks/review/admin.md): A7, A17, A10, A5.

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV20", name: "a copy that differs from the cloud (a row written into the organization's database past the cloud) stops the switch before it commits: the organization stays in the cloud", run: checkCountsBeforeSwitch},
		check{id: "MV21", name: "lokadb.moving set by an application session does not switch the refs triggers off; lokadb.split_org (dead code) is not in the database", run: checkMovingNeedsOwner},
		check{id: "MV22", name: "trigger mode: a table that references itself moves in batches whose rows come in another order than the FK", run: checkSelfReferenceCopy},
	)
}

// MV20 (A7). compare ran after the switch had committed: a copy that differed (a row written
// into the customer database past the cloud, a log entry not applied) was found when the
// organization already worked on it, and the way out was a move-back from that copy. Now the row
// counts are compared under the pause, before the commit.
func checkCountsBeforeSwitch(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var once sync.Once
	var werr error
	out, _, merr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "initial copy done") {
			once.Do(func() {
				// a row of the organization that the cloud does not have, in the organization's database
				werr = execSQL(e.ctx, e.customer(),
					"INSERT INTO documents__o1 (id, organization_id, user_id, title) VALUES (900001, 1, 2, 'past the cloud')")
			})
		}
	}, "-mode", "trigger")
	if werr != nil {
		return false, "the extra row could not be written: " + firstErrLine(werr), nil
	}
	moved, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	rows, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	// still writable in the cloud: no switch happened
	writable := "yes"
	if err := execSQL(e.ctx, e.cloud(), "UPDATE documents SET title = 'mv20' WHERE organization_id = 1 AND id = (SELECT min(id) FROM documents WHERE organization_id = 1)"); err != nil {
		writable = "no: " + firstErrLine(err)
	}
	ok := merr != nil && strings.Contains(out, "nothing switched") && moved == "0" && rows == "100" && writable == "yes"
	return ok, fmt.Sprintf("move-org with a stray row at the customer: %s / %s; organization 1 moved: %s, its rows in the cloud: %s, writable there: %s",
		errOr(merr, true), lastLine(out), moved, rows, writable), nil
}

// MV21 (A17, A10). lokadb.moving = on makes the refs and key triggers return at once, for the
// move's own statements. Any session could set it and write without its __refs row. It now
// counts only for the owner of schema lokadb (and its members). lokadb.split_org was never
// called and broken for uuid organizations: enable drops it.
func checkMovingNeedsOwner(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	app := dsn(e.cloudHost, e.db, "app")
	// one session: SET and the INSERT on the same connection
	c, err := pgx.Connect(e.ctx, app)
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := c.Exec(e.ctx, "SET lokadb.moving = 'on'"); err != nil {
		return false, "", err
	}
	if _, err := c.Exec(e.ctx, "INSERT INTO documents (id, organization_id, user_id, title) VALUES (900003, 1, 2, 'mv21')"); err != nil {
		return false, "the application's insert failed: " + firstErrLine(err), nil
	}
	refs, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents__refs WHERE id = 900003")
	if err != nil {
		return false, "", err
	}
	splitOrg, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_proc WHERE proname = 'split_org' AND pronamespace = 'lokadb'::regnamespace")
	if err != nil {
		return false, "", err
	}
	ok := refs == "1" && splitOrg == "0"
	return ok, fmt.Sprintf("a row written by the application role with lokadb.moving = on: its __refs row there: %s (want 1); lokadb.split_org functions: %s (want 0)", refs, splitOrg), nil
}

// MV22 (A5). The copy of the trigger mode goes in batches, each in a transaction of its own,
// and the FKs among the organization's tables exist in its database before the copy. A
// comment moved under a later comment (or two tables referencing each other) failed with
// 23503 whenever a batch ended between them; the same move in one batch passed. The copy now
// runs with the FK checks off, as the logical mode's does.
func checkSelfReferenceCopy(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE threads (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, parent_id bigint REFERENCES threads, body text)",
		"ALTER TABLE threads OWNER TO app",
		"INSERT INTO threads (organization_id, body) SELECT 1 + g % 2, 't' || g FROM generate_series(1, 40) g",
		// thread 2 (organization 1) under thread 38 (organization 1): the parent comes in a later batch
		"UPDATE threads SET parent_id = 38 WHERE id = 2",
		"UPDATE threads SET parent_id = 4 WHERE id = 36"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "threads", "tenant_column": "organization_id"}]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	out, _, merr := e.moveLines(1, nil, "-mode", "trigger", "-batch-rows", "3")
	if merr != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM threads")
	if err != nil {
		return false, "", err
	}
	p, err := one(e.ctx, e.customer(), "SELECT coalesce(string_agg(id || '>' || parent_id, ',' ORDER BY id), '') FROM threads WHERE parent_id IS NOT NULL")
	if err != nil {
		return false, "", err
	}
	ok := n == "20" && p == "2>38,36>4"
	return ok, fmt.Sprintf("the self-referencing table moved in batches of 3: %s rows at the organization (want 20), parents %s (want 2>38,36>4)", n, p), nil
}
