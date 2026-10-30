package main

// A real Prisma schema: Cal.com's, as its migrations made it before Prisma 6.7. Its implicit
// many-to-many tables ("_user_eventtype", "_PlatformOAuthClientToUser": A and B NOT NULL,
// UNIQUE (A, B), an index of B, no primary key) are in nearly every Prisma application made
// before then. check -org-table put them in the spec and said ready; enable refused them
// ("needs a primary key"). Now their unique index is their key: the whole schema goes through
// check, enable, a move and the way back. On the way: a key that is the organization column
// itself (PlatformBilling.id) and a unique column that is the organization column
// (OrganizationSettings.organizationId) failed enable too.
//
// The SQL is not in this repository: it is fetched from Cal.com's repository at calcomCommit,
// the migrations named in calcom_migrations.go, in order.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "PR1", name: "Cal.com's schema before Prisma 6.7 (implicit many-to-many tables without a primary key): check -org-table Team says ready and enable takes them by their unique (A, B); a move of an organization, writes of its links through the proxy, move-back: every link where it belongs", run: checkPrismaCalcom},
	)
}

const calcomCommit = "2e412471fc1f42c662005854a47c48128c479b0a"

var calcomOnce struct {
	sync.Mutex
	sql string
}

// calcomSQL: the migrations, fetched once per run (cached in the temporary directory).
func calcomSQL(ctx context.Context) (string, error) {
	calcomOnce.Lock()
	defer calcomOnce.Unlock()
	if calcomOnce.sql != "" {
		return calcomOnce.sql, nil
	}
	cache := filepath.Join(os.TempDir(), "calcom-"+calcomCommit[:12]+".sql")
	if b, err := os.ReadFile(cache); err == nil && len(b) > 0 {
		calcomOnce.sql = string(b)
		return calcomOnce.sql, nil
	}
	parts := make([]string, len(calcomMigrations))
	errs := make([]error, len(calcomMigrations))
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 30 * time.Second}
	for i, m := range calcomMigrations {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			url := "https://raw.githubusercontent.com/calcom/cal.com/" + calcomCommit + "/packages/prisma/migrations/" + m + "/migration.sql"
			for try := 0; try < 4; try++ {
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
				resp, err := client.Do(req)
				if err == nil {
					b, rerr := io.ReadAll(resp.Body)
					resp.Body.Close()
					if rerr == nil && resp.StatusCode == http.StatusOK {
						parts[i], errs[i] = string(b), nil
						return
					}
					err = fmt.Errorf("%s: %s %v", m, resp.Status, rerr)
				}
				errs[i] = err
				time.Sleep(time.Duration(try+1) * time.Second)
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return "", fmt.Errorf("fetching Cal.com's migrations: %w", err)
		}
	}
	calcomOnce.sql = strings.Join(parts, "\n;\n")
	_ = os.WriteFile(cache, []byte(calcomOnce.sql), 0o644)
	return calcomOnce.sql, nil
}

// PR1.
func checkPrismaCalcom(e *env) (bool, string, error) {
	sql, err := calcomSQL(e.ctx)
	if err != nil {
		return false, "", err
	}
	// Cal.com's schema in place of the usual one (it has a users table of its own)
	if err := execSQL(e.ctx, e.cloud(), "DROP TABLE document_versions, documents, users CASCADE"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), sql); err != nil {
		return false, "Cal.com's migrations: " + firstErrLine(err), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		`DO $$ DECLARE r record; BEGIN
			FOR r IN SELECT c.oid::regclass AS t FROM pg_class c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r' LOOP
				EXECUTE format('ALTER TABLE %s OWNER TO app', r.t);
			END LOOP; END $$`,
		`INSERT INTO "Team" (id, name, "isOrganization") VALUES (1, 'o1', true), (2, 'o2', true)`,
		`INSERT INTO users (id, email, "organizationId") VALUES (1, 'a@x', 1), (2, 'b@x', 2), (3, 'c@x', 1), (4, 'd@x', 1)`,
		`INSERT INTO "EventType" (id, title, slug, length, "teamId") VALUES (1, 'e1', 'e1', 30, 1), (2, 'e2', 'e2', 30, 2)`,
		`INSERT INTO "_user_eventtype" ("A", "B") VALUES (1, 1), (1, 3), (2, 2)`,
		`INSERT INTO "PlatformOAuthClient" (id, name, secret, permissions, "organizationId") VALUES ('c1', 'c1', 's', 0, 1), ('c2', 'c2', 's', 0, 2)`,
		`INSERT INTO "_PlatformOAuthClientToUser" ("A", "B") VALUES ('c1', 1), ('c1', 3), ('c2', 2)`); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}

	// check discovers the spec by the organizations table, as a user would run it
	specPath := filepath.Join(os.TempDir(), e.db+"-calcom-spec.json")
	out, code := e.runCheck("-db", e.cloud(), "-org-table", "Team", "-write-spec", specPath)
	body, err := os.ReadFile(specPath)
	if err != nil {
		return false, fmt.Sprintf("check -org-table Team wrote no spec (exit %d): %s", code, tail(out)), nil
	}
	e.spec = string(body)
	note(code != 3 && code != 5 && !reported(out, "id_missing") && strings.Contains(e.spec, `"_user_eventtype"`) && strings.Contains(e.spec, `"_PlatformOAuthClientToUser"`),
		fmt.Sprintf("check -org-table Team (exit %d): the many-to-many tables in the spec %v, id_missing %v", code,
			strings.Contains(e.spec, `"_user_eventtype"`) && strings.Contains(e.spec, `"_PlatformOAuthClientToUser"`), reported(out, "id_missing")))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	note(val(e.cloud(), `SELECT key_column FROM lokadb.tables WHERE table_name = '_user_eventtype'`) == `["A","B"]`,
		"the key of _user_eventtype: "+val(e.cloud(), `SELECT key_column FROM lokadb.tables WHERE table_name = '_user_eventtype'`))
	if mv, err := e.move(1); err != nil {
		note(false, "move-org 1 failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	links := `SELECT coalesce(string_agg("A" || '-' || "B", ',' ORDER BY "A", "B"), '-') FROM "_user_eventtype"`
	clients := `SELECT coalesce(string_agg("A" || '-' || "B", ',' ORDER BY "A", "B"), '-') FROM "_PlatformOAuthClientToUser"`
	note(val(e.customer(), links) == "1-1,1-3" && val(e.customer(), clients) == "c1-1,c1-3" &&
		val(e.customer(), `SELECT count(*) FROM pg_index WHERE indrelid = '"_user_eventtype"'::regclass AND indisprimary`) == "1",
		fmt.Sprintf("at the organization after the move: _user_eventtype %s (want 1-1,1-3), _PlatformOAuthClientToUser %s (want c1-1,c1-3), its primary key %s",
			val(e.customer(), links), val(e.customer(), clients), val(e.customer(), `SELECT count(*) FROM pg_index WHERE indrelid = '"_user_eventtype"'::regclass AND indisprimary`)))

	// the application links and unlinks through the proxy: by the parent (A), then by the key (A, B)
	stop, err := e.proxyWith(map[string]string{})
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		stop()
		return false, "", err
	}
	for _, s := range []string{
		`INSERT INTO "_user_eventtype" ("A", "B") VALUES (1, 4)`,
		`DELETE FROM "_user_eventtype" WHERE "A" = 1 AND "B" = 3`,
		`INSERT INTO "_PlatformOAuthClientToUser" ("A", "B") VALUES ('c1', 4)`,
	} {
		if _, err := simple(e.ctx, c, s); err != nil {
			note(false, s+" through the proxy: "+firstErrLine(err))
		}
	}
	c.Close(e.ctx)
	stop()
	note(val(e.customer(), links) == "1-1,1-4" && val(e.customer(), clients) == "c1-1,c1-3,c1-4",
		fmt.Sprintf("after the writes through the proxy: _user_eventtype %s (want 1-1,1-4), _PlatformOAuthClientToUser %s (want c1-1,c1-3,c1-4)", val(e.customer(), links), val(e.customer(), clients)))

	if mb, err := e.run6(e.moveBackArgs(1)...); err != nil {
		note(false, "move-back 1 failed: "+lastErr(mb))
		return false, strings.Join(notes, "; "), nil
	}
	note(val(e.cloud(), links) == "1-1,1-4,2-2" && val(e.cloud(), clients) == "c1-1,c1-3,c1-4,c2-2" &&
		val(e.cloud(), `SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = '"_user_eventtype"'::regclass AND c.relkind = 'f'`) == "0",
		fmt.Sprintf("in the cloud after move-back: _user_eventtype %s (want 1-1,1-4,2-2), _PlatformOAuthClientToUser %s (want c1-1,c1-3,c1-4,c2-2)", val(e.cloud(), links), val(e.cloud(), clients)))
	return ok, strings.Join(notes, "; "), nil
}
