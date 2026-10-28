package main

// ER1 (block 12's finding in the quickstart): a write the proxy had passed through plain,
// because the table was not LokaDB's yet, waits for the lock of enable and runs after its commit.
// For a child table the column of the organization is then filled by its default, which has
// nothing from the proxy: "the organization of the new row is unknown: its parent row was not
// found" (now: "reached the table without its organization"), while the parent is there.

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	// Red until the proxy repeats such a write (BACKLOG, block 1, item 16): registered only when
	// DEFECTS_ONLY names it, so that the set stays green.
	if !strings.Contains(","+os.Getenv("DEFECTS_ONLY")+",", ",ER1,") {
		return
	}
	checks = append(checks,
		check{id: "ER1", name: "a child row written through the proxy while enable holds the tables is stored under its parent's organization, with no error", run: checkEnableRace},
	)
}

func checkEnableRace(e *env) (bool, string, error) {
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	app, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer app.Close(e.ctx)
	// organization 2's document, and the proxy's session, which has read the metadata (no enable yet)
	rs, err := simple(e.ctx, app, "INSERT INTO documents (organization_id, title) VALUES (2, 'race') RETURNING id")
	if err != nil {
		return false, "", err
	}
	doc := string(rs[0].Rows[0][0])
	// the lock enable waits for: a reader of document_versions that has not finished
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	if _, err := holder.Exec(e.ctx, "BEGIN"); err != nil {
		return false, "", err
	}
	if _, err := holder.Exec(e.ctx, "LOCK TABLE document_versions IN ACCESS SHARE MODE"); err != nil {
		return false, "", err
	}
	enabled := make(chan error, 1)
	go func() { enabled <- e.enable() }()
	waiting := func(like string) bool {
		for i := 0; i < 300; i++ {
			n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query ILIKE '%"+like+"%'")
			if err == nil && n != "0" {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}
	if !waiting("ALTER TABLE document_versions") {
		return false, "enable never waited for the lock of document_versions", nil
	}
	// the application's insert of the child: the proxy does not know the table yet
	wrote := make(chan error, 1)
	go func() {
		_, err := simple(e.ctx, app, "INSERT INTO document_versions (document_id, content) VALUES ("+doc+", 'race')")
		wrote <- err
	}()
	if !waiting("INSERT INTO document_versions (document_id") {
		return false, "the insert of the child never waited behind enable", nil
	}
	if _, err := holder.Exec(e.ctx, "COMMIT"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	select {
	case err := <-enabled:
		if err != nil {
			return false, "enable failed: " + oneLine(err.Error()), nil
		}
	case <-time.After(120 * time.Second):
		return false, "enable did not end", nil
	}
	select {
	case err := <-wrote:
		if err != nil {
			ok = false
			notes = append(notes, "WRONG the insert failed: "+oneLine(err.Error()))
		} else {
			notes = append(notes, "the insert passed")
		}
	case <-time.After(60 * time.Second):
		return false, "the insert did not end", nil
	}
	got, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(organization_id::text, ','), 'none') FROM document_versions WHERE content = 'race' AND document_id = "+doc)
	if err != nil {
		return false, "", err
	}
	if got != "2" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("the child row's organization: %s (want 2)", got))
	return ok, strings.Join(notes, "; "), nil
}
