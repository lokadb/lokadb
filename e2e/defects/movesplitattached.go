package main

// Block 5 (moving an organization): a split of two tables that fails on the second must leave the
// first one as it is. Its rows left DEFAULT in its last transaction, and its partition holds the
// only copy of them; the failure's cleanup emptied it (risk hunt R37: 1 002 007 rows of an
// organization, 234 left, the references to the rest still in documents__refs).

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV40", name: "a split that fails on its second table leaves the first one split: the organization's rows and their references are all there, and the next move-org finishes", run: checkSplitFailsOnSecondTable},
	)
}

func checkSplitFailsOnSecondTable(e *env) (bool, string, error) {
	// organization 3 with its rows and versions before enable (a version needs its organization,
	// which only enable's trigger and the proxy give it)
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		fmt.Sprintf("INSERT INTO document_versions (document_id, content) SELECT id, 'v' FROM documents WHERE organization_id = %d LIMIT 1000", bigOrg)); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	state := func() (string, error) {
		return one(e.ctx, e.cloud(), fmt.Sprintf(`SELECT (SELECT count(*) FROM documents WHERE organization_id = %[1]d)::text || ' documents, '
			|| (SELECT count(*) FROM document_versions WHERE document_id IN (SELECT id FROM documents WHERE organization_id = %[1]d)) || ' versions, '
			|| (SELECT count(*) FROM documents__refs WHERE tenant = %[1]d) || ' references'`, bigOrg))
	}
	before, err := state()
	if err != nil {
		return false, "", err
	}
	hold, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer hold.Close(e.ctx)
	// The first table is split out when its line is printed; the second one's DEFAULT is then
	// held against the split (SHARE UPDATE EXCLUSIVE, which the split takes first), for longer than
	// the split waits (-lock-timeout times -attempts).
	const holdMax = 100 * time.Second
	locked, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	take := func() {
		defer close(released)
		if _, err := hold.Exec(e.ctx, "BEGIN; LOCK TABLE document_versions__cloud IN SHARE UPDATE EXCLUSIVE MODE"); err == nil {
			close(locked)
			select {
			case <-released:
			case <-time.After(holdMax):
			}
			_, _ = hold.Exec(e.ctx, "ROLLBACK")
		}
	}
	started := false
	out, _, merr := e.moveLines(bigOrg, func(l string) {
		if strings.Contains(l, "organization split out of DEFAULT") && strings.Contains(l, "table=documents ") {
			once.Do(func() { started = true; go take() })
		}
	}, "-lock-timeout", "300ms")
	_, _ = hold.Exec(e.ctx, "ROLLBACK")
	if started {
		<-released
	}
	if !started {
		return false, "the first table was never split out: " + lastLine(out), nil
	}
	if merr == nil {
		return false, "move-org succeeded: the second table's split was not held back", nil
	}
	after, err := state()
	if err != nil {
		return false, "", err
	}
	attached, _ := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT (SELECT count(*) FROM documents__o%d)::text || ' in documents__o%d, ' || (SELECT count(*) FROM pg_inherits WHERE inhrelid = to_regclass('documents__o%d'))::text || ' attached'", bigOrg, bigOrg, bigOrg))
	named := strings.Contains(out, "already split out of DEFAULT") && strings.Contains(out, "documents")
	// the next one: the first table is split already, the second is done now
	_, rerr := e.lokadb(e.moveArgs(bigOrg, "-lock-timeout", "300ms")...)
	moved, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE organization_id = 3")
	ok := before == after && attached == fmt.Sprintf("%d in documents__o%d, 1 attached", bigRows, bigOrg) && named && rerr == nil && moved == fmt.Sprint(bigRows)
	return ok, fmt.Sprintf("move-org failed (%s); organization 3 before: %s, after: %s (want the same); %s (want %d in documents__o3, 1 attached); the error names the split table: %v; the next move-org: %s, %s rows at the organization (want %d)",
		lastLine(out), before, after, attached, bigRows, named, errOr(rerr, rerr == nil), moved, bigRows), nil
}
