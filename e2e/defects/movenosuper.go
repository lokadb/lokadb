package main

// Block 5 (moving an organization) on a managed Postgres (RDS, Cloud SQL, Yandex): the move's
// role is no superuser, a member of the tables' owner with CREATEROLE (risk hunt R35, R36).

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "MV38", name: "no superuser, autovacuum on DEFAULT after the split (a worker this role cannot see): the switch waits past deadlock_timeout, Postgres cancels the worker and move-org succeeds", run: checkAutovacuumNoSuper},
		check{id: "MV39", name: "no superuser (no session_replication_role): the organization is split out of DEFAULT online, the writes of the other organizations are not held back for the split", run: checkSplitOnlineNoSuper},
	)
}

// asAdmin makes the cloud's admin of a managed service (as NS1) and enables LokaDB as it; the
// move's commands then run as it.
func asAdmin(e *env) error {
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"),
		`DO $$ BEGIN CREATE ROLE lokadb_saas LOGIN PASSWORD 'lokadb_saas' CREATEROLE; EXCEPTION WHEN duplicate_object THEN END $$`,
		"GRANT app TO lokadb_saas"); err != nil {
		return err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"GRANT CREATE ON DATABASE "+e.db+" TO lokadb_saas",
		"GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO lokadb_saas, app"); err != nil {
		return err
	}
	e.admin = dsn(e.cloudHost, e.db, "lokadb_saas")
	p, err := e.specFile(e.specBody())
	if err != nil {
		return err
	}
	_, err = e.lokadb("enable", "-db", e.admin, "-spec", p, "-owner", "app", "-allow-downtime")
	return err
}

func (e *env) specBody() string {
	if e.spec != "" {
		return e.spec
	}
	return spec
}

// MV38. As MV34 (a worker that holds DEFAULT for minutes after the split deleted the
// organization's rows), but the move runs as lokadb_saas: pg_stat_activity shows it nothing of the
// worker (an empty backend_type) and it may not cancel it. The tries stayed under
// deadlock_timeout and ended in "switch not possible right now" (55P03).
func checkAutovacuumNoSuper(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER SYSTEM SET autovacuum_naptime = '1s'",
		"SELECT pg_reload_conf()",
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1 + (g % 5 = 0)::int, 1, repeat('x', 200) || g FROM generate_series(1, 600000) g"); err != nil {
		return false, "", err
	}
	if err := asAdmin(e); err != nil {
		return false, "enable as lokadb_saas failed: " + lastErr(err.Error()), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents__cloud SET (autovacuum_vacuum_threshold = 1000, autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_cost_delay = 20, autovacuum_vacuum_cost_limit = 10)"); err != nil {
		return false, "", err
	}
	out, _, moveErr := e.moveLines(1, nil, "-mode", "trigger")
	paused := durationIn(out, `msg="organization resumed".* paused=([0-9.]+[a-zµ]+)`)
	hidden := strings.Count(out, "cannot see it")
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	inCloud, cerr := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents__cloud WHERE organization_id = 2")
	if moveErr != nil {
		lines := strings.Split(strings.TrimSpace(out), "\n")
		return false, fmt.Sprintf("move-org as lokadb_saas failed (%d times it saw a holder it could not see): %s | log: %s", hidden, lastErr(out),
			strings.Join(strings.Fields(strings.Join(lines[max(0, len(lines)-12):], " / ")), " ")), nil
	}
	ok := paused > 0 && paused <= 30*time.Second && n == "480100" && inCloud == "120100"
	var seen []string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "switch") || strings.Contains(l, "autovacuum") || strings.Contains(l, "cannot see") {
			seen = append(seen, strings.Join(strings.Fields(l[min(len(l), 26):min(len(l), 230)]), " "))
		}
	}
	return ok, fmt.Sprintf("holders the move could not see and waited for: %d; organization paused %s (want at most 30s); organization 1 at its database: %s rows (want 480100); organization 2 in the cloud: %s (want 120100) %v | %s",
		hidden, paused.Round(time.Millisecond), n, inCloud, errOr(cerr, true), strings.Join(seen, " / ")), nil
}

// MV39. 600 000 documents, 480 000 of organization 1, moved by lokadb_saas, which may not set
// session_replication_role. The split used lokadb.partition_org then: DEFAULT locked from the
// copy to the attach, every cloud organization's writes waiting ~5 minutes for 1.4 million rows
// (R35). Now the copy and the log are online as with the right, and the last transaction deletes
// the rows with LokaDB's own triggers standing down. Organization 2 writes all the while, in
// the cloud: its longest insert is the wait this check bounds.
func checkSplitOnlineNoSuper(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1 + (g % 5 = 0)::int, 1, repeat('x', 200) || g FROM generate_series(1, 600000) g"); err != nil {
		return false, "", err
	}
	if err := asAdmin(e); err != nil {
		return false, "enable as lokadb_saas failed: " + lastErr(err.Error()), nil
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var worst time.Duration
	var writes int
	var werr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (2, 1, 'during the split')")
			mu.Lock()
			if err != nil && werr == nil {
				werr = err
			}
			worst, writes = max(worst, time.Since(start)), writes+1
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	out, took, moveErr := e.moveLines(1, nil, "-mode", "trigger")
	close(stop)
	wg.Wait()
	if moveErr != nil {
		return false, "move-org as lokadb_saas failed: " + lastErr(out), nil
	}
	if werr != nil {
		return false, "a write of organization 2 failed: " + lastErr(werr.Error()), nil
	}
	splitWas := durationIn(out, `msg="organization split out of DEFAULT.*took=([0-9.]+[a-zµ]+)`)
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	fks, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM pg_constraint WHERE conrelid = 'documents'::regclass AND contype = 'f' AND NOT convalidated`)
	under := strings.Contains(out, "under a lock")
	ok := worst <= 8*time.Second && n == "480100" && !under && fks == "0"
	return ok, fmt.Sprintf("split %s, whole move %s; the longest insert of organization 2 during it: %s (want at most 8s) in %d inserts; organization 1 at its database: %s rows (want 480100); split under a lock: %v (want false); FKs not validated: %s (want 0)",
		splitWas.Round(time.Millisecond), took.Round(time.Second), worst.Round(time.Millisecond), writes, n, under, fks), nil
}
