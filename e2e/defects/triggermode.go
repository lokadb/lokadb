package main

// The move without logical replication (move-org -mode trigger, chosen by auto when the
// cloud or the organization's database cannot replicate logically).

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "T1", name: "a move by triggers under inserts, updates and deletes loses nothing", run: checkTriggerUnderLoad},
		check{cloudHost: "cloudplain", id: "T2", name: "a cloud without wal_level = logical moves (auto picks triggers)", run: checkPlainCloud},
		check{id: "T3", name: "an organization's role that cannot subscribe moves (auto picks triggers)", run: checkNoSubscribeRole},
		check{id: "T4", name: "Ctrl-C during a move by triggers leaves no trigger and no log", run: checkTriggerInterrupted},
	)
}

// T1. The application keeps inserting, updating and deleting organization 1 all along.
func checkTriggerUnderLoad(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	var ids []int64
	rows, err := w.Query(e.ctx, "SELECT id FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	ids, err = pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return false, "", err
	}
	state := map[int64]string{} // id -> last title written, "" = deleted
	var mu sync.Mutex
	stop, done := make(chan struct{}), make(chan struct{})
	ops := 0
	go func() {
		defer close(done)
		r := rand.New(rand.NewSource(1))
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			title := fmt.Sprint("t1-", i)
			mu.Lock()
			switch i % 3 {
			case 0:
				var id int64
				if w.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, $1) RETURNING id", title).Scan(&id) == nil {
					ids = append(ids, id)
					state[id] = title
					ops++
				}
			case 1:
				id := ids[r.Intn(len(ids))]
				if !deleted(state, id) {
					if tag, err := w.Exec(e.ctx, "UPDATE documents SET title = $1 WHERE organization_id = 1 AND id = $2", title, id); err == nil && tag.RowsAffected() == 1 {
						state[id] = title
						ops++
					}
				}
			case 2:
				id := ids[r.Intn(len(ids))]
				if tag, err := w.Exec(e.ctx, "DELETE FROM documents WHERE organization_id = 1 AND id = $1", id); err == nil && tag.RowsAffected() == 1 {
					state[id] = ""
					ops++
				}
			}
			mu.Unlock()
		}
	}()
	time.Sleep(300 * time.Millisecond)
	out, moveErr := e.move(1, "-mode", "trigger")
	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-done
	if moveErr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	got := map[int64]string{}
	r, err := pgx.Connect(e.ctx, e.customer())
	if err != nil {
		return false, "", err
	}
	defer r.Close(e.ctx)
	rs, err := r.Query(e.ctx, "SELECT id, title FROM documents")
	if err != nil {
		return false, "", err
	}
	for rs.Next() {
		var id int64
		var t string
		if err := rs.Scan(&id, &t); err != nil {
			return false, "", err
		}
		got[id] = t
	}
	if err := rs.Err(); err != nil {
		return false, "", err
	}
	wrong := 0
	for id, want := range state {
		t, present := got[id]
		if want == "" && present || want != "" && t != want {
			wrong++
		}
	}
	pause := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "msg=switched") {
			if i := strings.Index(l, "writes_paused="); i >= 0 {
				pause = strings.Fields(l[i:])[0]
			}
		}
	}
	return wrong == 0, fmt.Sprintf("%d writes during the move (inserts, updates, deletes), %d rows differ at the organization; %s", ops, wrong, pause), nil
}

func deleted(state map[int64]string, id int64) bool {
	t, ok := state[id]
	return ok && t == ""
}

// T2. wal_level = replica (the default of many managed services): no logical replication at all.
func checkPlainCloud(e *env) (bool, string, error) {
	if moveMode == "logical" { // a cloud without wal_level = logical cannot subscribe: that is what T2 is about
		return true, "DEFECTS_MODE=logical: the check is about the mode auto picks without logical replication", nil
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	out, err := e.move(1)
	if err != nil {
		return false, "move-org failed: " + short(err), nil
	}
	n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	foreign, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid WHERE c.relname = 'documents__org1'")
	if err != nil {
		return false, "", err
	}
	trig := strings.Contains(out, "mode=trigger")
	return trig && n == "100" && foreign == "1", fmt.Sprintf("mode trigger chosen: %v; organization 1 reads %s documents from its own database (foreign partition: %s)", trig, n, foreign), nil
}

// T3. The organization's admin role may create its tables but not subscriptions.
func checkNoSubscribeRole(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		`DO $$ BEGIN CREATE ROLE plainadm LOGIN PASSWORD 'plainadm'; EXCEPTION WHEN duplicate_object THEN END $$`,
		"GRANT CREATE ON DATABASE "+e.db+" TO plainadm",
		"GRANT lokadb_app TO plainadm"); err != nil {
		return false, "", err
	}
	// plainadm creates the tables, lokadb_app owns them (it needs CREATE on the schema for that)
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON SCHEMA public TO plainadm, lokadb_app"); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	out, err := e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1",
		"-customer", dsn("customer", e.db, "plainadm"), "-customer-app", e.appCustomer())
	if err != nil {
		m := strings.Join(strings.Fields(err.Error()), " ")
		if len(m) > 400 {
			m = "…" + m[len(m)-400:]
		}
		return false, "move-org failed: " + m, nil
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	subs, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_subscription")
	if err != nil {
		return false, "", err
	}
	trig := strings.Contains(out, "mode=trigger")
	return trig && n == "100" && subs == "0", fmt.Sprintf("mode trigger chosen: %v; documents at the organization: %s; subscriptions there: %s", trig, n, subs), nil
}

// T4. Like R4, for the move by triggers.
func checkTriggerInterrupted(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	cmd := exec.CommandContext(e.ctx, e.bin, e.moveArgs(1, "-mode", "trigger")...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return false, "", err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return false, "", err
	}
	sc := bufio.NewScanner(out)
	sent := false
	var all strings.Builder
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if !sent && strings.Contains(sc.Text(), "initial copy done") {
			_ = cmd.Process.Signal(os.Interrupt)
			sent = true
		}
	}
	waitErr := cmd.Wait()
	if !sent {
		return false, "the move never reached the end of the copy: " + tail(all.String()), nil
	}
	left, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	return waitErr != nil && left == "" && n == "100", fmt.Sprintf("move-org stopped (%v); left behind: %q; organization 1 in the cloud: %s rows", waitErr, left, n), nil
}
