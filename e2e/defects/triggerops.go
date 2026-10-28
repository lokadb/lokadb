package main

// What the trigger mode leaves when move-org cannot clean up (kill -9), and how a move
// that may have missed a change refuses to finish.

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "T5", name: "after kill -9 the capture stops by itself and move-cleanup removes it", run: checkKilledCapture},
		check{id: "T6", name: "a move whose capture may have missed a change gives up", run: checkLostCapture},
	)
}

// startMove runs move-org -mode trigger and calls at(line) for each line until it returns true.
// wait reads the rest of the output, then waits for move-org.
func (e *env) startMove(at func(string) bool) (cmd *exec.Cmd, wait func() (string, error), err error) {
	cmd = exec.CommandContext(e.ctx, e.bin, e.moveArgs(1, "-mode", "trigger")...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	var all strings.Builder
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if at(sc.Text()) {
			break
		}
	}
	return cmd, func() (string, error) {
		for sc.Scan() {
			all.WriteString(sc.Text() + "\n")
		}
		err := cmd.Wait()
		return all.String(), err
	}, nil
}

// T5. move-org killed: no cleanup ran. The capture must stop logging once the heartbeat is
// old, and move-cleanup must remove it; a new move then works.
func checkKilledCapture(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	cmd, wait, err := e.startMove(func(l string) bool { return strings.Contains(l, "step 5/6") })
	if err != nil {
		return false, "", err
	}
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_, _ = wait()
	before, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	// ten minutes later, as far as the capture can tell
	if err := execSQL(e.ctx, e.cloud(), "UPDATE lokadb.move_heartbeat SET at = now() - interval '1 hour'"); err != nil {
		return false, "", err
	}
	logBefore, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.move_log")
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) SELECT 1, 2, 't5' FROM generate_series(1, 5)"); err != nil {
		return false, "", err
	}
	logAfter, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.move_log")
	out, err := e.lokadb("move-cleanup", "-db", e.cloud())
	if err != nil {
		return false, "", err
	}
	after, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-mode", "trigger"); err != nil {
		return false, "a new move after the cleanup failed: " + short(err), nil
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	ok := strings.Contains(before, "capture trigger") && logBefore == logAfter && after == "" && n == "105"
	return ok, fmt.Sprintf("left by kill -9: %q; log rows before/after 5 writes with an old heartbeat: %s/%s; move-cleanup: %s; left after it: %q; a new move: %s documents",
		before, logBefore, logAfter, strings.TrimSpace(lastLine(out)), after, n), nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

// T6. A writer found the heartbeat missing and did not log its change (lost): the move
// must not finish without that change.
func checkLostCapture(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// From the capture on, the switch waits for this lock, so the move cannot finish before
	// lost is set. (Taken earlier, it would block the CHECK on DEFAULT.)
	hold, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer hold.Close(e.ctx)
	var tx pgx.Tx
	var lockErr error
	marked := false
	_, wait, err := e.startMove(func(l string) bool {
		if strings.Contains(l, "step 4/6") {
			if tx, lockErr = hold.Begin(e.ctx); lockErr == nil {
				_, lockErr = tx.Exec(e.ctx, "LOCK TABLE documents IN ACCESS SHARE MODE")
			}
			return lockErr != nil
		}
		if tx != nil && strings.Contains(l, "initial copy done") {
			marked = execSQL(e.ctx, e.cloud(), "UPDATE lokadb.move_heartbeat SET lost = true WHERE org = 1") == nil
			_ = tx.Rollback(e.ctx)
			return true
		}
		return false
	})
	if err != nil {
		return false, "", err
	}
	all, waitErr := wait()
	if lockErr != nil {
		return false, "", lockErr
	}
	left, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	gaveUp := strings.Contains(all, "change capture stopped")
	detail := fmt.Sprintf("move-org gave up: %v (%v); left behind: %q; organization 1 still in the cloud: %s rows", gaveUp, waitErr, left, n)
	if !gaveUp {
		for _, l := range strings.Split(all, "\n") {
			if !strings.Contains(l, "busy, retrying") && (strings.Contains(l, "ERROR") || strings.Contains(l, "move-org:")) {
				detail += "; " + l
			}
		}
	}
	return marked && waitErr != nil && gaveUp && left == "" && n == "100", detail, nil
}
