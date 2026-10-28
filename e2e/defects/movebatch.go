package main

// Block 5 (moving an organization), A9: a large organization split out of DEFAULT. The split
// copies in batches, waits a bounded time for each lock, and pauses the organization at most
// -max-split-pause; whatever stops it, the organization's rows stay in DEFAULT.

import (
	"bufio"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV2", name: "a large organization is split out of DEFAULT in batches, with progress", run: checkSplitBatches},
		check{id: "MV2", name: "a lock held on DEFAULT does not stretch the split's pause: VALIDATE waits a bounded time", run: checkSplitValidateLock},
		check{id: "MV2", name: "a pause of the split longer than -max-split-pause stops it, the organization's rows stay in DEFAULT", run: checkSplitPauseLimit},
	)
}

const (
	bigOrg  = 3
	bigRows = 120000 // a check gets 3 minutes on a busy machine
)

// bigOrgRows gives organization 3 (created after enable: all in DEFAULT) many rows.
func bigOrgRows(e *env) error {
	return execSQL(e.ctx, e.cloud(),
		fmt.Sprintf("INSERT INTO documents (organization_id, user_id, title) SELECT %d, 2, 'x' || g FROM generate_series(1, %d) g", bigOrg, bigRows))
}

// unknownFlag: the flag an older lokadb does not know, from its usage message.
var unknownFlag = regexp.MustCompile(`flag provided but not defined: -([a-z-]+)`)

// moveLines runs move-org for org with extra flags; on(line) sees each line as it comes.
// A flag the binary does not know (the code before a fix) is left out: the same move without it.
func (e *env) moveLines(org int, on func(string), extra ...string) (string, time.Duration, error) {
	for {
		out, took, err := e.moveLinesOnce(org, on, extra...)
		m := unknownFlag.FindStringSubmatch(out)
		if err == nil || m == nil {
			return out, took, err
		}
		var rest []string
		for i := 0; i < len(extra); i++ {
			if extra[i] == "-"+m[1] {
				i++ // its value
				continue
			}
			rest = append(rest, extra[i])
		}
		if len(rest) == len(extra) {
			return out, took, err
		}
		extra = rest
	}
}

func (e *env) moveLinesOnce(org int, on func(string), extra ...string) (string, time.Duration, error) {
	start := time.Now()
	cmd := exec.CommandContext(e.ctx, e.bin, e.moveArgs(org, extra...)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return "", 0, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return "", 0, err
	}
	var all strings.Builder
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if on != nil {
			on(sc.Text())
		}
	}
	err = cmd.Wait()
	return all.String(), time.Since(start), err
}

// bigOrgState: where organization 3's rows are and whether they are writable.
func bigOrgState(e *env) (inDefault, inPart, chk string, upd error, err error) {
	if inDefault, err = one(e.ctx, e.cloud(), fmt.Sprintf("SELECT count(*) FROM documents__cloud WHERE organization_id = %d", bigOrg)); err != nil {
		return
	}
	if inPart, err = one(e.ctx, e.cloud(), fmt.Sprintf("SELECT coalesce(to_regclass('documents__o%d')::text, 'none')", bigOrg)); err != nil {
		return
	}
	if inPart != "none" {
		if inPart, err = one(e.ctx, e.cloud(), fmt.Sprintf("SELECT count(*) FROM documents__o%d", bigOrg)); err != nil {
			return
		}
	}
	if chk, err = one(e.ctx, e.cloud(), fmt.Sprintf(`SELECT count(*) FROM pg_constraint WHERE conrelid = 'documents__cloud'::regclass AND conname = 'lokadb_not_org%d'`, bigOrg)); err != nil {
		return
	}
	_, upd = one(e.ctx, e.cloud(), fmt.Sprintf("WITH u AS (UPDATE documents SET title = 'mv2' WHERE organization_id = %d AND id = (SELECT min(id) FROM documents WHERE organization_id = %d) RETURNING 1) SELECT count(*) FROM u", bigOrg, bigOrg))
	return
}

// MV2. Before: one DELETE ... RETURNING INSERT moved every row of the organization in one
// transaction. Now the copy commits -batch-rows rows at a time (the source keeps them: the
// xmin of the retained copy tells the transactions apart) and reports its progress.
func checkSplitBatches(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	progress := 0
	out, took, err := e.moveLines(bigOrg, func(l string) {
		if strings.Contains(l, "msg=progress") && strings.Contains(l, "step=split") {
			progress++
		}
	}, "-batch-rows", fmt.Sprint(bigRows/6), "-retain-source")
	if err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	txs, err := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT count(DISTINCT xmin::text) FROM documents__o%d_source", bigOrg))
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.customer(), fmt.Sprintf("SELECT count(*) FROM documents WHERE organization_id = %d", bigOrg))
	if err != nil {
		return false, "", err
	}
	nt, _ := strconv.Atoi(txs)
	ok := nt >= 6 && progress >= 2 && n == fmt.Sprint(bigRows)
	return ok, fmt.Sprintf("%d rows split by %s transaction(s) (want 6 of %d); split progress lines: %d; rows in the organization's database: %s; move took %s",
		bigRows, txs, bigRows/6, progress, n, took.Round(time.Second)), nil
}

// MV2. A long transaction holds DEFAULT in SHARE UPDATE EXCLUSIVE (a VACUUM, CREATE INDEX
// CONCURRENTLY, ANALYZE) while the organization is split. Before, VALIDATE waited for it
// with no limit, the organization and every statement over all organizations paused all
// along. Now it gives up after its lock timeouts, the rows stay in DEFAULT, and move-cleanup
// removes the CHECK once DEFAULT is free.
func checkSplitValidateLock(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	hold, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer hold.Close(e.ctx)
	// Once the split's CHECK is on DEFAULT (the rows are being moved), the lock is taken and
	// held until move-org is done, or 100 s.
	const holdMax = 100 * time.Second
	locked := make(chan struct{})
	released := make(chan struct{})
	go func() {
		defer close(released)
		for i := 0; i < 1200; i++ {
			n, err := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT count(*) FROM pg_constraint WHERE conrelid = 'documents__cloud'::regclass AND conname = 'lokadb_not_org%d'", bigOrg))
			if err == nil && n == "1" {
				if _, err := hold.Exec(e.ctx, "BEGIN; LOCK TABLE documents__cloud IN SHARE UPDATE EXCLUSIVE MODE"); err == nil {
					close(locked)
					select {
					case <-released:
					case <-time.After(holdMax):
					}
					_, _ = hold.Exec(e.ctx, "ROLLBACK")
				}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	out, took, merr := e.moveLines(bigOrg, nil, "-lock-timeout", "300ms")
	wasLocked := false
	select {
	case <-locked:
		wasLocked = true
	default:
	}
	_, _ = hold.Exec(e.ctx, "ROLLBACK") // let the holder go if it still waits
	<-released
	if !wasLocked {
		return false, "the split's CHECK never appeared on DEFAULT: " + lastLine(out), nil
	}
	if merr == nil {
		return false, fmt.Sprintf("move-org waited for the lock and succeeded after %s (the organization paused all along)", took.Round(time.Second)), nil
	}
	cout, cerr := e.lokadb("move-cleanup", "-db", e.cloud())
	if cerr != nil {
		return false, "", cerr
	}
	inDef, inPart, chk, upd, err := bigOrgState(e)
	if err != nil {
		return false, "", err
	}
	ok := took < holdMax*3/4 && inDef == fmt.Sprint(bigRows) && (inPart == "0" || inPart == "none") && chk == "0" && upd == nil
	return ok, fmt.Sprintf("move-org gave up after %s (%s); after move-cleanup (%s): organization 3 rows in DEFAULT %s, in its unattached partition %s, CHECK %s, UPDATE %s",
		took.Round(100*time.Millisecond), lastLine(out), strings.TrimSpace(lastLine(cout)), inDef, inPart, chk, errOr(upd, true)), nil
}

// MV2. -max-split-pause bounds the organization's pause during the split: past it the copy
// stops, the organization resumes, its rows still in DEFAULT and writable.
func checkSplitPauseLimit(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	// the copy runs without a pause now (the organization writes on): the limit is for the pause
	// that follows it, the rest of the log and the last transactions, which cannot fit in 1 ms
	out, took, merr := e.moveLines(bigOrg, nil, "-max-split-pause", "1ms", "-batch-rows", "1000")
	if merr == nil {
		return false, "move-org succeeded: the limit was not reached (" + took.String() + ")", nil
	}
	paused := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "organization resumed") {
			paused = l[strings.Index(l, "paused="):]
		}
	}
	inDef, inPart, chk, upd, err := bigOrgState(e)
	if err != nil {
		return false, "", err
	}
	ok := strings.Contains(out, "max-split-pause") && inDef == fmt.Sprint(bigRows) && (inPart == "0" || inPart == "none") && chk == "0" && upd == nil
	return ok, fmt.Sprintf("move-org: %s; %s; organization 3 rows in DEFAULT %s, in its unattached partition %s, CHECK %s, UPDATE %s",
		lastLine(out), paused, inDef, inPart, chk, errOr(upd, true)), nil
}
