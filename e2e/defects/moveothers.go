package main

// Block 5 (moving an organization), items 6 and 7: writers that pass by the proxy while the
// organization is split out of DEFAULT, and several organizations moved one after another.

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "MV8", name: "writers past the proxy during the split: nothing is lost silently", run: checkSplitBypass},
		check{id: "MV9", name: "two organizations moved one after the other, a second move-org at once refused", run: checkTwoOrgs},
	)
}

// MV8 (item 6). The pause of the split holds only statements that go through the proxy. A
// connection straight to the cloud, while the rows are moved, deletes one row of the
// organization, updates another and inserts a third. Each must either fail (the CHECK on
// DEFAULT) or be in the organization's rows afterwards: never succeed and be lost.
func checkSplitBypass(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	ids, err := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT min(id)::text || ',' || max(id)::text FROM documents WHERE organization_id = %d", bigOrg))
	if err != nil {
		return false, "", err
	}
	del, upd, _ := strings.Cut(ids, ",")
	var once sync.Once
	var delErr, updErr, insErr error
	var updN string
	ran := false
	out, _, merr := e.moveLines(bigOrg, func(l string) {
		if strings.Contains(l, "organization paused") {
			once.Do(func() {
				ran = true
				time.Sleep(300 * time.Millisecond) // the rows are being moved
				_, delErr = one(e.ctx, e.cloud(), "WITH d AS (DELETE FROM documents WHERE id = "+del+" RETURNING 1) SELECT count(*) FROM d")
				updN, updErr = one(e.ctx, e.cloud(), "WITH u AS (UPDATE documents SET title = 'mv8' WHERE id = "+upd+" RETURNING 1) SELECT count(*) FROM u")
				_, insErr = one(e.ctx, e.cloud(), fmt.Sprintf("WITH i AS (INSERT INTO documents (organization_id, user_id, title) VALUES (%d, 2, 'mv8-new') RETURNING 1) SELECT count(*) FROM i", bigOrg))
			})
		}
	}, "-batch-rows", "2000")
	if !ran {
		return false, "the organization was never paused: " + lastLine(out), nil
	}
	if merr != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	gone, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE id = "+del)
	if err != nil {
		return false, "", err
	}
	title, err := one(e.ctx, e.customer(), "SELECT title FROM documents WHERE id = "+upd)
	if err != nil {
		return false, "", err
	}
	added, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'mv8-new'")
	if err != nil {
		return false, "", err
	}
	lost := []string{}
	if delErr == nil && gone != "0" {
		lost = append(lost, "the delete")
	}
	if updErr == nil && (title != "mv8" || updN != "1") {
		lost = append(lost, "the update ("+updN+" rows)")
	}
	if insErr == nil && added != "1" {
		lost = append(lost, "the insert")
	}
	ok := len(lost) == 0
	return ok, fmt.Sprintf("past the proxy during the split: delete %s (row there afterwards: %s), update %s (title afterwards: %s), insert %s (rows afterwards: %s); lost: %v",
		errOr(delErr, true), gone, errOr(updErr, true), title, errOr(insErr, true), added, lost), nil
}

// MV9 (item 7). Several organizations: one move-org at a time per cloud database (D6 checks
// the refusal); one after the other, each lands in its own database, the cloud keeps the rest.
func checkTwoOrgs(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"), "DROP DATABASE IF EXISTS "+e.db+"_b WITH (FORCE)", "CREATE DATABASE "+e.db+"_b OWNER app"); err != nil {
		return false, "", err
	}
	t1 := time.Now()
	out1, err := e.move(1)
	if err != nil {
		return false, "move of organization 1 failed: " + firstErrLine(err), nil
	}
	took1 := time.Since(t1)
	p, _ := e.specFile(spec)
	args := []string{"move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "2",
		"-customer", dsn("customer", e.db+"_b", "postgres"), "-publisher", e.cloud()}
	if moveMode != "" {
		args = append(args, "-mode", moveMode)
	}
	t2 := time.Now()
	if _, err := e.lokadb(args...); err != nil {
		return false, "move of organization 2 failed: " + firstErrLine(err), nil
	}
	took2 := time.Since(t2)
	a, err := one(e.ctx, e.customer(), "SELECT count(*)::text || '/' || count(*) FILTER (WHERE organization_id = 1)::text FROM documents")
	if err != nil {
		return false, "", err
	}
	b, err := one(e.ctx, dsn("customer", e.db+"_b", "postgres"), "SELECT count(*)::text || '/' || count(*) FILTER (WHERE organization_id = 2)::text FROM documents")
	if err != nil {
		return false, "", err
	}
	orgs, err := one(e.ctx, e.cloud(), "SELECT string_agg(org::text, ',' ORDER BY org) FROM lokadb.orgs")
	if err != nil {
		return false, "", err
	}
	// through the parent (FDW), each organization reads its own rows from its own database
	both, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id IN (1, 2)")
	if err != nil {
		return false, "", err
	}
	ok := a == "100/100" && b == "100/100" && orgs == "1,2" && both == "200"
	return ok, fmt.Sprintf("organization 1's database: %s rows (all its own), organization 2's: %s; lokadb.orgs: %s; both read through the cloud: %s; the moves took %s and %s; steps of the first: %s",
		a, b, orgs, both, took1.Round(100*time.Millisecond), took2.Round(100*time.Millisecond), stepTimes(out1)), nil
}

// stepTimes: when each step of a move began, from its log (time=... msg="step N/6: ...").
func stepTimes(out string) string {
	var first time.Time
	var parts []string
	for _, l := range strings.Split(out, "\n") {
		ts, _, ok := strings.Cut(strings.TrimPrefix(l, "time="), " ")
		if !ok || !strings.HasPrefix(l, "time=") {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			continue
		}
		if first.IsZero() {
			first = at
		}
		if i := strings.Index(l, `msg="step `); i >= 0 {
			parts = append(parts, fmt.Sprintf("%s at %s", l[i+10:i+13], at.Sub(first).Round(100*time.Millisecond)))
		} else if strings.Contains(l, "move-org done") {
			parts = append(parts, fmt.Sprintf("done at %s", at.Sub(first).Round(100*time.Millisecond)))
		}
	}
	return strings.Join(parts, ", ")
}
