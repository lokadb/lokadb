package ext

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lokadb/lokadb/proxy/check"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// latencyCase: a session without a database and the -rtt extension over it.
type latencyCase struct {
	s    *check.Session
	rep  *check.Report
	meta *proxy.Meta
	rtt  float64
}

func (c *latencyCase) latency() error { return (&latency{rtt: c.rtt}).Run(context.Background(), c.s) }

// latencyChecker: documents (organization_id) is LokaDB, users a cloud table; the recording
// of database app holds the given statements and shapes.
func latencyChecker(t *testing.T, stmts map[string]int64, txs []*proxy.RecordedTx) *latencyCase {
	t.Helper()
	meta := &proxy.Meta{Tables: map[string]*proxy.Table{
		"documents": {Name: "documents", TenantCol: "organization_id", Refs: "documents__refs", Cloud: "documents__cloud",
			Cols: []string{"id", "organization_id", "title"}, ColType: map[string]string{"id": "bigint", "organization_id": "bigint", "title": "text"}},
	}, Lookups: map[string]map[string]string{"documents": {"organization_id": "", "id": "documents__refs"}}}
	wd := &proxy.WorkloadDB{Transactions: txs}
	for sql, n := range stmts {
		wd.Statements = append(wd.Statements, &proxy.RecordedStatement{SQL: sql, Count: n})
	}
	b, _ := json.Marshal(proxy.Workload{Version: 1, Databases: map[string]*proxy.WorkloadDB{"app": wd}})
	path := filepath.Join(t.TempDir(), "workload.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	fan, _ := proxy.Config{}.FanoutFor("app")
	rep := &check.Report{Database: "app", Findings: []check.Finding{}}
	s := check.NewOfflineSession(check.Offline{Options: check.Options{Workload: []string{path}}, Meta: meta, Fanout: fan, Transactions: txs, Report: rep})
	return &latencyCase{s: s, rep: rep, meta: meta, rtt: 20}
}

func TestLatency(t *testing.T) {
	const (
		sel    = "SELECT id, title FROM documents WHERE organization_id = $1 AND id = $2"
		upd    = "UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3"
		ins    = "INSERT INTO documents (organization_id, title) VALUES ($1, $2)"
		cloud  = "SELECT name FROM users WHERE id = $1"
		byList = "SELECT id FROM documents WHERE organization_id = $1"
	)
	txs := []*proxy.RecordedTx{
		{Statements: []string{sel}, Count: 100},               // on its own: fdw 5, direct 1
		{Statements: []string{cloud, byList, sel}, Count: 10}, // a list, then each row (N+1)
		{Statements: []string{ins, upd}, Count: 4},            // a transaction: stays on FDW
		{Statements: []string{cloud}, Count: 50},              // cloud only: not costed
	}
	// sel runs 100 times alone and 6 times in each of the 10 list transactions
	c := latencyChecker(t, map[string]int64{sel: 160, byList: 10, cloud: 60, ins: 4, upd: 4}, txs)
	if err := c.latency(); err != nil {
		t.Fatal(err)
	}
	l := c.rep.Latency
	if l == nil || l.Model != "estimate" || len(l.Transactions) != 3 {
		t.Fatalf("latency: %+v", l)
	}
	byFirst := map[string]check.LatencyTx{}
	for _, tx := range l.Transactions {
		for _, s := range tx.Statements {
			if s.Kind != "cloud" {
				byFirst[s.SQL] = tx
				break
			}
		}
	}
	single := byFirst[sel]
	if single.FDW != 5 || single.Direct != 1 || single.FDWms != 100 || single.DirectMs != 20 || !single.Single {
		t.Errorf("a SELECT on its own: %+v, want 5 exchanges (100 ms) on FDW, 1 (20 ms) direct", single)
	}
	list := byFirst[byList]
	// 2 (transaction) + 3 (the list) + 3 × 6 (each row) = 23; the cloud statement costs nothing
	// the direct path takes all of it once learnt (8a.2): 1 (the list) + 6 (rows) + COMMIT = 8
	if list.FDW != 23 || list.Direct != 8 || list.Statements[2].Repeats != 6 || list.Statements[0].Kind != "cloud" {
		t.Errorf("list then rows: %+v, want 23 exchanges on FDW, 8 direct, the row query repeated 6 times", list)
	}
	// the goal: one exchange per run and one for COMMIT; a statement on its own, one
	if single.Target != 1 || list.Target != 1+6+1 || byFirst[ins].Target != 1+1+1 || list.TargetMs != 160 {
		t.Errorf("goal: single %g, list %g (%g ms), insert+update %g; want 1, 8 (160 ms: the cloud statement costs none), 3", single.Target, list.Target, list.TargetMs, byFirst[ins].Target)
	}
	tx := byFirst[ins]
	// 2 + 3 (insert) + 6 (update: lokadb's row triggers keep it from being a Foreign Update);
	// the insert has no id and this meta no sequence of the organization's database for it:
	// the direct path does not take it (DirectTakes), the shape stays on FDW
	if tx.FDW != 11 || tx.Direct != 11 || tx.Single {
		t.Errorf("insert and update in a transaction: %+v, want 11 exchanges on both paths", tx)
	}
	if l.Transactions[0].FDW != 23 {
		t.Errorf("the most expensive transaction is not first: %+v", l.Transactions[0])
	}
	if want := 100*100 + 10*460 + 4*220.0; l.TotalFDWms != want {
		t.Errorf("total fdw %g ms, want %g", l.TotalFDWms, want)
	}
	codes := map[string][]check.Finding{}
	for _, f := range c.rep.Findings {
		codes[f.Code] = append(codes[f.Code], f)
	}
	if len(codes["latency_estimate"]) != 1 || len(codes["latency_expensive_transaction"]) != 3 {
		t.Errorf("findings: %v", codes)
	}
	np := codes["latency_n_plus_one"]
	if len(np) != 1 || !strings.Contains(np[0].Message, "6.0 times") || np[0].Patterns[0].SQL != sel || np[0].Key == "latency_n_plus_one" {
		t.Errorf("N+1: %+v", np)
	}
	for _, f := range c.rep.Findings {
		if f.Severity != check.Info {
			t.Errorf("%s is %s: -rtt only informs, it must not change the verdict", f.Code, f.Severity)
		}
	}
}

func TestLatencyNoRecording(t *testing.T) {
	c := latencyChecker(t, nil, nil)
	if err := c.latency(); err != nil {
		t.Fatal(err)
	}
	if len(c.rep.Findings) != 1 || c.rep.Findings[0].Code != "latency_no_recording" {
		t.Errorf("findings: %+v", c.rep.Findings)
	}
}

// A recording that counts repeats (RecordedTx.Repeats): they are taken as counted, not estimated.
func TestLatencyCountedRepeats(t *testing.T) {
	const (
		list = "SELECT id FROM documents WHERE organization_id = $1"
		row  = "SELECT id, title FROM documents WHERE organization_id = $1 AND id = $2"
	)
	txs := []*proxy.RecordedTx{{Statements: []string{list, row}, Count: 12, Repeats: []int64{10, 80}, RepeatsTx: 10}}
	c := latencyChecker(t, map[string]int64{list: 12, row: 500}, txs)
	if err := c.latency(); err != nil {
		t.Fatal(err)
	}
	st := c.rep.Latency.Transactions[0].Statements[1]
	if st.Repeats != 8 || !st.Counted || c.rep.Latency.Transactions[0].FDW != 2+3+3*8 {
		t.Errorf("counted repeats: %+v, want 8 counted, %d exchanges", st, 2+3+3*8)
	}
}

// A transaction whose statements the direct path takes goes direct once learnt (8a.2): one
// exchange per statement and one for COMMIT; a statement it cannot take keeps the shape on FDW.
func TestLatencyDirectTransactions(t *testing.T) {
	const (
		sel   = "SELECT id, title FROM documents WHERE organization_id = $1 AND id = $2"
		upd   = "UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3"
		cloud = "SELECT name FROM users WHERE id = $1"
		set   = "SET search_path = x"
		fan   = "SELECT count(*) FROM documents"
	)
	txs := []*proxy.RecordedTx{
		{Statements: []string{sel, upd}, Count: 10},       // fits: both statements, one organization
		{Statements: []string{cloud, sel, upd}, Count: 5}, // a cloud statement between: still fits
		{Statements: []string{sel, set}, Count: 4},        // a setting: the cloud's way
		{Statements: []string{sel, fan}, Count: 3},        // a statement without the organization: not one database
	}
	c := latencyChecker(t, map[string]int64{sel: 22, upd: 15, cloud: 5, set: 4, fan: 3}, txs)
	if err := c.latency(); err != nil {
		t.Fatal(err)
	}
	got := map[string]check.LatencyTx{}
	for _, tx := range c.rep.Latency.Transactions {
		key := ""
		for _, s := range tx.Statements {
			key += string(s.Kind[0])
		}
		got[key+fmt.Sprint(tx.Count)] = tx
	}
	fit := got["su10"] // select, update
	if fit.Direct != fit.Target || fit.Direct != 3 || fit.FDW <= fit.Direct {
		t.Errorf("fitting transaction: fdw %g, direct %g, target %g; want direct = target = 3 (1 + 1 + COMMIT)", fit.FDW, fit.Direct, fit.Target)
	}
	if mixed := got["csu5"]; mixed.Direct != mixed.Target || mixed.Direct != 3 {
		t.Errorf("with a cloud statement: %+v, want 3", mixed)
	}
	for _, key := range []string{"sc4", "ss3", "sf3"} {
		if tx, ok := got[key]; ok && tx.Direct != tx.FDW {
			t.Errorf("%s: direct %g, want the FDW way %g", key, tx.Direct, tx.FDW)
		}
	}
}

// A pipeline (the Executes of one Sync outside a block) of reads and UPDATEs is one exchange
// with the organization's database, with an INSERT or DELETE one and its commit; several
// statements of one simple query stay on FDW; the same statements in a block cost one
// exchange each.
func TestLatencyPipelines(t *testing.T) {
	const (
		sel = "SELECT id, title FROM documents WHERE organization_id = $1 AND id = $2"
		upd = "UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3"
		del = "DELETE FROM documents WHERE organization_id = $1 AND id = $2"
	)
	txs := []*proxy.RecordedTx{
		{Statements: []string{sel, upd}, Count: 10, Form: "batch"},
		{Statements: []string{sel, upd}, Count: 6, Form: "block"},
		{Statements: []string{sel, del}, Count: 4, Form: "batch"},
		{Statements: []string{sel, upd}, Count: 3, Form: "script"},
	}
	c := latencyChecker(t, map[string]int64{sel: 23, upd: 19, del: 4}, txs)
	if err := c.latency(); err != nil {
		t.Fatal(err)
	}
	want := map[string][2]float64{"batch10": {1, 1}, "block6": {3, 3}, "batch4": {0, 0}, "script3": {0, 0}}
	for _, tx := range c.rep.Latency.Transactions {
		form := map[int64]string{10: "batch", 6: "block", 4: "batch", 3: "script"}[tx.Count]
		key := fmt.Sprint(form, tx.Count)
		switch key {
		case "batch10", "block6":
			if tx.Direct != want[key][0] || tx.Target > tx.Direct {
				t.Errorf("%s: direct %g target %g, want direct %g", key, tx.Direct, tx.Target, want[key][0])
			}
		case "batch4": // with a DELETE: all in one exchange, then the commit
			if tx.Direct != 2 {
				t.Errorf("%s: direct %g, want 2 (the pipeline and its commit)", key, tx.Direct)
			}
		default: // several statements of one simple query: the cloud's way, as many as on FDW
			if tx.Direct != tx.FDW {
				t.Errorf("%s: direct %g, want the FDW way %g", key, tx.Direct, tx.FDW)
			}
		}
	}
}

// PF10 of the performance review: on the direct path a DELETE, an upsert or an INSERT … SELECT
// on its own costs two exchanges with the organization's database (the statement, then its
// COMMIT after the cloud's refs rows and FKs); a read, an UPDATE, an INSERT … VALUES one.
func TestLatencySingleWrites(t *testing.T) {
	for sql, want := range map[string]float64{
		"SELECT id FROM documents WHERE organization_id = $1 AND id = $2":                                                       1,
		"UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3":                                                1,
		"DELETE FROM documents WHERE organization_id = $1 AND id = $2":                                                          2,
		"INSERT INTO documents (organization_id, title) SELECT $1, title FROM documents WHERE organization_id = $2 AND id = $3": 2,
	} {
		if !proxy.Direct(sql, latencyChecker(t, nil, nil).meta, false).OK {
			t.Errorf("%s: not on the direct path in this meta, the test compares nothing", sql)
			continue
		}
		c := latencyChecker(t, map[string]int64{sql: 5}, []*proxy.RecordedTx{{Statements: []string{sql}, Count: 5}})
		if err := c.latency(); err != nil {
			t.Fatal(err)
		}
		if got := c.rep.Latency.Transactions[0].Direct; got != want {
			t.Errorf("%s: %g exchanges on the direct path, want %g", sql, got, want)
		}
	}
	if !commitsAfterCloud("INSERT INTO documents (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET title = $2") ||
		commitsAfterCloud("INSERT INTO documents (id) VALUES ($1)") || !commitsAfterCloud("DELETE FROM documents WHERE id = $1") {
		t.Error("commitsAfterCloud: an upsert and a DELETE wait for the cloud, an INSERT … VALUES does not")
	}
}

// The model against what it comes from: block 14's measurement on a real network
// (research/bench/2026-10-27-rtt, organization 4: RTT 43.251 ms, pgx, bench/load's operations,
// p50 of one client). Exchanges × RTT must land within 10% of each measured p50.
func TestLatencyMatchesMeasurement(t *testing.T) {
	const (
		rtt    = 43.251
		read1  = "SELECT title FROM documents WHERE organization_id = $1 AND id = $2"
		read2  = "SELECT count(*) FROM documents WHERE organization_id = $1"
		feed   = "SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id DESC LIMIT 20"
		update = "UPDATE documents SET title = $3 WHERE organization_id = $1 AND id = $2"
		tenant = "SELECT set_config('lokadb.tenant', '<organization>', $1)"
	)
	for _, m := range []struct {
		name       string
		stmts      []string
		fdw, direc float64 // measured p50, ms
	}{
		{"feed on its own", []string{feed}, 220.4, 44.21},
		{"update on its own", []string{update}, 353.13, 44.86},
		{"feed in a transaction", []string{tenant, feed}, 219.99, 88.4},
		{"read in a transaction (two statements)", []string{tenant, read1, read2}, 351.71, 132.52},
		{"update in a transaction", []string{tenant, update}, 351.5, 89.04},
	} {
		tx := &proxy.RecordedTx{Statements: m.stmts, Count: 1, Form: "block"}
		if len(m.stmts) == 1 {
			tx.Form = ""
		}
		c := latencyChecker(t, nil, []*proxy.RecordedTx{tx})
		c.rtt = rtt
		if err := c.latency(); err != nil {
			t.Fatal(err)
		}
		lt := c.rep.Latency.Transactions[0]
		near := func(got, want float64) bool { return got > want*0.9 && got < want*1.1 }
		if !near(lt.FDWms, m.fdw) || !near(lt.DirectMs, m.direc) {
			t.Errorf("%s: model fdw %g ms (%g exchanges), direct %g ms (%g); measured %g and %g", m.name, lt.FDWms, lt.FDW, lt.DirectMs, lt.Direct, m.fdw, m.direc)
		}
	}
}
