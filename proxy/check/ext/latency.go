package ext

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/check"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// latency: check -rtt (an extension of check, registered in ext.go).
type latency struct{ rtt float64 }

func (l *latency) Flags(fs *flag.FlagSet) {
	fs.Float64Var(&l.rtt, "rtt", 0, "estimate the latency of a moved organization's recorded transactions at this round trip time to its database, ms (fdw and fdw+direct)")
}

// check -rtt: how long the recorded transactions of a moved organization would take with the
// given round trip time between the cloud and the organization's database. postgres_fdw
// talks to that database in several exchanges per statement; the direct path (fdw+direct) in
// one. The counts below are block 14's measurement on a real network (research/bench/2026-10-27-rtt:
// organizations 9 to 43 ms from the cloud, pgx, the operations of bench/load; docs/performance.md,
// the cost of the fdw profile over a long network); the report says it is an estimate.

// Exchanges with the organization's database, by what a statement does through FDW. A
// transaction pays fdwTxExchanges once (START TRANSACTION, COMMIT), every statement in it
// its own count; a statement on its own pays both (a SELECT: 2 + 3 = 5, as measured).
const (
	fdwTxExchanges     = 2 // START TRANSACTION, COMMIT
	fdwSelectExchanges = 3 // DECLARE CURSOR, FETCH, CLOSE (a big result fetches more)
	fdwInsertExchanges = 3 // PREPARE, EXECUTE, DEALLOCATE (one row per EXECUTE with RETURNING or row triggers)
	// fdwUpdatePartExchanges: an UPDATE the proxy sends straight to the organization's foreign
	// partition. It was 1 (a Foreign Update); since lokadb's row triggers sit on the foreign
	// partitions (0ba6d64: __refs, parent, FKs onto cloud tables, key maps) postgres_fdw cannot
	// send it whole, and it costs as through the parent (measured: 8 on its own, 6 in a
	// transaction). The field stays in the report's exchanges, of the same value.
	fdwUpdatePartExchanges  = fdwUpdateParentExchange
	fdwUpdateParentExchange = 6 // UPDATE or DELETE: a cursor FOR UPDATE, then by ctid (PREPARE, EXECUTE, DEALLOCATE), CLOSE
	directExchanges         = 1 // the direct path: the statement itself
	// target: the direct path's limit, a goal and not what the proxy does now (block 10, items
	// 6-9 and 8a.2: batches and transactions on the direct path): one exchange per statement,
	// one more for COMMIT of a transaction of several.
	targetCommitExchanges = 1
	// nPlusOne: a statement that runs this many times per transaction on average is a chain.
	nPlusOne = 1.5
	// latencyTop: the transactions and chains named in findings.
	latencyTop = 5
)

func (l *latency) Run(_ context.Context, s *check.Session) error {
	if l.rtt <= 0 {
		return nil
	}
	rtt := l.rtt
	lr := &check.LatencyReport{RTTms: rtt, Model: "estimate", Transactions: []check.LatencyTx{},
		Exchanges: check.LatencyExchanges{Transaction: fdwTxExchanges, Select: fdwSelectExchanges, Insert: fdwInsertExchanges,
			UpdateDirect: fdwUpdatePartExchanges, UpdateParent: fdwUpdateParentExchange, Direct: directExchanges, TargetCommit: targetCommitExchanges}}
	s.Report().Latency = lr
	calls, err := recordedCalls(s)
	if err != nil {
		return err
	}
	if len(s.Transactions()) == 0 {
		s.Add(check.Info, "latency_no_recording", "", "-rtt needs the proxy's recording of transactions (proxy -record FILE, then -workload FILE): pg_stat_statements has no transaction boundaries", nil)
		return nil
	}
	// repeats per fingerprint (a shape keeps one text per fingerprint, the statements every
	// text): calls beyond one per transaction, over the transactions of shapes with more
	// than one statement (a statement on its own is one call per transaction)
	alone, inTx := map[string]int64{}, map[string]int64{}
	for _, t := range s.Transactions() {
		for _, st := range t.Statements {
			if len(t.Statements) == 1 {
				alone[fpKey(st)] += t.Count
			} else {
				inTx[fpKey(st)] += t.Count
			}
		}
	}
	repeats := map[string]float64{}
	for k, n := range inTx {
		if extra := calls[k] - alone[k] - n; extra > 0 {
			repeats[k] = round1(1 + float64(extra)/float64(n))
		}
	}
	fps := map[int]string{} // the shapes' fingerprints, by their place in lr.Transactions (the findings' keys)
	for _, t := range s.Transactions() {
		lt, fp := latencyTx(s, t, repeats)
		if lt == nil {
			continue
		}
		fps[len(lr.Transactions)] = fp
		lt.FDWms, lt.DirectMs, lt.TargetMs = round1(lt.FDW*rtt), round1(lt.Direct*rtt), round1(lt.Target*rtt)
		lr.TotalFDWms += float64(lt.Count) * lt.FDWms
		lr.TotalDirectMs += float64(lt.Count) * lt.DirectMs
		lr.TotalTargetMs += float64(lt.Count) * lt.TargetMs
		lr.Transactions = append(lr.Transactions, *lt)
	}
	lr.TotalFDWms, lr.TotalDirectMs, lr.TotalTargetMs = round1(lr.TotalFDWms), round1(lr.TotalDirectMs), round1(lr.TotalTargetMs)
	order := make([]int, len(lr.Transactions))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := lr.Transactions[order[i]], lr.Transactions[order[j]]
		return a.FDWms > b.FDWms || a.FDWms == b.FDWms && a.Count > b.Count
	})
	sorted, sortedFps := make([]check.LatencyTx, len(order)), make([]string, len(order))
	for i, k := range order {
		sorted[i], sortedFps[i] = lr.Transactions[k], fps[k]
	}
	lr.Transactions = sorted
	s.Add(check.Info, "latency_estimate", "", fmt.Sprintf("at %g ms between the cloud and the organization's database, the %s recorded on LokaDB tables would wait about %s ms on the FDW path (fdw) and %s ms with the direct path as the proxy takes it now (fdw+direct: single statements, and transactions whose statements it all takes, once it has learnt their form); the limit, with everything that is left on the direct path too, is %s ms (one exchange per statement and one for COMMIT). An estimate: exchanges per statement are from block 14's measurement on a real network, research/bench/2026-10-27-rtt (on FDW a SELECT 5, an insert 3 and 2 per transaction, an UPDATE or DELETE 8; direct 1 per statement and 1 for COMMIT), repeats within a transaction from the recording's counts, big results and multi-row inserts take more. It is the steady state: a session's first direct connection to the organization's database (4-6 exchanges without direct_pool) and the probe after 30 s idle are not counted, so for short sessions it is optimistic",
		rtt, check.Plural(len(lr.Transactions), "transaction shape", "transaction shapes"), fmtMs(lr.TotalFDWms), fmtMs(lr.TotalDirectMs), fmtMs(lr.TotalTargetMs)), nil)

	for i, lt := range lr.Transactions {
		if i == latencyTop || lt.FDWms == 0 {
			break
		}
		var ex []string
		for _, st := range lt.Statements {
			if st.Kind == "cloud" {
				continue
			}
			rep := ""
			if st.Repeats > 1.05 {
				rep = fmt.Sprintf(" ×%.1f", st.Repeats)
			}
			ex = append(ex, fmt.Sprintf("%g exchanges%s  %s", st.FDW, rep, check.Short(st.SQL)))
		}
		direct := ""
		if lt.Direct < lt.FDW {
			direct = fmt.Sprintf("; with the direct path %s ms (%g)", fmtMs(lt.DirectMs), lt.Direct)
		}
		direct += fmt.Sprintf("; the direct path's goal %s ms (%g)", fmtMs(lt.TargetMs), lt.Target)
		s.AddFinding(check.Finding{Severity: check.Info, Code: "latency_expensive_transaction", Subject: &check.Subject{Kind: "transaction", Fingerprint: sortedFps[i]},
			Message: fmt.Sprintf("a transaction of %s, seen %d times, would take about %s ms (%g exchanges with the organization's database at %g ms)%s",
				check.Plural(len(lt.Statements), "statement", "statements"), lt.Count, fmtMs(lt.FDWms), lt.FDW, rtt, direct), Examples: ex})
	}

	// chains: a statement repeated within transactions (N+1)
	type chain struct {
		s     check.LatencyStatement
		count int64
		cost  float64
	}
	chains := map[string]*chain{}
	for _, lt := range lr.Transactions {
		for _, st := range lt.Statements {
			if st.Kind == "cloud" || st.Repeats < nPlusOne {
				continue
			}
			k := st.Fingerprint
			if k == "" {
				k = st.SQL
			}
			ch := chains[k]
			if ch == nil {
				ch = &chain{s: st}
				chains[k] = ch
			}
			ch.count += lt.Count
			ch.cost += float64(lt.Count) * st.FDW * rtt
		}
	}
	var cs []*chain
	for _, ch := range chains {
		cs = append(cs, ch)
	}
	sort.Slice(cs, func(i, j int) bool {
		return cs[i].cost > cs[j].cost || cs[i].cost == cs[j].cost && cs[i].s.SQL < cs[j].s.SQL
	})
	for i, ch := range cs {
		if i == latencyTop {
			break
		}
		per := ch.s.FDW / ch.s.Repeats
		s.AddFinding(check.Finding{Severity: check.Info, Code: "latency_n_plus_one", Subject: &check.Subject{Kind: "pattern", Fingerprint: ch.s.Fingerprint},
			Message: fmt.Sprintf("a statement runs about %.1f times per transaction (a chain of the same query, N+1): each costs %g exchanges with the organization's database, %s ms at %g ms, so about %s ms per transaction. One query for all the rows (IN, a JOIN) costs one",
				ch.s.Repeats, per, fmtMs(per*rtt), rtt, fmtMs(ch.s.FDW*rtt)),
			Examples: []string{check.Short(ch.s.SQL)},
			Patterns: []check.Pattern{{Fingerprint: ch.s.Fingerprint, SQL: ch.s.SQL, Calls: int64(math.Round(ch.s.Repeats * float64(ch.count)))}}})
	}
	return nil
}

// latencyTx costs one transaction shape, with the shape's fingerprint; nil when it touches no
// LokaDB table.
func latencyTx(se *check.Session, t *proxy.RecordedTx, repeats map[string]float64) (*check.LatencyTx, string) {
	lt := &check.LatencyTx{Count: t.Count, Statements: []check.LatencyStatement{}}
	meta := se.Meta()
	lokadb := false
	named := false // the shape names its organization (lokadb.tenant), so it is a transaction
	fits := true   // every statement of the shape fits a direct transaction (tasks/direct-path.md, section 4)
	var fps []string
	counted := t.RepeatsTx > 0 && len(t.Repeats) == len(t.Statements) // the recording counted them (proxy record.go)
	for i, s := range t.Statements {
		sql := check.Analyzable(s)
		if check.TenantSet(sql) {
			named = true // a statement of its own: the shape is a transaction, with a COMMIT
			// the cooperative mode: a transaction that names its organization goes direct
			// (measured: BEGIN, set_config('lokadb.tenant', …), the statements, COMMIT); one that
			// names every organization ('*') does not
			if _, all := check.TenantNamed(sql); all {
				fits = false
			}
			continue
		}
		fp, err := pg.Fingerprint(sql)
		if err != nil {
			fp = ""
		}
		fps = append(fps, fp)
		ls := check.LatencyStatement{Fingerprint: fp, SQL: s, Kind: "cloud", Repeats: 1}
		switch {
		case counted:
			ls.Repeats, ls.Counted = round1(float64(t.Repeats[i])/float64(t.RepeatsTx)), true
		case repeats[fpKey(s)] > 0 && len(t.Statements) > 1:
			ls.Repeats = repeats[fpKey(s)]
		}
		k := se.Classify(s)
		v := proxy.Direct(sql, meta, false)
		if len(v.Lokadb) > 0 && k != check.ClassDDL && k != check.ClassDDLRejected && k != check.ClassForbidden && k != check.ClassParse {
			lokadb = true
			fits = fits && proxy.DirectTakes(sql, meta, false) && k != check.ClassFanout && k != check.ClassAllowed
			ls.Kind = statementKind(sql)
			per := float64(fdwSelectExchanges)
			switch ls.Kind {
			case "insert":
				per = fdwInsertExchanges
			case "update":
				per = fdwUpdateParentExchange
				if k == check.ClassRouted && len(v.Lokadb) == 1 {
					per = fdwUpdatePartExchanges
				}
			case "delete":
				per = fdwUpdateParentExchange
			}
			ls.AllOrgs = k == check.ClassFanout || k == check.ClassAllowed
			lt.AllOrgs = lt.AllOrgs || ls.AllOrgs
			ls.FDW = round1(per * ls.Repeats)
			ls.Direct = ls.FDW
			ls.Target = ls.Repeats
		}
		if ls.Kind == "cloud" && !cloudStatement(sql) {
			fits = false // a setting or a statement a direct transaction does not carry
		}
		lt.Statements = append(lt.Statements, ls)
	}
	if !lokadb {
		return nil, ""
	}
	for _, s := range lt.Statements {
		lt.FDW += s.FDW
		lt.Target += s.Target
	}
	lt.FDW += fdwTxExchanges
	lt.Direct = lt.FDW
	// the direct path takes a statement on its own (one exchange), and a transaction whose
	// every statement on LokaDB tables it takes (DirectTakes), the others on cloud tables
	// alone: after the proxy learns the form (two such transactions committed in a row
	// through the cloud; Server.directOK), BEGIN rides with the first statement, so each
	// statement costs one exchange and COMMIT one. Batches of the extended protocol with
	// BEGIN in the pipeline stay on the cloud's way (block 10, item 6).
	// a statement on its own: not one after lokadb.tenant (BEGIN, set_config, the statement,
	// COMMIT: the commit is an exchange of its own, measured), not one of a block
	single := len(lt.Statements) == 1 && lt.Statements[0].Repeats == 1 && !named && t.Form != "block"
	if !single {
		lt.Target += targetCommitExchanges
	}
	switch {
	case t.Form == "batch" && fits && len(lt.Statements) > 0:
		// a pipeline of one organization (pgx.Batch, JDBC executeBatch): all of it in one
		// exchange with the organization's database; reads and UPDATEs commit with it, a
		// pipeline with INSERT or DELETE (8b: ids, refs rows) commits in a second one
		// (tasks/direct-path.md, 4a)
		lt.Direct = directExchanges
		if !onlyReadsAndUpdates(lt.Statements) {
			lt.Direct += targetCommitExchanges
		}
		for i := range lt.Statements {
			lt.Statements[i].Direct = 0
		}
	case single:
		lt.Single = true
		if v := proxy.Direct(check.Analyzable(lt.Statements[0].SQL), meta, false); v.OK {
			lt.Direct = directExchanges
			if commitsAfterCloud(lt.Statements[0].SQL) {
				// a DELETE, an upsert, an INSERT … SELECT on its own: the statement, then the
				// organization's COMMIT once the cloud has its refs rows and FKs (block 10,
				// performance review PF10); in a transaction they commit with it
				lt.Direct += targetCommitExchanges
				lt.Target = lt.Direct
			}
			lt.Statements[0].Direct = lt.Direct
		}
	case fits && (t.Form == "block" || t.Form == ""): // several statements of one simple query stay on the cloud's way
		lt.Direct = lt.Target
		for i := range lt.Statements {
			lt.Statements[i].Direct = lt.Statements[i].Target
		}
	}
	if lt.Direct < lt.Target { // the limit is never above what the direct path does now (a pipeline: one exchange)
		lt.Target = lt.Direct
	}
	lt.FDW, lt.Direct, lt.Target = round1(lt.FDW), round1(lt.Direct), round1(lt.Target)
	sort.Strings(fps)
	return lt, strings.Join(fps, "+")
}

// statementKind: what a statement does, as FDW pays for it.
func statementKind(sql string) string {
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) == 0 {
		return "other"
	}
	switch s := tree.Stmts[0].Stmt; {
	case s.GetSelectStmt() != nil:
		return "select"
	case s.GetInsertStmt() != nil:
		return "insert"
	case s.GetUpdateStmt() != nil:
		return "update"
	case s.GetDeleteStmt() != nil:
		return "delete"
	}
	return "other"
}

// recordedCalls: how many times each statement ran, from the proxy recordings only (the
// shapes come from there; pg_stat_statements counts other runs too).
func recordedCalls(s *check.Session) (map[string]int64, error) {
	calls := map[string]int64{}
	for _, path := range s.Options().Workload {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var w proxy.Workload
		if err := json.Unmarshal(b, &w); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if wd := w.Databases[s.Report().Database]; wd != nil {
			for _, st := range wd.Statements {
				calls[fpKey(st.SQL)] += st.Count
			}
		}
	}
	return calls, nil
}

// fpKey: the pg_query fingerprint of a statement, its text when it does not parse.
func fpKey(sql string) string {
	if fp, err := pg.Fingerprint(check.Analyzable(sql)); err == nil {
		return fp
	}
	return sql
}

// commitsAfterCloud: a statement the direct path runs on its own in two exchanges with the
// organization's database: its COMMIT waits for the cloud, which writes the refs rows (of the
// keys the organization returned) or runs its FKs (a DELETE's RESTRICT, CASCADE) first.
func commitsAfterCloud(sql string) bool {
	tree, err := pg.Parse(check.Analyzable(sql))
	if err != nil || len(tree.Stmts) != 1 {
		return false
	}
	st := tree.Stmts[0].Stmt
	if st.GetDeleteStmt() != nil {
		return true
	}
	ins := st.GetInsertStmt()
	if ins == nil {
		return false
	}
	if ins.OnConflictClause != nil {
		return true // an upsert
	}
	sel := ins.SelectStmt.GetSelectStmt()
	return sel != nil && len(sel.ValuesLists) == 0 // INSERT … SELECT, not … VALUES
}

// onlyReadsAndUpdates: every statement on LokaDB tables is a read or an UPDATE (no INSERT or
// DELETE of 8b: those need the cloud's ids and refs rows, and their commit follows).
func onlyReadsAndUpdates(l []check.LatencyStatement) bool {
	for _, s := range l {
		if s.Kind != "cloud" && s.Kind != "select" && s.Kind != "update" {
			return false
		}
	}
	return true
}

// cloudStatement: a statement on cloud tables alone that a direct transaction sends to the cloud.
func cloudStatement(sql string) bool {
	switch statementKind(sql) {
	case "select", "insert", "update", "delete":
		return true
	}
	return false
}

func round1(f float64) float64 { return math.Round(f*10) / 10 }

func fmtMs(f float64) string {
	if f >= 100 {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%.1f", f)
}

// WriteText: the latency section of the text report, after the summary.
func (*latency) WriteText(w io.Writer, r *check.Report) {
	l := r.Latency
	if l == nil || len(l.Transactions) == 0 {
		return
	}
	fmt.Fprintf(w, "\nLatency of a moved organization at RTT %g ms (estimate, from block 14's measurement on a real network, research/bench/2026-10-27-rtt; the steady state: a session's first direct connection and the probe after idle are not counted)\n", l.RTTms)
	fmt.Fprintf(w, "  all recorded transactions: %s ms on FDW, %s ms with the direct path now; goal %s ms (the limit: one exchange per statement and one for COMMIT)\n",
		fmtMs(l.TotalFDWms), fmtMs(l.TotalDirectMs), fmtMs(l.TotalTargetMs))
	fmt.Fprintf(w, "  %11s  %11s  %11s\n", "fdw", "direct now", "goal")
	for i, t := range l.Transactions {
		if i == latencyTop {
			fmt.Fprintf(w, "  ... and %d more transaction shapes\n", len(l.Transactions)-i)
			break
		}
		first := ""
		for _, s := range t.Statements {
			if s.Kind != "cloud" {
				first = check.Short(s.SQL)
				break
			}
		}
		fmt.Fprintf(w, "  %8s ms  %8s ms  %8s ms  x%-6d %s %s\n", fmtMs(t.FDWms), fmtMs(t.DirectMs), fmtMs(t.TargetMs), t.Count,
			check.Plural(len(t.Statements), "statement:", "statements, first:"), first)
	}
}
