package check

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// check -profile: what a routing profile (tasks/routing-declarations.md, section 7) would
// do with the workload seen, before the organization is switched to it: the estimate of its
// gain and of what changes. The transaction shapes are walked with the forms the proxy's
// direct path takes (proxy.DirectTakes), so their finding is what the proxy will do.

// ProfileReport: the -profile section of the report.
type ProfileReport struct {
	Profile string `json:"profile"`
	Org     int64  `json:"org,omitempty"`
	// CurrentProfile: the organization's profile now (lokadb.orgs.profile; -org of a moved organization).
	CurrentProfile string `json:"current_profile,omitempty"`
	// Patterns, Calls: query patterns on LokaDB tables and their executions; *Direct: those
	// that would go straight to the organization's database.
	Patterns       int   `json:"query_patterns"`
	PatternsDirect int   `json:"query_patterns_direct"`
	Calls          int64 `json:"calls"`
	CallsDirect    int64 `json:"calls_direct"`
	// DirectByValues: direct patterns whose organization comes from a list of values (IN,
	// ANY, several rows): direct only when the values belong to one organization.
	DirectByValues int `json:"query_patterns_direct_by_values"`
	// Reasons: why the other patterns stay on the FDW path (profile direct: why they fail).
	Reasons      []ProfileReason `json:"reasons"`
	Transactions *ProfileTx      `json:"transactions,omitempty"`
	// Forms: the recorded transaction forms the proxy decides by their first statement (8a.2):
	// those that go direct whole, and those it would refuse once (-direct-forms writes them
	// for lokadb.direct_learnt and lokadb.direct_cloud_first; not in the report's JSON)
	Forms []DirectForm `json:"-"`
}

type ProfileReason struct {
	Reason       string   `json:"reason"` // proxy.Direct* constants
	Patterns     int      `json:"query_patterns"`
	Calls        int64    `json:"calls"`
	Fingerprints []string `json:"fingerprints"` // pg_query fingerprints of the patterns
	Examples     []string `json:"examples"`
	pats         []*profilePattern
}

// ProfileTx: the transaction shapes of the proxy recording, as the profile would run them.
// DirectForm is a transaction form by its first statement: Fingerprint as the proxy computes
// it (pg_query), Named: the organization was named before it (lokadb.tenant); Direct: every
// statement goes direct (learnt ahead), else it goes direct first and then needs the cloud
// (the cloud's way from the start: no refusal).
type DirectForm struct {
	Fingerprint string `json:"fingerprint"`
	Named       bool   `json:"named"`
	Direct      bool   `json:"direct"`
	Example     string `json:"example"`
}

type ProfileTx struct {
	Shapes int   `json:"shapes"` // shapes touching LokaDB tables
	Count  int64 `json:"transactions"`
	// Direct: every statement on LokaDB tables goes direct and no cloud table is touched (one connection).
	Direct      int   `json:"shapes_direct"`
	DirectCount int64 `json:"transactions_direct"`
	// AfterCloud: shapes with statements that could go direct but stay on FDW because the
	// transaction touched the cloud first.
	AfterCloud int `json:"shapes_fdw_after_cloud"`
	// TwoConnections: shapes that start direct and then touch cloud tables: the organization's
	// database and the cloud each get a transaction, committed in that order.
	TwoConnections int `json:"shapes_two_connections"`
	// FDWAfterDirect: shapes that start direct and later need FDW (a JOIN with a cloud table):
	// FDW would not see the direct path's uncommitted rows.
	FDWAfterDirect int `json:"shapes_fdw_after_direct"`
	// Named: shapes that name the organization (SET [LOCAL] lokadb.tenant).
	Named int `json:"shapes_naming_organization"`
}

type profilePattern struct {
	sql, fp string
	fpOK    bool // fp is the pg_query fingerprint (else the text: it does not parse)
	calls   int64
	v       proxy.DirectVerdict
	writes  bool
	// form: why the proxy's direct path does not take it by its form, the organization named
	// (proxy.DirectRefusal: an application type, an UPDATE of the key, a trigger of the cloud…);
	// "" when it does or when DirectIn refuses it already (v.Reason)
	form string
}

func (c *checker) profile() error {
	p := c.o.Profile
	if !admin.ValidProfile(p) {
		return fmt.Errorf("unknown profile %q: one of %s", p, strings.Join(admin.Profiles, ", "))
	}
	pr := &ProfileReport{Profile: p, Org: c.o.Org, Reasons: []ProfileReason{}}
	c.rep.Profile = pr
	if c.o.Org != 0 {
		cur, err := c.orgProfile(c.o.Org)
		if err != nil {
			return err
		}
		pr.CurrentProfile = cur
		switch {
		case cur == "":
			c.add(Info, "profile_org_not_moved", "", fmt.Sprintf("organization %d is not in its own database yet: a profile applies after move-org; the estimate assumes it is moved", c.o.Org), nil)
		case cur == p:
			c.add(Info, "profile_unchanged", "", fmt.Sprintf("organization %d is already on profile %s", c.o.Org, p), nil)
		}
	}
	if p == admin.ProfileFDW {
		c.add(Info, "profile_fdw", "", "profile fdw is how the proxy routes now: every statement goes to the cloud, which reaches the organization's database by postgres_fdw; the rest of this report is its check", nil)
		return nil
	}
	if !admin.ProfileServed[p] {
		c.add(Info, "profile_estimate_only", "", fmt.Sprintf("profile %s is not routed by this version of lokadb yet (lokadb org-profile refuses it): this is an estimate of what it would do with the workload seen", p), nil)
	}

	c.idleSessionTimeout()

	// statement patterns, one per fingerprint, the organization not named
	byFP := map[string]*profilePattern{}
	for sql, n := range c.pats {
		s := analyzable(sql)
		v := proxy.DirectIn(s, c.meta, false, c.directPath())
		if len(v.Lokadb) == 0 {
			continue
		}
		fp, err := pg.Fingerprint(s)
		if err != nil {
			fp = sql
		}
		g := byFP[fp]
		if g == nil {
			g = &profilePattern{fp: fp, fpOK: err == nil, calls: -1}
			byFP[fp] = g
		}
		if n > g.calls {
			g.sql, g.v, g.writes = sql, v, len(c.writes(sql).all()) > 0 || len(v.Inserts) > 0
			g.form = proxy.DirectRefusal(s, c.meta, true, c.directPath())
		}
		g.calls = max(g.calls, 0) + n
	}
	pats := make([]*profilePattern, 0, len(byFP))
	for _, g := range byFP {
		pats = append(pats, g)
	}
	sort.Slice(pats, func(i, j int) bool {
		return pats[i].calls > pats[j].calls || pats[i].calls == pats[j].calls && pats[i].sql < pats[j].sql
	})
	if len(pats) == 0 {
		if len(c.rep.Sources) > 0 {
			c.add(Info, "profile_no_lokadb_queries", "", "no query pattern on LokaDB tables was seen: nothing to estimate for the profile", nil)
		}
		return nil
	}

	reasons := map[string]*ProfileReason{}
	addReason := func(r string, g *profilePattern) {
		pr := reasons[r]
		if pr == nil {
			pr = &ProfileReason{Reason: r}
			reasons[r] = pr
		}
		pr.Patterns++
		pr.Calls += g.calls
		pr.Fingerprints = append(pr.Fingerprints, g.fp)
		pr.pats = append(pr.pats, g)
		if len(pr.Examples) < 8 {
			pr.Examples = append(pr.Examples, fmt.Sprintf("x%d  %s", g.calls, short(g.sql)))
		}
	}
	var cloudID map[string]int64
	seqID := map[string]bool{}
	star := c.acrossOrgs() // run after SET LOCAL lokadb.tenant = '*': across organizations
	for _, g := range pats {
		pr.Patterns++
		pr.Calls += g.calls
		if g.v.OK && g.form != "" { // Direct's verdict, but not a form the proxy takes (review of check, 2)
			addReason(g.form, g)
			continue
		}
		if g.v.OK && star[g.fp] && p == admin.ProfileDirect { // refused there (direct_only.go)
			addReason(acrossOrgs, g)
			continue
		}
		if g.v.OK {
			pr.PatternsDirect++
			pr.CallsDirect += g.calls
			if g.v.ByValues {
				pr.DirectByValues++
			}
			for _, t := range g.v.Inserts {
				if c.insertsWithoutID(g.sql, t, seqID) {
					if cloudID == nil {
						cloudID = map[string]int64{}
					}
					cloudID[t] += g.calls
				}
			}
			continue
		}
		addReason(g.v.Reason, g)
	}
	for _, r := range reasons {
		pr.Reasons = append(pr.Reasons, *r)
	}
	sort.Slice(pr.Reasons, func(i, j int) bool {
		return pr.Reasons[i].Calls > pr.Reasons[j].Calls || pr.Reasons[i].Calls == pr.Reasons[j].Calls && pr.Reasons[i].Reason < pr.Reasons[j].Reason
	})
	named := c.profileTransactions(pr)

	if p == admin.ProfileFDWDirect {
		c.add(Info, "profile_direct_share", "", fmt.Sprintf("profile fdw+direct: %d of %s on LokaDB tables (%s of %d executions) would go straight to the organization's database, in one round trip instead of postgres_fdw's several%s",
			pr.PatternsDirect, plural(pr.Patterns, "query pattern", "query patterns"), percent(pr.CallsDirect, pr.Calls), pr.Calls, byValuesNote(pr.DirectByValues)), nil)
		for _, r := range pr.Reasons {
			c.add(Info, "profile_fdw_reason", r.Reason, fmt.Sprintf("%s (%d executions) stay on the FDW path: %s", plural(r.Patterns, "query pattern", "query patterns"), r.Calls, reasonOf(r.Reason)), r.Examples)
			c.withPatterns(r.pats)
		}
		if tx := pr.Transactions; tx != nil {
			if tx.TwoConnections > 0 {
				c.addFinding(Finding{Severity: Change, Code: "profile_two_connections", Transactions: c.twoShapes, Message: fmt.Sprintf("%s go direct first and then touch cloud tables: they run on two connections, the organization's database commits first and then the cloud, without two-phase commit (as postgres_fdw does now)",
					plural(tx.TwoConnections, "transaction shape", "transaction shapes"))})
			}
			if tx.FDWAfterDirect > 0 {
				c.add(Warning, "profile_fdw_after_direct", "", fmt.Sprintf("%s go direct first and then need the cloud for a statement on LokaDB tables (a JOIN with a cloud table, another organization, upsert, MERGE, COPY, INSERT … SELECT, a table with a trigger of the cloud): postgres_fdw would not see the rows the transaction wrote directly, so the first time such a transaction runs on fdw+direct the proxy refuses that statement (0A000) and the transaction has to be run again; from then on transactions that start the same way go through the cloud (lokadb.direct_cloud_first). To keep them direct, restrict the statement to the forms the direct path takes, or run it before the direct statements",
					plural(tx.FDWAfterDirect, "transaction shape", "transaction shapes")), nil)
				c.rep.Findings[len(c.rep.Findings)-1].Transactions = c.fdwAfterShapes
			}
			if tx.AfterCloud > 0 {
				c.add(Info, "profile_fdw_after_cloud", "", fmt.Sprintf("%s touch cloud tables first: their later statements on LokaDB tables stay on the FDW path", plural(tx.AfterCloud, "transaction shape", "transaction shapes")), nil)
			}
		}
		for _, t := range sortedKeysInt(cloudID) {
			c.add(Info, "profile_insert_cloud_id", t, fmt.Sprintf("direct inserts into %s without an id (%d executions) take it from the cloud sequence: one more round trip to the cloud each; ranges of ids per organization would remove it", t, cloudID[t]), nil)
		}
		return nil
	}

	// profile direct: no FDW. A statement that cannot go direct fails, and reads must name
	// the organization (lokadb.tenant): a read guessed wrong returns nothing, silently.
	mirrors := map[string]*ProfileReason{}
	for _, g := range pats {
		if g.v.Reason != proxy.DirectCloudTable {
			continue
		}
		for _, t := range g.v.Cloud {
			m := mirrors[t]
			if m == nil {
				m = &ProfileReason{Reason: t}
				mirrors[t] = m
			}
			m.Patterns++
			m.Calls += g.calls
			m.pats = append(m.pats, g)
			if len(m.Examples) < 8 {
				m.Examples = append(m.Examples, fmt.Sprintf("x%d  %s", g.calls, short(g.sql)))
			}
		}
	}
	for _, t := range sortedReasons(mirrors) {
		c.add(Blocker, "profile_direct_needs_mirror", t.Reason, fmt.Sprintf("%s join LokaDB tables with cloud table %s (%d executions): profile direct refuses them without a copy of %s in the organization's database (routing.mirrors, not available yet) or a rewrite",
			plural(t.Patterns, "query pattern", "query patterns"), t.Reason, t.Calls, t.Reason), t.Examples)
		c.withPatterns(t.pats)
	}
	// the forms the proxy's direct path does not take, named or not: refused on direct
	forms := map[string]*ProfileReason{}
	for _, g := range pats {
		if g.form == "" {
			continue
		}
		r := forms[g.form]
		if r == nil {
			r = &ProfileReason{Reason: g.form}
			forms[g.form] = r
		}
		r.Patterns++
		r.Calls += g.calls
		r.pats = append(r.pats, g)
		if len(r.Examples) < 8 {
			r.Examples = append(r.Examples, fmt.Sprintf("x%d  %s", g.calls, short(g.sql)))
		}
	}
	for _, r := range sortedReasons(forms) {
		c.add(Blocker, "profile_direct_refused_form", r.Reason, fmt.Sprintf("%s (%d executions) cannot go to the organization's database: %s; profile direct has no other way, so the proxy refuses them (0A000)",
			plural(r.Patterns, "query pattern", "query patterns"), r.Calls, formText(r.Reason)), r.Examples)
		c.withPatterns(r.pats)
	}
	var unnamedReads, unnamedWrites, allOrgs []string
	var unnamedReadsP, unnamedWritesP, allOrgsP []*profilePattern
	for _, g := range pats {
		switch {
		case g.form != "":
			continue
		case star[g.fp]:
		case named[g.fp], g.v.Reason == proxy.DirectCloudTable, g.v.Reason == proxy.DirectDDL, unroutable[g.v.Reason]:
			continue
		}
		ex := fmt.Sprintf("x%d  %s", g.calls, short(g.sql))
		switch {
		case g.v.Reason == proxy.DirectNoOrg, star[g.fp]:
			allOrgs, allOrgsP = append(allOrgs, ex), append(allOrgsP, g)
		case g.writes:
			unnamedWrites, unnamedWritesP = append(unnamedWrites, ex), append(unnamedWritesP, g)
		default:
			unnamedReads, unnamedReadsP = append(unnamedReads, ex), append(unnamedReadsP, g)
		}
	}
	if len(allOrgs) > 0 {
		c.add(Blocker, "profile_direct_all_organizations", "", fmt.Sprintf("%s on LokaDB tables name no organization (reports, admin pages, jobs, or a missing filter): profile direct has no path across organizations; they need a separate path (an analytics copy, or a proxy role on profile fdw)",
			plural(len(allOrgs), "query pattern", "query patterns")), firstN(allOrgs, 8))
		c.withPatterns(allOrgsP)
	}
	if len(unnamedReads) > 0 {
		c.add(Blocker, "profile_direct_unnamed_read", "", fmt.Sprintf("%s read LokaDB tables without the application naming the organization (SET LOCAL lokadb.tenant): on profile direct a read guessed wrong returns nothing, silently, so reads must name it",
			plural(len(unnamedReads), "query pattern", "query patterns")), firstN(unnamedReads, 8))
		c.withPatterns(unnamedReadsP)
	}
	if len(unnamedWrites) > 0 {
		c.add(Warning, "profile_direct_unnamed_write", "", fmt.Sprintf("%s write LokaDB tables without the application naming the organization: on profile direct the proxy guesses it from the statement; a wrong guess fails on the organization's CHECK, so this works but lokadb.tenant is safer",
			plural(len(unnamedWrites), "query pattern", "query patterns")), firstN(unnamedWrites, 8))
		c.withPatterns(unnamedWritesP)
	}
	for _, r := range pr.Reasons {
		if unroutable[r.Reason] {
			c.add(Blocker, "profile_direct_unroutable", r.Reason, fmt.Sprintf("%s (%d executions) cannot go to the organization's database: %s", plural(r.Patterns, "query pattern", "query patterns"), r.Calls, reasonOf(r.Reason)), r.Examples)
			c.withPatterns(r.pats)
		}
	}
	c.directTxRefused()
	if len(c.toCloud) > 0 { // the coordinator's decision of 25.10: a finding of the profile, not a blocker of enable or move-org
		c.add(Blocker, "profile_direct_fk_cloud", "", "FKs from LokaDB tables to cloud tables: profile direct refuses writes of these tables (their FK is checked by a trigger of the cloud, and an organization on direct has no way through it); keep these organizations on fdw or fdw+direct, or drop the FK in favour of a check of the application", c.toCloud)
	}
	refs, err := c.cloudRefs()
	if err != nil {
		return err
	}
	if len(refs) > 0 {
		c.add(Info, "profile_direct_refs", "", "foreign keys of cloud tables onto LokaDB rows: on the direct path the proxy keeps the refs tables in the cloud up to date itself", refs)
	}
	return nil
}

// profileTransactions runs the recorded transaction shapes through the fdw+direct path
// choice and returns the fingerprints of statements that ran after lokadb.tenant.
func (c *checker) profileTransactions(pr *ProfileReport) map[string]bool {
	named := map[string]bool{}
	if len(c.txs) == 0 {
		return named
	}
	tx := &ProfileTx{}
	pr.Transactions = tx
	forms := map[string]*DirectForm{}
	for _, t := range c.txs {
		state := "" // "", direct, cloud, fdw, two
		isNamed, lokadb, allDirect, afterCloud, two, fdwAfter := false, false, true, false, false, false
		var first *DirectForm // the first statement but lokadb.tenant (and transaction statements)
		for _, s := range t.Statements {
			sql := analyzable(s)
			if tenantSet(sql) {
				isNamed, _ = tenantNamed(sql)
				continue
			}
			if first == nil && !txControl(sql) {
				first = &DirectForm{Named: isNamed, Example: s}
				if fp, err := pg.Fingerprint(s); err == nil {
					first.Fingerprint = fp
				}
			}
			v := proxy.DirectIn(sql, c.meta, isNamed, c.directPath())
			if isNamed && len(v.Lokadb) > 0 {
				if fp, err := pg.Fingerprint(sql); err == nil {
					named[fp] = true
				}
			}
			switch {
			case v.Reason == proxy.DirectNoTables || v.Reason == proxy.DirectDDL || v.Reason == proxy.DirectRefused:
			case v.Reason == proxy.DirectCloudOnly:
				switch state {
				case "":
					state = "cloud"
				case "direct":
					state, two = "two", true
				}
			case v.OK && proxy.DirectTxTakesIn(sql, c.meta, isNamed, c.directPath()): // the forms the proxy takes (8a, 8b, 8b.2)
				lokadb = true
				switch state {
				case "", "direct":
					state = "direct"
				case "cloud", "fdw":
					afterCloud, allDirect = true, false
				}
			default: // a LokaDB statement that needs FDW
				lokadb, allDirect = true, false
				switch state {
				case "", "cloud":
					state = "fdw"
				case "direct", "two":
					fdwAfter = true
				}
			}
		}
		if !lokadb {
			continue
		}
		tx.Shapes++
		tx.Count += t.Count
		if allDirect && state == "direct" {
			tx.Direct++
			tx.DirectCount += t.Count
		}
		if afterCloud {
			tx.AfterCloud++
		}
		if two {
			tx.TwoConnections++
			c.twoShapes = append(c.twoShapes, txShapeOf(t))
		}
		if fdwAfter {
			tx.FDWAfterDirect++
			c.fdwAfterShapes = append(c.fdwAfterShapes, txShapeOf(t))
		}
		if isNamed {
			tx.Named++
		}
		// the proxy goes direct only when the first statement does; it learns a form by it
		if first != nil && first.Fingerprint != "" && (allDirect && (state == "direct" || state == "two") || fdwAfter) {
			key := first.Fingerprint + fmt.Sprint(first.Named)
			if f := forms[key]; f == nil {
				first.Direct = !fdwAfter
				forms[key] = first
			} else if fdwAfter {
				f.Direct = false // a branch of the same form needs the cloud: the cloud's way
			}
		}
	}
	keys := make([]string, 0, len(forms))
	for k := range forms {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		pr.Forms = append(pr.Forms, *forms[k])
	}
	return named
}

// withPatterns gives the finding just added the complete list of its patterns (its examples are cut).
func (c *checker) withPatterns(pats []*profilePattern) {
	f := &c.rep.Findings[len(c.rep.Findings)-1]
	for _, g := range pats {
		fp := g.fp
		if !g.fpOK {
			fp = ""
		}
		f.Patterns = append(f.Patterns, Pattern{Fingerprint: fp, SQL: g.sql, Calls: g.calls, Write: g.writes, Tables: g.v.Lokadb})
	}
}

// tenantSet: SET [LOCAL] lokadb.tenant (the cooperative mode, section 6) or its set_config.
// txControl: sql is a transaction statement (BEGIN, SAVEPOINT …): a form is known by what follows.
func txControl(sql string) bool {
	tree, err := pg.Parse(sql)
	return err == nil && len(tree.Stmts) == 1 && tree.Stmts[0].Stmt.GetTransactionStmt() != nil
}

// DirectFormsSQL: the forms of the report as SQL for psql against the cloud database:
// direct ones into lokadb.direct_learnt, the others into lokadb.direct_cloud_first (org-profile
// creates both tables), then NOTIFY lokadb_meta so running proxies read them.
func DirectFormsSQL(forms []DirectForm) string {
	var b strings.Builder
	b.WriteString("-- lokadb check -profile fdw+direct: transaction forms by their first statement (tasks/direct-path.md, section 4)\n")
	b.WriteString("BEGIN;\n")
	for _, f := range forms {
		ex := strings.Join(strings.Fields(f.Example), " ")
		if len(ex) > 120 {
			ex = ex[:120] + "…"
		}
		// a line comment: the example has no line breaks left, and a /* of the application's text
		// would open a nested block comment to the end of the file (block 9's review)
		if f.Direct {
			fmt.Fprintf(&b, "INSERT INTO lokadb.direct_learnt (fingerprint, named) VALUES ('%s', %v) ON CONFLICT DO NOTHING; -- %s\n", f.Fingerprint, f.Named, ex)
		} else {
			fmt.Fprintf(&b, "INSERT INTO lokadb.direct_cloud_first (fingerprint, reason) VALUES ('%s', 'check -profile: goes direct first, then needs the cloud') ON CONFLICT DO NOTHING; -- %s\n", f.Fingerprint, ex)
		}
	}
	b.WriteString("COMMIT;\nNOTIFY lokadb_meta;\n")
	return b.String()
}

func tenantSet(sql string) bool {
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return false
	}
	if vs := tree.Stmts[0].Stmt.GetVariableSetStmt(); vs != nil {
		return vs.Name == "lokadb.tenant" && vs.Kind == pg.VariableSetKind_VAR_SET_VALUE
	}
	return strings.Contains(sql, "set_config('lokadb.tenant'")
}

// tenantNamed: what a recorded statement that sets lokadb.tenant (tenantSet) names, as the
// proxy's recording keeps it (proxy/record_tenant.go): an organization ('<organization>', or
// a constant of a workload written by hand), or every organization ('*'). A parameter ($1: a
// recording of before 26.10, where '*' and an organization were one, or pg_stat_statements)
// names neither: not known, the statements after it are judged by their own conditions.
func tenantNamed(sql string) (named, all bool) {
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return false, false
	}
	var v *pg.Node
	if vs := tree.Stmts[0].Stmt.GetVariableSetStmt(); vs != nil && len(vs.Args) == 1 {
		v = vs.Args[0]
	} else if sel := tree.Stmts[0].Stmt.GetSelectStmt(); sel != nil && len(sel.TargetList) == 1 {
		if fc := sel.TargetList[0].GetResTarget().GetVal().GetFuncCall(); fc != nil && len(fc.Args) >= 2 {
			v = fc.Args[1]
		}
	}
	if v == nil {
		return false, false
	}
	if tc := v.GetTypeCast(); tc != nil {
		v = tc.Arg
	}
	c := v.GetAConst()
	switch {
	case c == nil: // a parameter
		return false, false
	case c.GetSval() != nil && c.GetSval().Sval == proxy.TenantAll:
		return false, true
	}
	return true, false
}

// classifyNamed is classify with the cooperative mode: a statement the application ran only
// after naming the organization (namedOnly) is routed, the proxy takes that organization.
func (c *checker) classifyNamed(sql string, named map[string]bool) (kind, string) {
	k, note := c.classify(sql)
	if k == kFanout && named[sql] {
		return kRouted, note
	}
	return k, note
}

// namedOnly: the statements that every recorded transaction shape runs after naming the
// organization (tenantSet): the proxy takes that organization, so they are routed whatever
// they name themselves. A statement seen once without it is not. A shape that names '*' (a
// statement across organizations, which the fanout policy still judges) does not count; the
// recording keeps the constant only when the proxy records it as is.
func namedOnly(txs []*proxy.RecordedTx) map[string]bool {
	seen := map[string]byte{} // 1: after a name, 2: without one
	for _, tx := range txs {
		named := false
		for _, s := range tx.Statements {
			if tenantSet(analyzable(s)) {
				named, _ = tenantNamed(analyzable(s))
				continue
			}
			if named {
				seen[s] |= 1
			} else {
				seen[s] |= 2
			}
		}
	}
	out := map[string]bool{}
	for s, v := range seen {
		if v == 1 {
			out[s] = true
		}
	}
	return out
}

// tenantSessionSets warns of lokadb.tenant named for the whole session in the recording (SET
// without LOCAL, set_config(…, false)): behind a pooler in transaction mode clients share the
// proxy's session, and the value reaches the next client (tenant.local_only refuses it).
func (c *checker) tenantSessionSets(txs []*proxy.RecordedTx) {
	stmts := map[string]bool{}
	var calls int64
	for _, tx := range txs {
		for _, s := range tx.Statements {
			if tenantSessionSet(analyzable(s)) {
				stmts[s] = true
				calls += tx.Count
			}
		}
	}
	if len(stmts) == 0 {
		return
	}
	ex := make([]string, 0, len(stmts))
	for s := range stmts {
		ex = append(ex, s)
	}
	sort.Strings(ex)
	if len(ex) > 3 {
		ex = ex[:3]
	}
	var pats []Pattern // every statement, each a -ci item (review 3, M3)
	for _, s := range keys(stmts) {
		fp, err := pg.Fingerprint(analyzable(s))
		if err != nil {
			fp = ""
		}
		pats = append(pats, Pattern{Fingerprint: fp, SQL: s})
	}
	c.add(Warning, "tenant_session_set", "", fmt.Sprintf("%d statement(s) in %d transaction(s) name the organization for the whole session (SET lokadb.tenant without LOCAL, set_config(…, false)): behind a pooler in transaction mode the value reaches the next client of the same server connection. Name it in every transaction (SET LOCAL, set_config(…, true)): the proxy refuses such a SET unless \"tenant\": {\"local_only\": false} is in its config, which is safe only without a pooler before it or with one in session mode", len(stmts), calls), ex)
	c.rep.Findings[len(c.rep.Findings)-1].Patterns = pats
}

// tenantSessionSet: sql names lokadb.tenant for the session (not RESET, not SET LOCAL).
func tenantSessionSet(sql string) bool {
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return false
	}
	n := tree.Stmts[0].Stmt
	if vs := n.GetVariableSetStmt(); vs != nil {
		return strings.EqualFold(vs.Name, proxy.TenantSetting) && vs.Kind == pg.VariableSetKind_VAR_SET_VALUE && !vs.IsLocal
	}
	sel := n.GetSelectStmt()
	if sel == nil || len(sel.TargetList) != 1 {
		return false
	}
	fc := sel.TargetList[0].GetResTarget().GetVal().GetFuncCall()
	if fc == nil || len(fc.Args) != 3 || fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval() != "set_config" ||
		!strings.EqualFold(fc.Args[0].GetAConst().GetSval().GetSval(), proxy.TenantSetting) {
		return false
	}
	b := fc.Args[2].GetAConst().GetBoolval()
	return b == nil || !b.Boolval // is_local not a constant true: the proxy refuses it under local_only
}

// orgProfile: the organization's profile, "" when it is not in its own database.
func (c *checker) orgProfile(org int64) (string, error) {
	r, err := c.rows(`SELECT (to_regclass('lokadb.orgs') IS NOT NULL)::text,
		EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass('lokadb.orgs') AND attname = 'profile' AND NOT attisdropped)::text`)
	if err != nil || r[0][0] != "true" {
		return "", err
	}
	col := "NULL::text"
	if r[0][1] == "true" {
		col = "profile"
	}
	if r, err = c.rows(`SELECT coalesce(`+col+`, 'fdw') FROM lokadb.orgs WHERE org = $1`, org); err != nil || len(r) == 0 {
		return "", err
	}
	return r[0][0], nil
}

// insertsWithoutID: the statement inserts into table t without its id, and the id comes
// from a sequence (a DEFAULT nextval or an identity column) that lives in the cloud.
func (c *checker) insertsWithoutID(sql, t string, seqID map[string]bool) bool {
	tree, err := pg.Parse(analyzable(sql))
	if err != nil {
		return false
	}
	missing := false
	for _, raw := range tree.Stmts {
		ins := raw.Stmt.GetInsertStmt()
		if ins == nil || ins.Relation == nil || ins.Relation.Relname != t {
			continue
		}
		has := false
		for _, col := range ins.Cols {
			has = has || col.GetResTarget().GetName() == "id"
		}
		missing = missing || !has
	}
	if !missing {
		return false
	}
	if v, ok := seqID[t]; ok {
		return v
	}
	r, err := c.rows(`SELECT (a.attidentity <> '' OR coalesce(pg_get_expr(d.adbin, d.adrelid), '') LIKE '%nextval(%')::text
		FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = to_regclass('public.' || quote_ident($1)) AND a.attname = 'id'`, t)
	seqID[t] = err == nil && len(r) == 1 && r[0][0] == "true"
	return seqID[t]
}

// cloudRefs: foreign keys of cloud tables that point at the refs tables of LokaDB tables.
func (c *checker) cloudRefs() ([]string, error) {
	if c.meta == nil {
		return nil, nil
	}
	var refs []string
	for _, t := range sortedTables(c.meta) {
		if tb := c.meta.Tables[t]; tb.Refs != "" {
			refs = append(refs, tb.Refs)
		}
	}
	if len(refs) == 0 {
		return nil, nil
	}
	r, err := c.rows(`SELECT conrelid::regclass::text || '.' || conname || ' -> ' || confrelid::regclass::text FROM pg_constraint
		WHERE contype = 'f' AND confrelid::regclass::text = ANY($1) ORDER BY 1`, refs)
	if err != nil {
		return nil, err
	}
	return flat(r), nil
}

// unroutable: reasons a statement cannot go direct even when the application names the organization.
var unroutable = map[string]bool{proxy.DirectFunction: true, proxy.DirectSequence: true, proxy.DirectRefused: true}

var reasonText = map[string]string{
	proxy.DirectCloudTable: "they also read cloud tables (a JOIN or a subquery), which the organization's database does not have",
	proxy.DirectNoOrg:      "no LokaDB table of theirs names the organization (with the cooperative mode, SET LOCAL lokadb.tenant, they would go direct)",
	proxy.DirectPartialOrg: "the organization is named on some of their LokaDB tables but not on every one; the direct path needs it on each (or lokadb.tenant), since a wrong database answers silently",
	proxy.DirectForm:       "a form the analyzer cannot confine to one organization (MERGE, INSERT ... SELECT); with lokadb.tenant they would go direct",
	proxy.DirectFunction:   "they call a function the direct path does not take: one outside pg_catalog may read cloud tables, and some of pg_catalog answer otherwise in the organization's database (current_user, current_database(), pg_notify, advisory locks, set_config, ::regclass, a type of the application's)",
	proxy.DirectSequence:   "they call a sequence function; sequences live in the cloud",
	proxy.DirectDDL:        "DDL runs in the cloud and is replayed in the organizations' databases",
	proxy.DirectRefused:    "the proxy refuses them on every path",
}

func byValuesNote(n int) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("; %s of them take the organization from a list of values (IN, ANY, several rows) and go direct only when all the values belong to one organization", plural(n, "pattern", "patterns"))
}

func percent(a, b int64) string {
	if b == 0 {
		return "0%"
	}
	return strconv.FormatFloat(float64(a)*100/float64(b), 'f', 1, 64) + "%"
}

func sortedKeysInt(m map[string]int64) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedReasons(m map[string]*ProfileReason) []*ProfileReason {
	var out []*ProfileReason
	for _, r := range m {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Calls > out[j].Calls || out[i].Calls == out[j].Calls && out[i].Reason < out[j].Reason
	})
	return out
}

func firstN(l []string, n int) []string {
	if len(l) <= n {
		return l
	}
	return append(l[:n:n], fmt.Sprintf("... and %d more", len(l)-n))
}

// idleSessionTimeout (profiles fdw+direct and direct; review of the protocol, PR-5): a direct
// read holds the organization's pause in the cloud's session, idle outside a transaction while
// the organization's database runs the statement. The cloud's idle_session_timeout ends that
// session, and the client's connection with it, when the statement runs longer. (The cloud's
// transactions of the direct path set idle_in_transaction_session_timeout = 0 for themselves.)
func (c *checker) idleSessionTimeout() {
	if c.tx == nil { // a workload judged without a database (tests)
		return
	}
	r, err := c.rows(`SELECT setting FROM pg_settings WHERE name = 'idle_session_timeout'`)
	if err != nil || len(r) != 1 || r[0][0] == "0" || r[0][0] == "" {
		return
	}
	c.add(Warning, "profile_idle_session_timeout", "", fmt.Sprintf("the cloud's idle_session_timeout is %s ms: on the direct path the cloud's session waits idle, holding the organization's pause, while the organization's database runs a read; a read that runs longer ends the session and the client's connection with it. Set it to 0 for the application's role (ALTER ROLE ... SET idle_session_timeout = 0) or above the longest read", r[0][0]), nil)
}

// formText: the reason of a form the proxy's direct path does not take (proxy.DirectRefusal).
func formText(r string) string {
	if t := formTexts[r]; t != "" {
		return t
	}
	return "a form the direct path does not take (" + r + ")"
}

// acrossOrgs: the reason of a statement run after SET LOCAL lokadb.tenant = '*'.
const acrossOrgs = "across_organizations"

var formTexts = map[string]string{
	acrossOrgs:                  "they run after SET LOCAL lokadb.tenant = '*', across organizations: profile direct has no path across organizations' databases",
	proxy.ShapeTwoInserts:       "they insert into two LokaDB tables (a data-modifying CTE)",
	proxy.ShapeStatements:       "several statements in one query string",
	proxy.ShapeAmbiguousName:    "a bare table name of several schemas, the search path unknown",
	proxy.ShapeStatementForm:    "a form the direct path does not write (SELECT INTO, an upsert or INSERT … SELECT it cannot rewrite)",
	proxy.ShapeModifyingCTE:     "a data-modifying CTE",
	proxy.ShapeMerge:            "MERGE",
	proxy.ShapeRowTrigger:       "an UPDATE of a table with a row trigger of the cloud (an FK onto a cloud table among them), which would not fire in the organization's database",
	proxy.ShapeKeyUpdate:        "an UPDATE of the key or the organization column (the cloud's refs tables follow the key)",
	proxy.ShapeOperator:         "an operator of an extension in the cloud, which the organization's database does not have under that schema",
	proxy.ShapeUserType:         "a column of a type of the application's (an enum, a domain): its OID differs in each database",
	proxy.ShapeStatementTrigger: "a table with a statement trigger of the cloud, which would not fire in the organization's database",
	proxy.ShapeChangesLookups:   "a change of a column the proxy finds organizations by",
}

// acrossOrgs: the fingerprints of the statements recorded after SET LOCAL lokadb.tenant = '*'
// (a statement across organizations): profile direct refuses those on LokaDB tables
// (direct_only.go), whatever they name.
func (c *checker) acrossOrgs() map[string]bool {
	out := map[string]bool{}
	for _, t := range c.txs {
		star := false
		for _, s := range t.Statements {
			sql := analyzable(s)
			if tenantSet(sql) {
				_, star = tenantNamed(sql)
				continue
			}
			if star {
				if fp, err := pg.Fingerprint(sql); err == nil {
					out[fp] = true
				}
			}
		}
	}
	return out
}

// directTxRefused (profile direct): the transaction shapes with a statement on LokaDB tables the
// proxy's direct transaction does not take (proxy.DirectTxTakesIn): on direct the proxy refuses
// it inside the transaction, which is then aborted.
func (c *checker) directTxRefused() {
	var shapes []string
	var txs []TxShape
	for _, t := range c.txs {
		named := false
		for _, s := range t.Statements {
			sql := analyzable(s)
			if tenantSet(sql) {
				named, _ = tenantNamed(sql)
				continue
			}
			if txControl(sql) {
				continue
			}
			v := proxy.DirectIn(sql, c.meta, named, c.directPath())
			if len(v.Lokadb) == 0 || !v.OK || proxy.DirectTxTakesIn(sql, c.meta, named, c.directPath()) {
				continue
			}
			why := proxy.DirectRefusal(sql, c.meta, named, c.directPath())
			shapes = append(shapes, fmt.Sprintf("x%d  %s  (%s)", t.Count, short(s), formText(why)))
			txs = append(txs, txShapeOf(t))
			break
		}
	}
	if len(shapes) > 0 {
		c.add(Blocker, "profile_direct_tx_refused", "", fmt.Sprintf("%s run a statement on LokaDB tables the direct path does not take: on profile direct the proxy refuses it inside the transaction, which is then aborted",
			plural(len(shapes), "transaction shape", "transaction shapes")), firstN(shapes, 8))
		c.rep.Findings[len(c.rep.Findings)-1].Transactions = txs // every shape, each a -ci item (review 3, M3)
	}
}

// reasonOf: the text of a reason a pattern does not go direct: Direct's, or a form's.
func reasonOf(r string) string {
	if t := reasonText[r]; t != "" {
		return t
	}
	return formText(r)
}

// orgsOnFDW (after enable): the moved organizations on profile fdw (decision of 26.10, block
// 14's measurements): every statement of theirs costs admin.FDWCost; fdw+direct, move-org's
// default when the proxy has the direct path's credentials, about 1.5 exchanges.
func (c *checker) orgsOnFDW() {
	r, err := c.rows(`SELECT (to_regclass('lokadb.orgs') IS NOT NULL)::text`)
	if err != nil || len(r) == 0 || r[0][0] != "true" {
		return
	}
	q := `SELECT org::text FROM lokadb.orgs ORDER BY org`
	if r, err = c.rows(`SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = 'lokadb.orgs'::regclass AND attname = 'profile' AND NOT attisdropped)::text`); err == nil && len(r) == 1 && r[0][0] == "true" {
		q = `SELECT org::text FROM lokadb.orgs WHERE coalesce(profile, 'fdw') = 'fdw' ORDER BY org`
	}
	orgs, err := c.rows(q)
	if err != nil || len(orgs) == 0 {
		return
	}
	var list []string
	for _, o := range orgs {
		list = append(list, "organization "+o[0])
	}
	c.add(Info, "org_profile_fdw", "", fmt.Sprintf("%s moved to their own databases are on profile fdw: it costs %s. Give the proxy the direct path's credentials of each (customers_app, or lokadb org-credentials -kind direct) and run lokadb org-profile -org N fdw+direct (move-org sets it itself when given -proxy-config with them)",
		plural(len(orgs), "organization", "organizations"), admin.FDWCost), firstN(list, 20))
}
