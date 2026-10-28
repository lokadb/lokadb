package check

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// kind is what the proxy would do with a statement.
type kind int

const (
	kCloud       kind = iota // touches no LokaDB table
	kRouted                  // confined to the organization it names
	kFanout                  // LokaDB tables without the organization: run in every organization's database
	kRefused                 // ... and the fanout policy refuses it
	kAllowed                 // ... and the fanout policy allows it explicitly
	kUpsert                  // rewritten upsert
	kDDL                     // replayed in the organizations' databases
	kDDLRejected             // refused after enable
	kForbidden               // LokaDB's own objects or FDW commands: refused after enable
	kParse                   // not understood
)

type group struct {
	kind        kind
	sql         string // the most executed text of the group
	fingerprint string // its pg_query fingerprint, "" when it does not parse
	top         int64
	count       int64
	writes      bool
	note        string
	from        origin
}

// origin: where the statements of a pattern were seen, and the roles that sent them.
type origin struct{ roles, sources map[string]bool }

func (o *origin) add(role, source string) {
	if o.sources == nil {
		o.roles, o.sources = map[string]bool{}, map[string]bool{}
	}
	if role != "" {
		o.roles[role] = true
	}
	o.sources[source] = true
}

func (o *origin) merge(p origin) {
	for r := range p.roles {
		o.add(r, "")
	}
	for s := range p.sources {
		o.add("", s)
	}
	delete(o.sources, "")
}

func (c *checker) workload() error {
	pats := map[string]int64{} // normalized SQL -> executions
	from := map[string]*origin{}
	seen := func(sql, role, source string) {
		if from[sql] == nil {
			from[sql] = &origin{}
		}
		from[sql].add(role, source)
	}
	var txs []*proxy.RecordedTx

	cfg, src := proxy.Config{}, "default, no -proxy-config"
	if c.o.Proxy != nil {
		cfg, src = *c.o.Proxy, c.o.ProxyFile
	}
	fan, err := cfg.FanoutFor(c.rep.Database)
	if err != nil {
		return err
	}
	c.fan = fan
	c.rep.FanoutPolicy = fmt.Sprintf("%s, %s (%s)", fan.Mode, plural(fan.Allowed, "statement allowed", "statements allowed"), src)

	ext, err := c.rows(`SELECT n.nspname::text FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'pg_stat_statements'`)
	if err != nil {
		return err
	}
	if len(ext) == 0 {
		c.add(Info, "no_pg_stat_statements", "", "pg_stat_statements is not installed in this database: its statistics are not used", nil)
	} else {
		sp, err := c.tx.Begin(c.ctx) // a savepoint: the view errors out when the library is not preloaded
		if err != nil {
			return err
		}
		rows, err := rowsOf(c.ctx, sp, fmt.Sprintf(`SELECT s.query, s.calls::text, coalesce(r.rolname::text, ''), current_user::text
			FROM %s.pg_stat_statements s LEFT JOIN pg_roles r ON r.oid = s.userid
			WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND s.toplevel`, q(ext[0][0])))
		if err != nil {
			_ = sp.Rollback(c.ctx)
			c.add(Info, "pg_stat_statements_unreadable", "", "pg_stat_statements is installed but cannot be read: "+firstLine(err.Error()), nil)
		} else {
			_ = sp.Commit(c.ctx)
			// By default the statements of the role running the check are left out: that is
			// normally an admin role whose own enable/move-org statements are not the application.
			roles := map[string]bool{}
			for _, r := range c.o.AppRoles {
				roles[r] = true
			}
			hidden, kept := 0, 0
			skipped := map[string]int{}
			self := ""
			for _, r := range rows {
				query, role := r[0], r[2]
				self = r[3]
				switch {
				case query == "<insufficient privilege>":
					hidden++
				case strings.HasPrefix(query, strings.TrimSpace(ownMark)), c.notApplication(query):
				case len(roles) > 0 && !roles[role], len(roles) == 0 && role == self:
					skipped[role]++
				default:
					n, _ := strconv.ParseInt(r[1], 10, 64)
					pats[query] = max(pats[query], n)
					seen(query, role, "pg_stat_statements")
					kept++
					if role != "" {
						if c.seenRoles == nil {
							c.seenRoles = map[string]bool{}
						}
						c.seenRoles[role] = true
					}
				}
			}
			src := fmt.Sprintf("pg_stat_statements: %d statements", kept)
			if len(skipped) > 0 {
				var l []string
				for role, n := range skipped {
					l = append(l, fmt.Sprintf("%d of %s", n, role))
				}
				sort.Strings(l)
				src += fmt.Sprintf(" (left out by role: %s; -app-role chooses the roles)", strings.Join(l, ", "))
			}
			c.rep.Sources = append(c.rep.Sources, src)
			if kept == 0 && skipped[self] > 0 && len(c.o.AppRoles) == 0 && len(c.o.Workload) == 0 {
				if err := c.ownRoleOnly(self, skipped[self]); err != nil {
					return err
				}
			}
			if hidden > 0 {
				c.add(Warning, "pg_stat_statements_hidden", "", fmt.Sprintf("%d statements of other roles are hidden from this role: grant it pg_read_all_stats", hidden), nil)
			}
		}
	}

	for _, path := range c.o.Workload {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var w proxy.Workload
		if err := json.Unmarshal(b, &w); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		wd := w.Databases[c.rep.Database]
		if wd == nil {
			c.add(Warning, "workload_empty", path, fmt.Sprintf("the recording %s has nothing for database %s", path, c.rep.Database), nil)
			continue
		}
		for _, s := range wd.Statements {
			pats[s.SQL] = max(pats[s.SQL], s.Count)
			seen(s.SQL, "", "recording:"+path)
		}
		txs = append(txs, wd.Transactions...)
		c.rep.Sources = append(c.rep.Sources, fmt.Sprintf("proxy recording %s: %d statements, %d transaction shapes, updated %s",
			path, len(wd.Statements), len(wd.Transactions), w.Updated.Format(time.RFC3339)))
		if wd.Dropped > 0 {
			c.add(Info, "recording_truncated", path, fmt.Sprintf("the recording hit its limits and left out %d statements or transaction shapes", wd.Dropped), nil)
		}
	}
	c.viewsAndFuncs()
	c.readPath()
	c.relations()
	c.unlinked()
	if len(c.rep.Sources) == 0 {
		c.add(Warning, "no_workload", "", "no queries were checked: install pg_stat_statements, or record traffic through the proxy (proxy -record FILE) and pass -workload FILE", nil)
		return nil
	}
	c.pats, c.txs = pats, txs

	// Statements that differ only in constants or list lengths share a fingerprint; a group
	// is a fingerprint with one verdict: a fingerprint keeps less than the analyzer looks at
	// (`id = $1` and `id = ANY($1)` share one).
	groups := map[string]*group{}
	named := namedOnly(txs) // block 10: statements the application ran only after naming the organization (profile.go)
	c.tenantSessionSets(txs)
	for sql, n := range pats {
		k, note := c.classifyNamed(sql, named)
		if k == kFanout {
			k = c.fanoutKind(sql)
		}
		fp, err := pg.Fingerprint(analyzable(sql))
		key := strconv.Itoa(int(k)) + " " + fp
		if err != nil {
			fp, key = "", strconv.Itoa(int(k))+" "+sql
		}
		g := groups[key]
		if g == nil {
			g = &group{kind: k, note: note, top: -1, fingerprint: fp}
			groups[key] = g
		}
		g.count += n
		if o := from[sql]; o != nil {
			g.from.merge(*o)
		}
		if n > g.top {
			g.sql, g.top = sql, n
			g.writes = len(c.writes(sql).all()) > 0
		}
	}
	byKind := map[kind][]*group{}
	for _, g := range groups {
		byKind[g.kind] = append(byKind[g.kind], g)
		if g.kind != kCloud {
			c.rep.Summary.QueryPatterns++
		}
	}
	for _, gs := range byKind {
		sort.Slice(gs, func(i, j int) bool {
			return gs[i].count > gs[j].count || gs[i].count == gs[j].count && gs[i].sql < gs[j].sql
		})
	}
	// report: one finding per kind; one/many start the message ("1 pattern touches", "3 patterns touch").
	report := func(k kind, sev Severity, code, one, many, rest string) {
		gs := byKind[k]
		if len(gs) == 0 {
			return
		}
		var ex []string
		var pats []Pattern
		for _, g := range gs {
			pats = append(pats, Pattern{Fingerprint: g.fingerprint, SQL: g.sql, Calls: g.count, Write: g.writes, Tables: c.lokadbTables(g.sql),
				Roles: keys(g.from.roles), Sources: keys(g.from.sources), Note: g.note, Gaps: c.gapsOf(g.kind, g.sql)})
		}
		for i, g := range gs {
			if i == 8 {
				ex = append(ex, fmt.Sprintf("... and %d more", len(gs)-i))
				break
			}
			prefix := ""
			if (k == kFanout || k == kRefused || k == kAllowed) && g.writes {
				prefix = "write: "
			}
			if g.note != "" && (k == kDDLRejected || k == kForbidden) {
				prefix = g.note + ": "
			} else if g.note != "" {
				prefix += g.note + ": "
			}
			ex = append(ex, fmt.Sprintf("x%d  %s%s", g.count, prefix, short(g.sql)))
		}
		c.addFinding(Finding{Severity: sev, Code: code, Message: plural(len(gs), one, many) + rest, Examples: ex, Patterns: pats})
	}
	c.rep.Summary.UnroutedPatterns = len(byKind[kFanout]) + len(byKind[kRefused]) + len(byKind[kAllowed])
	c.rep.Summary.RefusedPatterns = len(byKind[kRefused])
	report(kForbidden, Blocker, "query_forbidden", "statement is", "statements are",
		" refused by the proxy after enable whatever its fanout policy: they address LokaDB's own objects (an organization's partition, a refs table, schema lokadb) or are foreign data wrapper commands. The application has to use the LokaDB table itself")
	report(kRefused, Blocker, "query_refused", "query pattern touches", "query patterns touch",
		fmt.Sprintf(" LokaDB tables without naming the organization, and after enable the proxy refuses them (fanout policy %q of database %s): the application gets an error. Restrict them by the organization, or review them and add them to \"allow\" in the proxy config",
			c.fan.Mode, c.rep.Database))
	report(kFanout, Warning, "query_without_organization", "query pattern touches", "query patterns touch",
		fmt.Sprintf(" LokaDB tables without naming the organization; the fanout policy %q lets them run in every organization's database, so they are slower and fail while any of those databases is unreachable",
			c.fan.Mode))
	report(kAllowed, Info, "query_fanout_allowed", "query pattern without the organization is", "query patterns without the organization are",
		" on the allow list of the proxy config: they run in every organization's database")
	report(kDDLRejected, Warning, "ddl_rejected", "DDL pattern on LokaDB tables is", "DDL patterns on LokaDB tables are",
		" refused by the proxy after enable. Past migrations can be ignored; new migrations of this kind have to change")
	report(kParse, Warning, "parse_error", "statement is", "statements are", " not understood by the Postgres parser the proxy uses (PostgreSQL 17's grammar) and name no LokaDB table: the proxy sends them to the cloud as they are. A statement that names one is judged by the fanout policy like a query without the organization (query_refused, query_without_organization)")
	report(kUpsert, Info, "upsert_rewritten", "upsert pattern on LokaDB tables is", "upsert patterns on LokaDB tables are",
		" rewritten by the proxy; under concurrent inserts of the same key one of them can fail with a unique violation instead of updating")
	report(kDDL, Info, "ddl_replayed", "DDL pattern on LokaDB tables is", "DDL patterns on LokaDB tables are", " replayed in each organization's database before COMMIT")
	c.copyFromCost(byKind[kRouted])
	for _, g := range groups {
		c.groups = append(c.groups, g)
	}

	c.transactions(txs)
	c.ddlWriteTransactions(txs) // block 7: replay.go
	return nil
}

// ownRoleOnly: every statement pg_stat_statements has is of the role running the check, and
// the check leaves those out as the admin's own (enable, move-org): no query was checked. In a
// small setup the application often uses the same role (postgres); a superuser or a BYPASSRLS
// role is one the proxy does not let in (review 2, R3).
func (c *checker) ownRoleOnly(self string, n int) error {
	r, err := c.rows(`SELECT CASE WHEN rolsuper THEN 'a superuser' WHEN rolbypassrls THEN 'a BYPASSRLS role' ELSE '' END FROM pg_roles WHERE rolname = $1`, self)
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("no query was checked: all %s in pg_stat_statements are of %s, the role running the check, and are left out as its own (enable, move-org). If the application uses this role too, run the check with -app-role %s", plural(n, "statement", "statements"), self, self)
	if len(r) > 0 && r[0][0] != "" {
		msg += fmt.Sprintf("; but %s is %s, and the proxy does not let such a role in (row-level security does not apply to it): give the application a role of its own and connect it through the proxy", self, r[0][0])
	}
	c.add(Warning, "workload_own_role_only", self, msg, nil)
	return nil
}

// gapsOf: why the proxy finds no organization in a statement that names none.
func (c *checker) gapsOf(k kind, sql string) []proxy.Gap {
	if k != kFanout && k != kRefused && k != kAllowed {
		return nil
	}
	return c.analyze(analyzable(sql), c.meta).Gaps
}

// transactions finds transaction shapes that write cloud and LokaDB tables together.
func (c *checker) transactions(txs []*proxy.RecordedTx) {
	if len(c.o.Workload) == 0 {
		c.add(Warning, "transactions_not_checked", "", "transactions were not checked: pg_stat_statements has no transaction boundaries. Record traffic through the proxy (proxy -record FILE) and pass -workload FILE", nil)
		return
	}
	c.rep.Summary.TxPatternsChecked = true
	type mix struct {
		cloud, lokadb []string
		shapes        int
		count         int64
		example       []string
		stmts         []TxStatement
		top           int64
	}
	mixes := map[string]*mix{}
	for _, tx := range txs {
		var all writeSet
		var writing []string
		var stmts []TxStatement
		for _, s := range tx.Statements {
			w := c.writes(s)
			if len(w.all()) > 0 {
				writing = append(writing, short(s))
			}
			all.merge(w)
			fp, _ := pg.Fingerprint(analyzable(s))
			stmts = append(stmts, TxStatement{Fingerprint: fp, SQL: s, Write: len(w.all()) > 0, Tables: w.all()})
		}
		if len(all.lokadb) == 0 {
			continue
		}
		c.rep.Summary.TxPatterns++
		if len(all.cloud) == 0 {
			continue
		}
		cl, by := keys(all.cloud), keys(all.lokadb)
		key := strings.Join(cl, ",") + " | " + strings.Join(by, ",")
		m := mixes[key]
		if m == nil {
			m = &mix{cloud: cl, lokadb: by, top: -1}
			mixes[key] = m
		}
		m.shapes++
		m.count += tx.Count
		if tx.Count > m.top {
			m.top, m.example, m.stmts = tx.Count, writing, stmts
		}
	}
	var ms []*mix
	for _, m := range mixes {
		ms = append(ms, m)
		c.rep.Summary.MixedTxPatterns += m.shapes
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].count > ms[j].count })
	for _, m := range ms {
		c.addFinding(Finding{Severity: Change, Code: "mixed_write_transaction", Object: strings.Join(m.cloud, ", ") + " + " + strings.Join(m.lokadb, ", "),
			Subject:      &Subject{Kind: "tables", CloudTables: m.cloud, LokadbTables: m.lokadb},
			Transactions: []TxShape{{CloudTables: m.cloud, LokadbTables: m.lokadb, Shapes: m.shapes, Count: m.count, Statements: m.stmts}},
			Examples:     m.example, Message: fmt.Sprintf("%s cloud tables (%s) and LokaDB tables (%s) together, %s seen. Once the organization lives in its own database these are two commits without two-phase commit: its database commits first, and a failure in between leaves only that side written",
				plural(m.shapes, "transaction shape writes", "transaction shapes write"), strings.Join(m.cloud, ", "), strings.Join(m.lokadb, ", "), plural(int(m.count), "transaction", "transactions"))})
	}
}

var paramRe = regexp.MustCompile(`\$[0-9]+`)

// analyzable: pg_stat_statements (PostgreSQL 18) and pg_query replace constants with $n in
// utility statements too (`savepoint $1`, `SET x = $1`), where the parser refuses them.
// Such text is read with $n as a name instead; text that parses is returned as is.
func analyzable(sql string) string {
	if _, err := pg.Parse(sql); err == nil {
		return sql
	}
	return paramRe.ReplaceAllString(sql, `"$0"`)
}

var (
	orgPartRe   = regexp.MustCompile(`^(.+)__org[0-9]+$`) // an organization's foreign partition
	localPartRe = regexp.MustCompile(`^(.+)__o[0-9]+$`)   // an organization's local partition
)

// notApplication: statements pg_stat_statements counts under the application's role that
// the application never sent (see proxyMade and serverMade).
func (c *checker) notApplication(sql string) bool { return c.proxyMade(sql) || c.serverMade(sql) }

// serverMade: statements Postgres runs on its own. Foreign key checks (deferred ones run at
// COMMIT and count as top level) and the initial copy of an organization's partition that
// logical replication makes during move-org.
func (c *checker) serverMade(sql string) bool {
	s := strings.TrimSpace(sql)
	if strings.HasPrefix(s, "SELECT") && strings.Contains(s, "OPERATOR(pg_catalog.=)") && strings.Contains(s, "FOR KEY SHARE OF x") {
		return true
	}
	tree, err := pg.Parse(s)
	if err != nil || len(tree.Stmts) != 1 {
		return false
	}
	cp := tree.Stmts[0].Stmt.GetCopyStmt()
	if cp == nil || cp.IsFrom || c.meta == nil {
		return false
	}
	internal, other := 0, 0
	proxy.Walk(cp, func(m proto.Message) {
		rv, ok := m.(*pg.RangeVar)
		if !ok {
			return
		}
		if p := localPartRe.FindStringSubmatch(rv.Relname); rv.Schemaname == "lokadb" || p != nil && c.meta.Tables[p[1]] != nil {
			internal++
		} else {
			other++
		}
	})
	return internal > 0 && other == 0
}

// metaTables: what the proxy reads when a client connects (proxy.LoadMeta).
var metaTables = map[string]bool{"tables": true, "lookups": true}

// proxyMade: statements the proxy itself sends after enable on the application's connection,
// seen by pg_stat_statements under the application's role but not written by it: rewritten
// upserts, lokadb.org, the pause lock, unique indexes per partition, refs lookups, the table owner check,
// refusals (DO $lokadb$), the metadata read at connect, SELECT/UPDATE/DELETE aimed straight
// at an organization's foreign partition and a cloud organization's upsert in its local one. The proxy recording holds only what the client sent and
// needs no filter.
func (c *checker) proxyMade(sql string) bool {
	s := strings.TrimSpace(sql)
	switch {
	case strings.Contains(s, "__lokadb_ord"), strings.Contains(s, "'lokadb.org'"), strings.Contains(s, "lokadb.create_index"), strings.Contains(s, "lokadb.grant_sync("),
		strings.Contains(s, "(lokadb.org_lock("),            // the pause lock of a move
		strings.Contains(s, "FROM lokadb.replay_targets()"), // the DDL replay's targets (block 7, proxy.replayTargets)
		strings.Contains(s, "to_regprocedure('lokadb.replay_targets()')"),
		strings.Contains(s, "lock(hashtext("),                                                 // a rewritten upsert's lock (proxy.upsertLock)
		s == "SELECT org::text, schema FROM lokadb.orgs",                                      // the schemas of the organizations' tables (proxy.LoadMeta)
		s == "SELECT org::text, profile FROM lokadb.orgs WHERE profile IS NOT NULL",           // their routing profiles (proxy.LoadMeta)
		s == "SELECT org::text, db_id FROM lokadb.orgs WHERE db_id IS NOT NULL",               // their databases' ids (proxy.LoadMeta, the check of the database)
		strings.HasPrefix(s, "SELECT coalesce((SELECT profile FROM lokadb.orgs WHERE org = "), // the direct path's gate (fdw+direct)
		strings.HasPrefix(s, "SELECT current_setting($1)"),
		strings.Contains(s, "lokadb.direct_cloud_first"),                                                                     // the direct transactions' shapes (block 10, 8a.2)
		strings.Contains(s, "lokadb.direct_learnt"),                                                                          // the forms learnt ahead (block 10, 8a.2, -direct-forms)
		strings.Contains(s, "lokadb_k AS ("), strings.Contains(s, "lokadb_v(id"), strings.Contains(s, " AS lokadb_d WHERE "), // a direct write's refs rows (block 10, 8b)
		(strings.HasPrefix(s, "SELECT id::text, tenant::text FROM") || strings.HasPrefix(s, "SELECT id::text, coalesce(tenant::text, ")) && strings.Contains(s, "__refs"),
		strings.HasPrefix(s, "SELECT id::text, coalesce(") && strings.Contains(s, "WHERE id IN"), // a cloud table's lookup (routing.cloud_tables)
		strings.Contains(s, "pg_has_role(current_user, c.relowner"),
		strings.HasPrefix(s, "DO $lokadb$"):
		return true
	}
	tree, err := pg.Parse(s)
	if err != nil || len(tree.Stmts) != 1 {
		return false
	}
	if sel := tree.Stmts[0].Stmt.GetSelectStmt(); sel != nil {
		// a SELECT of one organization's table aimed at its foreign partition (block 2), alone
		// or joined with cloud tables
		if c.meta != nil {
			aimed := false
			var visit func(n *pg.Node)
			visit = func(n *pg.Node) {
				if rv := n.GetRangeVar(); rv != nil {
					if m := orgPartRe.FindStringSubmatch(rv.Relname); m != nil && c.meta.Tables[m[1]] != nil {
						aimed = true
					}
				} else if j := n.GetJoinExpr(); j != nil {
					visit(j.Larg)
					visit(j.Rarg)
				}
			}
			for _, it := range sel.FromClause {
				visit(it)
			}
			if aimed {
				return true
			}
		}
		// the metadata read: lokadb.tables / lokadb.lookups joined with the catalogs only
		meta, other := 0, 0
		proxy.Walk(sel, func(m proto.Message) {
			if rv, ok := m.(*pg.RangeVar); ok {
				switch {
				case rv.Schemaname == "lokadb" && metaTables[rv.Relname]:
					meta++
				case (rv.Schemaname == "" || rv.Schemaname == "pg_catalog") && strings.HasPrefix(rv.Relname, "pg_"):
				default:
					other++
				}
			}
		})
		return meta > 0 && other == 0
	}
	// a cloud organization's upsert, natively in its local partition (DEFAULT or
	// <t>__o<N>, block 2): the application never names a partition, the proxy does
	if ins := tree.Stmts[0].Stmt.GetInsertStmt(); ins != nil && ins.OnConflictClause != nil && c.meta != nil {
		name := ins.Relation.GetRelname()
		if m := localPartRe.FindStringSubmatch(name); m != nil && c.meta.Tables[m[1]] != nil {
			return true
		}
		for _, t := range c.meta.Tables {
			if t.Cloud == name {
				return true
			}
		}
		return false
	}
	var rv *pg.RangeVar
	if u := tree.Stmts[0].Stmt.GetUpdateStmt(); u != nil {
		rv = u.Relation
	} else if d := tree.Stmts[0].Stmt.GetDeleteStmt(); d != nil {
		rv = d.Relation
	}
	if rv == nil || c.meta == nil {
		return false
	}
	m := orgPartRe.FindStringSubmatch(rv.Relname)
	return m != nil && c.meta.Tables[m[1]] != nil
}

// fanoutKind: what the proxy's fanout policy does with a statement that names no organization.
func (c *checker) fanoutKind(sql string) kind {
	sql = analyzable(sql)
	switch c.fan.Decide(sql, c.analyze(sql, c.meta)) {
	case proxy.FanoutRefused:
		return kRefused
	case proxy.FanoutAllowed:
		return kAllowed
	}
	return kFanout
}

func (c *checker) classify(sql string) (kind, string) {
	sql = analyzable(sql)
	a := c.analyze(sql, c.meta)
	switch {
	case a.ParseError && a.Lokadb && a.Unhinted:
		// the proxy's parser (PostgreSQL 17's grammar) does not read it, but it names a LokaDB
		// table, a view or a function over one: the proxy takes it for a statement without the
		// organization and its fanout policy decides (proxy analyze.go unparsed; review 2, R1)
		return kFanout, "not understood by the proxy's parser"
	case a.ParseError:
		return kParse, ""
	case a.Forbidden != "" && !a.FanoutByParam: // the proxy checks it first too
		// (a set_config with a parameter for its name may set lokadb.fanout, and the proxy
		// refuses it; here every constant is a parameter, so it is not counted)
		return kForbidden, strings.TrimPrefix(a.Forbidden, "lokadb: ")
	case a.DDLError != "":
		return kDDLRejected, strings.TrimPrefix(a.DDLError, "lokadb: ")
	case len(a.DDL) > 0:
		return kDDL, ""
	case !a.Lokadb:
		return kCloud, ""
	case a.Unhinted:
		return kFanout, ""
	case a.Rewritten != "" && isCopy(sql): // COPY FROM loaded through a staging table (block 2): routed, not an upsert
		return kRouted, ""
	case a.Rewritten != "":
		return kUpsert, ""
	}
	return kRouted, ""
}

// isCopy: a COPY statement.
func isCopy(sql string) bool {
	tree, err := pg.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if raw.Stmt.GetCopyStmt() != nil {
			return true
		}
	}
	return false
}

// isCopyFrom: a COPY … FROM statement.
func isCopyFrom(sql string) bool {
	tree, err := pg.Parse(analyzable(sql))
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if cs := raw.Stmt.GetCopyStmt(); cs != nil && cs.IsFrom {
			return true
		}
	}
	return false
}

// copyFromCost: COPY FROM into a LokaDB table works (block 2) but costs far more than into a
// plain table, on the FDW path every row through the parent's triggers (Z3 of block 14's trial):
// a cloud organization's rows each maintain the refs table, a moved organization's are inserted
// one by one through postgres_fdw and each checks its FKs onto cloud tables in the cloud.
func (c *checker) copyFromCost(routed []*group) {
	var pats []Pattern
	var ex []string
	for _, g := range routed {
		if !isCopyFrom(g.sql) {
			continue
		}
		pats = append(pats, Pattern{Fingerprint: g.fingerprint, SQL: g.sql, Calls: g.count, Write: true, Tables: c.lokadbTables(g.sql),
			Roles: keys(g.from.roles), Sources: keys(g.from.sources), Note: g.note})
		if len(ex) < 8 {
			ex = append(ex, fmt.Sprintf("x%d  %s", g.count, short(g.sql)))
		}
	}
	if len(pats) == 0 {
		return
	}
	c.addFinding(Finding{Severity: Info, Code: "copy_from_cost", Patterns: pats, Examples: ex,
		Message: plural(len(pats), "COPY FROM pattern loads", "COPY FROM patterns load") +
			" LokaDB tables. On the FDW path (profile fdw) the proxy copies the rows into a temporary table and inserts them through the parent, and every row pays the parent's row triggers: about 10 times a plain COPY for an organization in the cloud (the refs table kept row by row) and about 85 times for a moved one (rows inserted one by one through postgres_fdw, FKs onto cloud tables checked in the cloud), at 10,000 rows. For large imports of a moved organization use profile fdw+direct and name the organization (lokadb.tenant): the COPY then streams into the organization's database (about 15 times faster than on fdw); a table with an FK onto a cloud table still goes the FDW way (check -profile fdw+direct tells which ones go direct)"})
}

func (c *checker) lokadbRel(rv *pg.RangeVar) bool {
	return c.meta.TableIn(rv, c.path) != nil
}

type writeSet struct{ lokadb, cloud map[string]bool }

func (w *writeSet) merge(o writeSet) {
	if w.lokadb == nil {
		w.lokadb, w.cloud = map[string]bool{}, map[string]bool{}
	}
	for t := range o.lokadb {
		w.lokadb[t] = true
	}
	for t := range o.cloud {
		w.cloud[t] = true
	}
}

func (w writeSet) all() []string { return append(keys(w.lokadb), keys(w.cloud)...) }

var systemSchemas = map[string]bool{"pg_catalog": true, "information_schema": true, "lokadb": true, "pg_temp": true}

// writes lists the tables a statement changes (data-modifying CTEs included), split into
// LokaDB and cloud tables.
func (c *checker) writes(sql string) writeSet {
	w := writeSet{lokadb: map[string]bool{}, cloud: map[string]bool{}}
	tree, err := pg.Parse(analyzable(sql))
	if err != nil {
		return w
	}
	add := func(rv *pg.RangeVar) {
		switch {
		case rv == nil || systemSchemas[rv.Schemaname]:
		case c.lokadbRel(rv): // by the table's key: sales.orders and orders are two tables (review 2, R16)
			w.lokadb[c.meta.TableIn(rv, c.path).Name] = true
		case rv.Schemaname != "":
			w.cloud[rv.Schemaname+"."+rv.Relname] = true
		default:
			w.cloud[rv.Relname] = true
		}
	}
	for _, raw := range tree.Stmts {
		proxy.Walk(raw, func(m proto.Message) {
			switch s := m.(type) {
			case *pg.InsertStmt:
				add(s.Relation)
			case *pg.UpdateStmt:
				add(s.Relation)
			case *pg.DeleteStmt:
				add(s.Relation)
			case *pg.MergeStmt:
				add(s.Relation)
			case *pg.CopyStmt:
				if s.IsFrom {
					add(s.Relation)
				}
			case *pg.TruncateStmt:
				for _, r := range s.Relations {
					add(r.GetRangeVar())
				}
			}
		})
	}
	return w
}

func keys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func short(sql string) string {
	s := strings.Join(strings.Fields(sql), " ")
	if len(s) > 160 {
		s = s[:157] + "..."
	}
	return s
}

func firstLine(s string) string {
	l, _, _ := strings.Cut(s, "\n")
	return l
}
