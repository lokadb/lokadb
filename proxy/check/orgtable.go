package check

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// Discovery of the tables by the organizations table (check -org-table). The organization is
// the row of one table, not a column name: every table points at it in a column of its own
// (tenant_id, owner_company_id, ...) or through another table that does.

// Roles of a table in a Discovery.
const (
	RoleOrganization = "organization" // the organizations table itself: stays in the cloud
	RoleRoot         = "root"         // a foreign key to the organizations table: its tenant_column
	RoleChild        = "child"        // reaches the organization through a parent table in the spec
	RoleCloud        = "cloud"        // stays in the cloud, with the reason
	RoleAmbiguous    = "ambiguous"    // two readings, not guessed: the options are listed
)

// Why a table is not in the spec (TableRole.Code).
const (
	CodeNoPath      = "no_path"          // no foreign key path to the organizations table
	CodeLink        = "link_table"       // a membership table between the organizations table and a cloud table
	CodeVia         = "via_cloud"        // reaches the organization only through a table that stays in the cloud
	CodePolymorphic = "polymorphic"      // <x>_id with <x>_type
	CodeComposite   = "composite_fk"     // the foreign key to the organization's chain is of several columns
	CodeNotKey      = "fk_not_key"       // the foreign key goes to a unique column that is not the key
	CodeTwoFKs      = "two_foreign_keys" // two foreign keys to the same table, or two readings of the path
	CodeConflict    = "paths_disagree"
	CodeParent      = "parent_ambiguous"
)

// Discovery is what DiscoverByOrgTable proposes: the spec, and for every table why.
type Discovery struct {
	OrgTable string `json:"org_table"`
	Key      string `json:"key"`      // the organizations table's key column
	KeyKind  string `json:"key_kind"` // admin.TenantInt or admin.TenantUUID
	// Spec: the tables (tenant_column or parent_table) and, where the schema lacks them, the
	// routing declarations (relations of tables found by name, link tables as cloud_tables).
	Spec   admin.Spec  `json:"spec"`
	Tables []TableRole `json:"tables"`
}

// TableRole explains one table of the database.
type TableRole struct {
	Table string `json:"table"`
	Role  string `json:"role"`
	// Column: a root's organization column, a child's column to its parent.
	Column string `json:"column,omitempty"`
	Parent string `json:"parent,omitempty"`
	// Path: from the table up to the organizations table (child -> parent -> ... -> organizations).
	Path []string `json:"path,omitempty"`
	// Basis: "fk" (a foreign key says so) or "name" (a column named <x>_id, verified on the data).
	Basis  string `json:"basis,omitempty"`
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
	// Options: the columns an ambiguous table could be reached by.
	Options []OrgOption `json:"options,omitempty"`
}

type OrgOption struct {
	Column string `json:"column"`
	Target string `json:"target"`
}

// OrgCandidate: a table the organizations may live in, ranked by the tables that reach it.
type OrgCandidate struct {
	Table    string `json:"table"`
	Key      string `json:"key"`
	KeyKind  string `json:"key_kind"`
	Reach    int    `json:"reach"` // tables that reach it by a foreign key or by a column named <x>_id
	ByFK     int    `json:"by_fk"` // of them, by foreign keys alone
	Evidence string `json:"evidence"`
	// Excluded: why it is ranked last though many tables reach it (a users table, which is on the
	// many side of a membership table)
	Excluded string `json:"excluded,omitempty"`
}

// DiscoverByOrgTable proposes the spec for the organizations table (a table name, schema
// qualified outside public). The connection is used in a read-only transaction.
func DiscoverByOrgTable(ctx context.Context, conn *pgx.Conn, table string) (*Discovery, error) {
	c, done, err := orgChecker(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer done()
	return c.discoverOrg(table)
}

// OrgTableCandidates ranks the tables that could be the organizations table, best first.
func OrgTableCandidates(ctx context.Context, conn *pgx.Conn) ([]OrgCandidate, error) {
	c, done, err := orgChecker(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer done()
	return c.orgCandidates()
}

func orgChecker(ctx context.Context, conn *pgx.Conn) (*checker, func(), error) {
	tx, err := beginCheck(ctx, conn)
	if err != nil {
		return nil, nil, err
	}
	c := &checker{ctx: ctx, tx: tx, o: Options{StatementTimeout: 20 * time.Second}}
	return c, func() { _ = tx.Rollback(ctx) }, nil
}

type orgCol struct{ name, typ, kind string }

type orgTab struct {
	key    string
	pk     string // the single-column primary key; "" when there is none
	cols   []orgCol
	byName map[string]orgCol
}

type orgFK struct{ src, col, dst, dstCol string }

type orgGraph struct {
	tabs   map[string]*orgTab
	keys   []string // sorted
	fks    []orgFK  // single-column, in the order of (source, constraint name)
	multi  map[string]map[string]bool
	fkCols map[string]map[string]bool // source -> columns that any foreign key holds
	bare   map[string][]string        // bare table name -> keys
	into   map[string][]orgFK         // destination -> the foreign keys onto its key
}

func (g *orgGraph) isKey(f orgFK) bool { return g.tabs[f.dst] != nil && g.tabs[f.dst].pk == f.dstCol }

func bareName(key string) string { _, n := rel.SplitKey(key); return n }

func (c *checker) orgGraph() (*orgGraph, error) {
	g := &orgGraph{tabs: map[string]*orgTab{}, multi: map[string]map[string]bool{}, fkCols: map[string]map[string]bool{}, bare: map[string][]string{}, into: map[string][]orgFK{}}
	cols, err := c.rows(`SELECT ` + relKeySQL("c") + `, a.attname::text, a.atttypid::regtype::text FROM pg_class c
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		WHERE c.relnamespace NOT IN (` + sysSchemas + `) AND c.relkind = 'r' AND NOT c.relispartition ORDER BY 1, a.attnum`)
	if err != nil {
		return nil, err
	}
	for _, r := range cols {
		t := g.tabs[r[0]]
		if t == nil {
			t = &orgTab{key: r[0], byName: map[string]orgCol{}}
			g.tabs[r[0]] = t
			g.keys = append(g.keys, r[0])
		}
		oc := orgCol{name: r[1], typ: r[2], kind: admin.KeyKind(r[2])}
		t.cols = append(t.cols, oc)
		t.byName[r[1]] = oc
	}
	sort.Strings(g.keys)
	for _, k := range g.keys {
		g.bare[bareName(k)] = append(g.bare[bareName(k)], k)
	}
	pks, err := c.rows(`SELECT ` + relKeySQL("c") + `, a.attname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE i.indisprimary AND i.indnkeyatts = 1 AND c.relnamespace NOT IN (` + sysSchemas + `) AND c.relkind = 'r' AND NOT c.relispartition`)
	if err != nil {
		return nil, err
	}
	for _, r := range pks {
		if t := g.tabs[r[0]]; t != nil {
			t.pk = r[1]
		}
	}
	fks, err := c.rows(`SELECT ` + relKeySQL("s") + `, ` + relKeySQL("d") + `, array_length(c.conkey, 1)::text, a.attname::text, r.attname::text
		FROM pg_constraint c
		JOIN pg_class s ON s.oid = c.conrelid AND s.relkind = 'r' AND NOT s.relispartition AND s.relnamespace NOT IN (` + sysSchemas + `)
		JOIN pg_class d ON d.oid = c.confrelid AND d.relkind = 'r' AND NOT d.relispartition AND d.relnamespace NOT IN (` + sysSchemas + `)
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
		JOIN pg_attribute r ON r.attrelid = c.confrelid AND r.attnum = c.confkey[1]
		WHERE c.contype = 'f' AND c.conparentid = 0
		ORDER BY 1, c.conname`)
	if err != nil {
		return nil, err
	}
	for _, r := range fks {
		if g.fkCols[r[0]] == nil {
			g.fkCols[r[0]] = map[string]bool{}
		}
		g.fkCols[r[0]][r[3]] = true
		if r[2] != "1" {
			if g.multi[r[0]] == nil {
				g.multi[r[0]] = map[string]bool{}
			}
			g.multi[r[0]][r[1]] = true
			continue
		}
		f := orgFK{src: r[0], col: r[3], dst: r[1], dstCol: r[4]}
		g.fks = append(g.fks, f)
		if g.isKey(f) {
			g.into[f.dst] = append(g.into[f.dst], f)
		}
	}
	return g, nil
}

// resolve: the key of the table a user named ("tenants", "public.tenants", "app.tenants").
func (g *orgGraph) resolve(name string) (string, bool) {
	k := rel.RelKey(rel.SplitKey(name))
	if g.tabs[k] != nil {
		return k, true
	}
	if g.tabs[name] != nil {
		return name, true
	}
	if !strings.Contains(name, ".") && len(g.bare[name]) == 1 { // one table of that name in another schema
		return g.bare[name][0], true
	}
	return "", false
}

// singular, plurals: the naming convention <singular>_id -> <plural>.
func singular(n string) string {
	switch {
	case strings.HasSuffix(n, "ies") && len(n) > 3:
		return n[:len(n)-3] + "y"
	case strings.HasSuffix(n, "sses"), strings.HasSuffix(n, "xes"), strings.HasSuffix(n, "ches"), strings.HasSuffix(n, "shes"):
		return n[:len(n)-2]
	case strings.HasSuffix(n, "ss"):
		return n
	case strings.HasSuffix(n, "s") && len(n) > 1:
		return n[:len(n)-1]
	}
	return n
}

func plurals(x string) []string {
	out := []string{x, x + "s", x + "es"}
	if strings.HasSuffix(x, "y") {
		out = append(out, x[:len(x)-1]+"ies")
	}
	return out
}

// nameEdge: column col of src is named <x>_id, holds no foreign key, and a table named x or its
// plural exists whose key is of the column's kind. A column with a sibling <x>_type is a
// polymorphic reference and never an edge.
func (g *orgGraph) nameEdges(src string) []orgFK {
	t := g.tabs[src]
	var out []orgFK
	for _, col := range t.cols {
		x, ok := strings.CutSuffix(col.name, "_id")
		if !ok || x == "" || col.kind == "" || col.kind == admin.KindText || g.fkCols[src][col.name] || col.name == t.pk {
			continue
		}
		if _, poly := t.byName[x+"_type"]; poly {
			continue
		}
		schema, _ := rel.SplitKey(src)
		var dsts []string
		for _, p := range plurals(x) {
			for _, k := range g.bare[p] {
				if dt := g.tabs[k]; dt != nil && dt.pk != "" && dt.byName[dt.pk].kind == col.kind && k != src {
					if s, _ := rel.SplitKey(k); s == schema {
						dsts = append([]string{k}, dsts...)
					} else {
						dsts = append(dsts, k)
					}
				}
			}
			if len(dsts) > 0 {
				break
			}
		}
		if len(dsts) == 1 || (len(dsts) > 1 && sameSchema(dsts[0], schema)) {
			dt := g.tabs[dsts[0]]
			out = append(out, orgFK{src: src, col: col.name, dst: dsts[0], dstCol: dt.pk})
		}
	}
	return out
}

func sameSchema(k, schema string) bool { s, _ := rel.SplitKey(k); return s == schema }

func polymorphicColumns(t *orgTab) []string {
	var out []string
	for _, col := range t.cols {
		if x, ok := strings.CutSuffix(col.name, "_id"); ok && x != "" {
			if _, poly := t.byName[x+"_type"]; poly {
				out = append(out, col.name+"/"+x+"_type")
			}
		}
	}
	return out
}

// reaches: the tables with a path of foreign keys (and, with names, of <x>_id columns) onto
// target's key.
func (g *orgGraph) reaches(target string, nameEdges map[string][]orgFK, byFKOnly bool) map[string]bool {
	seen := map[string]bool{target: true}
	queue := []string{target}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, f := range g.into[p] {
			if !seen[f.src] {
				seen[f.src] = true
				queue = append(queue, f.src)
			}
		}
		if byFKOnly {
			continue
		}
		for _, f := range nameEdges[p] {
			if !seen[f.src] {
				seen[f.src] = true
				queue = append(queue, f.src)
			}
		}
	}
	delete(seen, target)
	return seen
}

var pivotColumns = map[string]bool{"created_at": true, "updated_at": true, "deleted_at": true, "createdat": true, "updatedat": true,
	"role": true, "roles": true, "status": true, "state": true, "permissions": true, "is_owner": true, "is_admin": true, "owner": true, "admin": true,
	"active": true, "is_active": true, "is_default": true, "default": true, "primary": true, "is_primary": true, "joined_at": true,
	"invited_at": true, "accepted_at": true, "expires_at": true, "level": true}

// linkTable: s has one foreign key to the organizations table and no data of its own: its other
// foreign keys all go to tables that do not reach it (tenant_users: tenant_id -> tenants,
// user_id -> users), and what else it holds is a role or a timestamp.
func (g *orgGraph) linkTable(s, org string, reach map[string]bool) (others []string, ok bool) {
	t := g.tabs[s]
	n := 0
	fkc := map[string]bool{}
	for _, f := range g.fks {
		if f.src != s {
			continue
		}
		fkc[f.col] = true
		switch {
		case f.dst == org:
			n++
		case reach[f.dst] || f.dst == s:
			return nil, false
		default:
			others = append(others, f.dst)
		}
	}
	if n != 1 || len(others) == 0 || len(g.multi[s]) > 0 {
		return nil, false
	}
	for _, col := range t.cols {
		if col.name == t.pk || col.name == "id" || fkc[col.name] || pivotColumns[strings.ToLower(col.name)] {
			continue
		}
		return nil, false
	}
	return others, true
}

type orgNode struct {
	col, parent string // a root has no parent
	basis       string
	depth       int
}

type orgRun struct {
	c          *checker
	g          *orgGraph
	org        string
	kind       string
	nodes      map[string]*orgNode
	order      []string
	roles      map[string]*TableRole
	hints      map[string][]string // why a column that looks like a reference was not taken, by table
	spec       admin.Spec
	byName     []string        // tables found by name
	unverified map[string]bool // taken by name from a table with no value to check
	pos        int             // r.order up to here has had its children looked for
	seen       map[string]foundResult
}

func (c *checker) discoverOrg(table string) (*Discovery, error) {
	g, err := c.orgGraph()
	if err != nil {
		return nil, err
	}
	org, ok := g.resolve(table)
	if !ok {
		return nil, fmt.Errorf("no table %s%s", table, c.similarTable(table))
	}
	ot := g.tabs[org]
	if ot.pk == "" {
		return nil, fmt.Errorf("%s has no primary key of one column: the organizations table needs one (%s)", org, admin.TenantTypes)
	}
	kind := admin.TenantKind(ot.byName[ot.pk].typ)
	if kind == "" {
		return nil, fmt.Errorf("the key of %s, %s, is %s: an organization is numbered by %s", org, ot.pk, ot.byName[ot.pk].typ, admin.TenantTypes)
	}
	r := &orgRun{c: c, g: g, org: org, kind: kind, nodes: map[string]*orgNode{}, roles: map[string]*TableRole{}, seen: map[string]foundResult{}, unverified: map[string]bool{}, hints: map[string][]string{}}
	if err := r.run(); err != nil {
		return nil, err
	}
	return r.result(), nil
}

func (r *orgRun) role(t string) *TableRole {
	if r.roles[t] == nil {
		r.roles[t] = &TableRole{Table: t}
	}
	return r.roles[t]
}

func (r *orgRun) resolved(t string) bool { return t == r.org || r.nodes[t] != nil || r.roles[t] != nil }

func (r *orgRun) choose(t string, n *orgNode) {
	r.nodes[t] = n
	r.order = append(r.order, t)
}

func (r *orgRun) run() error {
	g := r.g
	r.roles[r.org] = &TableRole{Table: r.org, Role: RoleOrganization, Reason: "the organizations table: its rows are the organizations, so it stays in the cloud"}
	fkReach := g.reaches(r.org, nil, true)

	// roots: a foreign key to the organizations table
	for _, s := range g.keys {
		if s == r.org {
			continue
		}
		var to []orgFK
		for _, f := range g.fks {
			if f.src == s && f.dst == r.org && g.isKey(f) {
				to = append(to, f)
			}
		}
		switch {
		case len(to) == 0:
		case len(to) > 1:
			var opts []OrgOption
			for _, f := range to {
				opts = append(opts, OrgOption{Column: f.col, Target: r.org})
			}
			r.ambiguous(s, CodeTwoFKs, fmt.Sprintf("%d foreign keys to %s (%s): which of them is the table's organization is not guessed; name one as tenant_column in the spec", len(to), r.org, optionColumns(opts)), opts)
		default:
			if others, link := g.linkTable(s, r.org, fkReach); link {
				r.linkRole(s, to[0].col, others)
				continue
			}
			r.choose(s, &orgNode{col: to[0].col, basis: "fk"})
		}
	}
	// children by foreign keys, then by names (a table found by name has children by foreign keys too)
	r.expandFK()
	for {
		n := len(r.order)
		if err := r.byNames(); err != nil {
			return err
		}
		if len(r.order) == n {
			break
		}
		r.expandFK()
	}
	r.verifyAlternatives()
	r.propagateAmbiguous()
	r.explainTheRest(fkReach)
	return nil
}

// usable: t is in the spec as it stands (chosen, not ambiguous).
func (r *orgRun) usable(t string) bool {
	return r.nodes[t] != nil && (r.roles[t] == nil || r.roles[t].Role == "")
}

// expandFK: foreign keys onto the key of a chosen table, shortest path first (r.order is by depth).
func (r *orgRun) expandFK() {
	for ; r.pos < len(r.order); r.pos++ {
		p := r.order[r.pos]
		for _, f := range r.g.into[p] {
			if r.resolved(f.src) {
				continue
			}
			r.choose(f.src, &orgNode{col: f.col, parent: p, basis: "fk", depth: r.nodes[p].depth + 1})
		}
	}
}

func optionColumns(o []OrgOption) string {
	var cols []string
	for _, x := range o {
		cols = append(cols, x.Column)
	}
	return strings.Join(cols, ", ")
}

func (r *orgRun) linkRole(s, col string, others []string) {
	tr := r.role(s)
	tr.Role, tr.Code, tr.Column = RoleCloud, CodeLink, col
	sort.Strings(others)
	tr.Reason = fmt.Sprintf("a membership table: %s -> %s and %s, with no data of its own. A query like \"the organizations of user X\" names no organization and would be refused, so it stays in the cloud",
		col, r.org, strings.Join(others, ", "))
	t := r.g.tabs[s]
	if t.byName["id"].kind != "" && t.byName[col].kind == r.kind {
		r.spec.Routing = ensureRouting(r.spec.Routing)
		r.spec.Routing.CloudTables = append(r.spec.Routing.CloudTables, admin.CloudTable{Table: s, TenantColumn: col})
		tr.Reason += fmt.Sprintf("; declared as routing.cloud_tables (tenant_column %s), so statements joining it to the organization's tables still name the organization", col)
	} else {
		tr.Reason += "; it has no column id of its own, so it cannot be declared as a cloud table"
	}
}

func ensureRouting(r *admin.Routing) *admin.Routing {
	if r == nil {
		return &admin.Routing{}
	}
	return r
}

// exprVia: SQL for the organization of a row of alias, reaching it through column col to the
// key of parent.
func (r *orgRun) exprVia(parent, col, alias string, depth int) string {
	p := fmt.Sprintf("p%d", depth+1)
	return fmt.Sprintf("(SELECT %s FROM %s %s WHERE %s.%s = %s.%s)", r.exprOf(parent, p, depth+1), qk(parent), p, p, q(r.g.tabs[parent].pk), alias, q(col))
}

func (r *orgRun) exprOf(key, alias string, depth int) string {
	n := r.nodes[key]
	if n.parent == "" {
		return alias + "." + q(n.col)
	}
	return r.exprVia(n.parent, n.col, alias, depth)
}

func (r *orgRun) descendsFrom(x, anc string) bool {
	for n := r.nodes[x]; n != nil && n.parent != ""; n = r.nodes[n.parent] {
		if n.parent == anc {
			return true
		}
	}
	return false
}

const agreeSample = 100000

// agree compares the organization the table's chosen path gives with the one another reading
// (a foreign key or a column) gives, on a sample of its rows: (rows compared, rows that differ).
func (r *orgRun) agree(s string, other orgFK) (compared, differ int64, err error) {
	n := r.nodes[s]
	a := r.exprVia(n.parent, n.col, "s0", 0)
	b := r.exprVia(other.dst, other.col, "s0", 0)
	rows, err := r.c.heavyRows(fmt.Sprintf(`SELECT count(*) FILTER (WHERE a IS NOT NULL AND b IS NOT NULL)::text,
		count(*) FILTER (WHERE a IS NOT NULL AND b IS NOT NULL AND a IS DISTINCT FROM b)::text
		FROM (SELECT %s AS a, %s AS b FROM (SELECT * FROM %s LIMIT %d) s0) x`, a, b, qk(s), agreeSample))
	if err != nil {
		return 0, 0, err
	}
	compared, _ = strconv.ParseInt(rows[0][0], 10, 64)
	differ, _ = strconv.ParseInt(rows[0][1], 10, 64)
	return compared, differ, nil
}

func (r *orgRun) note(s, text string) {
	tr := r.role(s)
	if tr.Reason != "" {
		tr.Reason += "; "
	}
	tr.Reason += text
}

// verifyAlternatives: a child that reaches the organization by two parents takes the shorter
// path if the data says the other gives the same organization; else it is ambiguous.
func (r *orgRun) verifyAlternatives() {
	for _, s := range append([]string(nil), r.order...) {
		n := r.nodes[s]
		if n.parent == "" || n.basis != "fk" || !r.usable(s) {
			continue
		}
		for _, f := range r.g.fks {
			if f.src != s || !r.g.isKey(f) || (f.dst == n.parent && f.col == n.col) || f.dst == s {
				continue
			}
			if !r.usable(f.dst) || r.descendsFrom(f.dst, s) {
				continue
			}
			compared, differ, err := r.agree(s, f)
			switch {
			case err != nil:
				r.note(s, fmt.Sprintf("the other path (%s -> %s) was not compared on the data (%s): the shorter one is taken", f.col, f.dst, firstLine(err.Error())))
			case differ > 0:
				r.ambiguous(s, CodeConflict, fmt.Sprintf("two foreign keys lead to an organization and the data says they differ (%d of %d rows compared give another organization): %s -> %s and %s -> %s. Name one in the spec",
					differ, compared, n.col, n.parent, f.col, f.dst), []OrgOption{{n.col, n.parent}, {f.col, f.dst}})
			default:
				r.note(s, fmt.Sprintf("the other path (%s -> %s) gives the same organization on %s compared; the shorter one is taken", f.col, f.dst, plural(int(compared), "row", "rows")))
			}
			if !r.usable(s) {
				break
			}
		}
	}
}

// propagateAmbiguous: what hangs below an ambiguous table has no organization either.
func (r *orgRun) propagateAmbiguous() {
	for changed := true; changed; {
		changed = false
		for _, s := range r.order {
			n := r.nodes[s]
			if n.parent != "" && r.usable(s) && !r.usable(n.parent) {
				r.ambiguous(s, CodeParent, fmt.Sprintf("its parent %s is ambiguous (see %s)", n.parent, n.parent), nil)
				changed = true
			}
		}
	}
}

func (r *orgRun) ambiguous(s, code, reason string, opts []OrgOption) {
	tr := r.role(s)
	tr.Role, tr.Code, tr.Reason, tr.Options = RoleAmbiguous, code, reason, opts
}

// byNames: tables with no foreign key path, whose column <singular of the organizations table>_id
// (or <x>_id with a chosen table <x>s) holds no foreign key but is verified on the data: every
// value is a key of that table. One pass; run again while it finds tables.
func (r *orgRun) byNames() error {
	g := r.g
	rootCol := singular(bareName(r.org)) + "_id"
	for _, s := range g.keys {
		if r.resolved(s) {
			continue
		}
		var cands []orgFK
		for _, e := range g.nameEdges(s) {
			if e.dst == r.org && e.col == rootCol || e.dst != r.org && r.usable(e.dst) {
				cands = append(cands, e)
			}
		}
		// the organization itself first: the table's own column
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].dst == r.org && cands[j].dst != r.org })
		var ok []orgFK
		for _, e := range cands {
			fr, err := r.found(e)
			good, why := fr.good, fr.why
			switch {
			case err == nil && fr.empty && e.dst == r.org: // an organization column of a table with no rows yet: the name is all there is
				r.unverified[s] = true
				ok = append(ok, e)
			case err != nil:
				r.hints[e.src] = append(r.hints[e.src], fmt.Sprintf("%s looks like a reference to %s by its name, but could not be checked on the data: %s", e.col, e.dst, firstLine(err.Error())))
			case !good:
				r.hints[e.src] = append(r.hints[e.src], fmt.Sprintf("%s looks like a reference to %s by its name, but %s", e.col, e.dst, why))
			default:
				ok = append(ok, e)
			}
		}
		if len(ok) == 0 {
			continue
		}
		if ok[0].dst == r.org {
			r.choose(s, &orgNode{col: ok[0].col, basis: "name"})
			r.byName = append(r.byName, s)
			continue
		}
		r.choose(s, &orgNode{col: ok[0].col, parent: ok[0].dst, basis: "name", depth: r.nodes[ok[0].dst].depth + 1})
		for _, e := range ok[1:] { // several chosen parents: they must agree on the data
			compared, differ, err := r.agree(s, e)
			if err == nil && differ == 0 {
				continue
			}
			why := fmt.Sprintf("two columns look like references to an organization's table (%s -> %s, %s -> %s) and the data does not show they agree", ok[0].col, ok[0].dst, e.col, e.dst)
			if err == nil {
				why = fmt.Sprintf("%s -> %s and %s -> %s give another organization on %d of %d rows compared", ok[0].col, ok[0].dst, e.col, e.dst, differ, compared)
			}
			r.ambiguous(s, CodeConflict, why, []OrgOption{{ok[0].col, ok[0].dst}, {e.col, e.dst}})
			break
		}
		if r.usable(s) {
			r.byName = append(r.byName, s)
			n := r.nodes[s]
			r.spec.Routing = ensureRouting(r.spec.Routing)
			r.spec.Routing.Relations = append(r.spec.Routing.Relations, admin.Relation{From: s + "." + n.col, To: n.parent + "." + g.tabs[n.parent].pk})
		}
	}
	return nil
}

// found: every value of the column is a key of the table (and there is something to check).
func (r *orgRun) found(e orgFK) (foundResult, error) {
	k := e.src + "\x00" + e.col + "\x00" + e.dst
	if v, ok := r.seen[k]; ok {
		return v, v.err
	}
	var chk admin.RelationCheck
	err := r.c.heavy(func(q pgx.Tx) error {
		var err error
		chk, err = admin.CheckRelation(r.c.ctx, q, e.src, e.col, "", e.dst, r.g.tabs[e.dst].pk, "")
		return err
	})
	v := foundResult{good: true}
	switch {
	case err != nil:
		v = foundResult{err: err}
	case chk.Rows == 0:
		v = foundResult{empty: true, why: "the column holds no value to check"}
	case chk.Found != chk.Rows:
		v = foundResult{why: fmt.Sprintf("%d of %d values are not rows of %s", chk.Rows-chk.Found, chk.Rows, e.dst)}
	}
	r.seen[k] = v
	return v, v.err
}

type foundResult struct {
	good  bool
	empty bool // the column holds no value: nothing to check, nothing contradicts
	why   string
	err   error
}

// explainTheRest: every table that is still unplaced stays in the cloud, and says why.
func (r *orgRun) explainTheRest(fkReach map[string]bool) {
	g := r.g
	for _, s := range g.keys {
		if r.nodes[s] != nil || (r.roles[s] != nil && r.roles[s].Role != "") {
			continue
		}
		tr := r.role(s)
		tr.Role = RoleCloud
		var via []string
		composite := false
		notKey := ""
		for _, f := range g.fks {
			if f.src != s {
				continue
			}
			if !g.isKey(f) {
				if r.nodes[f.dst] != nil || f.dst == r.org {
					notKey = fmt.Sprintf("%s -> %s.%s", f.col, f.dst, f.dstCol)
				}
				continue
			}
			if fkReach[f.dst] || f.dst == r.org {
				if r.roles[f.dst] != nil && r.roles[f.dst].Role == RoleAmbiguous {
					via = append(via, f.dst+" (ambiguous)")
				} else {
					via = append(via, f.dst)
				}
			}
		}
		for d := range g.multi[s] {
			if r.nodes[d] != nil || d == r.org || fkReach[d] {
				composite = true
			}
		}
		poly := polymorphicColumns(g.tabs[s])
		switch {
		case len(via) > 0:
			tr.Code = CodeVia
			sort.Strings(via)
			tr.Reason = fmt.Sprintf("reaches %s only through %s, which is not in the spec (see its entry)", r.org, strings.Join(via, ", "))
		case composite:
			tr.Code = CodeComposite
			tr.Reason = "its foreign key to the organization's tables is of several columns, which lokadb does not follow"
		case notKey != "":
			tr.Code = CodeNotKey
			tr.Reason = fmt.Sprintf("its foreign key %s points at a unique column that is not the key: the key is how a child finds its organization", notKey)
		default:
			tr.Code = CodeNoPath
			tr.Reason = fmt.Sprintf("no foreign key path to %s, and no column %s_id (or <x>_id of a table in the spec) that the data confirms", r.org, singular(bareName(r.org)))
		}
		if len(poly) > 0 {
			tr.Reason += fmt.Sprintf("; %s is a polymorphic reference, which is never followed", strings.Join(poly, ", "))
			if tr.Code == CodeNoPath {
				tr.Code = CodePolymorphic
			}
		}
		if h := r.hints[s]; len(h) > 0 {
			tr.Reason += "; " + strings.Join(h, "; ")
		}
	}
}

func (r *orgRun) result() *Discovery {
	d := &Discovery{OrgTable: r.org, Key: r.g.tabs[r.org].pk, KeyKind: r.kind}
	// the spec: roots, then children in the order they were found (parents first)
	var roots, children []string
	for _, s := range r.order {
		if tr := r.roles[s]; tr != nil && tr.Role != "" {
			continue
		}
		if r.nodes[s].parent == "" {
			roots = append(roots, s)
		} else {
			children = append(children, s)
		}
	}
	sort.Strings(roots)
	for _, s := range roots {
		d.Spec.Tables = append(d.Spec.Tables, admin.SpecTable{Name: s, TenantColumn: r.nodes[s].col})
	}
	for _, s := range children {
		n := r.nodes[s]
		d.Spec.Tables = append(d.Spec.Tables, admin.SpecTable{Name: s, ParentTable: n.parent, ParentColumn: n.col})
	}
	d.Spec.Routing = r.spec.Routing
	for _, s := range r.g.keys {
		tr := r.roles[s]
		if tr != nil && tr.Role != "" {
			d.Tables = append(d.Tables, *tr)
			continue
		}
		n := r.nodes[s]
		out := TableRole{Table: s, Column: n.col, Basis: n.basis}
		if tr != nil {
			out.Reason = tr.Reason
		}
		path := []string{s}
		for x := n; ; x = r.nodes[x.parent] {
			if x.parent == "" {
				break
			}
			path = append(path, x.parent)
		}
		out.Path = append(path, r.org)
		if n.parent == "" {
			out.Role = RoleRoot
		} else {
			out.Role, out.Parent = RoleChild, n.parent
		}
		if n.basis == "name" {
			what := "a column named by the convention"
			if out.Reason != "" {
				out.Reason += "; "
			}
			out.Reason += what + ", every value found in the table (verified on the data)"
			if r.unverified[s] {
				out.Reason = strings.TrimSuffix(out.Reason, what+", every value found in the table (verified on the data)") + what + ", but the table has no rows yet: NOT verified on the data"
			}
		}
		d.Tables = append(d.Tables, out)
	}
	return d
}

// Counts of a Discovery, for the report.
func (d *Discovery) counts() (roots, children, byName, cloud, ambiguous int) {
	for _, t := range d.Tables {
		switch t.Role {
		case RoleRoot:
			roots++
		case RoleChild:
			children++
		case RoleCloud:
			cloud++
		case RoleAmbiguous:
			ambiguous++
		}
		if t.Basis == "name" {
			byName++
		}
	}
	return
}

// ---- candidates ----

var knownOrgNames = map[string]bool{"organizations": true, "organization": true, "orgs": true, "tenants": true, "tenant": true,
	"accounts": true, "account": true, "companies": true, "company": true, "teams": true, "team": true, "workspaces": true, "workspace": true}

var userNames = map[string]bool{"users": true, "user": true, "members": true, "people": true, "persons": true, "employees": true, "staff": true, "profiles": true}

var userColumns = []string{"password", "password_hash", "password_digest", "encrypted_password", "remember_token"}

func (g *orgGraph) userLike(k string) bool {
	if userNames[bareName(k)] {
		return true
	}
	for _, c := range userColumns {
		if _, ok := g.tabs[k].byName[c]; ok {
			return true
		}
	}
	return false
}

func (c *checker) orgCandidates() ([]OrgCandidate, error) {
	g, err := c.orgGraph()
	if err != nil {
		return nil, err
	}
	edges := map[string][]orgFK{}
	for _, s := range g.keys {
		for _, e := range g.nameEdges(s) {
			edges[e.dst] = append(edges[e.dst], e)
		}
	}
	// the many side of a membership table: both its ends; the one that is a users table is out
	excluded := map[string]string{}
	for _, s := range g.keys {
		var dsts []string
		seen := map[string]bool{}
		fkc := map[string]bool{}
		for _, f := range g.fks {
			if f.src == s {
				fkc[f.col] = true
				if !seen[f.dst] && f.dst != s {
					seen[f.dst] = true
					dsts = append(dsts, f.dst)
				}
			}
		}
		if len(dsts) != 2 || len(g.multi[s]) > 0 {
			continue
		}
		t := g.tabs[s]
		pure := true
		for _, col := range t.cols {
			if !(col.name == t.pk || col.name == "id" || fkc[col.name] || pivotColumns[strings.ToLower(col.name)]) {
				pure = false
			}
		}
		if !pure {
			continue
		}
		a, b := dsts[0], dsts[1]
		if g.userLike(a) != g.userLike(b) {
			u, o := a, b
			if g.userLike(b) {
				u, o = b, a
			}
			excluded[u] = fmt.Sprintf("a users table: %s links it to %s", s, o)
		}
	}
	var out []OrgCandidate
	reachOf := map[string]map[string]bool{}
	for _, k := range g.keys {
		t := g.tabs[k]
		if t.pk == "" {
			continue
		}
		kind := admin.TenantKind(t.byName[t.pk].typ)
		if kind == "" {
			continue
		}
		reach := g.reaches(k, edges, false)
		reachOf[k] = reach
		byFK := g.reaches(k, nil, true)
		if len(reach) == 0 {
			continue
		}
		oc := OrgCandidate{Table: k, Key: t.pk, KeyKind: kind, Reach: len(reach), ByFK: len(byFK), Excluded: excluded[k]}
		oc.Evidence = fmt.Sprintf("%s: %s reach it, %d by FK", k, plural(oc.Reach, "table", "tables"), oc.ByFK)
		if oc.Reach > oc.ByFK {
			oc.Evidence += fmt.Sprintf(", %d by a column named <x>_id", oc.Reach-oc.ByFK)
		}
		if oc.Excluded != "" {
			oc.Evidence += " (" + oc.Excluded + ")"
		}
		out = append(out, oc)
	}
	// A lookup of another candidate: one table points at it, and that table (or what it leads to) is a
	// candidate that several tables point at (plans, which only organizations point at).
	direct := func(k string) int {
		seen := map[string]bool{}
		for _, f := range g.into[k] {
			seen[f.src] = true
		}
		for _, f := range edges[k] {
			seen[f.src] = true
		}
		return len(seen)
	}
	for i := range out {
		x := out[i]
		if x.Excluded != "" {
			continue
		}
		best := -1
		for j, y := range out {
			if y.Table == x.Table || direct(x.Table) != 1 || direct(y.Table) < 2 || !reachOf[x.Table][y.Table] || reachOf[y.Table][x.Table] {
				continue
			}
			inside := true
			for t := range reachOf[x.Table] {
				if t != y.Table && !reachOf[y.Table][t] {
					inside = false
					break
				}
			}
			if inside && (best < 0 || y.Reach > out[best].Reach) {
				best = j
			}
		}
		if best >= 0 {
			out[i].Excluded = fmt.Sprintf("a lookup of %s: every table that reaches it goes through %s", out[best].Table, out[best].Table)
			out[i].Evidence += " (" + out[i].Excluded + ")"
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if (a.Excluded == "") != (b.Excluded == "") {
			return a.Excluded == ""
		}
		if a.Reach != b.Reach {
			return a.Reach > b.Reach
		}
		if a.ByFK != b.ByFK {
			return a.ByFK > b.ByFK
		}
		if ka, kb := knownOrgNames[bareName(a.Table)], knownOrgNames[bareName(b.Table)]; ka != kb {
			return ka
		}
		return a.Table < b.Table
	})
	if len(out) > 25 {
		out = out[:25]
	}
	return out, nil
}

var errNoOrgTable = errors.New("give -spec, -org-table or -tenant-column")

// noDiscovery: the error of a check that was given nothing to find the tables by, with the
// tables that could be the organizations.
func (c *checker) noDiscovery() error {
	cands, err := c.orgCandidates()
	if err != nil || len(cands) == 0 {
		return errNoOrgTable
	}
	var parts []string
	for _, oc := range cands[:min(3, len(cands))] {
		parts = append(parts, oc.Evidence)
	}
	return fmt.Errorf("%w. Tables that look like the organizations: %s", errNoOrgTable, strings.Join(parts, "; "))
}

// orgDiscovered: the findings of a discovery.
func (c *checker) orgDiscovered(d *Discovery) {
	roots, children, byName, cloud, ambiguous := d.counts()
	c.add(Info, "discovered", d.OrgTable, fmt.Sprintf("%s found by the organizations table %s (key %s, %s): %d with a foreign key to it, %d through another table; %d stay in the cloud, %d are ambiguous. The spec decides which data leaves the cloud: review the suggested spec and the reason of every table (discovery in the JSON report), then pass the spec with -spec",
		plural(len(d.Spec.Tables), "table", "tables"), d.OrgTable, d.Key, d.KeyKind, roots, children, cloud, ambiguous), nil)
	var amb, name, link []string
	for _, t := range d.Tables {
		switch {
		case t.Role == RoleAmbiguous:
			amb = append(amb, fmt.Sprintf("%s: %s", t.Table, t.Reason))
		case t.Role == RoleCloud && t.Code == CodeLink:
			link = append(link, t.Table)
		}
		if t.Basis == "name" {
			name = append(name, fmt.Sprintf("%s.%s -> %s", t.Table, t.Column, t.Path[1]))
		}
	}
	if len(amb) > 0 {
		c.add(Warning, "discovery_ambiguous", d.OrgTable, fmt.Sprintf("%s could not be placed without a guess, so %s left out of the spec: name the right column in the spec (tenant_column, or parent_table and parent_column)",
			plural(len(amb), "table", "tables"), map[bool]string{true: "it is", false: "they are"}[len(amb) == 1]), amb)
	}
	if byName > 0 {
		c.add(Info, "discovery_by_name", d.OrgTable, fmt.Sprintf("%s found by the name of a column, not by a foreign key (the schema has none there): every value of the column is a row of the table it names. A relation is declared in the spec's routing where the proxy needs it",
			plural(byName, "table was", "tables were")), name)
	}
	if len(link) > 0 {
		c.add(Info, "discovery_link_tables", d.OrgTable, fmt.Sprintf("%s link the organizations table to a table that stays in the cloud: queries like \"the organizations of user X\" name no organization, so they stay in the cloud (proposed as routing.cloud_tables)",
			plural(len(link), "table", "tables")), link)
	}
	if len(d.Spec.Tables) == 0 {
		c.add(Warning, "discovery_empty", d.OrgTable, fmt.Sprintf("no table points at %s by a foreign key or a column named %s_id: is it the organizations table? OrgTableCandidates lists the tables that look like it", d.OrgTable, singular(bareName(d.OrgTable))), nil)
	}
	_ = cloud
}
