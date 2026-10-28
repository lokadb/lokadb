package ext

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/lokadb/lokadb/proxy/check"
	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// routing: the routing suggestions for patterns that name no organization, -suggest (save
// them as a patch) and -patch (judge the workload as if it were declared): an extension of
// check, registered in ext.go (tasks/routing-declarations.md, section 4). A suggestion is
// a guess from names: the data check comes with it, and the owner decides. It never changes
// the verdict.
type routing struct{ suggest, patch string }

func (r *routing) Flags(fs *flag.FlagSet) {
	fs.StringVar(&r.suggest, "suggest", "", "save the routing declarations the report suggests to this file (a patch for -patch, enable and routing apply)")
	fs.StringVar(&r.patch, "patch", "", "routing declarations (JSON, as -suggest writes them) to judge the workload with, as if applied")
}

// Configure reads -patch: check judges the workload with it (check.Options.Patch).
func (r *routing) Configure(o *check.Options) error {
	if r.patch == "" {
		return nil
	}
	b, err := os.ReadFile(r.patch)
	if err != nil {
		return err
	}
	var p admin.Spec
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("%s: %w", r.patch, err)
	}
	o.Patch = p.Routing
	return nil
}

func (r *routing) Run(_ context.Context, s *check.Session) error {
	if s.Meta() == nil {
		return nil
	}
	(&suggester{s: s, meta: s.Meta()}).suggest()
	return nil
}

// Finish writes -suggest: the suggestions as one patch.
func (r *routing) Finish(rep *check.Report) error {
	if r.suggest == "" {
		return nil
	}
	b, err := json.MarshalIndent(admin.Spec{Tables: []admin.SpecTable{}, Routing: rep.Routing}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.suggest, append(b, '\n'), 0o644)
}

// suggester: one pass of suggestions over a session.
type suggester struct {
	s    *check.Session
	meta *proxy.Meta
}

// suggestForms: the conditions a lookup turns into an organization.
var suggestForms = map[string]bool{"=": true, "IN": true, "= ANY": true, "INSERT": true}

// parentFor guesses the LokaDB table a column <x>_id of table names: <x>, <x>s, <x>es, <x-y>ies.
func (c *suggester) parentFor(table, col string) string {
	t := c.meta.Tables[table]
	if t == nil || col == t.TenantCol || !strings.HasSuffix(col, "_id") || len(col) <= 3 {
		return ""
	}
	if _, ok := c.meta.Lookups[table][col]; ok {
		return ""
	}
	if t.ColType[admin.PolymorphicType(col)] != "" { // points at one of several tables: not a relation
		return ""
	}
	x := strings.TrimSuffix(col, "_id")
	for _, p := range []string{x, x + "s", x + "es", strings.TrimSuffix(x, "y") + "ies"} {
		if pt := c.meta.Tables[p]; pt != nil {
			if _, ok := pt.ColType[check.TableKey(pt)]; ok {
				return p
			}
		}
	}
	return ""
}

// suggest proposes relations for the patterns that name no organization: a column <x>_id
// compared by value, with a LokaDB table <x>s. Each one is tried on all those patterns: it is
// suggested only if the proxy would then route some.
func (c *suggester) suggest() {
	var open []check.WorkloadPattern
	for _, g := range c.s.Patterns() { // most executed first
		if g.Class == check.ClassFanout || g.Class == check.ClassRefused || g.Class == check.ClassAllowed {
			open = append(open, g)
		}
	}
	cands := map[string]string{}  // table.column -> parent
	clouds := map[string]string{} // cloud table -> its organization column
	keys := map[string]bool{}     // table.column with a unique index of its own
	unique := map[string]map[string]bool{}
	for _, g := range open {
		for _, gp := range c.s.Analyze(g.SQL, c.meta).Gaps {
			if gp.Kind != proxy.GapUnknownColumn || !suggestForms[gp.Form] {
				continue
			}
			if gp.Cloud {
				if _, done := clouds[gp.Table]; !done {
					clouds[gp.Table] = c.cloudTenant(gp.Table, gp.Column)
				}
				continue
			}
			if p := c.parentFor(gp.Table, gp.Column); p != "" {
				cands[gp.Table+"."+gp.Column] = p
				continue
			}
			if t := c.meta.Tables[gp.Table]; t != nil && gp.Form != "INSERT" && gp.Column != t.TenantCol && gp.Column != "id" {
				if unique[gp.Table] == nil {
					unique[gp.Table] = c.s.UniqueColumns(gp.Table)
				}
				if unique[gp.Table][gp.Column] {
					keys[gp.Table+"."+gp.Column] = true
				}
			}
		}
	}
	c.suggestClouds(open, clouds)
	c.suggestKeys(open, keys)
	c.suggestNamedKeys(keys)
	var names []string
	for k := range cands {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, from := range names {
		src, col, _ := strings.Cut(from, ".")
		dst := cands[from]
		m := check.WithLookup(c.meta, src, col, admin.Helper(dst, "__refs"))
		s := &check.Suggestion{Kind: "relation", Routing: admin.Routing{Relations: []admin.Relation{{From: from, To: dst + "." + check.TableKey(c.meta.Tables[dst])}}}, Patterns: []string{}}
		var ex []string
		for _, g := range open {
			if c.s.Analyze(g.SQL, m).Unhinted {
				continue
			}
			s.Patterns = append(s.Patterns, g.Fingerprint)
			s.Calls += g.Calls
			if len(ex) < 5 {
				ex = append(ex, fmt.Sprintf("x%d  %s", g.Calls, check.Short(g.SQL)))
			}
		}
		if len(s.Patterns) == 0 {
			continue
		}
		data := "not checked"
		if chk, err := c.s.CheckRelation(src, col, dst); err == nil {
			s.DataCheck, data = &chk, chk.String()
			if !chk.Clean() {
				data += ": the relation does not hold for every row; enable takes it only confirmed"
			}
		} else if err != check.ErrNoDatabase {
			data += ": " + check.FirstLine(err.Error())
		}
		if c.s.Report().Routing == nil {
			c.s.Report().Routing = &admin.Routing{}
		}
		c.s.Report().Routing.Relations = append(c.s.Report().Routing.Relations, s.Routing.Relations...)
		frag, _ := json.Marshal(map[string]any{"routing": s.Routing})
		c.addSuggestion(s, from, fmt.Sprintf("%s has no FK, but by its name it holds the id of a %s row: declaring the relation routes %s (%s). Data: %s. Declaration: %s",
			from, dst, check.Plural(len(s.Patterns), "pattern", "patterns"), check.Plural(int(s.Calls), "call", "calls"), data, frag), ex)
	}
}

// cloudTenant guesses the organization column of a cloud table a pattern compares by value:
// the column itself if LokaDB tables name their organization so, else (the column is id) such
// a column of the table. "" when the table has none.
func (c *suggester) cloudTenant(table, col string) string {
	names := map[string]bool{}
	for _, t := range c.meta.Tables {
		names[t.TenantCol] = true
	}
	if names[col] {
		return col
	}
	if col != "id" {
		return ""
	}
	cols, err := c.s.CloudColumns(table)
	if err != nil || cols["id"] == "" {
		return ""
	}
	var found []string
	for n := range names {
		if cols[n] == admin.KindInt { // an organization column: an integer
			found = append(found, n)
		}
	}
	sort.Strings(found)
	if len(found) == 0 {
		return ""
	}
	return found[0]
}

// suggestClouds proposes cloud_tables for cloud tables the patterns compare by their
// organization column or id, when declaring one routes some of the patterns.
func (c *suggester) suggestClouds(open []check.WorkloadPattern, clouds map[string]string) {
	var tables []string
	for t, tc := range clouds {
		if tc != "" {
			tables = append(tables, t)
		}
	}
	sort.Strings(tables)
	for _, t := range tables {
		tc := clouds[t]
		m := check.WithCloud(c.meta, t, tc)
		s := &check.Suggestion{Kind: "cloud_table", Routing: admin.Routing{CloudTables: []admin.CloudTable{{Table: t, TenantColumn: tc}}}, Patterns: []string{}}
		var ex []string
		for _, g := range open {
			if c.s.Analyze(g.SQL, m).Unhinted {
				continue
			}
			s.Patterns = append(s.Patterns, g.Fingerprint)
			s.Calls += g.Calls
			if len(ex) < 5 {
				ex = append(ex, fmt.Sprintf("x%d  %s", g.Calls, check.Short(g.SQL)))
			}
		}
		if len(s.Patterns) == 0 {
			continue
		}
		if c.s.Report().Routing == nil {
			c.s.Report().Routing = &admin.Routing{}
		}
		c.s.Report().Routing.CloudTables = append(c.s.Report().Routing.CloudTables, s.Routing.CloudTables...)
		frag, _ := json.Marshal(map[string]any{"routing": s.Routing})
		c.addSuggestion(s, t, fmt.Sprintf("%s stays in the cloud but has the organization column %s: declaring it a cloud table routes %s (%s) that join it to LokaDB tables. Check that %s.%s is the organization of the LokaDB rows joined to it. Declaration: %s",
			t, tc, check.Plural(len(s.Patterns), "pattern", "patterns"), check.Plural(int(s.Calls), "call", "calls"), t, tc, frag), ex)
	}
}

// suggestKeys proposes keys for columns with a unique index of their own that patterns
// compare by value, when declaring one routes some of the patterns.
func (c *suggester) suggestKeys(open []check.WorkloadPattern, keys map[string]bool) {
	var names []string
	for k := range keys {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		table, col, _ := strings.Cut(name, ".")
		m := check.WithLookup(c.meta, table, col, "lokadb."+admin.KeyTable(table, col))
		s := &check.Suggestion{Kind: "key", Routing: admin.Routing{Keys: []admin.Key{{Table: table, Column: col}}}, Patterns: []string{}}
		var ex []string
		for _, g := range open {
			if c.s.Analyze(g.SQL, m).Unhinted {
				continue
			}
			s.Patterns = append(s.Patterns, g.Fingerprint)
			s.Calls += g.Calls
			if len(ex) < 5 {
				ex = append(ex, fmt.Sprintf("x%d  %s", g.Calls, check.Short(g.SQL)))
			}
		}
		if len(s.Patterns) == 0 {
			continue
		}
		data := "not checked"
		if chk, err := c.s.CheckKey(table, col); err == nil {
			s.KeyCheck, data = &chk, chk.String()
		}
		if c.s.Report().Routing == nil {
			c.s.Report().Routing = &admin.Routing{}
		}
		c.s.Report().Routing.Keys = append(c.s.Report().Routing.Keys, s.Routing.Keys...)
		frag, _ := json.Marshal(map[string]any{"routing": s.Routing})
		c.addSuggestion(s, name, fmt.Sprintf("%s has a unique index of its own: declaring it a key routes %s (%s); enable then keeps it unique across all organizations. Data: %s. Declaration: %s",
			name, check.Plural(len(s.Patterns), "pattern", "patterns"), check.Plural(int(s.Calls), "call", "calls"), data, frag), ex)
	}
}

func (c *suggester) addSuggestion(s *check.Suggestion, object, msg string, examples []string) {
	c.s.AddFinding(check.Finding{Severity: check.Info, Code: "routing_suggestion", Object: object, Message: msg, Examples: examples, Suggestion: s})
}

// keyName: a column named like what an application looks a row up by without its organization:
// a token, a number, an external id, a slug.
var keyName = regexp.MustCompile(`(^|_)(token|tokens|token_hash|number|slug|code|uuid|guid|key|api_key|secret|hash|reference|external_id|ext_id|external_ref|public_id)$|^(number|no|ref)$|_(no|nr|num)$`)

// suggestNamedKeys proposes a key for every column with a unique index of its own that is named
// like a token, a number or an external id, though no recorded query looks it up yet: the
// application finds such rows by the value alone, so unless it is a key a lookup by it runs in
// every organization's database after the move, and its uniqueness holds within one organization only.
func (c *suggester) suggestNamedKeys(done map[string]bool) {
	for _, k := range namedKeys(c.meta, c.s.UniqueColumns, done) {
		table, col := k.Table, k.Column
		name := table + "." + col
		s := &check.Suggestion{Kind: "key", Routing: admin.Routing{Keys: []admin.Key{{Table: table, Column: col}}}, Patterns: []string{}}
		data := "not checked"
		if chk, err := c.s.CheckKey(table, col); err == nil {
			s.KeyCheck, data = &chk, chk.String()
		}
		if c.s.Report().Routing == nil {
			c.s.Report().Routing = &admin.Routing{}
		}
		c.s.Report().Routing.Keys = append(c.s.Report().Routing.Keys, s.Routing.Keys...)
		frag, _ := json.Marshal(map[string]any{"routing": s.Routing})
		c.addSuggestion(s, name, fmt.Sprintf("%s is unique and named like a token, a number or an external id: if the application looks rows up by it without the organization (WHERE %s = $1), declaring it a key makes the proxy find the organization by the value, and enable keeps it unique across all organizations. No recorded query uses it, so the report cannot show that it routes anything. Data: %s. Declaration: %s",
			name, col, data, frag), nil)
	}
}

// namedKeys: the columns of the LokaDB tables that have a unique index of their own, are named
// like a key (keyName) and are no lookup yet, nor in done ("table.column"); unique gives a
// table's such columns.
func namedKeys(m *proxy.Meta, unique func(table string) map[string]bool, done map[string]bool) []admin.Key {
	var tables []string
	for t := range m.Tables {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	var keys []admin.Key
	for _, table := range tables {
		var cols []string
		for col := range unique(table) {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		for _, col := range cols {
			if _, has := m.Lookups[table][col]; has || done[table+"."+col] || col == "id" || col == m.Tables[table].TenantCol || !keyName.MatchString(col) {
				continue
			}
			keys = append(keys, admin.Key{Table: table, Column: col})
		}
	}
	return keys
}
