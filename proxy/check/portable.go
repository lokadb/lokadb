package check

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// dependency is something a LokaDB table needs in the organization's database: a type,
// function or operator class outside pg_catalog, or a collation.
type dependency struct {
	kind, schema, name string // name: quoted; for a function name(args)
	ext                string // the extension it belongs to, "" for the application's own
	oid                string // in the cloud database
	typtype            string // a type's kind: e enum, d domain, c composite, b base, r range...
	tables             []string
	plainIndex         bool // only plain indexes need it (admin.DependencySQL)
	// a function of the application: SECURITY DEFINER, and its language when not trusted (A2 of
	// the admin review: move-org makes it owned by the application's role, and refuses an
	// untrusted language)
	secdef    bool
	untrusted string
}

// portability checks what the table needs beyond the partitioning itself, mostly what
// move-org has to rebuild in the organization's database. Findings go through mark.
func (c *checker) portability(t, key string, cols map[string]column, mark func(string)) error {
	unsupported := func(code, object, msg string, examples []string) {
		mark(StateUnsupported)
		c.add(Unsupported, code, object, msg, examples)
	}
	change := func(code, object, msg string, examples []string) {
		mark(StateReview)
		c.add(Change, code, object, msg, examples)
	}

	// The key (a one-column primary key, or id): the refs table maps it to the organization,
	// so its values must be unique across organizations. A key of any kind (block 17).
	if k, ok := cols[key]; ok && admin.KeyKind(k.base) != "" {
		// A key that may be NULL (no primary key; id only UNIQUE, which allows NULLs, or no
		// index at all): the refs table's id is NOT NULL, so enable stops on a row without one.
		nn, err := c.rows(`SELECT attnotnull::text FROM pg_attribute WHERE attrelid = $1::regclass AND attname = $2`, qk(t), key)
		if err != nil {
			return err
		}
		if len(nn) > 0 && nn[0][0] != "true" {
			m, err := c.count(qk(t), "c", fmt.Sprintf("c.%s IS NULL", q(key)))
			if err != nil {
				return err
			}
			if m.Rows > 0 {
				mark(StateUnsupported)
				c.addFinding(Finding{Severity: Unsupported, Code: "id_null", Object: t + "." + key, Subject: subjectOf("id_null", t+"."+key), Measure: m,
					Message: fmt.Sprintf("%s of %s have no %s: lokadb finds a row's organization by its key (its refs table holds every key, NOT NULL), and enable stops on a row without one. Give them a key, or make %s a primary key, before enable",
						counted(m, "row", "rows"), t, key, key)})
			}
		}
		r, err := c.rows(`SELECT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = $1::regclass AND i.indisunique
			AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1
			AND i.indkey[0] = (SELECT attnum FROM pg_attribute WHERE attrelid = $1::regclass AND attname = $2))::text`, qk(t), key)
		if err != nil {
			return err
		}
		if r[0][0] != "true" {
			dup, err := c.heavyRows(fmt.Sprintf("SELECT (count(%s) - count(DISTINCT %s))::text FROM %s", q(key), q(key), qk(t)))
			if errors.Is(err, errTooLong) { // a sort of the whole key (review 3, M4)
				c.add(Warning, "id_unique_not_checked", t+"."+key, fmt.Sprintf("%s.%s has no unique index, and whether it repeats was not checked: %s. lokadb needs keys unique across organizations: enable checks it, and stops if it repeats", t, key, err), nil)
				dup, err = [][]string{{"0"}}, nil
			}
			if err != nil {
				return err
			}
			if n, _ := strconv.Atoi(dup[0][0]); n > 0 {
				unsupported("id_not_unique", t+"."+key, fmt.Sprintf("%s.%s repeats (%s more than there are distinct values): lokadb maps each key to one organization, so it needs keys unique across organizations so far (a key like (organization, id) is not supported yet)", t, key, plural(n, "row", "rows")), nil)
			} else {
				change("id_unique_after", t+"."+key, fmt.Sprintf("%s.%s has no unique index: after enable lokadb keeps it unique across all organizations (its refs table), and inserting a value that already exists fails", t, key), nil)
			}
		}
	}
	// CHECK constraints, generated columns, own collations and a missing primary key are
	// carried by move-org (admin.describe, needsFullIdentity); large objects cannot be.
	var lobs []string
	for _, name := range sortedCols(cols) {
		if col := cols[name]; col.base == "oid" || col.base == "lo" {
			lobs = append(lobs, name)
		}
	}
	if len(lobs) > 0 {
		change("large_objects", t, fmt.Sprintf("%s has oid columns, usually references to large objects: the rows move to the organization's database, the large objects stay in the cloud database (Postgres cannot move them; the application keeps reading them from the cloud)", t), lobs)
	}

	// The names lokadb gives its objects (admin.Helper: a name too long for Postgres is
	// shortened with a hash, never cut): one taken by an object of the application's is a conflict.
	own := []string{admin.Helper(t, "__cloud"), admin.Helper(t, "__refs"), admin.Helper(t, "__none"), admin.Helper(t, "__key")}
	for _, name := range sortedCols(cols) {
		if cols[name].identity { // enable turns each identity into a sequence of its own
			if name == "id" {
				own = append(own, admin.Helper(t, "__id_seq"))
			} else {
				own = append(own, admin.Helper(t, "__"+name+"_seq"))
			}
		}
	}
	fit := own
	taken, err := c.rows(`SELECT n FROM unnest($1::text[]) n WHERE to_regclass(`+admin.RelSQL("n")+`) IS NOT NULL ORDER BY 1`, fit)
	if err != nil {
		return err
	}
	if len(taken) > 0 {
		unsupported("name_conflict", t, fmt.Sprintf("objects with the names lokadb uses for %s already exist: lokadb cannot choose other names yet", t), flat(taken))
	}

	return c.dependencies(t)
}

// dependencies records what the table uses that the organization's database must have
// (admin.DependencySQL: the list move-org works from).
func (c *checker) dependencies(t string) error {
	if c.depsRead[t] { // an enabled table the spec lists again (check.go, newSpecTables)
		return nil
	}
	if c.depsRead == nil {
		c.depsRead = map[string]bool{}
	}
	c.depsRead[t] = true
	rows, err := c.rows(admin.DependencySQL, qk(t))
	if err != nil {
		return err
	}
	if c.deps == nil {
		c.deps = map[string]*dependency{}
	}
	for _, r := range rows {
		key := r[0] + " " + r[1] + "." + r[2]
		d := c.deps[key]
		if d == nil {
			d = &dependency{kind: r[0], schema: r[1], name: r[2], ext: r[3], oid: r[4], typtype: r[5], plainIndex: true}
			c.deps[key] = d
			if d.kind == "function" && d.ext == "" {
				secdef, lang, trusted, err := admin.FunctionRisk(c.ctx, c.tx, d.oid)
				if err == nil {
					d.secdef = secdef
					if !trusted {
						d.untrusted = lang
					}
				}
			}
		}
		d.plainIndex = d.plainIndex && r[6] == "true"
		d.tables = append(d.tables, t)
	}
	return nil
}

// created: what move-org makes in the organization's database itself when it is missing
// there. A collation of the application only if that Postgres has its provider and locale
// (customerDeps tries it); one of pg_catalog never.
func (d *dependency) created() bool {
	return d.ext != "" || d.kind == "function" || d.kind == "type" && strings.Contains("edc", d.typtype) ||
		d.kind == "collation" && d.schema != "pg_catalog"
}

// sortedDeps: the dependencies in a stable order.
func (c *checker) sortedDeps() []*dependency {
	var keys []string
	for k := range c.deps {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []*dependency
	for _, k := range keys {
		out = append(out, c.deps[k])
	}
	return out
}

func describeDeps(ds []*dependency) []string {
	var out []string
	for _, d := range ds {
		s := fmt.Sprintf("%s %s.%s", d.kind, d.schema, d.name)
		if d.ext != "" {
			s += " (extension " + d.ext + ")"
		}
		out = append(out, s+" — used by "+strings.Join(d.tables, ", "))
	}
	return out
}

func sortedCols(cols map[string]column) []string {
	var out []string
	for n := range cols {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// cdc finds what reads the cloud database's changes besides lokadb: publications that include
// LokaDB tables and logical replication slots. Once an organization is moved, its changes
// happen in its own database and these never see them.
func (c *checker) cdc() error {
	pubKey := rel.KeyOfSQL("pt.schemaname::text", "pt.tablename::text")
	var names []string
	for _, t := range c.rep.Tables {
		names = append(names, t.Name, admin.Helper(t.Name, "__cloud"))
	}
	if len(names) == 0 {
		return nil
	}
	pubs, err := c.rows(`SELECT p.pubname::text FROM pg_publication p
		WHERE p.pubname NOT LIKE 'lokadb\_%' AND (p.puballtables OR EXISTS (SELECT 1 FROM pg_publication_tables pt
			WHERE pt.pubname = p.pubname AND (`+pubKey+` = ANY($1::text[])
				OR EXISTS (SELECT 1 FROM unnest($1::text[]) n WHERE left(`+pubKey+`, length(n) + 3) = n || '__o'))))
		ORDER BY 1`, names)
	if err != nil {
		return err
	}
	if len(pubs) > 0 {
		c.add(Change, "cdc_publication", "", "publications in the cloud database include LokaDB tables: once an organization is moved, its changes happen in its own database, and the subscribers of these publications (CDC such as Debezium, replicas, outbox readers) no longer see them", flat(pubs))
	}
	slots, err := c.rows(`SELECT slot_name::text || coalesce(' (' || plugin || ')', '') FROM pg_replication_slots
		WHERE slot_type = 'logical' AND database = current_database() AND slot_name NOT LIKE 'lokadb\_%' ORDER BY 1`)
	if err != nil {
		return err
	}
	if len(slots) > 0 {
		c.add(Warning, "logical_slots", "", "logical replication slots read the cloud database's changes: if their consumers follow LokaDB tables, they will not see the changes of moved organizations", flat(slots))
	}
	return nil
}
