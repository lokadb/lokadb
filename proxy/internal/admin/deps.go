package admin

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DependencySQL lists what table $1 uses outside pg_catalog, which the organization's
// database must have before move-org can create the table there: the types of its columns
// (with the element, base and attribute types they are made of), the functions, operator
// classes and collations of its indexes, CHECK constraints and generated columns, and the
// columns' own collations. Columns: kind (type, function, operator class, collation),
// schema, name (a function's carries its argument types), extension it belongs to ("" for
// the application's own), oid, typtype (types only), and whether only plain indexes need it
// (not unique, not an exclusion constraint: without them the table holds and answers the
// same, slower). lokadb check reads the same list.
const DependencySQL = `WITH RECURSIVE used(cls, obj, plain) AS (
	SELECT 'pg_type'::regclass, a.atttypid, false FROM pg_attribute a
	WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
	UNION SELECT 'pg_collation'::regclass, a.attcollation, false FROM pg_attribute a JOIN pg_type ty ON ty.oid = a.atttypid
	WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND a.attcollation <> 0 AND a.attcollation <> ty.typcollation
	-- after enable the indexes live on the partitions (<table>__cloud), not on the parent
	UNION SELECT d.refclassid, d.refobjid, NOT (i.indisunique OR i.indisexclusion) FROM pg_depend d
	JOIN pg_index i ON d.classid = 'pg_class'::regclass AND i.indexrelid = d.objid
	WHERE d.refclassid IN ('pg_proc'::regclass, 'pg_opclass'::regclass, 'pg_collation'::regclass, 'pg_type'::regclass)
	  AND (i.indrelid = $1::regclass OR i.indrelid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = $1::regclass))
	UNION SELECT d.refclassid, d.refobjid, false FROM pg_depend d
	WHERE d.refclassid IN ('pg_proc'::regclass, 'pg_opclass'::regclass, 'pg_collation'::regclass, 'pg_type'::regclass)
	  AND (d.classid = 'pg_constraint'::regclass AND d.objid IN (SELECT oid FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'c')
	    -- generated columns only: the organization's tables get no plain defaults (postgres_fdw
	    -- sends every column), so what a default calls (lokadb.org_default) stays in the cloud
	    OR d.classid = 'pg_attrdef'::regclass AND d.objid IN (SELECT ad.oid FROM pg_attrdef ad
	      JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
	      WHERE ad.adrelid = $1::regclass AND a.attgenerated <> ''))
	UNION SELECT 'pg_type'::regclass, x.t, u.plain FROM used u JOIN pg_type ty ON u.cls = 'pg_type'::regclass AND ty.oid = u.obj
	CROSS JOIN LATERAL (SELECT ty.typelem UNION ALL SELECT ty.typbasetype
		UNION ALL SELECT a.atttypid FROM pg_attribute a WHERE ty.typtype = 'c' AND a.attrelid = ty.typrelid AND a.attnum > 0 AND NOT a.attisdropped) x(t)
	WHERE x.t <> 0)
SELECT k.kind, n.nspname::text, k.name, coalesce((SELECT e.extname::text FROM pg_depend x JOIN pg_extension e ON e.oid = x.refobjid
	WHERE x.classid = u.cls AND x.objid = u.obj AND x.deptype = 'e' LIMIT 1), ''), u.obj::text, k.typtype, bool_and(u.plain)::text
FROM used u
CROSS JOIN LATERAL (
	SELECT 'type' AS kind, typnamespace AS nsp, quote_ident(typname) AS name, typtype::text AS typtype FROM pg_type
	WHERE u.cls = 'pg_type'::regclass AND oid = u.obj AND NOT (typelem <> 0 AND typlen = -1)
	UNION ALL SELECT 'function', pronamespace, quote_ident(proname) || '(' || oidvectortypes(proargtypes) || ')', '' FROM pg_proc
	WHERE u.cls = 'pg_proc'::regclass AND oid = u.obj
	UNION ALL SELECT 'operator class', opcnamespace, opcname::text, '' FROM pg_opclass WHERE u.cls = 'pg_opclass'::regclass AND oid = u.obj
	UNION ALL SELECT 'collation', collnamespace, collname::text, '' FROM pg_collation WHERE u.cls = 'pg_collation'::regclass AND oid = u.obj
		AND collname NOT IN ('default', 'C', 'POSIX', 'ucs_basic')) k
JOIN pg_namespace n ON n.oid = k.nsp
WHERE k.kind = 'collation' OR n.nspname NOT IN ('pg_catalog', 'information_schema')
GROUP BY u.cls, u.obj, k.kind, n.nspname, k.name, k.typtype
ORDER BY 1, 2, 3`

// Dependency is one row of DependencySQL.
type Dependency struct {
	Kind, Schema, Name, Ext, OID, TypType string
	PlainIndex                            bool // only plain indexes need it
}

// Dependencies runs DependencySQL for table.
func Dependencies(ctx context.Context, conn querier, table string) ([]Dependency, error) {
	rows, err := rowsOf(ctx, conn, DependencySQL, qr(table))
	if err != nil {
		return nil, err
	}
	var out []Dependency
	for _, r := range rows {
		out = append(out, Dependency{Kind: r[0], Schema: r[1], Name: r[2], Ext: r[3], OID: r[4], TypType: r[5], PlainIndex: r[6] == "true"})
	}
	return out, nil
}

// FunctionRisk: whether function oid (in the database conn is to) is SECURITY DEFINER, and the
// language it is written in, trusted or not (pg_language.lanpltrusted: C, internal and the
// untrusted PLs such as plpython3u are not). move-org makes such a function in the
// organization's database as the application's role and refuses an untrusted language (A2 of
// the admin review); lokadb check reads the same.
func FunctionRisk(ctx context.Context, conn querier, oid string) (secdef bool, lang string, trusted bool, err error) {
	r, err := rowsOf(ctx, conn, `SELECT p.prosecdef::text, l.lanname::text, l.lanpltrusted::text
		FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang WHERE p.oid = $1::oid`, oid)
	if err != nil || len(r) == 0 {
		return false, "", true, err
	}
	return r[0][0] == "true", r[0][1], r[0][2] == "true", nil
}

// CollationSQL gives the options of CREATE COLLATION for collation $1 (oid) as the cloud has
// it: provider, locale, deterministic and ICU rules. The columns differ between Postgres
// versions (colliculocale before 17, colllocale since), so they are read through to_jsonb.
const CollationSQL = `SELECT '(provider = ' || CASE c.collprovider WHEN 'i' THEN 'icu' WHEN 'b' THEN 'builtin' ELSE 'libc' END
	|| CASE WHEN c.collprovider = 'c' THEN ', lc_collate = ' || quote_literal(c.collcollate) || ', lc_ctype = ' || quote_literal(c.collctype)
	        ELSE ', locale = ' || quote_literal(coalesce(j->>'colllocale', j->>'colliculocale', c.collcollate)) END
	|| CASE WHEN c.collisdeterministic THEN '' ELSE ', deterministic = false' END
	|| coalesce(', rules = ' || quote_literal(j->>'collicurules'), '') || ')'
	FROM pg_collation c CROSS JOIN LATERAL to_jsonb(c) j WHERE c.oid = $1::oid`

// CollationDDL is the CREATE COLLATION that makes collation d (a Dependency of kind
// collation, in its schema there) in another database the same as in the cloud.
func CollationDDL(ctx context.Context, cloud querier, d Dependency) (string, error) {
	r, err := rowsOf(ctx, cloud, CollationSQL, d.OID)
	if err != nil {
		return "", err
	}
	if len(r) == 0 {
		return "", fmt.Errorf("collation %s.%s is gone from the cloud database", d.Schema, d.Name)
	}
	return fmt.Sprintf("CREATE COLLATION %s.%s %s", q(d.Schema), q(d.Name), r[0][0]), nil
}

// customerDeps creates in the organization's database what the moved tables use and it
// lacks: extensions, then the application's collations, enums, domains and composite
// types, then its functions. What exists there already is left alone (an enum only gets
// missing labels). A collation of pg_catalog (made by initdb from the server's ICU or the
// operating system's locales) cannot be created: the organization's Postgres must have it.
// Operator classes of the application are not created (C code). schema: the organization's
// tables' (ChooseCustomerSchema); what the cloud has in public goes there instead.
func customerDeps(ctx context.Context, cloud, cust *pgx.Conn, tables []moveTable, appUser, schema string, log *slog.Logger) error {
	seen := map[string]bool{}
	var deps []Dependency
	for _, t := range tables {
		ds, err := Dependencies(ctx, cloud, t.name)
		if err != nil {
			return err
		}
		for _, d := range ds {
			if key := d.Kind + " " + d.OID; !seen[key] {
				seen[key] = true
				deps = append(deps, d)
			}
		}
	}
	moved := map[string]bool{} // the dependencies of public that go to schema
	if schema != "" && schema != "public" {
		for i := range deps {
			if deps[i].Schema == "public" {
				deps[i].Schema = schema
				moved[deps[i].Kind+" "+deps[i].OID] = true
			}
		}
	}
	exists := func(d Dependency) (bool, error) {
		probe := map[string]string{
			"type":           `SELECT (to_regtype(quote_ident($1) || '.' || $2) IS NOT NULL)::text`,
			"function":       `SELECT (to_regprocedure(quote_ident($1) || '.' || $2) IS NOT NULL)::text`,
			"operator class": `SELECT EXISTS (SELECT 1 FROM pg_opclass o JOIN pg_namespace n ON n.oid = o.opcnamespace WHERE n.nspname = $1 AND o.opcname = $2)::text`,
			"collation":      `SELECT EXISTS (SELECT 1 FROM pg_collation o JOIN pg_namespace n ON n.oid = o.collnamespace WHERE n.nspname = $1 AND o.collname = $2)::text`,
		}[d.Kind]
		r, err := rowsOf(ctx, cust, probe, d.Schema, d.Name)
		if err != nil {
			return false, fmt.Errorf("customer database: %w", err)
		}
		return r[0][0] == "true", nil
	}

	// Extensions first: their objects come with them, at the version the cloud runs
	// (extensions.go). One that only plain indexes need may fail to install: the table is made
	// without those indexes (customerSchema).
	var steps []string
	needs, err := ExtensionNeeds(ctx, cloud, deps, schema)
	if err != nil {
		return err
	}
	var optional [][]string
	for _, n := range needs {
		r, err := rowsOf(ctx, cust, `SELECT extversion::text FROM pg_extension WHERE extname = $1`, n.Name)
		if err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		if len(r) > 0 {
			if r[0][0] != n.CloudVersion {
				log.Warn("the organization's database has this extension in another version than the cloud: left as it is",
					"extension", n.Name, "cloud", n.CloudVersion, "organization", r[0][0])
			}
			continue
		}
		if n.PlainOnly {
			optional = append(optional, n.CreateSQL())
		} else {
			steps = append(steps, n.CreateSQL()...)
		}
	}
	if err := runCustomer(ctx, cust, steps, log); err != nil {
		return err
	}
	for _, ext := range optional {
		if err := runCustomer(ctx, cust, ext, log); err != nil {
			log.Warn("an extension only plain indexes use cannot be installed in the organization's database: its tables get no such indexes", "err", firstLine(err.Error()))
		}
	}

	// The application's own objects, types before the functions that may take them.
	order := map[string]int{"e": 0, "d": 1, "c": 2}
	var colls, types, funcs []Dependency
	for _, d := range deps {
		switch {
		case d.Ext != "":
		case d.Kind == "collation":
			colls = append(colls, d)
		case d.Kind == "type":
			types = append(types, d)
		case d.Kind == "function":
			funcs = append(funcs, d)
		}
	}
	for i := 1; i < len(types); i++ { // stable insertion sort: enums, domains, composites
		for j := i; j > 0 && order[types[j].TypType] < order[types[j-1].TypType]; j-- {
			types[j], types[j-1] = types[j-1], types[j]
		}
	}
	schemas := map[string]bool{}
	var ownerStep []string
	steps = nil
	// The application's functions are made and owned by its role there (-customer-app), not by
	// the administrator whose connection runs this: a SECURITY DEFINER function would otherwise
	// run with the administrator's rights for anyone who calls it, the cloud's role included
	// (A2 of the admin review). Without a separate role (it is the administrator's), nothing to
	// separate.
	owner := ""
	if appUser != "" {
		cu, err := rowsOf(ctx, cust, `SELECT current_user::text`)
		if err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		if cu[0][0] != appUser {
			owner = appUser
		}
	}
	for _, d := range append(append(colls, types...), funcs...) {
		ok, err := exists(d)
		if err != nil {
			return err
		}
		if d.Kind == "type" && d.TypType == "e" {
			// labels the cloud has and the organization's database not yet
			labels, err := rowsOf(ctx, cloud, `SELECT enumlabel::text FROM pg_enum WHERE enumtypid = $1::oid ORDER BY enumsortorder`, d.OID)
			if err != nil {
				return err
			}
			var ls []string
			for _, l := range labels {
				ls = append(ls, lit(l[0]))
			}
			if !ok {
				steps = append(steps, fmt.Sprintf("CREATE TYPE %s.%s AS ENUM (%s)", q(d.Schema), d.Name, strings.Join(ls, ", ")))
			} else {
				have, err := rowsOf(ctx, cust, `SELECT enumlabel::text FROM pg_enum WHERE enumtypid = to_regtype(quote_ident($1) || '.' || $2)`, d.Schema, d.Name)
				if err != nil {
					return fmt.Errorf("customer database: %w", err)
				}
				got := map[string]bool{}
				for _, h := range have {
					got[lit(h[0])] = true
				}
				// each where the cloud has it: before the next label the database has (the
				// order is what < and ORDER BY compare; review 3 of admin, R3-16)
				for i, l := range ls {
					if got[l] {
						continue
					}
					at := ""
					for _, next := range ls[i+1:] {
						if got[next] {
							at = " BEFORE " + next
							break
						}
					}
					steps = append(steps, fmt.Sprintf("ALTER TYPE %s.%s ADD VALUE %s%s", q(d.Schema), d.Name, l, at))
					got[l] = true
				}
			}
			schemas[d.Schema] = true
			continue
		}
		if ok {
			continue
		}
		var def string
		switch {
		case d.Kind == "collation" && d.Schema == "pg_catalog":
			return fmt.Errorf("the organization's database has no collation %s, which its Postgres makes itself from ICU or the operating system's locales: that server needs the same collation provider and locale", q(d.Name))
		case d.Kind == "collation":
			if def, err = CollationDDL(ctx, cloud, d); err != nil {
				return err
			}
		case d.Kind == "type" && d.TypType == "d":
			r, err := rowsOf(ctx, cloud, `SELECT format_type(t.typbasetype, t.typtypmod),
				CASE WHEN t.typcollation <> 0 AND t.typcollation <> b.typcollation THEN ' COLLATE ' || t.typcollation::regcollation::text ELSE '' END,
				coalesce(' DEFAULT ' || t.typdefault, ''), CASE WHEN t.typnotnull THEN ' NOT NULL' ELSE '' END,
				coalesce((SELECT string_agg(' CONSTRAINT ' || quote_ident(c.conname) || ' ' || pg_get_constraintdef(c.oid), '' ORDER BY c.conname)
					FROM pg_constraint c WHERE c.contypid = t.oid AND c.contype = 'c'), '')
				FROM pg_type t JOIN pg_type b ON b.oid = t.typbasetype WHERE t.oid = $1::oid`, d.OID)
			if err != nil {
				return err
			}
			def = fmt.Sprintf("CREATE DOMAIN %s.%s AS %s%s%s%s%s", q(d.Schema), d.Name, r[0][0], r[0][1], r[0][2], r[0][3], r[0][4])
		case d.Kind == "type" && d.TypType == "c":
			r, err := rowsOf(ctx, cloud, `SELECT string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
				FROM pg_type t JOIN pg_attribute a ON a.attrelid = t.typrelid WHERE t.oid = $1::oid AND a.attnum > 0 AND NOT a.attisdropped`, d.OID)
			if err != nil {
				return err
			}
			def = fmt.Sprintf("CREATE TYPE %s.%s AS (%s)", q(d.Schema), d.Name, r[0][0])
		case d.Kind == "type":
			return fmt.Errorf("the organization's database has no type %s.%s, and lokadb creates only enums, domains and composite types: create it there", d.Schema, d.Name)
		default:
			secdef, lang, trusted, err := FunctionRisk(ctx, cloud, d.OID)
			if err != nil {
				return err
			}
			if !trusted {
				return fmt.Errorf("function %s.%s is written in language %s, which is not trusted: lokadb does not create it in the organization's database (it would run code on that server). Create it there yourself, or keep the table in the cloud", d.Schema, d.Name, lang)
			}
			r, err := rowsOf(ctx, cloud, `SELECT pg_get_functiondef($1::oid)`, d.OID)
			if err != nil {
				return err
			}
			def = r[0][0]
			if moved[d.Kind+" "+d.OID] {
				def = strings.Replace(def, "FUNCTION public.", "FUNCTION "+q(schema)+".", 1)
			}
			if owner != "" {
				ownerStep = append(ownerStep, fmt.Sprintf("ALTER FUNCTION %s.%s OWNER TO %s", q(d.Schema), d.Name, q(owner)))
				if secdef {
					log.Info("a SECURITY DEFINER function is created owned by the application's role, not by the administrator: it runs with that role's rights", "function", d.Schema+"."+d.Name, "owner", owner)
				}
			}
		}
		if len(ownerStep) > 0 {
			// one string, one implicit transaction (the simple protocol): a function is never
			// left owned by the administrator, a SECURITY DEFINER one running with its rights
			// for whoever calls it, when its ALTER OWNER fails (review 3 of admin, R3-8)
			def += ";\n" + strings.Join(ownerStep, ";\n")
			ownerStep = nil
		}
		steps = append(steps, def)
		schemas[d.Schema] = true
	}
	var pre []string
	for s := range schemas {
		if s != "public" && s != schema {
			create := "CREATE SCHEMA IF NOT EXISTS " + q(s)
			if owner != "" {
				create += " AUTHORIZATION " + q(owner) // the role must be able to own functions in it
			}
			pre = append(pre, create, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", q(s), q(appUser)))
		}
	}
	return runCustomer(ctx, cust, append(pre, steps...), log)
}

func runCustomer(ctx context.Context, cust *pgx.Conn, steps []string, log *slog.Logger) error {
	for _, s := range steps {
		if _, err := cust.Exec(ctx, s); err != nil {
			return fmt.Errorf("customer database: %w\n  in: %s", err, s)
		}
		log.Info("created in the organization's database", "sql", firstLine(s))
	}
	return nil
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i] + " …"
	}
	return s
}
