package main

// Block 6's finding, my X20 completed: a table of public named "acct.docs" moved to an
// organization whose role cannot create in public (the tables go to schema lokadb there): the
// move named it public."acct.docs" in CREATE TABLE and lokadb."acct.docs" in CREATE INDEX, and
// failed at the index.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X33", name: `a table of public named "acct.docs" moved to a database where public takes no tables (schema lokadb): move-org passes, the rows and the index are in lokadb, the proxy updates them there`, run: checkDotNameSchema},
	)
}

// X33.
func checkDotNameSchema(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE "acct.docs" (id bigserial PRIMARY KEY, org bigint NOT NULL, title text, code int)`,
		`ALTER TABLE "acct.docs" OWNER TO app`,
		`CREATE INDEX acct_docs_code ON "acct.docs" (code)`,
		`INSERT INTO "acct.docs" (org, title, code) SELECT 1 + g % 2, 'd' || g, g FROM generate_series(1, 40) g`); err != nil {
		return false, "", err
	}
	e.spec = dotSpec
	adm := "np_" + e.db
	if err := role(e, "customer", adm); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON DATABASE "+e.db+" TO "+adm); err != nil {
		return false, "", err
	}
	admDSN := dsn("customer", e.db, adm)
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	p, _ := e.specFile(dotSpec)
	mv, err := e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1", "-customer", admDSN, "-publisher", e.cloud())
	if err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	rows := val(e.customer(), `SELECT count(*) FROM lokadb."acct.docs"`)
	inPublic := val(e.customer(), `SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relname LIKE 'acct.docs%'`)
	idx := val(e.customer(), `SELECT count(*) FROM pg_indexes WHERE schemaname = 'lokadb' AND tablename = 'acct.docs' AND indexdef LIKE '%(code)%'`)
	idxs := val(e.customer(), `SELECT coalesce(string_agg(indexname || ' ' || indexdef, ' | '), '-') FROM pg_indexes WHERE schemaname = 'lokadb'`)
	var notes []string
	ok := rows == "20" && inPublic == "0" && idx == "1"
	notes = append(notes, fmt.Sprintf("at the organization: lokadb.\"acct.docs\" %s rows (want 20), tables of that name in public %s (want 0), the index on code there %s (want 1); indexes in lokadb: %s", rows, inPublic, idx, idxs))
	stop, err := e.proxyWith(map[string]string{})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, `UPDATE "acct.docs" SET code = 7 WHERE org = 1 AND id = 2`); err != nil {
		ok = false
		notes = append(notes, "WRONG UPDATE through the proxy: "+firstErrLine(err))
	} else {
		got := val(e.customer(), `SELECT code FROM lokadb."acct.docs" WHERE id = 2`)
		if got != "7" {
			ok = false
		}
		notes = append(notes, "the UPDATE reached lokadb.\"acct.docs\": code "+got+" (want 7)")
	}
	return ok, strings.Join(notes, "; "), nil
}
