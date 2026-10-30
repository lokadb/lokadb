package main

// The SaaS (cloud) database runs PostgreSQL 15 or later (the organizations' databases 13 to
// 18): before 15 the moves cannot work as they should (move-org: async_capable is 14; move-back:
// its bridge views need security_invoker, 15, or they read an organization's rows past row
// security). On a 13 or 14 cloud check reports cloud_version_unsupported and enable refuses
// before it changes anything (CV13, CV14); 15 to 17 go the whole way round (PV15 to PV17; 18 is
// DS1 on the cloud stand). Until 30.10 a 13 or 14 cloud was run the whole way (PV13, PV14, PP13,
// PP14, PL14, and NS2 on 14). And a cloud without a superuser (a managed Postgres): the admin is
// a member of the tables' owner with CREATEROLE (NS1 on 18, NS2 on 15).

import (
	"fmt"
	"strings"
)

func init() {
	for _, v := range []struct{ host, ver string }{{"cloud15", "15"}, {"cloud16", "16"}, {"cloud17", "17"}} {
		checks = append(checks, check{cloudHost: v.host, id: "PV" + v.ver,
			name: "a Postgres " + v.ver + " cloud with a publication of a LokaDB table: enable, move-org, move-back, disable; schema and rows as before enable, the publication publishes the table again",
			run:  checkVersionRoundTrip})
	}
	for _, v := range []struct{ host, ver string }{{"cloud13", "13"}, {"cloud14", "14"}} {
		checks = append(checks, check{cloudHost: v.host, id: "CV" + v.ver,
			name: "a Postgres " + v.ver + " cloud: check reports cloud_version_unsupported as a blocker, enable refuses before it changes anything, the version named",
			run:  checkCloudTooOld})
	}
	checks = append(checks,
		check{id: "NS1", name: "enable, move-org, move-back and disable run by an admin that is no superuser (a member of the tables' owner with CREATEROLE, as on a managed Postgres) and may not set session_replication_role; the owner is left without CREATE on schema lokadb", run: checkNoSuperuser},
		check{cloudHost: "cloud15", id: "NS2", name: "the same on a Postgres 15 cloud, the oldest the SaaS database may run", run: checkNoSuperuser},
	)
}

// CV13, CV14.
func checkCloudTooOld(e *env) (bool, string, error) {
	ver, err := one(e.ctx, e.cloud(), "SELECT (current_setting('server_version_num')::int / 10000)::text")
	if err != nil {
		return false, "", err
	}
	out, code := e.checkWith(spec)
	blocked := reported(out, "cloud_version_unsupported") && code == 3
	enableErr := e.enable()
	refused := enableErr != nil && strings.Contains(enableErr.Error(), "enable changed nothing") && strings.Contains(enableErr.Error(), "PostgreSQL 15 or later")
	left, err := one(e.ctx, e.cloud(), `SELECT (SELECT count(*) FROM pg_namespace WHERE nspname = 'lokadb')::text || ' ' || (SELECT relkind::text FROM pg_class WHERE oid = 'documents'::regclass)`)
	if err != nil {
		return false, "", err
	}
	msg := "-"
	if enableErr != nil {
		msg = lastErr(enableErr.Error())
	}
	return blocked && refused && left == "0 r", fmt.Sprintf("Postgres %s cloud: check exit %d, %s; enable refused: %v (%s); schema lokadb and documents after it: %s (want 0 r)",
		ver, code, finding(out, "cloud_version_unsupported"), refused, msg, left), nil
}

// PV15–PV17 (PV18 is DS1 on the cloud stand). The round trip of DS1, with a publication
// holding documents.
func checkVersionRoundTrip(e *env) (bool, string, error) {
	ver, err := one(e.ctx, e.cloud(), "SELECT (current_setting('server_version_num')::int / 10000)::text")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "CREATE PUBLICATION pv FOR TABLE documents"); err != nil {
		return false, "", err
	}
	ok, msg, err := checkRoundTrip(e)
	if err != nil {
		return false, "", fmt.Errorf("Postgres %s cloud: %w", ver, err)
	}
	pub, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(schemaname || '.' || tablename, ', '), 'nothing') FROM pg_publication_tables WHERE pubname = 'pv'")
	if err != nil {
		return false, "", err
	}
	return ok && pub == "public.documents", fmt.Sprintf("Postgres %s cloud: %s; publication pv after disable: %s", ver, msg, pub), nil
}

// NS1. lokadb_saas has CREATEROLE and is a member of app, the tables' owner; the provider
// installed postgres_fdw. It may not set session_replication_role, which some providers do not
// give at all (disable needed it until 30.10). lokadb runs as lokadb_saas in the cloud (the
// organization's side keeps its superuser: that is not what this checks). The move is
// -mode auto in either mode of the run: a logical move needs a role with REPLICATION, which a
// CREATEROLE admin without it cannot make, so auto moves with triggers.
func checkNoSuperuser(e *env) (bool, string, error) {
	ver, err := one(e.ctx, e.cloud(), "SELECT (current_setting('server_version_num')::int / 10000)::text")
	if err != nil {
		return false, "", err
	}
	setup := []string{
		`DO $$ BEGIN CREATE ROLE lokadb_saas LOGIN PASSWORD 'lokadb_saas' CREATEROLE; EXCEPTION WHEN duplicate_object THEN END $$`,
		"GRANT app TO lokadb_saas"}
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), setup...); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"GRANT CREATE ON DATABASE "+e.db+" TO lokadb_saas",
		"GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO lokadb_saas, app"); err != nil {
		return false, "", err
	}
	admin := dsn(e.cloudHost, e.db, "lokadb_saas")
	as := func(args []string) []string { // the cloud's DSN of the command: the admin's
		out := append([]string(nil), args...)
		for i := 1; i+1 < len(out); i++ {
			if (out[i] == "-db" || out[i] == "-publisher") && out[i+1] == e.cloud() {
				out[i+1] = admin
			}
		}
		return out
	}
	p, err := e.specFile(spec)
	if err != nil {
		return false, "", err
	}
	if out, err := e.lokadb("enable", "-db", admin, "-spec", p, "-owner", "app", "-allow-downtime"); err != nil {
		return false, fmt.Sprintf("Postgres %s: enable as lokadb_saas failed: %s", ver, lastErr(out)), nil
	}
	lent, err := one(e.ctx, e.cloud(), "SELECT has_schema_privilege('app', 'lokadb', 'CREATE')::text")
	if err != nil {
		return false, "", err
	}
	fns, err := one(e.ctx, e.cloud(), `SELECT count(*)::text FROM pg_proc WHERE pronamespace = 'lokadb'::regnamespace AND proname ~ '^fk_(check|act)__' AND pg_get_userbyid(proowner) = 'app'`)
	if err != nil {
		return false, "", err
	}
	if out, err := e.lokadb(as(e.moveArgs(1, "-mode", "auto"))...); err != nil {
		return false, fmt.Sprintf("Postgres %s: enabled, move-org as lokadb_saas failed: %s", ver, lastErr(out)), nil
	}
	moved, err := one(e.ctx, e.customer(), "SELECT count(*)::text FROM documents")
	if err != nil {
		return false, "", err
	}
	if out, err := e.lokadb(as(e.moveBackArgs(1))...); err != nil {
		return false, fmt.Sprintf("Postgres %s: moved (%s documents there), move-back as lokadb_saas failed: %s", ver, moved, lastErr(out)), nil
	}
	// disable as well, with no right to set session_replication_role (block 6, 30.10: it needed it)
	if out, err := e.lokadb(as(e.disableArgs())...); err != nil {
		return false, fmt.Sprintf("Postgres %s: moved and back, disable as lokadb_saas failed: %s", ver, lastErr(out)), nil
	}
	after, err := one(e.ctx, e.cloud(), `SELECT (SELECT count(*) FROM documents)::text || ' documents, ' || (SELECT count(*) FROM document_versions) || ' versions, '
		|| (SELECT relkind::text FROM pg_class WHERE oid = 'documents'::regclass) || ', lokadb ' || (SELECT count(*) FROM pg_namespace WHERE nspname = 'lokadb')`)
	if err != nil {
		return false, "", err
	}
	want := "200 documents, 200 versions, r, lokadb 0"
	ok := lent == "false" && fns == "2" && moved == "100" && after == want
	return ok, fmt.Sprintf("Postgres %s: enable gave app the triggers' functions of the FK to users (%s, want 2), app has CREATE on schema lokadb: %s (want false); %s documents moved (want 100); after move-back and disable: %s (want %s)",
		ver, fns, lent, moved, strings.TrimSpace(after), want), nil
}
