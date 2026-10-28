package proxy

import (
	"strings"
	"testing"
)

func TestDirect(t *testing.T) {
	m := testMeta()
	cases := []struct {
		name, sql string
		named     bool
		ok        bool
		reason    string
		byValues  bool
	}{
		{"find by id", `select * from "documents" where "documents"."id" = $1 limit 1`, false, true, "", false},
		{"list by organization", `SELECT * FROM documents WHERE organization_id = $1 ORDER BY id`, false, true, "", false},
		{"eager load in", `select * from document_versions where document_id in (1, 2, 3)`, false, true, "", true},
		{"eager load any", `select * from document_versions where document_id = ANY($1)`, false, true, "", true},
		{"insert", `insert into documents (organization_id, slug) values ($1, $2) returning id`, false, true, "", false},
		{"insert child", `insert into document_versions (content, document_id) values ($1, $2)`, false, true, "", false},
		{"update by id", `update documents set title = $1 where id = $2`, false, true, "", false},
		{"delete child", `delete from document_versions where document_id = $1`, false, true, "", false},
		{"upsert", `insert into documents (id, organization_id, slug) values ($1, $2, $3) on conflict (id) do update set slug = excluded.slug`, false, true, "", false},
		{"join both confined", `select * from documents d join document_versions v on v.document_id = d.id where d.organization_id = $1 and v.organization_id = $1`, false, true, "", false},
		{"join through the fk", `select * from documents d join document_versions v on v.document_id = d.id where d.id = $1`, false, true, "", false},
		{"join through the organization column", `select * from documents d, document_versions v where d.organization_id = v.organization_id and v.id = $1`, false, true, "", false},
		{"outer join on is not a condition", `select * from documents d left join document_versions v on v.document_id = d.id and v.organization_id = 1 where d.id = $1`, false, false, DirectPartialOrg, false},
		{"join on an unrelated column", `select * from documents d join document_versions v on v.id = d.project_id where d.id = $1`, false, false, DirectPartialOrg, false},
		{"self join: each alias", `select * from documents a join documents b on a.slug = b.slug where a.id = $1`, false, false, DirectPartialOrg, false},
		{"no organization", `select * from documents where slug = $1`, false, false, DirectNoOrg, false},
		{"cloud table", `select * from documents d join projects p on p.id = d.project_id where d.organization_id = $1`, false, false, DirectCloudTable, false},
		{"cloud only", `select * from projects where id = 1`, false, false, DirectCloudOnly, false},
		{"no tables", `select 1`, false, false, DirectNoTables, false},
		{"set", `SET statement_timeout = 0`, false, false, DirectNoTables, false},
		{"catalog", `select * from documents, pg_catalog.pg_class where documents.id = 1`, false, false, DirectCloudTable, false},
		{"cte is not a table", `with x as (select * from documents where id = $1) select * from x`, false, true, "", false},
		{"for update of an alias is not a table", `select * from documents d where d.id = $1 for update of d`, false, true, "", false},
		{"for update of a cloud table's alias", `select * from documents d join projects p on p.id = d.project_id where d.id = $1 for update of p`, false, false, DirectCloudTable, false},
		{"subquery confined", `select * from documents where id in (select document_id from document_versions where organization_id = $1) and organization_id = $1`, false, true, "", false},
		{"correlated subquery", `select * from documents d where d.id = $1 and exists (select 1 from document_versions v where v.document_id = d.id)`, false, false, DirectPartialOrg, false},
		{"insert select naming the organization", `insert into document_versions (document_id, content) select id, title from documents where id = $1`, false, true, "", false},
		{"insert select without it", `insert into document_versions (document_id, content) select id, title from documents`, false, false, DirectForm, false},
		{"merge", `merge into documents d using (values (1)) s(id) on d.id = s.id when matched then delete`, false, false, DirectForm, false},
		{"user function", `select app.summary(id) from documents where id = $1`, false, false, DirectFunction, false},
		{"builtin function", `select lower(slug), now(), count(*) over () from documents where id = $1`, false, true, "", false},
		{"builtin casts", `select id::text, $2::bigint, cast(slug as varchar(10)) from documents where id = $1::int8`, false, true, "", false},
		// review of block 9: functions of pg_catalog that answer otherwise in the organization's database
		{"current_user", `select current_user, title from documents where id = $1`, false, false, DirectFunction, false},
		{"current_database", `select current_database() from documents where id = $1`, false, false, DirectFunction, false},
		{"pg_notify", `select pg_notify('x', title) from documents where id = $1`, false, false, DirectFunction, false},
		{"advisory lock", `select pg_advisory_xact_lock(id) from documents where id = $1`, false, false, DirectFunction, false},
		{"set_config", `select set_config('x.y', title, false) from documents where id = $1`, false, false, DirectFunction, false},
		{"pg_catalog qualified", `select pg_catalog.pg_backend_pid() from documents where id = $1`, false, false, DirectFunction, false},
		{"regclass", `select 'documents'::regclass from documents where id = $1`, false, false, DirectFunction, false},
		{"a type of the application", `select status::doc_status from documents where id = $1`, false, false, DirectFunction, false},
		{"pg_sleep", `select pg_sleep(1) from documents where id = $1`, false, false, DirectFunction, false},
		{"UPDATE to current_user", `update documents set title = current_user where id = $1`, false, false, DirectFunction, false},
		{"current_setting", `select current_setting('search_path') from documents where id = $1`, false, false, DirectFunction, false},
		{"pg_terminate_backend", `select pg_terminate_backend(1) from documents where id = $1`, false, false, DirectFunction, false},
		{"sequence", `insert into documents (id, organization_id) values (nextval('documents_id_seq'), $1)`, false, false, DirectSequence, false},
		{"ddl", `alter table documents add column x int`, false, false, DirectDDL, false},
		{"forbidden", `select * from documents__refs`, false, false, DirectRefused, false},
		{"named: no condition needed", `select * from documents where slug = $1`, true, true, "", false},
		{"named: merge", `merge into documents d using (values (1)) s(id) on d.id = s.id when matched then delete`, true, true, "", false},
		{"named: cloud table still not", `select * from documents d join projects p on p.id = d.project_id`, true, false, DirectCloudTable, false},
	}
	for _, c := range cases {
		v := Direct(c.sql, m, c.named)
		if v.OK != c.ok || v.Reason != c.reason || v.ByValues != c.byValues {
			t.Errorf("%s: got ok=%v reason=%q byValues=%v, want ok=%v reason=%q byValues=%v", c.name, v.OK, v.Reason, v.ByValues, c.ok, c.reason, c.byValues)
		}
	}
}

// directSetting repeats a settings statement by its own text, cut out of the client's
// string: never the string's other statements (review of block 9, 3).
func TestDirectSettingPieces(t *testing.T) {
	cases := []struct {
		sql     string
		want    []string
		unknown bool
	}{
		{"SET TimeZone = 'UTC'", []string{"SET TimeZone = 'UTC'"}, false},
		{"UPDATE documents SET title = 'x' WHERE id = 5; SET statement_timeout = 5000", []string{"SET statement_timeout = 5000"}, false},
		{"SET lock_timeout = 1; SELECT 1; RESET ALL;  DISCARD ALL", []string{"SET lock_timeout = 1", "RESET ALL", "DISCARD ALL"}, false},
		{"SET a.b = 1", nil, true},
		{"SET row_security = off", nil, true},
		{"SET session_replication_role = replica", nil, true},
		{"RESET default_transaction_read_only", nil, true},
		{"SET TIME ZONE 'UTC'; SET NAMES 'UTF8'", []string{"SET TIME ZONE 'UTC'", "SET NAMES 'UTF8'"}, false},
		{"SET client_min_messages TO 'warning'", []string{"SET client_min_messages TO 'warning'"}, false},
		{`set search_path to "public"`, []string{"noted search_path"}, false},
		{"UPDATE documents SET title = 'x' WHERE id = 5", nil, false},
		{"SET LOCAL statement_timeout = 5; SET lokadb.tenant = '1'", nil, false},
		{"SET ROLE other", nil, true},
		{"DISCARD PLANS", nil, true},
		{"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE", nil, true},
		{"SELECT set_config('x.y', '1', false)", nil, true},
		{"SELECT set_config('row_security', 'off', false)", nil, true},
		{"SELECT set_config('search_path', 'app', false)", []string{"noted search_path"}, false},
		{"SELECT set_config('TimeZone'::text, $1, false)", []string{"cloud timezone"}, false},
		{"SELECT set_config($1, $2, false)", nil, true},
		{"SELECT set_config('role', 'other', false)", nil, true},
		{"SELECT set_config('lokadb.tenant', $1, false)", nil, false},
	}
	for _, c := range cases {
		sets, unknown := directSetting(c.sql)
		var got []string
		for _, d := range sets {
			if d.cloud {
				got = append(got, "cloud "+d.name)
			} else if d.sql == "" {
				got = append(got, "noted "+d.name)
			} else {
				got = append(got, d.sql)
			}
		}
		if strings.Join(got, "|") != strings.Join(c.want, "|") || unknown != c.unknown {
			t.Errorf("%q: repeats %q, unknown %v; want %q, %v", c.sql, got, unknown, c.want, c.unknown)
		}
	}
}

func TestDirectOptions(t *testing.T) {
	for v, want := range map[string]bool{
		"": true,
		"-c statement_timeout=5000 -c DateStyle=ISO": true,
		"-cTimeZone=UTC --lock-timeout=10":           true,
		"-c row_security=off":                        false,
		"--session_replication_role=replica":         false,
		"-c search_path=app":                         false,
		`-c application_name=a\ b`:                   false,
		"statement_timeout=5":                        false,
	} {
		if got := directOptions(v); got != want {
			t.Errorf("%q: %v, want %v", v, got, want)
		}
	}
}

// A table with a key of several columns and no organization column in it (kind B) is confined
// when the statement binds every column of the key (tasks/composite-direct.md, 2.1).
func TestDirectTuples(t *testing.T) {
	m := compositeMeta()
	for _, c := range []struct {
		name, sql string
		ok        bool
		reason    string
		byValues  bool
	}{
		{"the whole key", `SELECT weight FROM parcels WHERE tracking_no = $1 AND piece = $2`, true, "", false},
		{"a row of the key", `SELECT * FROM parcels WHERE (tracking_no, piece) = (1002, 2)`, true, "", false},
		{"rows of the key", `SELECT count(*) FROM parcels WHERE (tracking_no, piece) IN ((1001, 1), (1001, 2))`, true, "", true},
		{"a list in one column", `SELECT * FROM parcels WHERE tracking_no IN (1001, 1002) AND piece = 1`, true, "", true},
		{"part of the key", `SELECT * FROM parcels WHERE tracking_no = $1`, false, DirectNoOrg, false},
		{"a row of part of the key", `SELECT * FROM parcels p WHERE (p.tracking_no, p.weight) = (1, 2)`, false, DirectNoOrg, false},
		{"kind A by the organization column", `SELECT title FROM orders WHERE account_id = $1 AND id = $2`, true, "", false},
		{"kind A by the id alone", `SELECT title FROM orders WHERE id = $1`, false, DirectNoOrg, false},
		{"update by the whole key", `UPDATE parcels SET weight = 1 WHERE tracking_no = $1 AND piece = $2`, true, "", false},
	} {
		v := Direct(c.sql, m, false)
		if v.OK != c.ok || v.Reason != c.reason || v.ByValues != c.byValues {
			t.Errorf("%s: got ok=%v reason=%q byValues=%v, want ok=%v reason=%q byValues=%v", c.name, v.OK, v.Reason, v.ByValues, c.ok, c.reason, c.byValues)
		}
	}
}
