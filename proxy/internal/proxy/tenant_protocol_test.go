package proxy

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The cooperative mode through a session (tasks/cooperative-mode.md): the organization the
// application names is taken as it is, reconciled with the statement, and lasts as long as
// Postgres keeps the setting.

// sessionTenant: the cooperative mode with session values allowed (local_only false).
func sessionTenant() *TenantConfig {
	off := false
	return &TenantConfig{LocalOnly: &off}
}

// refused: the proxy sent its refusal (a simple query's DO … RAISE, which the fake upstream
// answers without an error) and not the statement.
func refused(log []string, sql string) bool {
	return ranAny(log, "RAISE EXCEPTION") && !ranAny(log, sql)
}

func ranAny(log []string, sub string) bool {
	for _, q := range log {
		if strings.Contains(q, sub) {
			return true
		}
	}
	return false
}

// errorCode reads until ReadyForQuery and returns the code of the error the client got ("" none).
func errorCode(t *testing.T, fe *pgproto3.Frontend) string {
	t.Helper()
	code := ""
	for {
		m, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			code = m.Code
		case *pgproto3.ReadyForQuery:
			return code
		}
	}
}

// P4 and P3: with the organization named, an INSERT with an explicit id looks nothing up in
// the refs tables, inside a pipeline too; it runs under the named organization.
func TestTenantNoLookup(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe,
		&pgproto3.Parse{Name: "t", Query: "SELECT set_config('lokadb.tenant', $1, true)"},
		&pgproto3.Bind{PreparedStatement: "t", Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "v", Query: "INSERT INTO documents (id, title) VALUES ($1, 'x')"},
		&pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("100")}}, &pgproto3.Execute{},
		&pgproto3.Flush{})
	_ = c
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("101")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if code := errorCode(t, fe); code != "" {
		t.Fatalf("error %s; upstream ran %q", code, f.log())
	}
	log := f.log()
	if ranAny(log, "__refs") {
		t.Fatalf("looked up the refs tables: %q", log)
	}
	var orgs []string // the organization each INSERT ran under (the set_config of lokadb.tenant before ran under -1: openOrg)
	for _, v := range []string{"VALUES (100, 'x')", "VALUES (101, 'x')"} {
		o, _ := orgsBefore(log, v)
		orgs = append(orgs, o)
	}
	if !reflect.DeepEqual(orgs, slices.Repeat([]string{"SELECT set_config('lokadb.org', 7, false)"}, len(orgs))) ||
		!ranAny(log, "VALUES (100, 'x')") || !ranAny(log, "VALUES (101, 'x')") {
		t.Fatalf("upstream ran %q, want both INSERTs under organization 7", log)
	}
}

// A statement that names another organization than the application did fails (42501) and
// does not run; the organization column is checked without a lookup.
func TestTenantMismatch(t *testing.T) {
	f := &fakeUpstream{}
	fe, _ := protoSession(t, f, testMeta())
	sendAll(t, fe,
		&pgproto3.Parse{Name: "t", Query: "SELECT set_config('lokadb.tenant', $1, true)"},
		&pgproto3.Bind{PreparedStatement: "t", Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "q", Query: "SELECT title FROM documents WHERE organization_id = $1"},
		&pgproto3.Bind{PreparedStatement: "q", Parameters: [][]byte{[]byte("2")}}, &pgproto3.Execute{},
		&pgproto3.Sync{})
	if code := errorCode(t, fe); code != "42501" {
		t.Fatalf("error %q, want 42501; upstream ran %q", code, f.log())
	}
	if ranAny(f.log(), "SELECT title") {
		t.Fatalf("the statement ran: %q", f.log())
	}
}

// A child row whose parent belongs to another organization than the named one fails: it
// would land in the named organization under another one's parent.
func TestTenantChildOfAnotherOrganization(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, s := protoSessionS(t, f, testMeta())
	s.orgCache["documents__refs"] = map[string]string{"5": "8"}
	s.orgVer = orgVersion("db").Load()
	sendAll(t, fe, &pgproto3.Query{String: "BEGIN"})
	errorCode(t, fe)
	sendAll(t, fe, &pgproto3.Query{String: "SET LOCAL lokadb.tenant = 7"})
	errorCode(t, fe)
	sendAll(t, fe, &pgproto3.Query{String: "INSERT INTO document_versions (document_id, content) VALUES (5, 'x')"})
	if code := errorCode(t, fe); code != "42501" {
		t.Fatalf("error %q, want 42501; upstream ran %q", code, f.log())
	}
	if ranAny(f.log(), "INSERT INTO document_versions") {
		t.Fatalf("the child row was written: %q", f.log())
	}
}

// A child row whose parent the session does not know runs without a lookup: lokadb_parent in
// the cloud refuses it when the parent belongs to another organization (23514, block 3).
func TestTenantChildNoLookup(t *testing.T) {
	f := &fakeUpstream{}
	fe, _ := protoSession(t, f, testMeta())
	for _, sql := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 7", "INSERT INTO document_versions (document_id, content) VALUES (5, 'x')"} {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		if code := errorCode(t, fe); code != "" {
			t.Fatalf("%s: error %s; upstream ran %q", sql, code, f.log())
		}
	}
	if log := f.log(); ranAny(log, "__refs") || !ranAny(log, "INSERT INTO document_versions") {
		t.Fatalf("upstream ran %q, want the INSERT without a lookup", log)
	}
}

// tenant.local_only (a pooler in transaction mode before the proxy): a statement that names
// the organization for the session is refused (0A000), in a transaction block too, and does
// not run; SET LOCAL, set_config(…, true) and clearing the value run.
func TestTenantLocalOnly(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, s := protoSessionS(t, f, testMeta())
	s.srv.cfg.Tenant = &TenantConfig{} // local_only on by default
	q := func(sql string) string {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		return errorCode(t, fe)
	}
	for sql, want := range map[string]string{
		"SET lokadb.tenant = 7":                          "0A000",
		"SET SESSION lokadb.tenant = 7":                  "0A000",
		"BEGIN; SET lokadb.tenant = 7; COMMIT":           "0A000",
		"SELECT set_config('lokadb.tenant', '7', false)": "0A000",
		"SET LOCAL lokadb.tenant = 7":                    "",
		"SELECT set_config('lokadb.tenant', '7', true)":  "",
		"RESET lokadb.tenant":                            "",
		"SET lokadb.tenant TO DEFAULT":                   "",
		"DISCARD ALL":                                    "",
	} {
		if code := q(sql); code != want {
			t.Errorf("%s: error %q, want %q", sql, code, want)
		}
	}
	for _, sql := range []string{"SET lokadb.tenant = 7", "SET SESSION lokadb.tenant = 7", "BEGIN; SET lokadb.tenant = 7; COMMIT", "set_config('lokadb.tenant', '7', false)"} {
		if ranAny(f.log(), sql) {
			t.Errorf("%s ran: %q", sql, f.log())
		}
	}
	// '*' for the session would reach the next client as well
	if code := q("SET lokadb.tenant = '*'"); code != "0A000" {
		t.Errorf("SET lokadb.tenant = '*': error %q, want 0A000", code)
	}
	// the extended protocol: set_config with a parameter, for the session and for the transaction
	for local, want := range map[string]string{"false": "0A000", "true": ""} {
		sendAll(t, fe,
			&pgproto3.Parse{Name: "t" + local, Query: "SELECT set_config('lokadb.tenant', $1, " + local + ")"},
			&pgproto3.Bind{PreparedStatement: "t" + local, Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{},
			&pgproto3.Sync{})
		if code := errorCode(t, fe); code != want {
			t.Errorf("set_config($1, %s): error %q, want %q", local, code, want)
		}
	}
}

// SET LOCAL lasts until the end of the transaction; ROLLBACK TO restores what the savepoint
// saw; a statement that names no organization is a fanout again afterwards (reject).
func TestTenantLocal(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, s := protoSessionS(t, f, testMeta())
	s.srv.cfg.Tenant = sessionTenant() // the session value below
	q := func(sql string) string {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		return errorCode(t, fe)
	}
	const sel = "SELECT title FROM documents WHERE slug = 'a'"
	for _, sql := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 7", "SAVEPOINT a", "SET LOCAL lokadb.tenant = 8", "ROLLBACK TO a"} {
		if code := q(sql); code != "" {
			t.Fatalf("%s: error %s", sql, code)
		}
	}
	n := len(f.log())
	if code := q(sel); code != "" {
		t.Fatalf("named: error %s; upstream ran %q", code, f.log()[n:])
	}
	if got := f.log()[n:]; len(got) < 2 || got[0] != "SELECT set_config('lokadb.org', '7', false)" || got[len(got)-1] != sel {
		t.Fatalf("named: upstream ran %q, want it under organization 7", got)
	}
	q("COMMIT")
	n = len(f.log())
	if q(sel); !refused(f.log()[n:], sel) {
		t.Fatalf("after COMMIT: upstream ran %q; want the fanout refused", f.log()[n:])
	}
	// SET LOCAL outside a transaction lasts for its own string only
	if code := q("SET LOCAL lokadb.tenant = 7; " + sel); code != "" {
		t.Fatalf("one string: error %s", code)
	}
	n = len(f.log())
	if q(sel); !refused(f.log()[n:], sel) {
		t.Fatalf("after the string: upstream ran %q; want the fanout refused", f.log()[n:])
	}
	// a session SET lasts; RESET ends it
	q("SET lokadb.tenant = 9")
	n = len(f.log())
	if code := q(sel); code != "" || !ranAny(f.log()[n:], "'lokadb.org', '9'") {
		t.Fatalf("session value: error %q, upstream ran %q", code, f.log()[n:])
	}
	q("RESET lokadb.tenant")
	n = len(f.log())
	if q(sel); !refused(f.log()[n:], sel) {
		t.Fatalf("after RESET: upstream ran %q", f.log()[n:])
	}
}

// The value is checked on the SET: not an organization, 22023.
func TestTenantBadValue(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, s := protoSessionS(t, f, testMeta())
	s.srv.cfg.Tenant = sessionTenant() // the value is checked, not where it is set
	sendAll(t, fe, &pgproto3.Query{String: "SET lokadb.tenant = 'acme'"})
	if code := errorCode(t, fe); code != "22023" {
		t.Fatalf("error %q, want 22023", code)
	}
	sendAll(t, fe, &pgproto3.Parse{Query: "SELECT set_config('lokadb.tenant', $1, false)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("acme")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if code := errorCode(t, fe); code != "22023" {
		t.Fatalf("parameter: error %q, want 22023", code)
	}
	if ranAny(f.log(), "set_config('lokadb.tenant', acme") || ranAny(f.log(), "SET lokadb.tenant = 'acme'") {
		t.Fatalf("the bad value reached the cloud: %q", f.log())
	}
}

// '*' under reject (decision of 23.10): only a role of Tenant.AllRoles runs a statement across
// organizations; for any other role it needs the allow list.
func TestTenantAll(t *testing.T) {
	const sel = "SELECT title FROM documents WHERE slug = 'a'"
	for _, c := range []struct {
		user    string
		refused bool
	}{{"app", true}, {"report", false}} {
		f := &fakeUpstream{}
		fe, _, s := protoSessionS(t, f, testMeta())
		s.srv.cfg.Tenant = sessionTenant()
		s.srv.cfg.Tenant.AllRoles = []string{"report"}
		s.user = c.user
		sendAll(t, fe, &pgproto3.Query{String: "SET lokadb.tenant = '*'"})
		errorCode(t, fe)
		sendAll(t, fe, &pgproto3.Query{String: sel})
		errorCode(t, fe)
		// the report runs with lokadb.org empty (it is empty upstream already: no set_config of any organization)
		if got := refused(f.log(), sel); got != c.refused || !got && ranAny(f.log(), "set_config('lokadb.org', '") && !ranAny(f.log(), "set_config('lokadb.org', '', false)") {
			t.Errorf("%s: refused %v, want %v; upstream ran %q", c.user, got, c.refused, f.log())
		}
	}
}

// The organization from the connection string: options=-c lokadb.tenant=42.
func TestStartupTenant(t *testing.T) {
	for params, want := range map[string]string{
		"-c lokadb.tenant=42":  "42",
		"-clokadb.tenant=42":   "42",
		"--lokadb.tenant=0042": "42",
		"-c search_path=x":     "",
	} {
		got, err := startupTenant(map[string]string{"options": params}, "", "")
		if err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", params, got, err, want)
		}
	}
	if got, _ := startupTenant(map[string]string{"lokadb.tenant": "5"}, "", ""); got != "5" {
		t.Errorf("parameter: %q", got)
	}
	if got, _ := startupTenant(map[string]string{"app.current_tenant": "6"}, "app.current_tenant", ""); got != "6" {
		t.Errorf("tenant_setting: %q", got)
	}
	if _, err := startupTenant(map[string]string{"options": "-c lokadb.tenant=acme"}, "", ""); err == nil {
		t.Error("acme accepted")
	}
	for _, params := range []map[string]string{
		{"options": `-c lokadb.tenant=1\ -c\ lokadb.org=2`}, // an escaped space: one value, "1 -c lokadb.org=2"
		{"options": "-c lokadb.tenant=1 -c lokadb.tenant=2"},
		{"options": "-c lokadb.tenant=1", "lokadb.tenant": "2"},
		{"lokadb.tenant": "1", "options": "-c lokadb.tenant=2"},
		{"lokadb.tenant": "1", "LOKADB.TENANT": "2"},
		{"lokadb.tenant": "1", "app.current_tenant": "2"},
		{"options": "-c lokadb.tenant=1 -c lokadb.tenant=acme"},
	} {
		if v, err := startupTenant(params, "app.current_tenant", ""); err == nil {
			t.Errorf("%v: accepted as %q", params, v)
		}
	}
	if v, err := startupTenant(map[string]string{"options": "-c lokadb.tenant=3 --LOKADB.TENANT=3"}, "", ""); err != nil || v != "3" {
		t.Errorf("the same value twice: %q %v", v, err)
	}
	if v, err := startupTenant(map[string]string{"lokadb.tenant": "07", "options": "-c lokadb.tenant=7"}, "", ""); err != nil || v != "7" {
		t.Errorf("the same organization twice: %q %v", v, err)
	}
}

// A config must not let the application set the proxy's own settings.
func TestTenantConfigValidate(t *testing.T) {
	for setting, ok := range map[string]bool{"": true, "app.current_tenant": true, "lokadb.tenant": true, "LOKADB.ORG": false, " lokadb.org ": false, "lokadb.other": false} {
		if err := (&TenantConfig{Setting: setting}).Validate(); (err == nil) != ok {
			t.Errorf("%q: %v", setting, err)
		}
	}
	if (&TenantConfig{Mismatch: "ignore"}).Validate() == nil {
		t.Error("mismatch ignore accepted")
	}
}

// "local_only": false (no pooler before the proxy, or one in session mode): the session value
// is taken.
func TestTenantLocalOnlyOff(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, s := protoSessionS(t, f, testMeta())
	s.srv.cfg.Tenant = sessionTenant()
	for _, sql := range []string{"SET lokadb.tenant = 7", "SELECT set_config('lokadb.tenant', '7', false)"} {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		if code := errorCode(t, fe); code != "" {
			t.Errorf("%s: error %q", sql, code)
		}
	}
}
