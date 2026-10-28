package check

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func ciReport(fps ...string) *Report {
	c := &checker{rep: &Report{SchemaVersion: SchemaVersion, Findings: []Finding{}}}
	var pats []Pattern
	for _, fp := range fps {
		pats = append(pats, Pattern{Fingerprint: fp, SQL: "SELECT * FROM tickets WHERE status = $1 /* " + fp + " */", Calls: 3})
	}
	c.addFinding(Finding{Severity: Blocker, Code: "query_refused", Message: "refused", Patterns: pats})
	c.addOn(Change, "unique_not_tenant_scoped", uniqueSubject("users", "users_email_key", "email"), "users_email_key", "global unique", nil)
	c.add(Info, "discovered", "", "found", nil)
	return c.rep
}

func TestCIBaseline(t *testing.T) {
	rep := ciReport("aa", "bb")
	if got := rep.Findings[1].Key; got != "unique_not_tenant_scoped:public.users:users_email_key" {
		t.Fatalf("key %q", got)
	}
	// nothing accepted: every pattern and the change are new, info takes no part
	res := rep.CompareBaseline(nil, "")
	if len(res.New) != 3 || res.ExitCode() != 6 {
		t.Fatalf("no baseline: %d new, exit %d", len(res.New), res.ExitCode())
	}

	// accept everything, round trip through the file
	path := filepath.Join(t.TempDir(), "accepted.json")
	b := rep.MakeBaseline(nil)
	b.Accepted[0].Reason = "admin report"
	if err := b.Write(path); err != nil {
		t.Fatal(err)
	}
	base, err := ReadBaseline(path)
	if err != nil {
		t.Fatal(err)
	}
	if res := ciReport("aa", "bb").CompareBaseline(base, path); len(res.New) != 0 || res.ExitCode() != 0 || res.Accepted != 3 || res.NoReason != 2 {
		t.Fatalf("all accepted: %+v", res)
	}

	// a new pattern fails, a pattern that went away is stale
	res = ciReport("aa", "cc").CompareBaseline(base, path)
	if len(res.New) != 1 || res.New[0].Key != "query_refused:fp:cc" || res.ExitCode() != 6 {
		t.Fatalf("new pattern: %+v", res.New)
	}
	if len(res.Stale) != 1 || res.Stale[0].Key != "query_refused:fp:bb" {
		t.Fatalf("stale: %+v", res.Stale)
	}
	var out bytes.Buffer
	res.WriteText(&out)
	for _, want := range []string{"FAILED, 1 new item", "query_refused:fp:cc", `"allow"`, "no longer found"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("text lacks %q:\n%s", want, out.String())
		}
	}

	// the same pattern under another code is new: the proxy's policy changed what happens to it
	rep = ciReport()
	rep.Findings[0].Code = "query_without_organization"
	rep.Findings[0].Patterns = []Pattern{{Fingerprint: "aa"}}
	if res := rep.CompareBaseline(base, path); len(res.New) != 1 {
		t.Fatalf("code changed: %+v", res.New)
	}

	// reasons survive -write-baseline
	if nb := ciReport("aa", "cc").MakeBaseline(base); nb.Accepted[0].Key != "query_refused:fp:aa" || nb.Accepted[0].Reason != "admin report" {
		t.Fatalf("reasons: %+v", nb.Accepted)
	}
}

func TestSubjectOf(t *testing.T) {
	for _, tc := range []struct {
		code, object string
		want         Subject
		key          string
	}{
		{"tenant_null", "tickets.org_id", Subject{Kind: "column", Schema: "public", Table: "tickets", Column: "org_id"}, "tenant_null:public.tickets:org_id"},
		{"orphan_rows", "comments", Subject{Kind: "table", Schema: "public", Table: "comments"}, "orphan_rows:public.comments"},
		{"orphan_rows", "app.comments", Subject{Kind: "table", Schema: "app", Table: "comments"}, "orphan_rows:app.comments"},
		// a name in quotes, as regnamespace prints a schema with a dot in it
		{"id_missing", `"x.13".audit`, Subject{Kind: "table", Schema: "x.13", Table: "audit"}, "id_missing:x.13.audit"},
		{"id_missing", "public.acct.docs", Subject{Kind: "table", Schema: "public", Table: "acct.docs"}, "id_missing:public.acct.docs"},
		{"tenant_null", `"x.13"."Notes".org_id`, Subject{Kind: "column", Schema: "x.13", Table: "Notes", Column: "org_id"}, "tenant_null:x.13.Notes:org_id"},
		{"cloud_wal_level", "wal_level", Subject{Kind: "setting", Side: "cloud", Setting: "wal_level"}, "cloud_wal_level:cloud:wal_level"},
		{"customer_app_superuser", "app", Subject{Kind: "role", Side: "customer", Role: "app"}, "customer_app_superuser:customer:app"},
	} {
		s := subjectOf(tc.code, tc.object)
		if s == nil {
			t.Fatalf("%s: no subject", tc.code)
		}
		a, _ := json.Marshal(s)
		b, _ := json.Marshal(tc.want)
		if string(a) != string(b) {
			t.Errorf("%s %s: %s, want %s", tc.code, tc.object, a, b)
		}
		if k := keyOf(tc.code, s); k != tc.key {
			t.Errorf("%s: key %q, want %q", tc.code, k, tc.key)
		}
	}
	if subjectOf("no_workload", "") != nil || keyOf("no_workload", nil) != "no_workload" {
		t.Error("a finding about the database as a whole has no subject")
	}
}

// The subject and key of the findings about constraints name the table: two tables may have
// constraints of the same name, and the baseline must tell them apart.
func TestConstraintSubjects(t *testing.T) {
	c := &checker{rep: &Report{Findings: []Finding{}}}
	c.addOn(Change, "unique_not_tenant_scoped", uniqueSubject("users", "users_email_key", "email"), "users_email_key", "x", nil)
	c.addOn(Change, "unique_not_tenant_scoped", uniqueSubject("app.accounts", "users_email_key", "email,name"), "users_email_key", "x", nil)
	c.addOn(Change, "exclude_not_tenant_scoped", uniqueSubject("rooms", "rooms_during_excl", "room_id,during"), "rooms_during_excl", "x", nil)
	c.addOn(Unsupported, "fk_not_to_id", fkSubject("docs_user_fkey", "docs", []string{"user_id"}, "users", []string{"email"}), "docs_user_fkey", "x", nil)
	c.add(Warning, "unlinked_table", "events.document_id", "x", nil)
	want := []struct{ key, subject string }{
		{"unique_not_tenant_scoped:public.users:users_email_key", `{"kind":"constraint","schema":"public","table":"users","constraint":"users_email_key","columns":["email"]}`},
		{"unique_not_tenant_scoped:app.accounts:users_email_key", `{"kind":"constraint","schema":"app","table":"accounts","constraint":"users_email_key","columns":["email","name"]}`},
		{"exclude_not_tenant_scoped:public.rooms:rooms_during_excl", `{"kind":"constraint","schema":"public","table":"rooms","constraint":"rooms_during_excl","columns":["room_id","during"]}`},
		{"fk_not_to_id:public.docs:docs_user_fkey", `{"kind":"constraint","schema":"public","table":"docs","constraint":"docs_user_fkey","columns":["user_id"],"ref_schema":"public","ref_table":"users","ref_columns":["email"]}`},
		{"unlinked_table:public.events:document_id", `{"kind":"column","schema":"public","table":"events","column":"document_id"}`},
	}
	for i, w := range want {
		f := c.rep.Findings[i]
		b, _ := json.Marshal(f.Subject)
		if f.Key != w.key || string(b) != w.subject {
			t.Errorf("%s: key %s subject %s, want key %s subject %s", f.Code, f.Key, b, w.key, w.subject)
		}
	}
}

// U7: an FK every partition declares is one finding on the root, naming the partitions; an FK
// of one partition only is a finding on that partition, naming its root.
func TestPartitionSubjects(t *testing.T) {
	s := fkSubject("payment_p1_code_fkey", "payment", []string{"code"}, "customer", []string{"code"})
	if ex := onPartitions(s, []string{"payment_p1", "payment_p2"}); len(ex) != 2 {
		t.Errorf("examples %v, want the two partitions", ex)
	}
	b, _ := json.Marshal(s)
	if want := `{"kind":"constraint","schema":"public","table":"payment","constraint":"payment_p1_code_fkey","columns":["code"],"ref_schema":"public","ref_table":"customer","ref_columns":["code"],"partitions":["payment_p1","payment_p2"]}`; string(b) != want {
		t.Errorf("several partitions: %s, want %s", b, want)
	}
	s = fkSubject("payment_p3_code_fkey", `"x.13".payment`, []string{"code"}, "customer", []string{"code"})
	if ex := onPartitions(s, []string{`"x.13".payment_p3`}); ex != nil {
		t.Errorf("examples %v, want none", ex)
	}
	b, _ = json.Marshal(s)
	if want := `{"kind":"constraint","schema":"x.13","table":"payment_p3","constraint":"payment_p3_code_fkey","columns":["code"],"ref_schema":"public","ref_table":"customer","ref_columns":["code"],"partition_of":"x.13.payment"}`; string(b) != want {
		t.Errorf("one partition: %s, want %s", b, want)
	}
	if k := keyOf("fk_not_to_id", s); k != "fk_not_to_id:x.13.payment_p3:payment_p3_code_fkey" {
		t.Errorf("key %s", k)
	}
}
