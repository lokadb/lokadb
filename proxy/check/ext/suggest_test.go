package ext

import (
	"context"
	"reflect"
	"testing"

	"github.com/lokadb/lokadb/proxy/check"
	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// railsSession: conversations and messages, both with account_id; messages.conversation_id
// has no FK. The patterns are refused (the fanout policy reject).
func railsSession(m func(*proxy.Meta), pats ...check.WorkloadPattern) *check.Session {
	tbl := func(name string, cols ...string) *proxy.Table {
		t := &proxy.Table{Name: name, TenantCol: "account_id", Refs: name + "__refs", Cloud: name + "__cloud", Cols: cols, ColType: map[string]string{}}
		for _, c := range cols {
			t.ColType[c] = "bigint"
		}
		return t
	}
	meta := &proxy.Meta{
		Tables: map[string]*proxy.Table{
			"conversations": tbl("conversations", "id", "account_id", "inbox_id"),
			"messages":      tbl("messages", "id", "account_id", "conversation_id", "sender_id"),
		},
		Lookups: map[string]map[string]string{
			"conversations": {"account_id": "", "id": "conversations__refs"},
			"messages":      {"account_id": "", "id": "messages__refs"},
		},
	}
	if m != nil {
		m(meta)
	}
	s := check.NewOfflineSession(check.Offline{Options: check.Options{Workload: []string{"recording"}}, Meta: meta, Patterns: pats,
		Spec: admin.Spec{Tables: []admin.SpecTable{{Name: "conversations", TenantColumn: "account_id"}, {Name: "messages", TenantColumn: "account_id"}}}})
	if err := (&routing{}).Run(context.Background(), s); err != nil {
		panic(err)
	}
	return s
}

func refused(sql string, calls int64, fp string) check.WorkloadPattern {
	return check.WorkloadPattern{SQL: sql, Fingerprint: fp, Calls: calls, Class: check.ClassRefused}
}

func TestSuggestRelation(t *testing.T) {
	feed := `SELECT "messages".* FROM "messages" WHERE "messages"."conversation_id" = $1`
	s := railsSession(nil,
		refused(feed, 40, "f1"),
		refused(`select * from messages where sender_id = $1`, 3, "f2"),
		refused(`select * from conversations where inbox_id = $1`, 2, "f3"))
	rep := s.Report()
	if len(rep.Findings) != 1 {
		t.Fatalf("findings: %+v", rep.Findings)
	}
	f := rep.Findings[0]
	sg := f.Suggestion
	if f.Code != "routing_suggestion" || sg == nil || !reflect.DeepEqual(sg.Patterns, []string{"f1"}) || sg.Calls != 40 ||
		!reflect.DeepEqual(sg.Routing.Relations, []admin.Relation{{From: "messages.conversation_id", To: "conversations.id"}}) {
		t.Fatalf("suggestion: %+v %+v", f, sg)
	}
	if rep.Routing == nil || len(rep.Routing.Relations) != 1 {
		t.Fatalf("patch: %+v", rep.Routing)
	}
}

func TestSuggestCloudTable(t *testing.T) {
	feed := `select m.* from messages m join conversations c on c.id = m.conversation_id join inboxes i on i.id = c.inbox_id where i.account_id = $1 and m.sender_id = $2`
	rep := railsSession(nil, refused(feed, 7, "f1")).Report()
	if len(rep.Findings) != 1 || rep.Findings[0].Suggestion == nil || rep.Findings[0].Suggestion.Kind != "cloud_table" ||
		!reflect.DeepEqual(rep.Routing.CloudTables, []admin.CloudTable{{Table: "inboxes", TenantColumn: "account_id"}}) {
		t.Fatalf("findings %+v routing %+v", rep.Findings, rep.Routing)
	}
}

// A polymorphic reference (taggable_type next to taggable_id) is not suggested.
func TestSuggestPolymorphic(t *testing.T) {
	rep := railsSession(func(m *proxy.Meta) { m.Tables["messages"].ColType["conversation_type"] = "text" },
		refused(`select * from messages where conversation_id = $1`, 1, "f")).Report()
	if len(rep.Findings) != 0 {
		t.Errorf("suggested: %+v", rep.Findings)
	}
}

// The target of a relation is the table's key, not always id (Pagila: customer_id).
func TestSuggestRelationToKey(t *testing.T) {
	rep := railsSession(func(m *proxy.Meta) {
		m.Tables["conversations"].Key = "conversation_no"
		m.Tables["conversations"].ColType["conversation_no"] = "bigint"
	}, refused(`select * from messages where conversation_id = $1`, 3, "f")).Report()
	if len(rep.Findings) != 1 || rep.Findings[0].Suggestion == nil ||
		!reflect.DeepEqual(rep.Findings[0].Suggestion.Routing.Relations, []admin.Relation{{From: "messages.conversation_id", To: "conversations.conversation_no"}}) {
		t.Fatalf("suggested: %+v", rep.Findings)
	}
}

// Routed patterns are not the suggestions' (they name their organization).
func TestSuggestOnlyOpenPatterns(t *testing.T) {
	p := refused(`select * from messages where conversation_id = $1`, 3, "f")
	p.Class = check.ClassRouted
	if rep := railsSession(nil, p).Report(); len(rep.Findings) != 0 {
		t.Errorf("suggested for a routed pattern: %+v", rep.Findings)
	}
}

// A unique column named like a token, a number or an external id is a key candidate though no
// recorded query uses it; others are not.
func TestNamedKeys(t *testing.T) {
	m := &proxy.Meta{
		Tables: map[string]*proxy.Table{
			"channels": {Name: "channels", TenantCol: "tenant_id"},
			"orders":   {Name: "orders", TenantCol: "tenant_id"},
			"widgets":  {Name: "widgets", TenantCol: "tenant_id"},
		},
		Lookups: map[string]map[string]string{"orders": {"id": "orders__refs", "number": "lokadb.orders__key__number"}},
	}
	unique := map[string]map[string]bool{
		"channels": {"token": true, "id": true, "tenant_id": true, "name": true, "webhook_token": true, "token_hash": true},
		"orders":   {"number": true, "external_id": true},
		"widgets":  {"title": true, "serial_no": true, "slug": true, "codename": true},
	}
	got := namedKeys(m, func(t string) map[string]bool { return unique[t] }, map[string]bool{"widgets.slug": true})
	want := []admin.Key{{Table: "channels", Column: "token"}, {Table: "channels", Column: "token_hash"}, {Table: "channels", Column: "webhook_token"},
		{Table: "orders", Column: "external_id"}, {Table: "widgets", Column: "serial_no"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
}
