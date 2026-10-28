package check

// The advice of check on a Laravel-like SaaS (users -> tenant_users -> tenants), on a live
// Postgres (skipped without one): LOKADB_LIVE_DSN=postgres://postgres:pw@host:5432 go test ./check -run Advice

import (
	"context"
	"strings"
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

const saasShape = `
CREATE TABLE role_mappings (code text PRIMARY KEY, title text);
CREATE TABLE users (id bigserial PRIMARY KEY, email text, password text);
CREATE TABLE tenants (id bigserial PRIMARY KEY, name text);
CREATE TABLE tenant_users (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants ON DELETE CASCADE, user_id bigint NOT NULL REFERENCES users ON DELETE CASCADE);
CREATE TABLE channels (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants ON DELETE CASCADE, token text NOT NULL UNIQUE,
  kind text REFERENCES role_mappings ON DELETE CASCADE);
CREATE TABLE orders (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants ON DELETE CASCADE, number text NOT NULL,
  created_by bigint REFERENCES users ON DELETE SET NULL, UNIQUE (number));
CREATE TABLE payments (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants ON DELETE CASCADE, provider text NOT NULL, provider_payment_id text NOT NULL,
  UNIQUE (provider, provider_payment_id));
CREATE UNIQUE INDEX orders_code_active ON orders (number) WHERE tenant_id > 0;
CREATE TABLE members (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants ON DELETE CASCADE, role_code text);
INSERT INTO role_mappings VALUES ('r', 'role');
INSERT INTO tenants (name) VALUES ('a'), ('b');
INSERT INTO users (email) VALUES ('u1');
INSERT INTO tenant_users (tenant_id, user_id) VALUES (1, 1), (2, 1);
INSERT INTO channels (tenant_id, token) VALUES (1, 't1'), (2, 't2');
INSERT INTO orders (tenant_id, number, created_by) VALUES (1, 'n1', 1), (2, 'n2', 1);
INSERT INTO payments (tenant_id, provider, provider_payment_id) VALUES (1, 'p', 'x1'), (2, 'p', 'x2'), (2, 'q', 'x3');
INSERT INTO members (tenant_id, role_code) VALUES (1, 'r');`

func findingsOf(rep *Report, code string) []Finding {
	var out []Finding
	for _, f := range rep.Findings {
		if f.Code == code {
			out = append(out, f)
		}
	}
	return out
}

func findingFor(t *testing.T, rep *Report, code, object string) Finding {
	t.Helper()
	for _, f := range rep.Findings {
		if f.Code == code && f.Object == object {
			return f
		}
	}
	t.Fatalf("no %s for %s in %+v", code, object, rep.Findings)
	return Finding{}
}

func rejectWrites() *proxy.Config {
	return &proxy.Config{Fanout: map[string]proxy.FanoutPolicy{"*": {Mode: proxy.FanoutRejectWrites}}}
}

func TestAdviceUniqueAndDeletes(t *testing.T) {
	dsn, _ := orgDB(t, "advice_saas", saasShape)
	rep, err := Run(context.Background(), Options{DSN: dsn, OrgTable: "tenants", CountLimit: -1, Proxy: rejectWrites(), ProxyFile: "/in/proxy.json"})
	if err != nil {
		t.Fatal(err)
	}
	// 1: a single-column global unique: the routing key first, with the fragment to paste
	f := findingFor(t, rep, "unique_not_tenant_scoped", "channels_token_key")
	for _, want := range []string{"declare channels.token a routing key", `"keys":[{"table":"channels","column":"token"}]`, "WHERE token = $1", "add tenant_id to the key"} {
		if !strings.Contains(f.Message, want) {
			t.Errorf("channels.token: %q lacks %q", f.Message, want)
		}
	}
	if f := findingFor(t, rep, "unique_not_tenant_scoped", "orders_number_key"); !strings.Contains(f.Message, "routing key") {
		t.Errorf("orders.number: %s", f.Message)
	}
	// a composite unique cannot be a routing key (yet): the old advice
	f = findingFor(t, rep, "unique_not_tenant_scoped", "payments_provider_provider_payment_id_key")
	if !strings.Contains(f.Message, "keep this uniqueness in a cloud table") {
		t.Errorf("payments: %s", f.Message)
	}
	// ... but provider_payment_id is unique on the data: a routing key on it routes the webhooks today
	for _, want := range []string{"payments.provider_payment_id is unique on the data, though not by a constraint", "add a unique index on it", `"keys":[{"table":"payments","column":"provider_payment_id"}]`} {
		if !strings.Contains(f.Message, want) {
			t.Errorf("payments: %q lacks %q", f.Message, want)
		}
	}
	if strings.Contains(f.Message, `"column":"provider"}`) {
		t.Errorf("payments: provider is not unique: %s", f.Message)
	}
	// a partial unique index is not a global unique: no key
	if f := findingFor(t, rep, "unique_not_tenant_scoped", "orders_code_active"); strings.Contains(f.Message, "routing key") {
		t.Errorf("partial index: %s", f.Message)
	}

	// 3: the organizations table: deleting a tenant concerns one organization, whatever the policy
	f = findingFor(t, rep, "fk_to_cloud_delete", "tenants")
	if f.Severity != Info || !strings.Contains(f.Message, "the organizations table") || !strings.Contains(f.Message, "no declaration needed") || strings.Contains(f.Message, "refuses") {
		t.Errorf("tenants: %s %s", f.Severity, f.Message)
	}
	// users: shared through tenant_users, no organization column to declare
	f = findingFor(t, rep, "fk_to_cloud_delete", "users")
	if f.Severity != Warning {
		t.Errorf("users: %s", f.Severity)
	}
	for _, want := range []string{"shared by organizations (tenant_users links it", "cannot be declared in routing.cloud_tables", "allow-list entry", "deleted_at"} {
		if !strings.Contains(f.Message, want) {
			t.Errorf("users: %q lacks %q", f.Message, want)
		}
	}
	if strings.Contains(f.Message, "declare users in routing.cloud_tables with the column") {
		t.Errorf("users: impossible advice: %s", f.Message)
	}
	// a dictionary without an organization column: the same options, without the link sentence
	f = findingFor(t, rep, "fk_to_cloud_delete", "role_mappings")
	if !strings.Contains(f.Message, "has no column that holds the organization") || strings.Contains(f.Message, "shared by organizations") ||
		strings.Contains(f.Message, "declare role_mappings in routing.cloud_tables with") {
		t.Errorf("role_mappings: %s", f.Message)
	}

	// 4: the sentence, and no path of the caller's
	for _, f := range findingsOf(rep, "fk_to_cloud_delete") {
		if strings.Contains(f.Message, "/in/proxy.json") || strings.Contains(f.Message, "policy is ") {
			t.Errorf("%s: %s", f.Object, f.Message)
		}
	}
	if f := findingFor(t, rep, "fk_to_cloud_delete", "users"); !strings.Contains(f.Message, "With the fanout policy reject-writes (the proxy config) lokadb refuses") {
		t.Errorf("users: %s", f.Message)
	}
}

// A routing key silences unique_not_tenant_scoped for its column and for it alone.
func TestAdviceDeclaredKeyPins(t *testing.T) {
	dsn, _ := orgDB(t, "advice_keys", saasShape)
	d := orgDiscoverDSN(t, dsn)
	spec := d.Spec
	spec.Routing = ensureRouting(spec.Routing)
	spec.Routing.Keys = []admin.Key{{Table: "channels", Column: "token"}}
	rep, err := Run(context.Background(), Options{DSN: dsn, Spec: &spec, CountLimit: -1})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findingsOf(rep, "unique_not_tenant_scoped") {
		if f.Object == "channels_token_key" {
			t.Errorf("declared key still reported: %s", f.Message)
		}
	}
	findingFor(t, rep, "unique_not_tenant_scoped", "orders_number_key")
	findingFor(t, rep, "key_declared", "channels.token")
	// the organizations table is a cloud table with its id as the organization, declared or not
	if f := findingFor(t, rep, "fk_to_cloud_delete", "tenants"); f.Severity != Info {
		t.Errorf("tenants: %s %s", f.Severity, f.Message)
	}
}

func orgDiscoverDSN(t *testing.T, dsn string) *Discovery {
	t.Helper()
	_, conn := orgConn(t, dsn)
	return orgDiscover(t, conn, "tenants")
}
