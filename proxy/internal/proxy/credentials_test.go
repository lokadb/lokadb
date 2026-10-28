package proxy

import (
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// Stage 9a: an organization whose direct path credentials are in the control plane alone (no
// customers_app in the config) goes direct; the owner's there win over customers for the DDL
// replay.
func TestCredentialsFromControlPlane(t *testing.T) {
	d := directSession(t, false)
	d.s.srv.cfg.CustomersApp = nil // the config has none
	set := creds.Set{"db": {"1": {creds.Direct: "host=org1", creds.Owner: "host=org1-owner-cp"}}}
	d.s.srv.creds.set.Store(&set)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || !d.direct("SELECT title") {
		t.Fatalf("credentials of the control plane alone: %q, org1 %q", code, d.org1.log())
	}
	if !d.s.srv.directDSNs("db") || d.s.srv.directDSNs("other") {
		t.Error("directDSNs")
	}
	owners := d.s.srv.ownerDSNs("db")
	if owners["1"] != "host=org1-owner-cp" || owners["2"] != "host=org2-owner" {
		t.Errorf("owners: %v", owners)
	}
}
