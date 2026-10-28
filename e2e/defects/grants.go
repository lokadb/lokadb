package main

// Block 2, a finding of block 10: the proxy sends a statement of one organization in its own
// database straight to its foreign partition (the parent's RLS filter cannot be shipped
// there). The partition had no privileges but its owner's: a role that may read the parent
// got permission denied for documents__org1.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "G1", name: "a role with SELECT on the parent only reads a moved organization through the proxy and still cannot UPDATE it, granted before move-org or after", run: checkPartitionGrants},
	)
}

func checkPartitionGrants(e *env) (bool, string, error) {
	reader := "g1r_" + e.db
	// the role exists before enable and move-org: enable carries its grant, move-org maps it
	if err := role(e, "cloud", reader); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO "+reader); err != nil {
		return false, "", err
	}
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ok := true
	var notes []string
	read := func(who, what string) {
		c, err := e.viaProxyAs(who)
		if err != nil {
			ok = false
			notes = append(notes, what+": connect: "+short(err))
			return
		}
		defer c.Close(e.ctx)
		res, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 1")
		n := ""
		if err == nil && len(res) == 1 && len(res[0].Rows) == 1 {
			n = string(res[0].Rows[0][0])
		}
		id, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
		res, errID := simple(e.ctx, c, "SELECT title FROM documents WHERE id = "+id)
		byID := 0
		if errID == nil && len(res) == 1 {
			byID = len(res[0].Rows)
		}
		_, errU := simple(e.ctx, c, "UPDATE documents SET title = 'g1' WHERE organization_id = 1")
		changed, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'g1'")
		denied := errU != nil && strings.Contains(errU.Error(), "SQLSTATE 42501")
		if err != nil || n != "100" || errID != nil || byID != 1 || !denied || changed != "0" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: organization 1 read %s rows (error %s), by id %d (error %s); UPDATE %s, %s rows changed",
			what, n, short(err), byID, short(errID), short(errU), changed))
	}
	read(reader, "SELECT on the parent")
	// the functions that create user mappings and grants are the tables' owner's to call (the
	// proxy's GRANT), not an application role's: it gets 42501 calling them itself
	var calls []string
	for _, f := range []string{"lokadb.sync_mappings()", "lokadb.sync_refs()", "lokadb.grant_sync('SELECT 1', '{}')"} {
		_, err := one(e.ctx, dsn(e.cloudHost, e.db, reader), "SELECT "+f+"::text")
		denied := err != nil && strings.Contains(err.Error(), "SQLSTATE 42501")
		if !denied {
			ok = false
		}
		calls = append(calls, fmt.Sprintf("%s %s", f, short(err)))
	}
	notes = append(notes, "called by the role itself: "+strings.Join(calls, ", "))
	priv, _ := one(e.ctx, e.cloud(), "SELECT has_table_privilege($1, 'documents__org1', 'SELECT')::text || '/' || has_table_privilege($1, 'documents__org1', 'UPDATE')::text", reader)
	notes = append(notes, "on documents__org1 SELECT/UPDATE: "+priv)
	if priv != "true/false" {
		ok = false
	}
	// the partition without it (a grant made around lokadb, a role that could not copy it):
	// the proxy reads through the parent
	if err := execSQL(e.ctx, e.cloud(), "REVOKE SELECT ON documents__org1 FROM "+reader); err != nil {
		return false, "", err
	}
	read(reader, "the partition's grant revoked")
	// GRANT and REVOKE on the parent through the proxy, after move-org, reach the foreign
	// partitions, and the role gets a user mapping on the organization's server (move-org
	// mapped the roles granted then): it reads the organization's rows
	other := "g1o_" + e.db
	if err := role(e, "cloud", other); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	res, errG := simple(e.ctx, c, "GRANT SELECT, UPDATE ON documents TO "+other)
	tag := ""
	if errG == nil && len(res) == 1 {
		tag = res[0].CommandTag.String()
	}
	granted, _ := one(e.ctx, e.cloud(), "SELECT has_table_privilege($1, 'documents__org1', 'SELECT')::text || '/' || has_table_privilege($1, 'documents__org1', 'UPDATE')::text", other)
	_, errR := simple(e.ctx, c, "REVOKE UPDATE ON documents FROM "+other)
	revoked, _ := one(e.ctx, e.cloud(), "SELECT has_table_privilege($1, 'documents__org1', 'SELECT')::text || '/' || has_table_privilege($1, 'documents__org1', 'UPDATE')::text", other)
	if errG != nil || tag != "GRANT" || granted != "true/true" || errR != nil || revoked != "true/false" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("GRANT through the proxy: %s (error %s), on documents__org1 SELECT/UPDATE %s; after REVOKE UPDATE %s (error %s)",
		tag, short(errG), granted, revoked, short(errR)))
	mappings := func() string {
		v, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_user_mappings WHERE usename = $1", other)
		return v
	}
	mapped := mappings()
	read(other, "granted after move-org")
	_, errL := simple(e.ctx, c, "REVOKE SELECT ON documents FROM "+other)
	unmapped := mappings()
	if mapped != "1" || errL != nil || unmapped != "0" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("its user mappings: %s after the GRANT, %s after the last REVOKE (error %s)", mapped, unmapped, short(errL)))
	return ok, strings.Join(notes, "; "), nil
}
