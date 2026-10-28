package main

// F7 of the third performance review, C1: for a statement of one organization the proxy adds
// the partition key's condition, coalesce(<organization column>, -2) = N, to every LokaDB
// table of every level (not to the side of an outer join that may be null): the planner prunes
// to that organization's partition at once instead of planning every partition. The condition
// is what the policy lokadb_tenant requires under lokadb.org = N, so no statement's rows change,
// whatever organization the proxy runs it under, a wrong one too. Checked here on a database
// with a foreign partition (organization 1, moved) and a local one (organization 2,
// lokadb.partition_org): each statement through the proxy under lokadb.tenant = N against the
// same statement straight in the cloud under lokadb.org = N, for N = 1 and 2; and EXPLAIN
// through the proxy plans organization 2's partition alone. Forms whose tables the statement's
// WHERE does not see by name (a join with an alias of its own, column aliases) go as they are.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "F7C1", name: "the partition key's condition the proxy adds changes no statement's rows, under the organization's own and another one; the plan prunes to the organization's partition", run: checkKeyCond},
	)
}

func checkKeyCond(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "move-org 1 failed: " + firstErrLine(err), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"SELECT lokadb.partition_org('documents', 2)", "SELECT lokadb.partition_org('document_versions', 2)", "ANALYZE"); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"tenant": map[string]any{"local_only": false},
	})
	if err != nil {
		return false, "", err
	}
	defer stop()
	p, err := e.viaProxyPgx("f7c1")
	if err != nil {
		return false, "", err
	}
	defer p.Close(e.ctx)
	cloud, err := pgx.Connect(e.ctx, dsn(e.cloudHost, e.db, "app"))
	if err != nil {
		return false, "", err
	}
	defer cloud.Close(e.ctx)
	one1, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	one2, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	queries := []string{
		"SELECT id, title FROM documents WHERE id = " + one1,
		"SELECT id, title FROM documents WHERE id = " + one2,
		"SELECT count(*) FROM documents d JOIN document_versions v ON v.document_id = d.id WHERE d.id IN (" + one1 + ", " + one2 + ")",
		"SELECT d.id, v.id IS NULL FROM documents d LEFT JOIN document_versions v ON v.document_id = d.id AND v.content = 'none' WHERE d.id IN (" + one1 + ", " + one2 + ") ORDER BY 1",
		"SELECT count(*) FROM documents WHERE id IN (SELECT document_id FROM document_versions WHERE document_id IN (" + one1 + ", " + one2 + "))",
		"SELECT id FROM documents WHERE id = " + one1 + " UNION ALL SELECT document_id FROM document_versions WHERE document_id = " + one2 + " ORDER BY 1",
		"WITH x AS (SELECT document_id FROM document_versions WHERE document_id IN (" + one1 + ", " + one2 + ")) SELECT count(*) FROM documents d, x WHERE d.id = x.document_id",
		// a join with an alias of its own hides its tables' names: the condition is not added
		// there (it failed: invalid reference to FROM-clause entry for table "d")
		"SELECT count(*) FROM (documents d JOIN document_versions v ON v.document_id = d.id) j WHERE j.document_id IN (" + one1 + ", " + one2 + ")",
		"SELECT j.id, j.content FROM (documents d JOIN (SELECT document_id AS id, content FROM document_versions) v USING (id)) j WHERE j.id IN (" + one1 + ", " + one2 + ") ORDER BY 1, 2",
		"SELECT count(*) FROM (documents d JOIN document_versions v ON v.document_id = d.id) j JOIN documents e ON e.id = j.document_id WHERE e.id IN (" + one1 + ", " + one2 + ")",
		"SELECT count(*) FROM documents d(i, o) WHERE o IN (1, 2) AND i IN (" + one1 + ", " + one2 + ")",
		"SELECT count(*) FROM (SELECT d.id FROM documents d WHERE d.id IN (" + one1 + ", " + one2 + ")) s",
		"SELECT d.id, l.n FROM documents d, LATERAL (SELECT count(*) AS n FROM document_versions v WHERE v.document_id = d.id) l WHERE d.id IN (" + one1 + ", " + one2 + ") ORDER BY 1",
		"UPDATE documents SET title = title WHERE id IN (" + one1 + ", " + one2 + ") RETURNING id",
	}
	var notes []string
	ok := true
	for _, org := range []string{"1", "2"} {
		if _, err := p.Exec(e.ctx, "SET lokadb.tenant = '"+org+"'"); err != nil {
			return false, "", fmt.Errorf("SET lokadb.tenant: %w", err)
		}
		if _, err := cloud.Exec(e.ctx, "SELECT set_config('lokadb.org', '"+org+"', false)"); err != nil {
			return false, "", err
		}
		same := 0
		for _, q := range queries {
			viaProxy := strings.Join(rowsText(e, p, q), "; ")
			inCloud := strings.Join(rowsText(e, cloud, q), "; ")
			if viaProxy != inCloud || strings.Contains(viaProxy, "error") {
				ok = false
				notes = append(notes, fmt.Sprintf("WRONG under organization %s: %s — through the proxy %q, in the cloud %q", org, q, viaProxy, inCloud))
				continue
			}
			same++
		}
		notes = append(notes, fmt.Sprintf("organization %s: %d of %d statements answer as in the cloud", org, same, len(queries)))
	}
	// the plan: organization 2's local partition alone, no partition to remove at run time
	if _, err := p.Exec(e.ctx, "SET lokadb.tenant = '2'"); err != nil {
		return false, "", err
	}
	plan := strings.Join(rowsText(e, p, "EXPLAIN SELECT title FROM documents WHERE id = "+one2), " | ")
	if !strings.Contains(plan, "documents__o2") || strings.Contains(plan, "documents__cloud") || strings.Contains(plan, "Subplans Removed") {
		ok = false
		notes = append(notes, "WRONG plan of a point read of organization 2: "+plan)
	} else {
		notes = append(notes, "a point read of organization 2 plans its partition alone")
	}
	return ok, strings.Join(notes, "; "), nil
}
