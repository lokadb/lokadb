package main

// Block 14 (load and scale), handed to block 2 with its fix: COPY to and from LokaDB tables
// had never been sent through the proxy. Uses outcome() of load.go.

import (
	"bytes"
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "LD1", name: "COPY to and from LokaDB tables through the proxy: rows in their organization; a whole-table COPY is refused like the SELECT, not silently empty", run: checkCopy},
	)
}

// proxyReject: the proxy with the default policy for statements of no organization.
func (e *env) proxyReject() (func(), error) {
	return e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
}

// LD1. The proxy finds the organization in SELECT/INSERT/UPDATE/DELETE; COPY had never been
// sent through it.
func checkCopy(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	count := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	// reference: a SELECT of every organization is refused under reject
	_, selErr := simple(e.ctx, c, "SELECT count(*) FROM documents")
	notes = append(notes, "SELECT of all documents: "+outcome(selErr))

	// COPY out of one organization: the query form is analyzed like a SELECT
	var buf bytes.Buffer
	_, err = c.CopyTo(e.ctx, &buf, "COPY (SELECT id, title FROM documents WHERE organization_id = 1) TO STDOUT")
	rows := strings.Count(buf.String(), "\n")
	note(err == nil && rows == 100 || err != nil && strings.HasPrefix(outcome(err), "refused"),
		fmt.Sprintf("COPY (SELECT ... organization 1) TO STDOUT: %s, %d rows (want 100)", outcome(err), rows))

	// COPY of the whole table: every organization, the same as the refused SELECT
	buf.Reset()
	_, err = c.CopyTo(e.ctx, &buf, "COPY documents TO STDOUT")
	rows = strings.Count(buf.String(), "\n")
	bypass := err == nil && selErr != nil
	// (after enable the table is a partitioned parent: COPY of the table itself reads ONLY the
	// parent, which holds no rows; under RLS Postgres turns it into such a SELECT)
	note(!bypass, fmt.Sprintf("COPY documents TO STDOUT: %s, %d rows (SELECT of all documents %s; 200 rows exist)", outcome(err), rows, map[bool]string{true: "refused", false: "allowed"}[selErr != nil]))

	// COPY into the table: a row of the moved organization 1, one of the cloud organization 2
	before1, before2 := count(e.customer(), "SELECT count(*) FROM documents"), count(e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	_, err = c.CopyFrom(e.ctx, strings.NewReader("1\t2\tk1-org1\n2\t2\tk1-org2\n"), "COPY documents (organization_id, user_id, title) FROM STDIN")
	in1 := count(e.customer(), "SELECT count(*) FROM documents WHERE title = 'k1-org1'")
	in2 := count(e.cloud(), "SELECT count(*) FROM documents WHERE title = 'k1-org2' AND organization_id = 2")
	inCloud1 := count(e.cloud(), "SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid WHERE title = 'k1-org1' AND c.relkind = 'r'")
	if err == nil {
		note(in1 == "1" && in2 == "1" && inCloud1 == "0", fmt.Sprintf("COPY documents FROM STDIN: ok; organization 1's row at the customer %s, in the cloud %s; organization 2's row in the cloud %s", in1, inCloud1, in2))
	} else {
		note(in1 == "0" && in2 == "0" && inCloud1 == "0", "COPY documents FROM STDIN: "+outcome(err)+"; rows written: "+in1+"/"+in2)
	}
	notes = append(notes, "documents at the customer "+before1+" -> "+count(e.customer(), "SELECT count(*) FROM documents")+
		", organization 2 "+before2+" -> "+count(e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2"))

	// COPY into the child table: the organization comes from the parent document
	doc1 := count(e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	_, err = c.CopyFrom(e.ctx, strings.NewReader(doc1+"\tk1-v1\n"), "COPY document_versions (document_id, content) FROM STDIN")
	v1 := count(e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'k1-v1'")
	vWrong := count(e.cloud(), "SELECT count(*) FROM document_versions v JOIN pg_class c ON c.oid = v.tableoid WHERE content = 'k1-v1' AND c.relkind = 'r'")
	if err == nil {
		note(v1 == "1" && vWrong == "0", fmt.Sprintf("COPY document_versions FROM STDIN (a version of organization 1's document): ok; at the customer %s, in the cloud %s", v1, vWrong))
	} else {
		note(v1 == "0" && vWrong == "0", "COPY document_versions FROM STDIN: "+outcome(err))
	}
	// the connection is still usable
	res, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 2")
	note(err == nil && len(res) == 1, "a query after the COPYs: "+outcome(err))
	return ok, strings.Join(notes, "; "), nil
}
