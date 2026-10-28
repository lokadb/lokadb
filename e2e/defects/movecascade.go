package main

// Block 5 (moving an organization): ON DELETE CASCADE between LokaDB tables after the move
// (block 17's U2): the children's rows leave <child>__refs too.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV18", name: "after the move a document deleted through the proxy takes its versions out of document_versions__refs, by a role with no right on document_versions", run: checkCascadeRefs},
	)
}

// MV18. document_versions references documents ON DELETE CASCADE. After the move a role
// with SELECT and DELETE on documents only deletes one of organization 1's documents through
// the proxy. Before: the organization's database cascaded to the versions itself, the cloud
// never saw them go, and their rows stayed in document_versions__refs: a version of the same
// id in another organization then failed on __refs (unique). Now the cloud deletes the versions
// first, as the owner of document_versions (as an FK's action runs).
func checkCascadeRefs(e *env) (bool, string, error) {
	fk, err := one(e.ctx, e.cloud(), "SELECT conname FROM pg_constraint WHERE conrelid = 'document_versions'::regclass AND contype = 'f'")
	if err != nil {
		return false, "", err
	}
	deleter := "mv18d_" + e.db
	if err := role(e, "cloud", deleter); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE document_versions DROP CONSTRAINT "+fk,
		"ALTER TABLE document_versions ADD FOREIGN KEY (document_id) REFERENCES documents ON DELETE CASCADE",
		"GRANT SELECT, DELETE ON documents TO "+deleter); err != nil {
		return false, "", err
	}
	stop, err := e.moved()
	if err != nil {
		return false, "move-org failed: " + firstErrLine(err), nil
	}
	defer stop()
	// the proxy reads lokadb's metadata as the session's role
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON ALL TABLES IN SCHEMA lokadb TO "+deleter); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.customer(), "SELECT min(document_id)::text FROM document_versions")
	if err != nil {
		return false, "", err
	}
	versions, err := one(e.ctx, e.customer(), "SELECT string_agg(id::text, ',') FROM document_versions WHERE document_id = "+doc)
	if err != nil {
		return false, "", err
	}
	del := "deleted"
	c, err := e.viaProxyAs(deleter)
	if err != nil {
		del = "connect: " + short(err)
	} else {
		defer c.Close(e.ctx)
		res, err := simple(e.ctx, c, "DELETE FROM documents WHERE organization_id = 1 AND id = "+doc)
		switch {
		case err != nil:
			del = "error: " + short(err)
		case len(res) == 1:
			del = res[0].CommandTag.String()
		}
	}
	there, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE document_id = "+doc)
	refs, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__refs WHERE id IN ("+versions+")")
	// the same id in organization 2 (still in the cloud)
	reused := "inserted"
	first := strings.Split(versions, ",")[0]
	if err := execSQL(e.ctx, e.cloud(), fmt.Sprintf(
		// past the proxy the child's organization is given (the proxy sets it from the parent)
		"INSERT INTO document_versions (id, document_id, content, organization_id) SELECT %s, min(id), 'mv18', 2 FROM documents WHERE organization_id = 2", first)); err != nil {
		reused = "refused: " + firstErrLine(err)
	}
	ok := del == "DELETE 1" && there == "0" && refs == "0" && reused == "inserted"
	return ok, fmt.Sprintf("document %s deleted through the proxy by a role with no right on document_versions: %s; its versions (%s) at the organization: %s, in document_versions__refs: %s; version %s again in organization 2: %s",
		doc, del, versions, there, refs, first, reused), nil
}
