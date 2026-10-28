package main

// DDL replay through the proxy (research/ANALYSIS.md 5.4: P3, P7–P10).

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "P7", name: "DDL undone by ROLLBACK TO SAVEPOINT is not replayed; the rest is", run: checkSavepointDDL},
		check{id: "P7", name: "DDL of a transaction that failed is not replayed", run: checkFailedTxDDL},
		check{id: "P7", name: "BEGIN; ALTER; COMMIT in one extended-protocol batch is replayed", run: checkPipelinedDDL},
		check{id: "P7", name: "autocommit DDL the cloud refuses is not replayed", run: checkCloudRefusesDDL},
		check{id: "P8", name: "DDL one organization's database refuses is applied nowhere", run: checkPartialDDL},
		check{id: "P9", name: "another session sees the new column at once", run: checkStaleMeta},
		check{id: "P10", name: "TRUNCATE, DROP INDEX, ADD COLUMN … REFERENCES are refused (CREATE INDEX CONCURRENTLY is built since block 7: M6)", run: checkUnreplayable},
		check{id: "P3", name: "a refused statement in a batch ending with Flush is refused", run: checkFlushRefusal},
	)
}

// moved: organization 1 in its own database, the proxy replaying DDL there as the unprivileged role.
func (e *env) moved() (func(), error) {
	if err := e.enable(); err != nil {
		return nil, err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return nil, err
	}
	return e.proxyWith(map[string]string{"1": e.appCustomer()})
}

func (e *env) columnsAt(d string, names ...string) (string, error) {
	return one(e.ctx, d, "SELECT coalesce(string_agg(column_name::text, ',' ORDER BY column_name), '-') FROM information_schema.columns WHERE table_name = 'documents' AND column_name = ANY($1)", names)
}

func (e *env) bothSides(names ...string) (string, error) {
	cl, err := e.columnsAt(e.cloud(), names...)
	if err != nil {
		return "", err
	}
	cu, err := e.columnsAt(e.customer(), names...)
	if err != nil {
		return "", err
	}
	return "cloud " + cl + ", organization " + cu, nil
}

func (e *env) script(stmts ...string) error {
	c, err := e.viaProxy()
	if err != nil {
		return err
	}
	defer c.Close(e.ctx)
	for _, q := range stmts {
		if _, err := simple(e.ctx, c, q); err != nil && !strings.HasPrefix(q, "--may fail") {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

// P7. The replay followed what the client sent: ROLLBACK TO SAVEPOINT (Django's nested
// atomic) did not undo the replayed column.
func checkSavepointDDL(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := e.script("BEGIN", "SAVEPOINT s1", "ALTER TABLE documents ADD COLUMN sp_x text",
		"ROLLBACK TO SAVEPOINT s1", "ALTER TABLE documents ADD COLUMN sp_y text", "COMMIT"); err != nil {
		return false, "", err
	}
	got, err := e.bothSides("sp_x", "sp_y")
	if err != nil {
		return false, "", err
	}
	return got == "cloud sp_y, organization sp_y", got + " (want sp_y on both sides)", nil
}

// P7. A COMMIT of a failed transaction rolls it back, the replay happened anyway.
func checkFailedTxDDL(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, q := range []string{"BEGIN", "ALTER TABLE documents ADD COLUMN ab_z text", "SELECT 1/0", "COMMIT"} {
		_, _ = simple(e.ctx, c, q)
	}
	got, err := e.bothSides("ab_z")
	if err != nil {
		return false, "", err
	}
	return got == "cloud -, organization -", got + " (want the column nowhere)", nil
}

// P7. BEGIN, ALTER and COMMIT pipelined in one batch: the COMMIT was seen before the ALTER ran.
func checkPipelinedDDL(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := raw(e, c, 0,
		&pgproto3.Parse{Name: "b", Query: "BEGIN"}, &pgproto3.Bind{DestinationPortal: "b", PreparedStatement: "b"}, &pgproto3.Execute{Portal: "b"},
		&pgproto3.Parse{Name: "a", Query: "ALTER TABLE documents ADD COLUMN pipe_w text"}, &pgproto3.Bind{DestinationPortal: "a", PreparedStatement: "a"}, &pgproto3.Execute{Portal: "a"},
		&pgproto3.Parse{Name: "c", Query: "COMMIT"}, &pgproto3.Bind{DestinationPortal: "c", PreparedStatement: "c"}, &pgproto3.Execute{Portal: "c"},
		&pgproto3.Sync{}); err != nil {
		return false, "", err
	}
	got, err := e.bothSides("pipe_w")
	if err != nil {
		return false, "", err
	}
	return got == "cloud pipe_w, organization pipe_w", got + " (want the column on both sides)", nil
}

// P7. Autocommit DDL was replayed before the cloud ran it: when the cloud then refused it
// (here: a lock it could not get in time), the organization's database had it alone.
func checkCloudRefusesDDL(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(e.ctx)
	if _, err := tx.Exec(e.ctx, "SELECT count(*) FROM documents__cloud"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, "SET lock_timeout = '300ms'"); err != nil {
		return false, "", err
	}
	_, altErr := simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN lt_v text")
	_ = tx.Rollback(e.ctx)
	got, err := e.bothSides("lt_v")
	if err != nil {
		return false, "", err
	}
	state := "the ALTER failed"
	if altErr == nil {
		state = "the ALTER succeeded"
	}
	return altErr != nil && got == "cloud -, organization -", state + "; " + got, nil
}

// P8. Organizations' databases got the DDL one after the other, each committing on its own:
// when one refused, those before it kept the change and the cloud had none.
func checkPartialDDL(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	second := e.db + "_b"
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"DROP DATABASE IF EXISTS "+second+" WITH (FORCE)", "CREATE DATABASE "+second+" OWNER app"); err != nil {
		return false, "", err
	}
	// as fresh() for the first: its tables in public (block 7)
	if err := execSQL(e.ctx, dsn("customer", second, "postgres"), "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	appB := dsn("customer", second, "lokadb_app")
	if _, err := e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "2",
		"-customer", dsn("customer", second, "postgres"), "-customer-app", appB, "-publisher", e.cloud()); err != nil {
		return false, "", err
	}
	// organization 2's database already has these columns: it refuses the ADD COLUMN
	if err := execSQL(e.ctx, dsn("customer", second, "postgres"),
		"ALTER TABLE documents ADD COLUMN p8_c text", "ALTER TABLE documents ADD COLUMN p8_d text"); err != nil {
		return false, "", err
	}
	stop, err := e.proxyWith(map[string]string{"1": e.appCustomer(), "2": appB})
	if err != nil {
		return false, "", err
	}
	defer stop()
	var notes []string
	ok := true
	for _, col := range []string{"p8_c", "p8_d"} { // the order of the databases varies: two tries
		c, err := e.viaProxy()
		if err != nil {
			return false, "", err
		}
		_, altErr := simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN "+col+" text")
		c.Close(e.ctx)
		got, err := e.bothSides(col)
		if err != nil {
			return false, "", err
		}
		if altErr == nil || got != "cloud -, organization -" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: error %v; %s (organization 1)", col, altErr != nil, got))
	}
	return ok, strings.Join(notes, "; "), nil
}

// P9. Only the session that ran the DDL read the metadata again: another one, already open,
// rewrote an upsert with the old columns.
func checkStaleMeta(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.viaProxy() // open before the change
	if err != nil {
		return false, "", err
	}
	defer a.Close(e.ctx)
	if _, err := simple(e.ctx, a, "SELECT count(*) FROM documents WHERE organization_id = 1"); err != nil {
		return false, "", err
	}
	if err := e.script("ALTER TABLE documents ADD COLUMN p9_n text"); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.customer(), "SELECT min(id) FROM documents")
	if err != nil {
		return false, "", err
	}
	_, upErr := simple(e.ctx, a, "INSERT INTO documents (id, organization_id, title, p9_n) VALUES ("+doc+", 1, 'p9', 'n9') ON CONFLICT (id) DO UPDATE SET p9_n = EXCLUDED.p9_n")
	v, err := one(e.ctx, e.customer(), "SELECT coalesce(p9_n, 'NULL') FROM documents WHERE id = "+doc)
	if err != nil {
		return false, "", err
	}
	errText := "none"
	if upErr != nil {
		errText = oneLine(upErr.Error())
	}
	return upErr == nil && v == "n9", fmt.Sprintf("upsert of the new column by the other session: error %s; value at the organization %s", errText, v), nil
}

// P10. These statements passed the proxy's classification: TRUNCATE emptied the table in
// every organization's database; the others left the organizations' schema behind.
func checkUnreplayable(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	idx, err := one(e.ctx, e.cloud(), "SELECT min(c.relname::text) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = 'documents__cloud'::regclass")
	if err != nil {
		return false, "", err
	}
	ok := true
	var ran []string
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, q := range []string{
		"TRUNCATE documents CASCADE",
		"ALTER TABLE documents ADD COLUMN owner_id bigint REFERENCES users",
		"DROP INDEX " + idx,
	} {
		if _, err := simple(e.ctx, c, q); err == nil {
			ok = false
			ran = append(ran, q)
		}
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	detail := "all refused"
	if len(ran) > 0 {
		detail = "ran: " + strings.Join(ran, " | ")
	}
	return ok && n == "100", detail + "; documents at the organization: " + n, nil
}

// P3. The proxy checked statements only in batches ending with Sync: after Flush a RENAME
// of a LokaDB table went through.
func checkFlushRefusal(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	fe := c.Frontend()
	fe.Send(&pgproto3.Parse{Query: "ALTER TABLE documents RENAME TO documents_renamed"})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Flush{})
	_ = fe.Flush()
	refused := false
	for i := 0; i < 4; i++ {
		msg, err := c.ReceiveMessage(e.ctx)
		if err != nil {
			return false, "", err
		}
		if er, ok := msg.(*pgproto3.ErrorResponse); ok {
			refused = strings.Contains(er.Message, "RENAME")
			break
		}
		if _, ok := msg.(*pgproto3.CommandComplete); ok {
			break
		}
	}
	if _, err := raw(e, c, 0, &pgproto3.Sync{}); err != nil {
		return false, "", err
	}
	name, err := one(e.ctx, e.cloud(), "SELECT coalesce(to_regclass('documents')::text, 'gone')")
	if err != nil {
		return false, "", err
	}
	return refused && name == "documents", fmt.Sprintf("refused: %v; the table in the cloud: %s", refused, name), nil
}
