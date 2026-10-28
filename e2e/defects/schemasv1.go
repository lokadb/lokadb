package main

// Block 4, item 9: tables outside schema public. check takes them, enable and move-org move
// them to the same schema at the organization (schemasroute.go: the proxy routes them).

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "X15", name: "tables of schema app (and one of public referencing them): check takes them, a move under writes takes them to schema app at the organization, FKs, CASCADE and RESTRICT work across schemas", run: checkSchemaMove},
		check{id: "X17", name: "the organization's database already has its own app.documents: check reports customer_table_exists for schema app, move-org refuses and leaves that table and the cloud as they were", run: checkOtherSchemaClash},
	)
}

const schemaSpec = `{"tables": [
  {"name": "app.documents", "tenant_column": "org"},
  {"name": "app.items", "parent_table": "app.documents", "parent_column": "document_id"},
  {"name": "remarks", "parent_table": "app.documents", "parent_column": "document_id"}
]}`

func schemaTables(e *env) error {
	return execSQL(e.ctx, e.cloud(),
		`CREATE SCHEMA app AUTHORIZATION app`,
		`CREATE TABLE app.documents (id bigserial PRIMARY KEY, org bigint NOT NULL, title text)`,
		`CREATE INDEX documents_title_idx ON app.documents (title)`,
		`CREATE TABLE app.items (id bigserial PRIMARY KEY, document_id bigint REFERENCES app.documents ON DELETE CASCADE, note text)`,
		`CREATE TABLE remarks (id bigserial PRIMARY KEY, document_id bigint REFERENCES app.documents ON DELETE RESTRICT, body text)`,
		`ALTER TABLE app.documents OWNER TO app`, `ALTER TABLE app.items OWNER TO app`, `ALTER TABLE remarks OWNER TO app`,
		`INSERT INTO app.documents (org, title) SELECT 1 + g % 2, 'd' || g FROM generate_series(1, 100) g`,
		`INSERT INTO app.items (document_id, note) SELECT id, 'i' || id FROM app.documents`,
		`INSERT INTO app.items (document_id, note) SELECT id, 'j' || id FROM app.documents`,
		`INSERT INTO remarks (document_id, body) SELECT id, 'r' || id FROM app.documents WHERE id % 5 = 0`)
}

// X15.
func checkSchemaMove(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	out, code := e.checkWith(schemaSpec)
	note(!reported(out, "table_not_in_public") && !reported(out, "table_outside_public") && code != 3 && code != 5,
		fmt.Sprintf("check: %s (exit %d), table_not_in_public %v", verdict(out), code, reported(out, "table_not_in_public")))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	where := val(e.cloud(), `SELECT string_agg(c.oid::regclass::text, ' ' ORDER BY c.oid::regclass::text) FROM pg_class c
		WHERE c.relname IN ('documents__cloud', 'items__cloud', 'documents__refs', 'remarks__cloud') AND c.relnamespace <> 'public'::regnamespace OR c.relname = 'remarks__cloud'`)
	tables := val(e.cloud(), `SELECT string_agg(table_name, ' ' ORDER BY table_name) FROM lokadb.tables`)
	note(where == "app.documents__cloud app.documents__refs app.items__cloud remarks__cloud" && tables == "app.documents app.items remarks",
		fmt.Sprintf("after enable: helper tables %s; lokadb.tables %s", where, tables))

	// The application writes organization 1 of app.documents all along the move.
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	rows, err := w.Query(e.ctx, "SELECT id FROM app.documents WHERE org = 1 AND id % 5 <> 0")
	if err != nil {
		return false, "", err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return false, "", err
	}
	state := map[int64]string{}
	var mu sync.Mutex
	stop, done := make(chan struct{}), make(chan struct{})
	ops := 0
	go func() {
		defer close(done)
		r := rand.New(rand.NewSource(15))
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			title := fmt.Sprint("x15-", i)
			mu.Lock()
			switch i % 3 {
			case 0:
				var id int64
				if w.QueryRow(e.ctx, "INSERT INTO app.documents (org, title) VALUES (1, $1) RETURNING id", title).Scan(&id) == nil {
					ids = append(ids, id)
					state[id] = title
					ops++
				}
			case 1:
				id := ids[r.Intn(len(ids))]
				if !deleted(state, id) {
					if tag, err := w.Exec(e.ctx, "UPDATE app.documents SET title = $1 WHERE org = 1 AND id = $2", title, id); err == nil && tag.RowsAffected() == 1 {
						state[id] = title
						ops++
					}
				}
			case 2:
				id := ids[r.Intn(len(ids))]
				if tag, err := w.Exec(e.ctx, "DELETE FROM app.documents WHERE org = 1 AND id = $1", id); err == nil && tag.RowsAffected() == 1 {
					state[id] = ""
					ops++
				}
			}
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	out, moveErr := e.move(1)
	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-done
	if moveErr != nil {
		note(false, "move-org failed: "+lastErr(out))
		return false, strings.Join(notes, "; "), nil
	}

	// At the organization: schema app has the documents and items, public the remarks.
	wrong := 0
	got := map[int64]string{}
	cr, err := pgx.Connect(e.ctx, e.customer())
	if err != nil {
		return false, "", err
	}
	defer cr.Close(e.ctx)
	rs, err := cr.Query(e.ctx, "SELECT id, coalesce(title, '') FROM app.documents")
	if err != nil {
		note(false, "app.documents at the organization: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	for rs.Next() {
		var id int64
		var t string
		if err := rs.Scan(&id, &t); err != nil {
			return false, "", err
		}
		got[id] = t
	}
	rs.Close()
	for id, t := range state {
		if g, there := got[id]; t == "" && there || t != "" && g != t {
			wrong++
		}
	}
	cloudLeft := val(e.cloud(), `SELECT count(*) FROM app.documents__cloud WHERE org = 1`)
	docs := val(e.cloud(), `SELECT count(*) FROM app.documents WHERE org = 1`)
	note(wrong == 0 && cloudLeft == "0" && fmt.Sprint(len(got)) == docs,
		fmt.Sprintf("%d writes during the move, %d rows differ at the organization; app.documents there %d, through the cloud %s, left in app.documents__cloud %s", ops, wrong, len(got), docs, cloudLeft))
	items := val(e.customer(), `SELECT count(*) FROM app.items i JOIN app.documents d ON d.id = i.document_id`)
	itemsAll := val(e.customer(), `SELECT count(*) FROM app.items`)
	remarks := val(e.customer(), `SELECT count(*) FROM public.remarks`)
	fks := val(e.customer(), `SELECT string_agg(conrelid::regclass::text || '->' || confrelid::regclass::text || ':' || confdeltype::text, ' ' ORDER BY conrelid::regclass::text) FROM pg_constraint
		WHERE contype = 'f' AND confrelid = 'app.documents'::regclass`)
	idx := val(e.customer(), `SELECT count(*) FROM pg_indexes WHERE schemaname = 'app' AND tablename = 'documents' AND indexname = 'documents_title_idx'`)
	note(items == itemsAll && remarks == "10" && fks == "app.items->app.documents:c remarks->app.documents:r" && idx == "1",
		fmt.Sprintf("at the organization: app.items %s (all with their document: %s), public.remarks %s, FKs %s, index %s", itemsAll, items, remarks, fks, idx))

	// CASCADE and RESTRICT across schemas, for the moved organization and the other.
	del := func(org int, cond string) error {
		_, err := one(e.ctx, e.cloud(), fmt.Sprintf(`WITH d AS (DELETE FROM app.documents WHERE id = (SELECT min(id) FROM app.documents WHERE org = %d AND %s) RETURNING 1) SELECT count(*) FROM d`, org, cond))
		return err
	}
	for _, org := range []int{1, 2} {
		where := fmt.Sprintf("org = %d", org)
		at := e.cloud()
		if org == 1 {
			where, at = "true", e.customer()
		}
		before := val(at, `SELECT count(*) FROM app.items WHERE document_id = (SELECT min(id) FROM app.documents WHERE `+where+` AND id % 5 <> 0 AND title LIKE 'd%')`)
		cascade := del(org, "id % 5 <> 0 AND title LIKE 'd%'")
		after := val(at, `SELECT count(*) FROM app.items i WHERE NOT EXISTS (SELECT 1 FROM app.documents d WHERE d.id = i.document_id)`)
		restrict := del(org, "id % 5 = 0")
		kept := val(e.cloud(), fmt.Sprintf(`SELECT count(*) FROM remarks WHERE org = %d`, org))
		note(cascade == nil && before == "2" && after == "0" && restrict != nil && strings.Contains(restrict.Error(), "remarks") && kept == "10",
			fmt.Sprintf("organization %d: a document deleted (%s): its %s items gone (left without a document %s); one with a remark: %s, remarks %s",
				org, firstErrLineOr(cascade), before, after, firstErrLineOr(restrict), kept))
	}
	return ok, strings.Join(notes, "; "), nil
}

// X17.
func checkOtherSchemaClash(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(),
		`CREATE SCHEMA IF NOT EXISTS app`,
		`CREATE TABLE app.documents (id bigint PRIMARY KEY, mine text)`,
		`INSERT INTO app.documents SELECT g, 'own' FROM generate_series(1, 7) g`); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	out, code := e.checkWith(schemaSpec, "-customer", e.customer(), "-org", "1")
	clash := finding(out, "customer_table_exists")
	note(strings.Contains(clash, "schema app already has a table documents") && reported(out, "customer_schema"),
		fmt.Sprintf("check -customer (exit %d): %s", code, clash))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	mout, err := e.move(1)
	refusal := lastErr(mout)
	note(err != nil && strings.Contains(mout, "schema app already has a table documents"), "move-org: "+map[bool]string{true: "refused: ", false: "not refused: "}[err != nil]+refusal)
	mine := val(e.customer(), `SELECT count(*) || ' ' || min(mine) FROM app.documents`)
	cols := val(e.customer(), `SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute WHERE attrelid = 'app.documents'::regclass AND attnum > 0`)
	inCloud := val(e.cloud(), `SELECT count(*) FROM app.documents__cloud WHERE org = 1`)
	moved := val(e.cloud(), `SELECT count(*) FROM lokadb.orgs WHERE org = 1`)
	note(mine == "7 own" && cols == "id,mine" && inCloud == "50" && moved == "0",
		fmt.Sprintf("the organization's app.documents: %s rows, columns %s; organization 1 in the cloud %s rows, moved %s", mine, cols, inCloud, moved))
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "X22", name: "documents of public and app.documents both in the spec, the organization's schema for public's tables app: move-org refuses before it changes anything (they would be one table there)", run: checkOrgSchemaClash},
	)
}

// X22.
func checkOrgSchemaClash(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [
  {"name": "documents", "tenant_column": "organization_id"},
  {"name": "app.documents", "tenant_column": "org"}
]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	out, err := e.move(1, "-customer-schema", "app")
	there, qerr := one(e.ctx, e.customer(), `SELECT count(*) FROM pg_class WHERE relname IN ('documents', 'documents__o1') AND relkind IN ('r', 'p')`)
	moved, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM lokadb.orgs`)
	if qerr != nil {
		there = "error: " + firstErrLine(qerr)
	}
	ok := err != nil && strings.Contains(out, "would be one table") && there == "0" && moved == "0"
	return ok, fmt.Sprintf("move-org -customer-schema app: %s; tables documents at the organization %s, organizations moved %s",
		map[bool]string{true: "refused: " + lastErr(out), false: "not refused"}[err != nil], there, moved), nil
}
