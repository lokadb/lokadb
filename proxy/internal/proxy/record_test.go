package proxy

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func recorded(r *recorder, db string) (map[string]int64, map[string]int64) {
	stmts, txs := map[string]int64{}, map[string]int64{}
	for _, s := range r.dbs[db].stmts {
		stmts[s.SQL] = s.Count
	}
	for _, tx := range r.dbs[db].txs {
		k := ""
		for i, s := range tx.Statements {
			if i > 0 {
				k += " ; "
			}
			k += s
		}
		txs[k] = tx.Count
	}
	return stmts, txs
}

func TestRecorderTransactions(t *testing.T) {
	r := &recorder{dbs: map[string]*recDB{}}
	tr := newTxTracker(r, "app")
	send := func(msgs ...pgproto3.FrontendMessage) {
		for _, m := range msgs {
			tr.observe(m)
		}
	}
	// autocommit simple queries: one transaction each, constants normalized away
	send(&pgproto3.Query{String: "select * from documents where id = 1"})
	send(&pgproto3.Query{String: "select * from documents where id = 2"})
	// explicit block over several simple queries, savepoint ignored
	send(&pgproto3.Query{String: "BEGIN"}, &pgproto3.Query{String: "insert into projects (name) values ('a')"},
		&pgproto3.Query{String: "SAVEPOINT s1"}, &pgproto3.Query{String: "insert into documents (organization_id) values (1)"},
		&pgproto3.Query{String: "COMMIT"})
	// extended protocol: Executes before one Sync form one implicit transaction
	send(&pgproto3.Parse{Name: "s1", Query: "update documents set title = $1 where id = $2"},
		&pgproto3.Parse{Name: "s2", Query: "update projects set name = $1 where id = $2"},
		&pgproto3.Bind{PreparedStatement: "s1"}, &pgproto3.Execute{},
		&pgproto3.Bind{PreparedStatement: "s2"}, &pgproto3.Execute{}, &pgproto3.Sync{})
	// an explicit block spanning several Syncs (pgx BeginFunc), the same statement twice
	send(&pgproto3.Parse{Query: "begin"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{},
		&pgproto3.Bind{PreparedStatement: "s1"}, &pgproto3.Execute{}, &pgproto3.Sync{},
		&pgproto3.Bind{PreparedStatement: "s1"}, &pgproto3.Execute{}, &pgproto3.Sync{},
		&pgproto3.Parse{Query: "commit"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	// several statements in one simple query share its implicit transaction
	send(&pgproto3.Query{String: "delete from documents where id = 5; delete from projects where id = 6"})

	stmts, txs := recorded(r, "app")
	wantStmts := map[string]int64{
		"select * from documents where id = $1":               2,
		"insert into projects (name) values ($1)":             1,
		"insert into documents (organization_id) values ($1)": 1,
		"update documents set title = $1 where id = $2":       3,
		"update projects set name = $1 where id = $2":         1,
		"delete from documents where id = $1":                 1,
		"delete from projects where id = $1":                  1,
	}
	wantTxs := map[string]int64{
		"select * from documents where id = $1":                                                         2,
		"insert into projects (name) values ($1) ; insert into documents (organization_id) values ($1)": 1,
		"update documents set title = $1 where id = $2 ; update projects set name = $1 where id = $2":   1,
		"update documents set title = $1 where id = $2":                                                 1,
		"delete from documents where id = $1 ; delete from projects where id = $1":                      1,
	}
	if !reflect.DeepEqual(stmts, wantStmts) {
		t.Errorf("statements:\n got %v\nwant %v", stmts, wantStmts)
	}
	if !reflect.DeepEqual(txs, wantTxs) {
		t.Errorf("transactions:\n got %v\nwant %v", txs, wantTxs)
	}
}

func TestRecorderFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workload.json")
	r := newRecorder(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr := newTxTracker(r, "app")
	tr.observe(&pgproto3.Query{String: "select * from documents where id in (1, 2)"})
	tr.observe(&pgproto3.Query{String: "select * from documents where id in (3, 4, 5)"})
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	// a restarted proxy continues the file: same-shape transactions keep adding up
	r2 := newRecorder(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	newTxTracker(r2, "app").observe(&pgproto3.Query{String: "select * from documents where id in (6)"})
	stmts, txs := recorded(r2, "app")
	if len(txs) != 1 || len(stmts) != 3 {
		t.Fatalf("after reload: %d transaction shapes (want 1), %d statements (want 3): %v %v", len(txs), len(stmts), txs, stmts)
	}
	for _, n := range txs {
		if n != 3 {
			t.Errorf("transaction shape count %d, want 3", n)
		}
	}
}

// Repeats: how many times each statement ran in the transactions of a shape (check -rtt
// counts N+1 chains by it), summed by fingerprint.
func TestRecorderRepeats(t *testing.T) {
	r := &recorder{dbs: map[string]*recDB{}}
	tr := newTxTracker(r, "app")
	send := func(msgs ...pgproto3.FrontendMessage) {
		for _, m := range msgs {
			tr.observe(m)
		}
	}
	tx := func(rows int, ids ...string) {
		send(&pgproto3.Query{String: "BEGIN"}, &pgproto3.Query{String: "select id from projects where organization_id = 1"})
		for i := 0; i < rows; i++ {
			send(&pgproto3.Query{String: "select * from documents where id = 7"})
		}
		for _, in := range ids { // texts that differ only in the list's length share a fingerprint
			send(&pgproto3.Query{String: "select * from comments where id in (" + in + ")"})
		}
		send(&pgproto3.Query{String: "COMMIT"})
	}
	tx(1, "1")
	tx(20, "1, 2", "3")
	if len(r.dbs["app"].txs) != 1 {
		t.Fatalf("%d shapes, want 1", len(r.dbs["app"].txs))
	}
	for _, s := range r.dbs["app"].txs {
		if !reflect.DeepEqual(s.Repeats, []int64{2, 21, 3}) || s.RepeatsTx != 2 || s.Count != 2 {
			t.Errorf("statements %v: repeats %v of %d transactions (count %d), want [2 21 3] of 2", s.Statements, s.Repeats, s.RepeatsTx, s.Count)
		}
	}
}

// A recording made before Repeats was kept: its shapes count on, their repeats start at zero
// with RepeatsTx 0, so the average covers only what was counted.
func TestRecorderRepeatsOldFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workload.json")
	old := `{"version": 1, "databases": {"app": {"statements": [{"sql": "select * from documents where id = $1", "count": 10}],
		"transactions": [{"statements": ["select * from documents where id = $1"], "count": 10}]}}}`
	if err := os.WriteFile(path, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	r := newRecorder(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	newTxTracker(r, "app").observe(&pgproto3.Query{String: "select * from documents where id = 3"})
	for _, s := range r.dbs["app"].txs {
		if s.Count != 11 || s.RepeatsTx != 1 || !reflect.DeepEqual(s.Repeats, []int64{1}) {
			t.Errorf("count %d, repeats %v of %d, want 11, [1] of 1", s.Count, s.Repeats, s.RepeatsTx)
		}
	}
}

// flush serializes copies made under the lock while record() goes on (go test -race).
func TestRecorderRepeatsFlushRace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workload.json")
	r := newRecorder(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr := newTxTracker(r, "app")
		for i := 0; i < 300; i++ {
			tr.observe(&pgproto3.Query{String: "select * from documents where id = 1; select * from documents where id = 2"})
		}
	}()
	for i := 0; i < 30; i++ {
		r.mu.Lock()
		r.dirty = true
		r.mu.Unlock()
		if err := r.flush(); err != nil {
			t.Fatal(err)
		}
	}
	<-done
}

// Form: how the client sent a shape. The same statements sent in a block, as a pipeline and
// as one simple query are three shapes.
func TestRecorderForm(t *testing.T) {
	r := &recorder{dbs: map[string]*recDB{}}
	tr := newTxTracker(r, "app")
	send := func(msgs ...pgproto3.FrontendMessage) {
		for _, m := range msgs {
			tr.observe(m)
		}
	}
	two := []pgproto3.FrontendMessage{
		&pgproto3.Parse{Name: "a", Query: "select * from documents where id = $1"}, &pgproto3.Parse{Name: "b", Query: "update documents set title = $1 where id = $2"},
	}
	send(two...)
	send(&pgproto3.Bind{PreparedStatement: "a"}, &pgproto3.Execute{}, &pgproto3.Bind{PreparedStatement: "b"}, &pgproto3.Execute{}, &pgproto3.Sync{})
	send(&pgproto3.Query{String: "BEGIN"}, &pgproto3.Query{String: "select * from documents where id = 1"},
		&pgproto3.Query{String: "update documents set title = 'x' where id = 1"}, &pgproto3.Query{String: "COMMIT"})
	send(&pgproto3.Query{String: "select * from documents where id = 1; update documents set title = 'x' where id = 1"})
	forms := map[string]int64{}
	for _, tx := range r.dbs["app"].txs {
		forms[tx.Form] += tx.Count
	}
	if !reflect.DeepEqual(forms, map[string]int64{"batch": 1, "block": 1, "script": 1}) {
		t.Errorf("forms %v, want one batch, one block, one script", forms)
	}
}

// PR-12 of the protocol review: a statement the parser does not take is recorded by its size,
// not with its literals (the clients' data).
func TestRecorderUnparsedHasNoLiterals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workload.json")
	r := newRecorder(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tr := newTxTracker(r, "app")
	secret := "select * from documents where email = 'anna@example.com' and note = $$secret-text$$ whereee id = 7"
	tr.observe(&pgproto3.Query{String: secret})
	tr.observe(&pgproto3.Query{String: secret})
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"anna@example.com", "secret-text", "documents"} {
		if strings.Contains(string(b), leak) {
			t.Errorf("the recording holds %q:\n%s", leak, b)
		}
	}
	want := fmt.Sprintf("<%d bytes that do not parse>", len(secret))
	stmts, txs := recorded(r, "app")
	if stmts[want] != 2 || len(txs) != 1 {
		t.Errorf("statements %v, transactions %v, want %q twice in one shape", stmts, txs, want)
	}
	// read back from the file, the placeholder is its own key: the shape keeps counting
	r2 := newRecorder(path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	newTxTracker(r2, "app").observe(&pgproto3.Query{String: secret})
	if _, txs := recorded(r2, "app"); len(txs) != 1 {
		t.Errorf("after reload %d shapes, want 1: %v", len(txs), txs)
	}
}

// Third review of check, H3 and M2: a transaction across organizations (lokadb.tenant = '*')
// and one that names an organization are two forms in the recording, the '*' kept; a
// set_config of lokadb.tenant keeps the setting's name, by a constant or by its Bind.
func TestRecorderTenantStar(t *testing.T) {
	r := &recorder{dbs: map[string]*recDB{}}
	tr := newTxTracker(r, "app")
	send := func(msgs ...pgproto3.FrontendMessage) {
		for _, m := range msgs {
			tr.observe(m)
		}
	}
	for _, v := range []string{"'42'", "'*'", "'7'"} {
		send(&pgproto3.Query{String: "BEGIN"}, &pgproto3.Query{String: "SET LOCAL lokadb.tenant = " + v},
			&pgproto3.Query{String: "select count(*) from documents"}, &pgproto3.Query{String: "COMMIT"})
	}
	send(&pgproto3.Query{String: "select set_config('lokadb.tenant', '*', true)"})
	send(&pgproto3.Query{String: "select set_config('TimeZone', 'UTC', false)"})
	send(&pgproto3.Parse{Name: "sc", Query: "select set_config($1, $2, $3)"},
		&pgproto3.Bind{PreparedStatement: "sc", Parameters: [][]byte{[]byte("lokadb.tenant"), []byte("*"), []byte("true")}}, &pgproto3.Execute{}, &pgproto3.Sync{},
		&pgproto3.Bind{PreparedStatement: "sc", Parameters: [][]byte{[]byte("lokadb.tenant"), []byte("9"), []byte("true")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	stmts, txs := recorded(r, "app")
	want := map[string]int64{
		"SET LOCAL lokadb.tenant = '<organization>' ; select count(*) from documents": 2,
		"SET LOCAL lokadb.tenant = '*' ; select count(*) from documents":              1,
	}
	for k, n := range want {
		if txs[k] != n {
			t.Errorf("transaction form %q: %d, want %d (all: %v)", k, txs[k], n, txs)
		}
	}
	for s, n := range map[string]int64{"SELECT set_config('lokadb.tenant', '*', $1)": 2, "SELECT set_config('lokadb.tenant', '<organization>', $1)": 1, "select set_config($1, $2, $3)": 1} {
		if stmts[s] != n {
			t.Errorf("statement %q: %d, want %d (all: %v)", s, stmts[s], n, stmts)
		}
	}
	for s := range stmts {
		if strings.Contains(s, "'42'") || strings.Contains(s, "'7'") || strings.Contains(s, "'9'") {
			t.Errorf("an organization's value is recorded: %q", s)
		}
	}
}

// A recording of before 26.10 kept "SET LOCAL lokadb.tenant = $1" for '*' and for an
// organization alike: read again, it stays a form of its own, neither named nor '*'.
func TestRecorderTenantOldRecording(t *testing.T) {
	for _, text := range []string{"SET LOCAL lokadb.tenant = $1", "SELECT set_config('lokadb.tenant', $1, $2)"} {
		k := statementKeys(text)
		if len(k) != 1 || k[0].norm != text || !strings.HasSuffix(k[0].fp, recordUnknown) {
			t.Errorf("%s read again: %+v, want itself with a fingerprint of its own (%s)", text, k, recordUnknown)
		}
	}
	org, old := statementKeys("SET LOCAL lokadb.tenant = '42'"), statementKeys("SET LOCAL lokadb.tenant = $1")
	if org[0].fp == old[0].fp || org[0].norm == old[0].norm {
		t.Errorf("an organization's and an old recording's form are one: %+v, %+v", org, old)
	}
}
