package proxy

import (
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// r4Session: a session whose database has fanout policy p.
func r4Session(t *testing.T, f *fakeUpstream, meta *Meta, p FanoutPolicy) (*pgproto3.Frontend, *session) {
	cli, cliSrv := tcpPair(t)
	up, upSrv := tcpPair(t)
	f.be = pgproto3.NewBackend(upSrv, upSrv)
	f.stmts, f.portals, f.parseOIDs = map[string]string{}, map[string]string{}, map[string][]uint32{}
	go f.run()
	cfg := Config{Fanout: map[string]FanoutPolicy{"db": p}}
	fo, err := cfg.FanoutFor("db")
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil)), cfg: cfg, fanout: map[string]*Fanout{"db": fo}}
	hj := &pgconn.HijackedConn{Conn: up, Frontend: pgproto3.NewFrontend(up, up)}
	s := newSession(srv, "db", cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, meta, nil)
	go s.run()
	return pgproto3.NewFrontend(cli, cli), s
}

func r4Policy(t *testing.T, p FanoutPolicy) *Fanout {
	f, err := Config{Fanout: map[string]FanoutPolicy{"db": p}}.FanoutFor("db")
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// routing-1: a table decision ("read" / "allow") of the FIRST unhinted table decides for the whole
// statement, also for other LokaDB tables of it that have no decision or are "refuse".
func TestR4DecisionOfOneTableCoversOthers(t *testing.T) {
	m := testMeta()
	cases := []struct {
		tables map[string]string
		mode   string
		sql    string
	}{
		// documents has no decision, mode reject: its rows must not be read everywhere
		{map[string]string{"document_versions": TableRead}, FanoutReject,
			`select v.content, d.title from document_versions v join documents d on d.id = v.document_id where v.content = $1`},
		{map[string]string{"document_versions": TableRead}, FanoutReject,
			`select * from document_versions where content = $1 and document_id in (select id from documents where title = $2)`},
		// documents decided "refuse": refused by its decision ... unless joined to a "read" table first
		{map[string]string{"document_versions": TableRead, "documents": TableRefuse}, FanoutReject,
			`select * from document_versions v, documents d where v.document_id = d.id and d.slug = $1`},
		// a write of documents (no decision) runs everywhere because document_versions is "allow"
		{map[string]string{"document_versions": TableAllow}, FanoutReject,
			`with x as (delete from documents where slug = $1 returning id) select * from document_versions where document_id in (select id from x)`},
		{map[string]string{"document_versions": TableAllow}, FanoutReject,
			`select * from document_versions where content = 'a'; delete from documents where slug = 'x'`},
		{map[string]string{"document_versions": TableAllow, "documents": TableRefuse}, FanoutReject,
			`update document_versions v set content = 'x' from documents d where d.id = v.document_id and d.slug = 'gone'; delete from documents where slug = 'gone'`},
	}
	for i, c := range cases {
		f := r4Policy(t, FanoutPolicy{Mode: c.mode, Tables: c.tables})
		an := analyze(c.sql, m)
		got := f.Decide(c.sql, an)
		t.Logf("case %d: table=%s write=%v gaps=%s verdict=%d", i, fanoutTable(an), an.Write, an.GapText(), got)
		if got == FanoutAllowed || got == FanoutRun {
			t.Errorf("case %d: %s\n  runs in every organization (verdict %d) by the decision on %s, though documents is %q",
				i, c.sql, got, fanoutTable(an), c.tables["documents"])
		}
	}
}

// the same through a session: the statement reaches the cloud under lokadb.org = ” (every organization)
func TestR4DecisionOfOneTableSession(t *testing.T) {
	f := &fakeUpstream{}
	fe, _ := r4Session(t, f, testMeta(), FanoutPolicy{Mode: FanoutReject, Tables: map[string]string{"document_versions": TableAllow, "documents": TableRefuse}})
	sql := `select * from document_versions where content = 'a'; delete from documents where slug = 'x'`
	sendAll(t, fe, &pgproto3.Query{String: sql})
	code := errorCode(t, fe)
	last, ran := orgsBefore(f.log(), "delete from documents")
	t.Logf("code=%q ran=%v last=%q log=%q", code, ran, last, f.log())
	if ran && !refused(f.log(), "delete from documents") {
		t.Errorf("DELETE of documents (decided refuse) ran after %q", last)
	}
}

// routing-1 bis: "refuse" of a table is bypassed under mode log when another LokaDB table comes first.
func TestR4RefuseBypassedUnderLog(t *testing.T) {
	m := testMeta()
	f := r4Policy(t, FanoutPolicy{Mode: FanoutLog, Tables: map[string]string{"documents": TableRefuse}})
	for _, sql := range []string{
		`select * from documents where slug = $1`,
		`select * from document_versions v join documents d on d.id = v.document_id where d.slug = $1`,
		`select * from document_versions where content = 'a'; delete from documents where slug = 'x'`,
	} {
		an := analyze(sql, m)
		if got := f.Decide(sql, an); got != FanoutDenied {
			t.Errorf("%s: documents is decided refuse, verdict %d", sql, got)
		}
	}
}

// the open list names the table nobody decided, not the one decided first
func TestR4OpenListNamesTheUndecidedTable(t *testing.T) {
	sql := `select v.content, d.title from document_versions v join documents d on d.id = v.document_id where v.content = $1`
	f := r4Policy(t, FanoutPolicy{Mode: FanoutReject, Tables: map[string]string{"document_versions": TableRead}})
	an := analyze(sql, testMeta())
	if got := f.Decide(sql, an); got != FanoutRefused {
		t.Fatalf("verdict %d, want refused", got)
	}
	v := f.openView(an)
	if fanoutTable(v) != "documents" {
		t.Errorf("listed under %q, want documents", fanoutTable(v))
	}
	for _, g := range v.Gaps {
		if g.Table == "document_versions" {
			t.Errorf("the decided table is still among the gaps: %+v", v.Gaps)
		}
	}
}
