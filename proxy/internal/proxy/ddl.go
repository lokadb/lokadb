package proxy

// Replaying the DDL of LokaDB tables in organizations' databases.
//
// A statement counts once the cloud has run it: the response of each statement (or simple
// query) applies its events (TxEvent) to the session's pending DDL, so a ROLLBACK TO
// SAVEPOINT or a failed transaction drops what it undid. At the commit point (a COMMIT, or
// the Sync or end of string of an implicit transaction) the proxy stops right before it,
// runs the pending DDL in every organization's database in a transaction left open, lets
// the cloud commit, and only then commits those (or rolls them back). The client's
// ReadyForQuery, and whatever it sent after the commit point, wait for those commits.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/ext"
)

// metaTTL: metadata older than this is read again (DDL by other proxies, enable, move-org).
const metaTTL = 30 * time.Second

type savept struct {
	name    string
	pending int // len(pending) when it was created
}

// applyEvents records what a statement the cloud ran successfully did (relay goroutine).
func (s *session) applyEvents(evs []TxEvent) {
	s.ddlMu.Lock()
	defer s.ddlMu.Unlock()
	find := func(name string) int {
		for i := len(s.savepts) - 1; i >= 0; i-- {
			if s.savepts[i].name == name {
				return i
			}
		}
		return -1
	}
	for _, e := range evs {
		switch e.Kind {
		case 'D':
			s.ddlPending = append(s.ddlPending, e.SQL)
		case 'S':
			s.savepts = append(s.savepts, savept{e.SQL, len(s.ddlPending)})
		case 'R':
			if i := find(e.SQL); i >= 0 {
				s.savepts = s.savepts[:i]
			}
		case 'T':
			if i := find(e.SQL); i >= 0 {
				s.ddlPending = s.ddlPending[:s.savepts[i].pending]
				s.savepts = s.savepts[:i+1]
			}
		case 'X', 'C': // COMMIT: replayed before it ran (or rolled back: the transaction had failed)
			s.ddlPending, s.savepts = nil, nil
		}
	}
}

func (s *session) takePending() []string {
	s.ddlMu.Lock()
	defer s.ddlMu.Unlock()
	p := s.ddlPending
	s.ddlPending = nil
	return p
}

func (s *session) hasPending() bool {
	s.ddlMu.Lock()
	defer s.ddlMu.Unlock()
	return len(s.ddlPending) > 0
}

func hasEvent(evs []TxEvent, kind byte) bool {
	for _, e := range evs {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

// ddlOf: the DDL that survives evs on its own (a string run as one transaction).
func ddlOf(evs []TxEvent) []string {
	tmp := &session{}
	tmp.applyEvents(evs)
	return tmp.ddlPending
}

// orgSchema: the schema of org's tables in its database (lokadb.orgs).
func (s *session) orgSchema(org string) string {
	if s.meta != nil && s.meta.Schemas[org] != "" {
		return s.meta.Schemas[org]
	}
	return "public"
}

// custTx: the DDL run, uncommitted, in every organization's database.
type custTx struct {
	s     *session
	conns map[string]*pgconn.PgConn
}

// replayTarget: an organization's database the DDL is replayed in. dsn holds the password of
// the organization's role: it lives for one replay and is never logged.
type replayTarget struct{ org, schema, dsn string }

// libpqKeys: the options of a postgres_fdw server or user mapping that are libpq's (the
// others, fetch_size and the like, are postgres_fdw's own).
var libpqKeys = map[string]bool{
	"host": true, "hostaddr": true, "port": true, "dbname": true, "user": true, "password": true,
	"connect_timeout": true, "sslmode": true, "sslkey": true, "sslcert": true, "sslrootcert": true,
	"sslpassword": true, "sslsni": true, "target_session_attrs": true, "application_name": true, "options": true,
}

// OptionsDSN makes a libpq DSN of FDW options ("key=value"), keeping only libpq's keys:
// the database of an organization as its FDW server and user mapping reach it.
func OptionsDSN(opts ...[]string) string {
	var parts []string
	for _, list := range opts {
		for _, o := range list {
			k, v, ok := strings.Cut(o, "=")
			if !ok || !libpqKeys[k] {
				continue
			}
			parts = append(parts, k+"='"+strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v)+"'")
		}
	}
	return strings.Join(parts, " ")
}

// replayTargets: where the DDL goes, read in the cloud transaction that runs it (so an
// organization moved a moment ago is there, one moved back is not): every organization of
// lokadb.orgs, its database as its FDW server and user mapping reach it (lokadb.replay_targets),
// or as customers in the config says, which wins (the proxy may reach it at another address).
// A moved organization with neither is an error: the DDL would leave it behind. A database
// enabled before replay_targets existed: customers, for the organizations of lokadb.orgs.
func (s *session) replayTargets(query func(string) ([][]string, error)) ([]replayTarget, error) {
	conf := s.srv.ownerDSNs(s.db) // the control plane's credentials over the config's (stage 9a)
	rows, err := query(fmt.Sprintf(`SELECT coalesce(has_function_privilege(to_regprocedure('lokadb.replay_targets()'), 'EXECUTE')::text, 'none'),
		coalesce(has_table_privilege(to_regclass('lokadb.orgs'), 'SELECT')::text, 'none'),
		EXISTS (SELECT FROM pg_locks WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			AND classid = %d AND objid = %d AND objsubid = 1 AND granted)::text`, moveLockKey>>32, moveLockKey&0xffffffff))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || len(rows[0]) < 3 { // no answer to read lokadb's state by: the config as it is
		rows = [][]string{{"none", "none", "false"}}
	}
	fn, orgs := rows[0][0], rows[0][1]
	if rows[0][2] == "true" {
		// The moving organization's tables were made from the schema the move read, and it
		// is not in lokadb.orgs before the switch: the DDL would reach neither its copy nor
		// its database.
		return nil, replayRefusal("move-org is running against this database: schema changes of LokaDB tables are refused until it ends; run the migration again then")
	}
	var ts []replayTarget
	switch {
	case fn == "true":
		rows, err = query(`SELECT org::text, org_schema, coalesce(array_to_json(server_options), '[]')::text,
			coalesce(array_to_json(mapping_options), '[]')::text FROM lokadb.replay_targets()`)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			var srv, um []string
			if err := json.Unmarshal([]byte(r[2]), &srv); err != nil {
				return nil, err
			}
			if err := json.Unmarshal([]byte(r[3]), &um); err != nil {
				return nil, err
			}
			t := replayTarget{org: r[0], schema: r[1], dsn: conf[r[0]]}
			if t.dsn == "" && len(srv) > 0 && len(um) > 0 {
				t.dsn = OptionsDSN(srv, um)
			}
			ts = append(ts, t)
		}
	case orgs == "true":
		if fn == "false" {
			s.log.Warn("ddl replay: this role may not call lokadb.replay_targets(): only the organizations of customers in the config get the DDL")
		}
		if rows, err = query(`SELECT org::text FROM lokadb.orgs ORDER BY org`); err != nil {
			return nil, err
		}
		for _, r := range rows {
			ts = append(ts, replayTarget{org: r[0], schema: s.orgSchema(r[0]), dsn: conf[r[0]]})
		}
	default: // no lokadb.orgs to go by: the config as it is
		for org, dsn := range conf {
			ts = append(ts, replayTarget{org: org, schema: s.orgSchema(org), dsn: dsn})
		}
		return ts, nil
	}
	moved := map[string]bool{}
	for _, t := range ts {
		moved[t.org] = true
		if t.dsn == "" {
			return nil, replayRefusal(fmt.Sprintf("the schema change cannot be replayed in organization %s's database: no credentials for it (no user mapping of its FDW server visible to lokadb.replay_targets(), no customers entry in the proxy's config)", t.org))
		}
	}
	for org := range conf {
		if !moved[org] {
			s.log.Info("organization is not moved: DDL not replayed there", "org", org)
		}
	}
	return ts, nil
}

// moveLockKey: the advisory lock a running move-org holds in the cloud database
// (admin.moveLockKey: "lokadb").
const moveLockKey int64 = 0x62796f6462

// replayRefusal: the proxy refuses the schema change before any organization's database saw it.
type replayRefusal string

func (r replayRefusal) Error() string { return string(r) }

// refusedMsg: what the client is told when the replay could not be prepared; outcome: what
// became of the cloud's side.
func refusedMsg(err error, outcome string) string {
	var r replayRefusal
	if errors.As(err, &r) {
		return "lokadb: " + string(r) + " (" + outcome + ")"
	}
	return "lokadb: an organization's database refused the schema change, " + outcome + ": " + err.Error()
}

// orgDDL: the schema change st as it runs in an organization's database whose tables are in
// schema sch (review R2). The cloud's public is sch there (orgSearchPath): an unqualified name
// finds the table through the search path, but public.t names public there, where move-org
// leaves the customer's own table of that name (it takes schema lokadb just when there is one,
// admin/custschema.go). So every LokaDB table of public, and index of one, that st names as
// public.x becomes sch.x, by the parse tree (relations, and schema.table.column references);
// types and functions stay in public, where move-org makes them. st is returned as it is when
// it names none.
func orgDDL(st, sch string, m *Meta) (string, error) {
	if op, _ := extStatement(st); op != "" {
		return orgExtension(st, sch)
	}
	if sch == "" || sch == "public" || m == nil || !strings.Contains(strings.ToLower(st), "public") {
		return st, nil
	}
	tree, err := pg.Parse(st)
	if err != nil {
		return "", err
	}
	lokadb := func(name string) bool {
		return m.Tables[name] != nil || m.indexOf("public", name) != ""
	}
	changed := false
	for _, raw := range tree.Stmts {
		walk(raw.ProtoReflect(), func(msg proto.Message) {
			switch x := msg.(type) {
			case *pg.RangeVar:
				if x.Schemaname == "public" && lokadb(x.Relname) {
					x.Schemaname, changed = sch, true
				}
			case *pg.ColumnRef:
				if n := len(x.Fields); n >= 3 {
					if s, r := x.Fields[n-3].GetString_(), x.Fields[n-2].GetString_(); s != nil && r != nil && s.Sval == "public" && m.Tables[r.Sval] != nil {
						x.Fields[n-3], changed = strNode(sch), true
					}
				}
			}
		})
	}
	if !changed {
		return st, nil
	}
	return pg.Deparse(tree)
}

// extStatement: what st does to an extension: "create", "alter" (UPDATE) or "drop", and its
// name; "" for any other statement.
func extStatement(st string) (op, name string) {
	low := strings.ToLower(strings.TrimSpace(st))
	if !strings.HasPrefix(low, "create") && !strings.HasPrefix(low, "alter") && !strings.HasPrefix(low, "drop") || !strings.Contains(low, "extension") {
		return "", ""
	}
	tree, err := pg.Parse(st)
	if err != nil || len(tree.Stmts) != 1 {
		return "", ""
	}
	switch n := tree.Stmts[0].Stmt; {
	case n.GetCreateExtensionStmt() != nil:
		return "create", n.GetCreateExtensionStmt().Extname
	case n.GetAlterExtensionStmt() != nil:
		return "alter", n.GetAlterExtensionStmt().Extname
	case n.GetDropStmt() != nil && n.GetDropStmt().RemoveType == pg.ObjectType_OBJECT_EXTENSION && len(n.GetDropStmt().Objects) > 0:
		return "drop", objName(n.GetDropStmt().Objects[0])
	}
	return "", ""
}

// orgExtension: an extension statement of the cloud as it runs in an organization's database
// whose tables are in schema sch (extensions after a move). CREATE EXTENSION gets IF NOT EXISTS
// (move-org installs the extensions the tables use; the cloud's CREATE may come after), and
// WITH SCHEMA public becomes the organization's schema as move-org does it; without SCHEMA the
// replay's search_path puts it there. DROP EXTENSION is IF EXISTS and RESTRICT there: the
// extension goes only if nothing in the organization's database uses it. Names that are
// the cloud's own (ext.CloudOnly) are not replayed at all (analyze.go).
func orgExtension(st, sch string) (string, error) {
	tree, err := pg.Parse(st)
	if err != nil {
		return "", err
	}
	n := tree.Stmts[0].Stmt
	switch {
	case n.GetCreateExtensionStmt() != nil:
		c := n.GetCreateExtensionStmt()
		c.IfNotExists = true
		if sch != "" && sch != "public" {
			for _, o := range c.Options {
				if de := o.GetDefElem(); de != nil && de.Defname == "schema" && de.Arg.GetString_().GetSval() == "public" {
					de.Arg = strNode(sch)
				}
			}
		}
	case n.GetDropStmt() != nil:
		d := n.GetDropStmt()
		d.MissingOk, d.Behavior = true, pg.DropBehavior_DROP_RESTRICT
	}
	return pg.Deparse(tree)
}

// extSchema: the schema CREATE EXTENSION st names ("" = none).
func extSchema(st string) string {
	tree, err := pg.Parse(st)
	if err != nil || len(tree.Stmts) != 1 || tree.Stmts[0].Stmt.GetCreateExtensionStmt() == nil {
		return ""
	}
	for _, o := range tree.Stmts[0].Stmt.GetCreateExtensionStmt().Options {
		if de := o.GetDefElem(); de != nil && de.Defname == "schema" {
			return de.Arg.GetString_().GetSval()
		}
	}
	return ""
}

func (s *session) prepareCustomers(stmts []string, query func(string) ([][]string, error)) (*custTx, error) {
	ct := &custTx{s: s, conns: map[string]*pgconn.PgConn{}}
	targets, err := s.replayTargets(query)
	if err != nil {
		return nil, err
	}
	for _, t := range targets {
		org := t.org
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		conn, err := tunnel.PGConnect(ctx, t.dsn)
		if err == nil {
			err = s.replayIdentity(ctx, conn, org, t.schema)
		}
		begin := "BEGIN;\n"
		if s.meta != nil && s.meta.SchemasUsed {
			// the session's search path there (tables outside public: schemas_path.go)
			begin += "SET LOCAL search_path TO " + s.orgSearchPath(t.schema) + ";\n"
		} else if sch := t.schema; sch != "" && sch != "public" {
			// the organization's tables are there; public keeps the types they use
			begin += "SET LOCAL search_path TO " + quoteIdent(sch) + ", public;\n"
		}
		own := stmts
		if err == nil {
			own = make([]string, len(stmts))
			for i, st := range stmts {
				if own[i], err = orgDDL(st, t.schema, s.meta); err != nil {
					break
				}
			}
		}
		if err == nil {
			if err = s.replayIn(ctx, conn, org, begin, own); err != nil {
				conn.Close(ctx)
			}
		}
		cancel()
		if err != nil {
			ct.rollback()
			return nil, fmt.Errorf("organization %s: %w", org, err)
		}
		ct.conns[org] = conn
	}
	return ct, nil
}

// replayIn runs stmts in an organization's database, in the transaction begin opens. A plain
// index whose operator class or function is not there (an extension the organization does
// not have, block 7 item 8) is left out, as move-org leaves it out: the table answers the
// same without it, slower; schema-diff shows it missing.
func (s *session) replayIn(ctx context.Context, conn *pgconn.PgConn, org, begin string, stmts []string) error {
	if _, err := conn.Exec(ctx, begin).ReadAll(); err != nil {
		return err
	}
	for _, st := range stmts {
		if !plainIndex(st) {
			if _, err := conn.Exec(ctx, st).ReadAll(); err != nil {
				if op, name := extStatement(st); op != "" {
					if h := ext.Hint(err, op, name, extSchema(st), org); h != "" {
						return replayRefusal(h)
					}
				}
				return err
			}
			continue
		}
		_, err := conn.Exec(ctx, "SAVEPOINT lokadb_index;\n"+st).ReadAll()
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "42704" || pgErr.Code == "42883") { // undefined object, function
			if _, err := conn.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_index").ReadAll(); err != nil {
				return err
			}
			s.log.Warn("index not created in the organization's database: what it uses is not there (its queries get slower, results stay the same)",
				"org", org, "index", brief(st), "err", pgErr.Message)
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// plainIndex: st is a CREATE INDEX that is not unique (an index nothing depends on).
func plainIndex(st string) bool {
	tree, err := pg.Parse(st)
	if err != nil || len(tree.Stmts) != 1 {
		return false
	}
	ix := tree.Stmts[0].Stmt.GetIndexStmt()
	return ix != nil && !ix.Unique
}

// commit runs after the cloud committed. A failure here leaves that organization's schema
// behind the cloud's: it is logged for the operator (the cloud cannot be rolled back now).
func (ct *custTx) commit(stmts []string) {
	for org, c := range ct.conns {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if _, err := c.Exec(ctx, "COMMIT").ReadAll(); err != nil {
			ct.s.log.Error("ddl committed in the cloud but not in the organization's database: run lokadb schema-sync", "org", org, "err", err)
		}
		c.Close(ctx)
		cancel()
	}
	var logged []string
	for _, st := range stmts {
		logged = append(logged, brief(st))
	}
	ct.s.log.Info("ddl replayed in customer databases", "orgs", len(ct.conns), "statements", logged)
	ct.s.srv.schemaVersion(ct.s.db).Add(1) // every session of this database reads the metadata again
	ct.s.metaDirty = true
}

func (ct *custTx) rollback() {
	for _, c := range ct.conns {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, _ = c.Exec(ctx, "ROLLBACK").ReadAll()
		c.Close(ctx)
		cancel()
	}
	ct.conns = nil
}

func (s *session) sendRFQ(tx byte) {
	s.cliMu.Lock()
	defer s.cliMu.Unlock()
	s.endRound()
	s.cliOut.Send(s.clientRFQ(tx))
	_ = s.cliOut.Flush()
}

// autocommitDDL runs a simple query with DDL outside a transaction: the server would commit
// it as soon as the string ends, before the proxy could replay it. So the proxy opens the
// transaction itself, holds the client's ReadyForQuery, prepares the replay, commits.
// "BEGIN; …; COMMIT" around the whole string is the same transaction, said by the client.
func (s *session) autocommitDDL(pre []outMsg, final outMsg, an *Analysis) {
	if st, t := concurrentIndex(an, s.meta, s.pathFor()); st != nil {
		s.runConcurrentIndex(pre, st, an.Stmts[0], t)
		return
	}
	evs := an.Events
	explicit := false
	if an.TxControl {
		n := len(an.Stmts)
		first, last := "", ""
		if n > 0 {
			first, last = strings.ToUpper(strings.TrimSpace(an.Stmts[0])), strings.ToUpper(strings.TrimSpace(an.Stmts[n-1]))
		}
		var control []byte // the transaction control of the string: exactly BEGIN first, COMMIT last
		for _, e := range evs {
			if e.Kind != 'D' {
				control = append(control, e.Kind)
			}
		}
		if n < 3 || string(control) != "BC" || !(strings.HasPrefix(first, "BEGIN") || strings.HasPrefix(first, "START")) ||
			!(strings.HasPrefix(last, "COMMIT") || strings.HasPrefix(last, "END")) {
			s.respondError("lokadb: DDL on LokaDB tables and transaction control in one query string: send the DDL in autocommit, or as BEGIN; …; COMMIT around the whole string", 'I')
			return
		}
		explicit = true
		ian := s.analysis(strings.Join(an.Stmts[1:n-1], ";\n"))
		q := strings.Join(an.Stmts[1:n-1], ";\n")
		if ian.Rewritten != "" {
			q = ian.Rewritten
		}
		final.msg, evs = &pgproto3.Query{String: q}, ian.Events
	}
	held := make(chan bool, 1)
	final.mode, final.notify, final.onOK = mHold, held, nil
	msgs := append([]outMsg{{msg: &pgproto3.Query{String: "BEGIN"}, mode: mDrop}}, pre...)
	if explicit {
		s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
	}
	if err := s.send(append(msgs, final)); err != nil {
		return
	}
	if ok := <-held; !ok { // the client has its error already
		if explicit { // its transaction failed, as it would have: the client rolls it back
			s.sendRFQ('E')
			return
		}
		_, _ = s.capture("ROLLBACK")
		s.upOrg = "?"
		s.sendRFQ('I')
		return
	}
	ddl := ddlOf(evs)
	ct, err := s.prepareCustomers(ddl, s.internal)
	if err != nil {
		msg := refusedMsg(err, "the cloud did not apply it either")
		if explicit {
			_, _ = s.capture(raiseSQL(msg)) // fails the client's transaction, as an error of its own would
			s.respondError(msg, 'E')
			return
		}
		_, _ = s.capture("ROLLBACK")
		s.upOrg = "?"
		s.respondError(msg, 'I')
		return
	}
	if _, err := s.capture("COMMIT"); err != nil {
		ct.rollback()
		s.upOrg = "?"
		s.respondError("lokadb: the cloud could not commit the schema change: "+err.Error(), 'I')
		return
	}
	ct.commit(ddl)
	if explicit {
		s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
	}
	s.sendRFQ('I')
}

// commitQuery runs a simple-query COMMIT of a transaction with DDL to replay.
func (s *session) commitQuery(pre []outMsg, final outMsg) {
	s.ddlOpen = false
	ddl := s.takePending()
	if len(ddl) == 0 {
		_ = s.send(append(pre, final))
		return
	}
	ct, err := s.prepareCustomers(ddl, s.internal)
	if err != nil {
		_, _ = s.capture("ROLLBACK")
		s.upOrg = "?"
		s.respondError(refusedMsg(err, "the transaction is rolled back"), 'I')
		return
	}
	// The ReadyForQuery waits for the organizations' commits: once the client has it, the
	// replayed DDL is there for this and every other connection.
	done := make(chan bool, 1)
	final.mode, final.notify, final.wantTag = mHold, done, "COMMIT"
	if err := s.send(append(pre, final)); err != nil {
		ct.rollback()
		return
	}
	if <-done {
		ct.commit(ddl)
	} else {
		ct.rollback()
	}
	s.sendRFQ(s.txStatus())
}

// commitInBatch sends a batch whose message at out[cut] commits (the COMMIT's Execute, or
// the Sync of an implicit transaction): what comes before first, then the replay is
// prepared, then the commit.
func (s *session) commitInBatch(out []outMsg, cut, commit int, isExec bool) {
	prefix := append(append([]outMsg{}, out[:cut]...), outMsg{msg: &pgproto3.Flush{}})
	suffix := append([]outMsg{}, out[cut:]...)
	if err := s.send(prefix); err != nil {
		return
	}
	ok, dead := s.q.waitEmpty()
	if !ok {
		return
	}
	s.ddlOpen = false
	ddl := s.takePending()
	if dead || len(ddl) == 0 { // dead: the server skips to Sync and rolls back
		_ = s.send(suffix)
		return
	}
	ct, err := s.prepareCustomers(ddl, s.internalFlush)
	if err != nil {
		s.upOrg = "?"
		_ = s.send(append(raise(raiseSQL(refusedMsg(err, "the transaction is rolled back"))), suffix...))
		return
	}
	// Nothing after the commit point reaches the server, nor its ReadyForQuery the client,
	// before the organizations' commits: what the client runs next sees the replayed DDL.
	done := make(chan bool, 1)
	at := commit - cut
	head, rest := append([]outMsg{}, suffix[:at+1]...), suffix[at+1:]
	head[at].notify = done
	if isExec {
		head[at].wantTag = "COMMIT"
		head = append(head, outMsg{msg: &pgproto3.Flush{}})
	} else {
		head[at].mode = mHold // the Sync: its ReadyForQuery is sent below
	}
	if err := s.send(head); err != nil {
		ct.rollback()
		return
	}
	if <-done {
		ct.commit(ddl)
	} else {
		ct.rollback()
	}
	if isExec {
		_ = s.send(rest)
	} else {
		s.sendRFQ(s.txStatus())
	}
}

// metaLook: the moment a session's metadata count from: now, less up to a sixth of metaTTL
// at random, so that sessions opened together do not fall due together every metaTTL (review
// F8: they all looked at the same moment, and so again 30 s later).
func metaLook() time.Time {
	return time.Now().Add(-rand.N(metaTTL / 6))
}

func (s *session) refreshDue() bool {
	return s.metaDirty || s.metaNotified.Load() || s.srv.schemaVersion(s.db).Load() != s.metaVer || time.Since(s.metaAt) > metaTTL
}

// refresh reloads the metadata when it may be stale, once the connection is idle outside a
// transaction: after a schema change here or in another session of the database, and every
// metaTTL (enable, move-org and other proxies change it too; a session opened before enable
// starts routing this way).
//
// A read that went through and found no LokaDB table (disable ran) leaves the session without
// metadata: statements go as they are, as before enable. A read that failed (a broken
// connection, a timeout, a query of loadMeta refused) keeps the metadata the session has and
// is tried again after metaBackoff: it says nothing of the database.
func (s *session) refresh(tx byte) {
	if tx != 'I' || !s.refreshDue() {
		return
	}
	// a lokadb command ran (disable, enable): the refs tables may be other ones under the same
	// names, and orgCache goes with the metadata it was read for
	changed := s.metaNotified.Load()
	verChanged := s.srv.schemaVersion(s.db).Load() != s.metaVer
	forced := changed || verChanged || s.metaDirty || s.metaRoleMoved
	if changed || verChanged {
		s.srv.directOK.reset(s.db) // a profile, a move, enable, DDL: the forms learnt for the direct path learn again
	}
	s.metaDirty = false
	if s.createdPending { // a function or view created here (created.go): committed now, the other sessions read it again
		s.createdPending = false
		s.srv.schemaVersion(s.db).Add(1)
	}
	s.metaNotified.Store(false) // a notification from now on means another change
	s.metaVer = s.srv.schemaVersion(s.db).Load()
	s.metaAt = metaLook()
	if changed { // repoint rewrote an organization's credentials before it notified
		s.srv.reloadIfNotified()
	}
	// the shared metadata of the role, as a session that verified them a moment ago left them
	// (block 9, review PF1): no question to the cloud, no read; only when nothing is known to
	// have changed for this session
	var m *Meta
	var err error
	if mc := s.srv.metaCache; mc != nil && !forced && s.metaRole != "" {
		if cm, ok := mc.recheck(s.db, s.metaRole, s.metaVer); ok && cm != nil {
			m = cm
		} else if cm, ok, cerr := mc.sharedStamp(s.db, s.metaRole, s.metaVer, s.internal); ok && (cm != nil || cerr != nil) {
			m, err = cm, cerr // one stamp for the role's sessions due now (review F8)
		}
	}
	if m == nil && err == nil {
		var role string
		m, role, err = s.srv.loadMetaFor(s.db, s.user, s.internal)
		if err == nil {
			s.metaRole, s.metaRoleMoved = role, false
		}
	}
	if err != nil {
		s.metaFails++
		s.metaAt = time.Now().Add(metaBackoff(s.metaFails) - metaTTL)
		if changed { // the retry does not know of the notification any more
			s.orgCache = map[string]map[string]string{}
		}
		return
	}
	if changed || !sameRefs(s.meta, m) {
		s.orgCache = map[string]map[string]string{}
		orgVersion(s.db).Add(1) // the process cache too (orgcache.go): refs tables under the same names may be others
	}
	if m != s.meta { // the shared entry read again and the same keeps its pointer, and the analyses made with it
		s.keyTexts, s.keyedStmts, s.inlineStmts = nil, nil, nil // made for the metadata before (keycond.go, inlinelookup.go)
		if (m == nil) != (s.meta == nil) {
			s.upOrg = "?" // what the client set while the statements went unrouted is not tracked (PF3: set_config is skipped when unchanged)
		}
		s.meta = m
		s.anCache.reset()
	}
	s.metaFails = 0
}

// metaRetry: how soon a metadata read that failed is tried again; each failure in a row
// doubles it, up to metaTTL (a read that keeps failing, such as a role without a right on
// schema lokadb, costs a session one read per metaTTL, not one a second).
const metaRetry = time.Second

// metaBackoff: the wait before the next read after fails failures in a row.
func metaBackoff(fails int) time.Duration {
	d := metaRetry
	for i := 1; i < fails && d < metaTTL; i++ {
		d *= 2
	}
	return min(d, metaTTL)
}

// sameRefs: a and b route through the same refs tables (the orgCache keys).
func sameRefs(a, b *Meta) bool {
	refs := func(m *Meta) map[string]bool {
		out := map[string]bool{}
		if m != nil {
			for _, t := range m.Tables {
				out[t.Refs] = true
			}
		}
		return out
	}
	return maps.Equal(refs(a), refs(b))
}

// schemaVersion counts the schema changes the proxy replayed in a database.
func (srv *Server) schemaVersion(db string) *atomic.Int64 {
	if v, ok := srv.schema.Load(db); ok { // no new counter for each call (third review of performance, perf3-25)
		return v.(*atomic.Int64)
	}
	v, _ := srv.schema.LoadOrStore(db, &atomic.Int64{})
	return v.(*atomic.Int64)
}
