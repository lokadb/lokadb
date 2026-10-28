package proxy

// CREATE INDEX CONCURRENTLY on a LokaDB table (block 7). Postgres cannot build an index on a
// partitioned table concurrently, and a plain CREATE INDEX holds writes of every organization
// (SHARE on the parent and each partition) while it builds. So the proxy builds it
// concurrently where the rows are: in every moved organization's database (the statement as
// the client sent it), then on every local partition of the cloud (DEFAULT, the partitions of
// organizations not moved), then creates the parent's index, which only attaches the ready
// partition indexes (milliseconds; a foreign partition gets none, as with a plain CREATE
// INDEX). A unique index gets no parent index: it cannot exist on a parent with foreign
// partitions (see the unique CREATE INDEX in analyze.go). A failure drops what was built.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// concurrentIndex: the statement of a query string that is one CREATE INDEX CONCURRENTLY on a
// LokaDB table, nil if it is not one.
func concurrentIndex(an *Analysis, m *Meta, path []string) (*pg.IndexStmt, *Table) {
	if len(an.Stmts) != 1 || len(an.DDL) != 1 {
		return nil, nil
	}
	tree, err := pg.Parse(an.Stmts[0])
	if err != nil || len(tree.Stmts) != 1 {
		return nil, nil
	}
	st := tree.Stmts[0].Stmt.GetIndexStmt()
	if st == nil || !st.Concurrent {
		return nil, nil
	}
	qualify(tree.Stmts[0].Stmt, m, path) // the table by the session's search path
	t := m.table(st.Relation)
	if t == nil {
		return nil, nil
	}
	return st, t
}

// partIndex: the name of a unique or concurrent index on one partition, as lokadb.create_index
// names it (shortened past the 63 bytes of a Postgres name: rel.HelperName).
func partIndex(name, part string) string {
	if r := []rune(name); len(r) > 30 { // characters, as left() in lokadb.create_index
		name = string(r[:30])
	}
	return rel.HelperName(name+"__"+part, "")
}

// indexSQL: st on the relation of key key, named name.
func indexSQL(st *pg.IndexStmt, key, name string, concurrent, ifNotExists bool) (string, error) {
	c := proto.Clone(st).(*pg.IndexStmt)
	c.Relation.Schemaname, c.Relation.Relname = rel.SplitKey(key)
	c.Idxname, c.Concurrent, c.IfNotExists = name, concurrent, ifNotExists
	return deparseStmt(&pg.Node{Node: &pg.Node_IndexStmt{IndexStmt: c}})
}

// runConcurrentIndex runs the client's CREATE INDEX CONCURRENTLY st (text) on table t outside
// a transaction (simple query, the session idle): pre goes first (lokadb.org and the like).
func (s *session) runConcurrentIndex(pre []outMsg, st *pg.IndexStmt, text string, t *Table) {
	fail := func(msg string) { s.respondError("lokadb: CREATE INDEX CONCURRENTLY on "+t.Name+": "+msg, 'I') }
	if st.Idxname == "" {
		fail("name the index (CREATE INDEX CONCURRENTLY name ON …): the proxy builds it under that name in every database")
		return
	}
	if len(pre) > 0 {
		if err := s.send(pre); err != nil {
			return
		}
	}
	exists, err := s.capture("SELECT (to_regclass(" + sqlString(qfull(inSchema(t.Name, st.Idxname))) + ") IS NOT NULL)::text")
	if err != nil {
		fail(err.Error())
		return
	}
	if exists[0][0] == "true" {
		if st.IfNotExists {
			s.toClient(&pgproto3.NoticeResponse{Severity: "NOTICE", Code: "42P07", Message: fmt.Sprintf("relation %q already exists, skipping", st.Idxname)})
			s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("CREATE INDEX")})
			s.sendRFQ('I')
			return
		}
		s.respondError(fmt.Sprintf("relation %q already exists", st.Idxname), 'I')
		return
	}
	// Rights first, in the cloud: an index built in the organizations' databases (as the owner
	// there) cannot be called back, only dropped again; a role that may not create this index
	// must not start the builds. CREATE INDEX needs the table's owner (or a member of that
	// role) and CREATE on its schema, as Postgres checks it.
	rights, err := s.capture("SELECT pg_has_role(current_user, c.relowner, 'USAGE')::text, has_schema_privilege(c.relnamespace, 'CREATE')::text FROM pg_class c WHERE c.oid = " +
		sqlString(qfull(t.Name)) + "::regclass") // the table's own schema (a table outside public, review 3 P12)
	if err != nil {
		fail(err.Error() + " (nothing was built)")
		return
	}
	if len(rights) > 0 && rights[0][0] != "true" {
		s.respondErrorCode("42501", "must be owner of table "+t.Name, 'I')
		return
	}
	if len(rights) > 0 && rights[0][1] != "true" {
		ts, _ := rel.SplitKey(t.Name)
		s.respondErrorCode("42501", "permission denied for schema "+ts, 'I')
		return
	}
	targets, err := s.replayTargets(s.internal)
	if err != nil {
		fail(refusedMsg(err, "nothing was built"))
		return
	}
	var undo []func() // what was built, dropped again on a failure
	rollback := func() {
		for i := len(undo) - 1; i >= 0; i-- {
			undo[i]()
		}
	}
	// 1. the organizations' databases: the statement as the client sent it, in their schema
	for _, tg := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		conn, err := pgconn.Connect(ctx, tg.dsn)
		if err == nil {
			// the database the address leads to must be this organization's, as for the
			// replay (prepareCustomers): an index is built in no other (review 3 P12)
			err = s.replayIdentity(ctx, conn, tg.org, tg.schema)
		}
		cancel()
		if err != nil {
			rollback()
			fail(fmt.Sprintf("organization %s: %v (nothing was built)", tg.org, err))
			return
		}
		sch := tg.schema
		if sch == "" {
			sch = "public"
		}
		path := quoteIdent(sch) + ", public"
		if s.meta.SchemasUsed {
			path = s.orgSearchPath(sch)
		}
		if ts, _ := rel.SplitKey(t.Name); ts != "public" {
			sch = ts // the table's own schema there too
		}
		// no timeout on the build itself: it takes as long as the organization's rows need
		_, err = conn.Exec(context.Background(), "SET search_path TO "+path).ReadAll()
		own := text
		if err == nil {
			own, err = orgDDL(text, tg.schema, s.meta) // public.t is the customer's own table there (R2)
		}
		if err == nil {
			_, err = conn.Exec(context.Background(), own).ReadAll()
		}
		drop := "DROP INDEX CONCURRENTLY IF EXISTS " + quoteIdent(sch) + "." + quoteIdent(st.Idxname)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "42704" || pgErr.Code == "42883") && !st.Unique {
			// what the index uses is not there (an extension): left out, as replayIn does
			conn.Close(context.Background())
			s.log.Warn("index not created in the organization's database: what it uses is not there (its queries get slower, results stay the same)",
				"org", tg.org, "index", st.Idxname, "err", pgErr.Message)
			continue
		}
		if err != nil {
			_, _ = conn.Exec(context.Background(), drop).ReadAll() // a failed build leaves an invalid index
			conn.Close(context.Background())
			rollback()
			fail(fmt.Sprintf("organization %s: %v (nothing was built)", tg.org, err))
			return
		}
		conn.Close(context.Background())
		org, dsn := tg.org, tg.dsn
		undo = append(undo, func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := pgconn.Connect(ctx, dsn)
			if err == nil {
				_, err = c.Exec(ctx, drop).ReadAll()
				c.Close(ctx)
			}
			if err != nil {
				s.log.Error("CREATE INDEX CONCURRENTLY failed and its index stays in the organization's database: drop it by hand", "org", org, "index", st.Idxname, "err", err)
			}
		})
	}
	// 2. every local partition of the cloud, concurrently
	parts, err := s.capture("SELECT " + keyOf("c") + " FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = " +
		sqlString(qfull(t.Name)) + "::regclass AND c.relkind = 'r' ORDER BY 1")
	if err != nil {
		rollback()
		fail(err.Error() + " (nothing was built)")
		return
	}
	for _, p := range parts {
		_, pn := rel.SplitKey(p[0])
		name := partIndex(st.Idxname, pn)
		q, err := indexSQL(st, p[0], name, true, true)
		if err == nil {
			_, err = s.capture(q)
		}
		drop := "DROP INDEX CONCURRENTLY IF EXISTS " + qfull(inSchema(t.Name, name))
		if err != nil {
			_, _ = s.capture(drop)
			rollback()
			fail(fmt.Sprintf("%s: %v (nothing was built)", p[0], err))
			return
		}
		undo = append(undo, func() { _, _ = s.capture(drop) })
	}
	// 3. the parent's index: attaches the partitions' (a unique one cannot exist there)
	if !st.Unique {
		q, err := indexSQL(st, t.Name, st.Idxname, false, false)
		if err == nil {
			_, err = s.capture(q)
		}
		if err != nil {
			rollback()
			fail(err.Error() + " (nothing was built)")
			return
		}
	}
	s.log.Info("CREATE INDEX CONCURRENTLY built in customer databases and the cloud's partitions", "orgs", len(targets), "partitions", len(parts), "index", st.Idxname)
	s.srv.schemaVersion(s.db).Add(1)
	s.metaDirty = true
	s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("CREATE INDEX")})
	s.sendRFQ('I')
}
