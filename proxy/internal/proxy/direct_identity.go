package proxy

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/rel"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Is the database a DSN reaches the organization's own (review of routing, R3)? The direct
// path takes the address from customers_app or the credentials store, the DDL replay from
// customers: an address that names another organization's database would let a session of
// organization 1 read and change organization 2's rows (lokadb_owner stops only an INSERT of a
// foreign row). The FDW path cannot go wrong this way: move-org made its server.
//
// move-org comments every table it creates in an organization's database "lokadb:
// organization N database <id>" (admin.TableMark, admin/dbid.go; older moves: without the
// database id). The proxy reads the lokadb comments of the tables in the organization's schemas
// once per connection, when it opens: a comment of another organization, or none of this one,
// and the connection is not used. A table a release added later (DDL replay) has no comment and
// does not count either way.
//
// The database id tells the organization's database from a copy of it (review of admin, A1):
// repoint marks the new database with a new id and records it in lokadb.orgs.db_id; the old
// database, still answering at an address the proxy kept, has the old id (or none) and is not
// used.

// identityMark: the comment move-org puts on organization n's tables (id "": an older one).
func identityMark(n string, id ...string) string {
	m := "lokadb: organization " + n
	if len(id) > 0 && id[0] != "" {
		m += " database " + id[0]
	}
	return m
}

// parseMark: the organization and database id of a mark ("" id: none); ok false for a lokadb
// comment that is no organization's mark.
func parseMark(m string) (org, id string, ok bool) {
	rest, ok := strings.CutPrefix(m, identityMark(""))
	if !ok {
		return "", "", false
	}
	org, id, _ = strings.Cut(rest, " database ")
	return org, id, org != ""
}

// identitySQL: the lokadb comments of the tables in schemas, one per row.
func identitySQL(schemas []string) string {
	var ns []string
	for _, s := range schemas {
		ns = append(ns, "to_regnamespace("+sqlString(s)+")")
	}
	return `SELECT DISTINCT d FROM (SELECT obj_description(c.oid, 'pg_class') AS d FROM pg_class c
  WHERE c.relkind IN ('r', 'p') AND c.relnamespace = ANY (ARRAY[` + strings.Join(ns, ", ") + `]::oid[])) x
  WHERE d LIKE 'lokadb:%' ORDER BY d /* lokadb: identity */`
}

// identityVerdict: nil when marks (the lokadb comments found) say the database is organization
// n's, the one the cloud uses when dbID (lokadb.orgs.db_id) is recorded; else what they say
// instead.
func identityVerdict(n, dbID string, marks []string) error {
	var others, ids []string
	found := false
	for _, m := range marks {
		org, id, ok := parseMark(m)
		switch {
		case !ok:
		case org != n:
			others = append(others, org)
		default:
			found = true
			if dbID != "" && id != dbID {
				if id == "" {
					id = "none"
				}
				ids = append(ids, id)
			}
		}
	}
	if len(others) > 0 {
		return fmt.Errorf("the database there holds the tables of organization %s, not %s: the address is another organization's", strings.Join(others, ", "), n)
	}
	if !found {
		return fmt.Errorf("no table there is marked %q (move-org marks those it creates): not this organization's database", identityMark(n))
	}
	if len(ids) > 0 {
		return fmt.Errorf("the database there is a copy of organization %s's, not the one the cloud uses (its database id %s, the cloud's %s): repoint moved the organization elsewhere; point this address there",
			n, strings.Join(ids, ", "), dbID)
	}
	return nil
}

// identitySchemas: the schemas of organization org's tables in its database (its schema for
// public's tables, the others as they are: orgRel).
func (s *session) identitySchemas(org, schema string) []string {
	if schema == "" {
		schema = "public"
	}
	set := map[string]bool{schema: true}
	if s.meta != nil {
		for t, parts := range s.meta.Foreign {
			if parts[org] == "" {
				continue
			}
			if sch, _ := rel.SplitKey(t); sch != "" && sch != "public" {
				set[sch] = true
			}
		}
	}
	var out []string
	for sch := range set {
		out = append(out, sch)
	}
	sort.Strings(out)
	return out
}

// directIdentity checks the database of a new direct connection (directConnFor, key: its
// directDown key). A mismatch closes the connection and keeps the organization off the
// direct path for directRetry, logged as an error: its statements go the cloud's way.
func (s *session) directIdentity(dc *directConn, key string) error {
	n := s.meta.orgNum(dc.org)
	err := s.identityOf(dc, n)
	if err == nil {
		return nil
	}
	dc.conn.Close()
	s.srv.directDown.Store(key, time.Now())
	err = fmt.Errorf("customers_app of organization %s (%s): %w", n, dc.addr, err)
	s.log.Error("direct path: not the organization's database, the connection is not used", "org", n, "err", err)
	return err
}

func (s *session) identityOf(dc *directConn, n string) error {
	dc.fe.Send(&pgproto3.Query{String: identitySQL(s.identitySchemas(dc.org, s.meta.Schemas[n]))})
	if err := dc.fe.Flush(); err != nil {
		return err
	}
	var marks []string
	var failed error
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return err
		}
		switch m := m.(type) {
		case *pgproto3.DataRow:
			if len(m.Values) > 0 {
				marks = append(marks, string(m.Values[0]))
			}
		case *pgproto3.ErrorResponse:
			failed = fmt.Errorf("the check failed: %s", m.Message)
		case *pgproto3.ReadyForQuery:
			if failed != nil {
				return failed
			}
			return identityVerdict(n, s.meta.DBIDs[n], marks)
		}
	}
}

// replayIdentity checks an organization's database before the DDL replay runs there
// (prepareCustomers; org: the organization's number). A mismatch closes conn: the schema
// change is refused, as when that database is down.
func (s *session) replayIdentity(ctx context.Context, conn *pgconn.PgConn, org, schema string) error {
	key := org
	if s.meta != nil {
		for _, parts := range s.meta.Foreign { // the organization's key in the metadata (a uuid's is not its number)
			for k := range parts {
				if s.meta.orgNum(k) == org {
					key = k
				}
			}
		}
	}
	res, err := conn.Exec(ctx, identitySQL(s.identitySchemas(key, schema))).ReadAll()
	if err == nil {
		var marks []string
		for _, r := range res {
			for _, row := range r.Rows {
				marks = append(marks, string(row[0]))
			}
		}
		var dbID string
		if s.meta != nil {
			dbID = s.meta.DBIDs[org]
		}
		err = identityVerdict(org, dbID, marks)
	}
	if err != nil {
		conn.Close(ctx)
		s.log.Error("ddl replay: not the organization's database, the schema change is refused", "org", org, "err", err)
	}
	return err
}
