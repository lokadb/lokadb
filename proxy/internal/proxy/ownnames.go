package proxy

import (
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// The proxy's own statements and portals upstream are named lokadb_* (lokadb_s, lokadb_ls and
// lokadb_lsv, prepared once per connection since PF3; lokadb_v<n>, lokadb_alt, lokadb_r…). A
// client that closed lokadb_s and Parsed its own text under the name had the proxy Bind it as
// its set_config of lokadb.org: a statement of organization 7 ran under the one before (second
// review of the protocol, P2), and with lokadb_ls a write went past move-org's pause. The
// client's names of that form are moved aside on the way in (ownName), and SQL PREPARE,
// EXECUTE and DEALLOCATE of such a name are refused (forbidden).

// clientPrefix: where a client's name lokadb_x goes upstream (lokadb_u_x). No name of the
// proxy's begins with it, and the mapping is one to one.
const clientPrefix = "lokadb_u_"

// ownName: the name upstream of a client's statement or portal name.
func ownName(n string) string {
	if strings.HasPrefix(n, "lokadb_") {
		return clientPrefix + n[len("lokadb_"):]
	}
	return n
}

// clientNames: m (a copy of the client's) with its statement and portal names as upstream.
func clientNames(m pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	switch m := m.(type) {
	case *pgproto3.Parse:
		m.Name = ownName(m.Name)
	case *pgproto3.Bind:
		m.PreparedStatement, m.DestinationPortal = ownName(m.PreparedStatement), ownName(m.DestinationPortal)
	case *pgproto3.Describe:
		m.Name = ownName(m.Name)
	case *pgproto3.Execute:
		m.Portal = ownName(m.Portal)
	case *pgproto3.Close:
		m.Name = ownName(m.Name)
	}
	return m
}

// ownStmtName: why a SQL PREPARE, EXECUTE or DEALLOCATE must not run: it names one of the
// proxy's statements.
func ownStmtName(n *pg.Node) string {
	name := ""
	switch {
	case n.GetPrepareStmt() != nil:
		name = n.GetPrepareStmt().Name
	case n.GetExecuteStmt() != nil:
		name = n.GetExecuteStmt().Name
	case n.GetDeallocateStmt() != nil:
		name = n.GetDeallocateStmt().Name
	}
	if strings.HasPrefix(name, "lokadb_") {
		return "lokadb: prepared statements named lokadb_* are the proxy's: choose another name"
	}
	return ""
}
