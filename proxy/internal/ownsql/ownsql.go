// Package ownsql marks the statements LokaDB itself sends to a database (the proxy's metadata,
// lookups, refs rows, locks; the commands' own queries), so that what reads a database's
// statements back (pg_stat_statements, the proxy's recording) can tell them from the
// application's: lokadb check analyses the application's workload, and a query of LokaDB's on
// its own schema is not one (it would be query_forbidden).
//
// The mark is a trailing comment: pg_stat_statements (PostgreSQL 18) drops a leading comment
// from the text it keeps, and keeps a trailing one when it is not after the statement's ';'.
// Statements that differ only in comments share a pg_stat_statements entry, whose text is the
// first one seen: a statement of LokaDB's identical to one of the application's (COMMIT) may
// be counted under either; LokaDB's own ones (on schema lokadb, its locks) are its alone.
package ownsql

import (
	"context"
	"encoding/binary"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// Mark is the comment that ends every statement LokaDB sends on its own.
const Mark = "/* lokadb */"

// Tag: sql marked as LokaDB's own (unchanged if it is already). A trailing ';' and spaces go
// first (a comment after the ';' would be dropped); the mark is on a line of its own, so that
// a trailing -- comment does not swallow it.
func Tag(sql string) string {
	if IsOwn(sql) {
		return sql
	}
	return strings.TrimRight(sql, " \t\r\n;") + "\n" + Mark
}

// IsOwn: sql is a statement LokaDB sent on its own (it ends with Mark).
func IsOwn(sql string) bool {
	return strings.HasSuffix(strings.TrimRight(sql, " \t\r\n;"), Mark)
}

// Configure makes every statement of connections of cfg LokaDB's own: Query and Parse
// messages are tagged on their way out (the commands run hundreds of statements through
// pgx; this is the one place they all pass).
func Configure(cfg *pgx.ConnConfig) { ConfigurePG(&cfg.Config) }

// Connect: pgx.Connect with the connection's statements tagged.
func Connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	Configure(cfg)
	return pgx.ConnectConfig(ctx, cfg)
}

// ConfigurePG: Configure for a pgconn connection.
func ConfigurePG(cfg *pgconn.Config) {
	// a port of the relays: this process proves its address first (tunnel/self.go, R01)
	cfg.DialFunc = tunnel.DialProving(cfg.DialFunc)
	cfg.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		return pgproto3.NewFrontend(r, &writer{w: w})
	}
}

// writer tags the Query and Parse messages written through it. The frontend writes whole
// messages; one that a write ends in the middle of (it never happens with pgproto3, but a
// writer must not assume it) is passed on as it is, and so is the rest of it in the next write.
// The first message of a connection, the startup message, has no type byte.
type writer struct {
	w       io.Writer
	started bool // the startup message is past
	skip    int  // bytes of a message begun in an earlier write, passed on as they are
}

func (t *writer) Write(p []byte) (int, error) {
	out := make([]byte, 0, len(p)+64)
	i := 0
	for i < len(p) {
		if t.skip > 0 {
			n := min(t.skip, len(p)-i)
			out = append(out, p[i:i+n]...)
			t.skip -= n
			i += n
			continue
		}
		if !t.started { // the startup message: length, then the rest; no type byte
			if len(p)-i < 4 {
				out = append(out, p[i:]...)
				break
			}
			n := int(binary.BigEndian.Uint32(p[i : i+4]))
			t.started = true
			end := i + n
			if end > len(p) {
				out = append(out, p[i:]...)
				t.skip = end - len(p)
				break
			}
			out = append(out, p[i:end]...)
			i = end
			continue
		}
		if len(p)-i < 5 { // a header cut by the write: nothing can be told of what follows
			out = append(out, p[i:]...)
			break
		}
		typ := p[i]
		n := int(binary.BigEndian.Uint32(p[i+1 : i+5]))
		end := i + 1 + n
		if n < 4 || end > len(p) {
			out = append(out, p[i:]...)
			if n >= 4 {
				t.skip = end - len(p)
			}
			break
		}
		body := p[i+5 : end]
		switch typ {
		case 'Q':
			if q, ok := cstring(body, 0); ok {
				out = message(out, 'Q', []byte(Tag(q)), []byte{0}, body[len(q)+1:])
				i = end
				continue
			}
		case 'P':
			if name, ok := cstring(body, 0); ok {
				if q, ok := cstring(body, len(name)+1); ok {
					rest := body[len(name)+1+len(q)+1:]
					out = message(out, 'P', body[:len(name)+1], []byte(Tag(q)), []byte{0}, rest)
					i = end
					continue
				}
			}
		}
		out = append(out, p[i:end]...)
		i = end
	}
	if _, err := t.w.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}

// cstring: the zero-terminated string of b at off.
func cstring(b []byte, off int) (string, bool) {
	if off > len(b) {
		return "", false
	}
	j := off
	for j < len(b) && b[j] != 0 {
		j++
	}
	if j == len(b) {
		return "", false
	}
	return string(b[off:j]), true
}

// message appends a message of type typ with the parts as its body.
func message(out []byte, typ byte, parts ...[]byte) []byte {
	n := 4
	for _, p := range parts {
		n += len(p)
	}
	out = append(out, typ)
	out = binary.BigEndian.AppendUint32(out, uint32(n))
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
