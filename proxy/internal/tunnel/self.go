package tunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LokaDB's own components reach the organizations' ports too: the proxy (the direct profile), the
// runner's jobs, move-org, check and the other commands of the CLI. They are admitted connection
// by connection, never by their address (a container's address goes to the next container, a
// copy of the SaaS database among them): the dialer of a component that holds the control plane
// opens the connection with a startup message that carries a one-time code (database
// lokadb_probe, lokadb_admit 1); the relay checks the code, answers with a notice, and the
// connection goes on as any other, its TLS included. The relays write the ports they serve to the
// registry (relay_ports), so a connection to a database elsewhere carries no code.

var self struct {
	mu    sync.Mutex
	cp    *PGStore
	ports map[string]bool
	read  time.Time
}

// OpenRegistry: the registry for ProveSelfWith, without applying its schema (the relays and the
// runner do; a proxy's role may have no DDL right). It connects when first used.
func OpenRegistry(ctx context.Context, dsn string) (*PGStore, error) {
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PGStore{db: db}, nil
}

// ProveSelfWith makes this process's connections to the relays' ports admitted (nil: they are not).
func ProveSelfWith(cp *PGStore) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.cp, self.read = cp, time.Time{}
}

// AdmitParam: the startup parameter of a component's admission (with database lokadb_probe and
// the code as the user); AdmitOK: the notice the relay answers it with.
const (
	AdmitParam = "lokadb_admit"
	AdmitOK    = "lokadb relay: connection admitted"
)

// DialProving: a dialer for connections that may go to the organizations' ports: to one, the
// connection is admitted first (a one-time code), then handed on; elsewhere it is next's alone.
func DialProving(next func(ctx context.Context, network, addr string) (net.Conn, error)) func(ctx context.Context, network, addr string) (net.Conn, error) {
	if next == nil {
		next = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 5 * time.Minute}).DialContext
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		cp := relayStore(ctx, addr)
		c, err := next(ctx, network, addr)
		if err != nil || cp == nil {
			return c, err
		}
		if err := admit(ctx, cp, c); err != nil {
			c.Close()
			return nil, fmt.Errorf("lokadb: the relay at %s did not admit this connection: %w", addr, err)
		}
		return c, nil
	}
}

// relayStore: the registry when addr is one of the relays' ports (nil otherwise).
func relayStore(ctx context.Context, addr string) *PGStore {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.cp == nil {
		return nil
	}
	if time.Since(self.read) > 30*time.Second {
		if ports, err := self.cp.relayPorts(ctx); err == nil {
			self.ports, self.read = ports, time.Now()
		}
	}
	if _, port, err := net.SplitHostPort(addr); err != nil || !self.ports[port] {
		return nil
	}
	return self.cp
}

// admit: the admission's startup message on c, and the relay's notice back.
func admit(ctx context.Context, cp *PGStore, c net.Conn) error {
	code, err := cp.NewProbe(ctx, 30*time.Second)
	if err != nil {
		return fmt.Errorf("the registry: %w", err)
	}
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.SetDeadline(time.Time{}) }()
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": code, "database": ProbeDatabase, AdmitParam: "1"}})
	if err := fe.Flush(); err != nil {
		return err
	}
	m, err := fe.Receive()
	if err != nil {
		return err
	}
	switch m := m.(type) {
	case *pgproto3.NoticeResponse:
		if m.Message == AdmitOK {
			return nil
		}
		return errors.New(m.Message)
	case *pgproto3.ErrorResponse:
		return errors.New(m.Message)
	}
	return fmt.Errorf("an unexpected answer %T", m)
}

// relayPorts: the ports the relays serve organizations on (written by each relay).
func (s *PGStore) relayPorts(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.Query(ctx, `SELECT port FROM lokadb_cp.relay_ports WHERE at > now() - interval '1 hour'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out[p] = true
	}
	return out, rows.Err()
}

// SetRelayPorts: the ports this relay serves organizations on now.
func (s *PGStore) SetRelayPorts(ctx context.Context, ports []string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO lokadb_cp.relay_ports (port) SELECT unnest($1::text[])
		ON CONFLICT (port) DO UPDATE SET at = now()`, ports)
	return err
}

// Admitting: cfg's connections to the relays' ports are admitted (DialProving).
func Admitting(cfg *pgconn.Config) { cfg.DialFunc = DialProving(cfg.DialFunc) }

// PGConnect: pgconn.Connect, admitted at a relay's port.
func PGConnect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	Admitting(cfg)
	return pgconn.ConnectConfig(ctx, cfg)
}

// PGXConnect: pgx.Connect, admitted at a relay's port.
func PGXConnect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	Admitting(&cfg.Config)
	return pgx.ConnectConfig(ctx, cfg)
}
