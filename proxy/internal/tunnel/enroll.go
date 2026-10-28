package tunnel

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the control plane registry: enrollment tokens and the certificates issued
// through them. It never holds a token in clear or a private key.
type Store interface {
	// TokenOrg returns the organization of a token that is still usable, without using it.
	TokenOrg(ctx context.Context, tokenHash string) (org string, err error)
	// EnrollCert uses a one-time token of organization org and records the certificate it
	// was signed for, in one transaction: a registry failure between the two must not burn
	// the token (review of the tunnel, N-4). errBadToken: used, expired, unknown or another
	// organization's.
	EnrollCert(ctx context.Context, tokenHash, remote, serial, org string, notAfter time.Time) error
	// RecordCert registers an issued certificate; parent is the certificate a renewal was
	// requested with ("" for an enrollment). The parent stays valid until the new one is
	// used (Confirm): an answer to a renewal that never reaches the agent must not lock it
	// out (review of the tunnel, V-1).
	RecordCert(ctx context.Context, serial, org string, notAfter time.Time, kind, remote, parent string) error
	// Confirm: the agent presented serial (a session, a renewal), so it has it: the
	// certificate it was renewed from, and the other renewals of that one nobody presented,
	// open no new session from now on.
	Confirm(ctx context.Context, serial string) error
	// CertValid reports whether a certificate was issued by enrollment, is not revoked and
	// was not replaced by a renewal.
	CertValid(ctx context.Context, serial string) (org string, ok bool, err error)
	// Revoked returns which of the given serials are revoked.
	Revoked(ctx context.Context, serials []string) ([]string, error)
	// SetCertKey records the sha256 of a certificate's public key; CertKey returns it ("":
	// none recorded). A session must present that key, not only the serial (review 3, R3-17).
	SetCertKey(ctx context.Context, serial, keyHash string) error
	CertKey(ctx context.Context, serial string) (string, error)
	// Superseded returns which of the given serials were replaced (Confirm) longer than
	// "for" ago: a session still on one is not its agent's (review 3 of the tunnel, R3-9).
	Superseded(ctx context.Context, serials []string, longer time.Duration) ([]string, error)
	// CloudWindow: whether the organization's agent may reach the cloud database now (a
	// window opened for a move, see OpenCloudFor), and for which role and database.
	CloudWindow(ctx context.Context, org string) (CloudGrant, error)
	// Endpoints returns the organizations registered for the relays (lokadb org-endpoint).
	Endpoints(ctx context.Context) (map[string]Endpoint, error)
}

// Endpoint is where the relays serve an organization inside the cloud network.
type Endpoint struct {
	Listen string
	TLS    string // "require": only connections that negotiate TLS with its database
}

var errBadToken = errors.New("invalid, used or expired enrollment token")

func hashToken(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// --- Postgres store ---

const cpSchema = `
CREATE SCHEMA IF NOT EXISTS lokadb_cp;
CREATE TABLE IF NOT EXISTS lokadb_cp.enroll_tokens (
  token_hash text PRIMARY KEY,
  org        text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  used_at    timestamptz,
  used_by    text
);
CREATE TABLE IF NOT EXISTS lokadb_cp.agent_certs (
  serial     text PRIMARY KEY,
  org        text NOT NULL,
  kind       text NOT NULL,          -- enroll | renew
  issued_at  timestamptz NOT NULL DEFAULT now(),
  not_after  timestamptz NOT NULL,
  remote     text,
  revoked_at timestamptz
);
-- parent: the certificate a renewal replaced; superseded_at: when it was replaced (it opens no
-- new session after that; revoking a certificate revokes its renewals too)
ALTER TABLE lokadb_cp.agent_certs ADD COLUMN IF NOT EXISTS parent text, ADD COLUMN IF NOT EXISTS superseded_at timestamptz;
-- cloud_access: an organization's agent may reach the cloud database until "until"
-- (relay cloud_access "window"); move-org opens it for the move and closes it after.
CREATE TABLE IF NOT EXISTS lokadb_cp.cloud_access (
  org   text PRIMARY KEY,
  until timestamptz NOT NULL
);
-- the role and database a window admits (the move's subscription)
ALTER TABLE lokadb_cp.cloud_access ADD COLUMN IF NOT EXISTS role text, ADD COLUMN IF NOT EXISTS dbname text;
-- relay_orgs: organizations the relays serve, on top of "orgs" in relay.json; a relay picks
-- up a change within seconds, without a restart (lokadb org-endpoint).
CREATE TABLE IF NOT EXISTS lokadb_cp.relay_orgs (
  org        text PRIMARY KEY,
  listen     text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE lokadb_cp.relay_orgs ADD COLUMN IF NOT EXISTS tls text NOT NULL DEFAULT '';
-- org_revocations: lokadb revoke -org of an organization at "at": a certificate of it issued
-- (its transaction begun) before then is revoked, though written after (an enrollment or a
-- renewal in flight while the organization was revoked: review 3 of the tunnel, R3-2)
ALTER TABLE lokadb_cp.agent_certs ADD COLUMN IF NOT EXISTS key_sha256 text;
CREATE TABLE IF NOT EXISTS lokadb_cp.org_revocations (
  org text PRIMARY KEY,
  at  timestamptz NOT NULL
);
-- cert_revoked: the certificate, one it was renewed from, or its organization (before it was
-- issued) is revoked. A renewal written after its parent was revoked is revoked with it.
CREATE OR REPLACE FUNCTION lokadb_cp.cert_revoked(s text) RETURNS boolean LANGUAGE sql STABLE AS $$
  WITH RECURSIVE up AS (
    SELECT serial, parent, org, revoked_at, issued_at, 1 AS depth FROM lokadb_cp.agent_certs WHERE serial = s
    UNION ALL
    SELECT c.serial, c.parent, c.org, c.revoked_at, c.issued_at, up.depth + 1
      FROM lokadb_cp.agent_certs c JOIN up ON c.serial = up.parent WHERE up.depth < 10000)
  SELECT EXISTS (SELECT FROM up WHERE revoked_at IS NOT NULL)
      OR EXISTS (SELECT FROM up JOIN lokadb_cp.org_revocations r ON r.org = up.org WHERE up.depth = 1 AND up.issued_at <= r.at)
$$;`

type PGStore struct{ db *pgxpool.Pool }

func OpenPGStore(ctx context.Context, dsn string) (*PGStore, error) {
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := applySchema(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &PGStore{db: db}, nil
}

// applySchema runs cpSchema once per version of it: a registry already at this version
// takes one read (a relay role without DDL rights starts), and relays starting together
// apply it one at a time (review of the tunnel, N-6: DDL on every start, a race of CREATE
// SCHEMA IF NOT EXISTS between relays).
func applySchema(ctx context.Context, db *pgxpool.Pool) error {
	sum := sha256.Sum256([]byte(cpSchema))
	version := hex.EncodeToString(sum[:8])
	applied := func(q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}) bool {
		var ok bool // two reads: a query naming a missing table fails (and aborts the transaction)
		if err := q.QueryRow(ctx, `SELECT to_regclass('lokadb_cp.schema_applied') IS NOT NULL`).Scan(&ok); err != nil || !ok {
			return false
		}
		err := q.QueryRow(ctx, `SELECT EXISTS (SELECT FROM lokadb_cp.schema_applied WHERE version = $1)`, version).Scan(&ok)
		return err == nil && ok
	}
	if applied(db) {
		return nil
	}
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7166812083)`); err != nil { // "lokadb_cp"
			return err
		}
		if applied(tx) {
			return nil
		}
		if _, err := tx.Exec(ctx, cpSchema+`
CREATE TABLE IF NOT EXISTS lokadb_cp.schema_applied (version text PRIMARY KEY, at timestamptz NOT NULL DEFAULT now());`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO lokadb_cp.schema_applied (version) VALUES ($1) ON CONFLICT DO NOTHING`, version)
		return err
	})
}

// OpenPGStoreForRelay is OpenPGStore for a relay: a registry that does not answer at start
// is not fatal (review 3 of the tunnel, R3-8). The relay starts on the organizations of its
// config file; its schema is applied in the background, every 5 s until it is; registry calls
// fail meanwhile as they do when the registry goes away later (registry_grace, refused TLS
// for organizations whose requirement is not known yet).
func OpenPGStoreForRelay(ctx context.Context, dsn string, log *slog.Logger) (*PGStore, error) {
	db, err := pgxpool.New(ctx, dsn) // does not connect yet
	if err != nil {
		return nil, err // a DSN that does not parse is a configuration error
	}
	if err := applySchema(ctx, db); err != nil {
		log.Error("registry unavailable at start: the relay starts without it and retries", "err", err)
		go func() {
			for {
				time.Sleep(5 * time.Second)
				c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := applySchema(c, db)
				cancel()
				if err == nil {
					log.Warn("registry back: its schema is in place")
					return
				}
			}
		}()
	}
	return &PGStore{db: db}, nil
}

func (s *PGStore) Close() { s.db.Close() }

// CreateToken issues a one-time enrollment token for an organization. Only its hash is stored.
func (s *PGStore) CreateToken(ctx context.Context, org string, ttl time.Duration) (string, error) {
	tok, err := newToken()
	if err != nil {
		return "", err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO lokadb_cp.enroll_tokens (token_hash, org, expires_at) VALUES ($1, $2, now() + $3::interval)`,
		hashToken(tok), org, fmt.Sprintf("%d milliseconds", ttl.Milliseconds()))
	return tok, err
}

func (s *PGStore) TokenOrg(ctx context.Context, tokenHash string) (string, error) {
	var org string
	err := s.db.QueryRow(ctx, `SELECT org FROM lokadb_cp.enroll_tokens
		WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now()`, tokenHash).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errBadToken
	}
	return org, err
}

func (s *PGStore) Endpoints(ctx context.Context) (map[string]Endpoint, error) {
	rows, err := s.db.Query(ctx, `SELECT org, listen, tls FROM lokadb_cp.relay_orgs`)
	if err != nil {
		return nil, err
	}
	out := map[string]Endpoint{}
	var org string
	var ep Endpoint
	_, err = pgx.ForEachRow(rows, []any{&org, &ep.Listen, &ep.TLS}, func() error { out[org] = ep; return nil })
	return out, err
}

// SetEndpoint makes the relays serve an organization (ep.Listen "" stops serving it).
func (s *PGStore) SetEndpoint(ctx context.Context, org string, ep Endpoint) error {
	if ep.Listen == "" {
		_, err := s.db.Exec(ctx, `DELETE FROM lokadb_cp.relay_orgs WHERE org = $1`, org)
		return err
	}
	if ep.TLS != "" && ep.TLS != "require" {
		return fmt.Errorf("tls: %q, want \"require\" or nothing", ep.TLS)
	}
	_, err := s.db.Exec(ctx, `INSERT INTO lokadb_cp.relay_orgs (org, listen, tls) VALUES ($1, $2, $3)
		ON CONFLICT (org) DO UPDATE SET listen = excluded.listen, tls = excluded.tls`, org, ep.Listen, ep.TLS)
	return err
}

func (s *PGStore) EnrollCert(ctx context.Context, tokenHash, remote, serial, org string, notAfter time.Time) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		var used string
		err := tx.QueryRow(ctx, `UPDATE lokadb_cp.enroll_tokens SET used_at = now(), used_by = $2
			WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now() RETURNING org`, tokenHash, remote).Scan(&used)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && used != org) {
			return errBadToken // the transaction rolls back: a token of another organization stays unused
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO lokadb_cp.agent_certs (serial, org, kind, not_after, remote)
			VALUES ($1, $2, 'enroll', $3, $4)`, serial, org, notAfter, remote)
		return err
	})
}

func (s *PGStore) UseToken(ctx context.Context, tokenHash, remote string) (string, error) {
	var org string
	err := s.db.QueryRow(ctx, `UPDATE lokadb_cp.enroll_tokens SET used_at = now(), used_by = $2
		WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now() RETURNING org`, tokenHash, remote).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errBadToken
	}
	return org, err
}

func (s *PGStore) RecordCert(ctx context.Context, serial, org string, notAfter time.Time, kind, remote, parent string) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO lokadb_cp.agent_certs (serial, org, kind, not_after, remote, parent)
			VALUES ($1, $2, $3, $4, $5, nullif($6, ''))`, serial, org, kind, notAfter, remote, parent)
		return err
	})
}

func (s *PGStore) Confirm(ctx context.Context, serial string) error {
	_, err := s.db.Exec(ctx, `UPDATE lokadb_cp.agent_certs c SET superseded_at = now()
		FROM (SELECT parent FROM lokadb_cp.agent_certs WHERE serial = $1 AND parent IS NOT NULL) p
		WHERE (c.serial = p.parent OR (c.parent = p.parent AND c.serial <> $1)) AND c.superseded_at IS NULL`, serial)
	return err
}

func (s *PGStore) CertValid(ctx context.Context, serial string) (string, bool, error) {
	var org string
	err := s.db.QueryRow(ctx, `SELECT org FROM lokadb_cp.agent_certs WHERE serial = $1 AND superseded_at IS NULL
		AND NOT lokadb_cp.cert_revoked(serial)`, serial).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return org, err == nil, err
}

// CloudGrant: an organization's window to the cloud database. Role and DB: the only role and
// database its agent's connections may name (the move's subscription; review 3 of the
// tunnel, R3-13); "" for a window opened without them, which admits none in "window" mode.
type CloudGrant struct {
	Open     bool
	Role, DB string
}

func (s *PGStore) CloudWindow(ctx context.Context, org string) (CloudGrant, error) {
	var g CloudGrant
	err := s.db.QueryRow(ctx, `SELECT coalesce(role, ''), coalesce(dbname, '') FROM lokadb_cp.cloud_access WHERE org = $1 AND until > now()`, org).Scan(&g.Role, &g.DB)
	if errors.Is(err, pgx.ErrNoRows) {
		return CloudGrant{}, nil
	}
	g.Open = err == nil
	return g, err
}

// OpenCloud lets the organization's agent reach the cloud database for ttl; ttl <= 0 closes it.
// OpenCloud closes the organization's window (ttl <= 0); a window is opened with OpenCloudFor.
func (s *PGStore) OpenCloud(ctx context.Context, org string, ttl time.Duration) error {
	if ttl <= 0 {
		_, err := s.db.Exec(ctx, `DELETE FROM lokadb_cp.cloud_access WHERE org = $1`, org)
		return err
	}
	return errors.New("a window to the cloud names the role and database it admits: OpenCloudFor")
}

// OpenCloudFor opens the organization's window for ttl, for role connecting to database db.
func (s *PGStore) OpenCloudFor(ctx context.Context, org string, ttl time.Duration, role, db string) error {
	if role == "" || db == "" {
		return errors.New("a window to the cloud needs its role and database")
	}
	_, err := s.db.Exec(ctx, `INSERT INTO lokadb_cp.cloud_access (org, until, role, dbname) VALUES ($1, now() + $2::interval, $3, $4)
		ON CONFLICT (org) DO UPDATE SET until = excluded.until, role = excluded.role, dbname = excluded.dbname`,
		org, fmt.Sprintf("%d seconds", int(ttl.Seconds())), role, db)
	return err
}

func (s *PGStore) Revoked(ctx context.Context, serials []string) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT serial FROM lokadb_cp.agent_certs WHERE serial = ANY($1) AND lokadb_cp.cert_revoked(serial)`, serials)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// Revoke revokes an organization's certificates (or one serial) and burns its unused tokens.
func (s *PGStore) SetCertKey(ctx context.Context, serial, keyHash string) error {
	_, err := s.db.Exec(ctx, `UPDATE lokadb_cp.agent_certs SET key_sha256 = $2 WHERE serial = $1`, serial, keyHash)
	return err
}

func (s *PGStore) CertKey(ctx context.Context, serial string) (string, error) {
	var k *string
	err := s.db.QueryRow(ctx, `SELECT key_sha256 FROM lokadb_cp.agent_certs WHERE serial = $1`, serial).Scan(&k)
	if errors.Is(err, pgx.ErrNoRows) || k == nil {
		return "", nil
	}
	return *k, err
}

func (s *PGStore) Superseded(ctx context.Context, serials []string, longer time.Duration) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT serial FROM lokadb_cp.agent_certs WHERE serial = ANY($1)
		AND superseded_at < now() - $2::interval`, serials, fmt.Sprintf("%d milliseconds", longer.Milliseconds()))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *PGStore) Revoke(ctx context.Context, org, serial string) (certs, tokens int64, err error) {
	// a serial revokes its agent: the whole renewal chain from the enrollment down, the
	// certificates it was renewed from included (a live session may run on one of them:
	// review of the tunnel, S-2); a renewal of it still in flight is revoked by cert_revoked
	err = pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if serial == "" { // the tokens first: an enrollment in flight waits on its token's row, then finds it burnt
			tag, err := tx.Exec(ctx, `UPDATE lokadb_cp.enroll_tokens SET expires_at = now() WHERE org = $1 AND used_at IS NULL AND expires_at > now()`, org)
			if err != nil {
				return err
			}
			tokens = tag.RowsAffected()
		}
		tag, err := tx.Exec(ctx, `WITH RECURSIVE up AS (
				SELECT serial, parent FROM lokadb_cp.agent_certs WHERE org = $1 AND serial = $2
				UNION SELECT c.serial, c.parent FROM lokadb_cp.agent_certs c JOIN up ON c.serial = up.parent),
			chain AS (
				SELECT serial FROM lokadb_cp.agent_certs WHERE org = $1 AND ($2 = '' OR serial IN (
					SELECT serial FROM up u WHERE NOT EXISTS (SELECT FROM lokadb_cp.agent_certs p WHERE p.serial = u.parent)))
				UNION SELECT c.serial FROM lokadb_cp.agent_certs c JOIN chain ON c.parent = chain.serial)
			UPDATE lokadb_cp.agent_certs SET revoked_at = now() WHERE revoked_at IS NULL AND serial IN (SELECT serial FROM chain)`, org, serial)
		if err != nil {
			return err
		}
		certs = tag.RowsAffected()
		if serial == "" { // the barrier, last: what began before it is revoked whenever it commits
			_, err = tx.Exec(ctx, `INSERT INTO lokadb_cp.org_revocations VALUES ($1, clock_timestamp())
				ON CONFLICT (org) DO UPDATE SET at = excluded.at`, org)
		}
		return err
	})
	return certs, tokens, err
}

// List returns every issued certificate, newest first.
func (s *PGStore) List(ctx context.Context) ([][]string, error) {
	rows, err := s.db.Query(ctx, `SELECT org, kind, serial, to_char(issued_at, 'YYYY-MM-DD HH24:MI:SS'),
		to_char(not_after, 'YYYY-MM-DD HH24:MI:SS'),
		CASE WHEN revoked_at IS NOT NULL THEN 'revoked' WHEN not_after < now() THEN 'expired' ELSE 'valid' END
		FROM lokadb_cp.agent_certs ORDER BY issued_at DESC`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) ([]string, error) {
		v := make([]string, 6)
		err := r.Scan(&v[0], &v[1], &v[2], &v[3], &v[4], &v[5])
		return v, err
	})
}

// --- in-memory store (tests) ---

type memCert struct {
	org, kind, parent   string
	revoked, superseded bool
	supersededAt        time.Time
	key                 string
}

type MemStore struct {
	mu      sync.Mutex
	tokens  map[string]string // hash -> org (deleted once used)
	certs   map[string]*memCert
	cloud   map[string]CloudGrant // organizations whose agent may reach the cloud database
	eps     map[string]Endpoint   // registered organization endpoints
	lookups int                   // token lookups (tests: the limiter keeps bad tokens away)
}

func (m *MemStore) Endpoints(context.Context) (map[string]Endpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]Endpoint{}
	for k, v := range m.eps {
		out[k] = v
	}
	return out, nil
}

// SetEndpoint registers (ep.Listen != "") or removes an organization endpoint.
func (m *MemStore) SetEndpoint(org string, ep Endpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.eps == nil {
		m.eps = map[string]Endpoint{}
	}
	if ep.Listen == "" {
		delete(m.eps, org)
	} else {
		m.eps[org] = ep
	}
}

func (m *MemStore) TokenOrg(_ context.Context, h string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lookups++
	org, ok := m.tokens[h]
	if !ok {
		return "", errBadToken
	}
	return org, nil
}

// TokenUsable reports whether a token can still enroll.
func (m *MemStore) TokenUsable(tok string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.tokens[hashToken(tok)]
	return ok
}

// Lookups is how many times a token was looked up.
func (m *MemStore) Lookups() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lookups
}

func (m *MemStore) CloudWindow(_ context.Context, org string) (CloudGrant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cloud[org], nil
}

// SetCloud opens (or closes) the cloud database to the organization's agent.
// SetCloud opens (for role and db) or closes (open false) the organization's window.
func (m *MemStore) SetCloud(org string, open bool, roleDB ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cloud == nil {
		m.cloud = map[string]CloudGrant{}
	}
	g := CloudGrant{Open: open}
	if open && len(roleDB) == 2 {
		g.Role, g.DB = roleDB[0], roleDB[1]
	}
	m.cloud[org] = g
}

func NewMemStore() *MemStore {
	return &MemStore{tokens: map[string]string{}, certs: map[string]*memCert{}}
}

func (m *MemStore) CreateToken(org string) string {
	tok, _ := newToken()
	m.mu.Lock()
	m.tokens[hashToken(tok)] = org
	m.mu.Unlock()
	return tok
}

func (m *MemStore) UseToken(_ context.Context, h, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lookups++
	org, ok := m.tokens[h]
	if !ok {
		return "", errBadToken
	}
	delete(m.tokens, h)
	return org, nil
}

func (m *MemStore) EnrollCert(ctx context.Context, h, remote, serial, org string, notAfter time.Time) error {
	m.mu.Lock()
	m.lookups++
	if m.tokens[h] != org || org == "" {
		m.mu.Unlock()
		return errBadToken
	}
	delete(m.tokens, h)
	m.mu.Unlock()
	return m.RecordCert(ctx, serial, org, notAfter, "enroll", remote, "")
}

func (m *MemStore) RecordCert(_ context.Context, serial, org string, _ time.Time, kind, _, parent string) error {
	m.mu.Lock()
	m.certs[serial] = &memCert{org: org, kind: kind, parent: parent}
	m.mu.Unlock()
	return nil
}

func (m *MemStore) Confirm(_ context.Context, serial string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.certs[serial]
	if c == nil || c.parent == "" {
		return nil
	}
	for s, o := range m.certs {
		if (s == c.parent || (o.parent == c.parent && s != serial)) && !o.superseded {
			o.superseded, o.supersededAt = true, time.Now()
		}
	}
	return nil
}

// RevokeSerial revokes the agent of serial: its chain from the enrollment down.
func (m *MemStore) RevokeSerial(serial string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for c := m.certs[serial]; c != nil && m.certs[c.parent] != nil; c = m.certs[c.parent] {
		serial = c.parent
	}
	for revoked := map[string]bool{serial: true}; len(revoked) > 0; {
		next := map[string]bool{}
		for s, c := range m.certs {
			if revoked[s] {
				c.revoked = true
			}
			if revoked[c.parent] && !c.revoked {
				next[s] = true
			}
		}
		revoked = next
	}
}

func (m *MemStore) CertValid(_ context.Context, serial string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.certs[serial]
	if !ok || m.revokedLocked(serial) || c.superseded {
		return "", false, nil
	}
	return c.org, true, nil
}

func (m *MemStore) Revoked(_ context.Context, serials []string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, s := range serials {
		if _, ok := m.certs[s]; ok && m.revokedLocked(s) {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *MemStore) SetCertKey(_ context.Context, serial, keyHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c := m.certs[serial]; c != nil {
		c.key = keyHash
	}
	return nil
}

func (m *MemStore) CertKey(_ context.Context, serial string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c := m.certs[serial]; c != nil {
		return c.key, nil
	}
	return "", nil
}

func (m *MemStore) Superseded(_ context.Context, serials []string, longer time.Duration) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, s := range serials {
		if c, ok := m.certs[s]; ok && c.superseded && time.Since(c.supersededAt) > longer {
			out = append(out, s)
		}
	}
	return out, nil
}

// revokedLocked: serial or one it was renewed from is revoked (PGStore: cert_revoked).
func (m *MemStore) revokedLocked(serial string) bool {
	for c, n := m.certs[serial], 0; c != nil && n < 10000; c, n = m.certs[c.parent], n+1 {
		if c.revoked {
			return true
		}
	}
	return false
}

func (m *MemStore) RevokeOrg(org string) {
	m.mu.Lock()
	for _, c := range m.certs {
		if c.org == org {
			c.revoked = true
		}
	}
	m.mu.Unlock()
}

func (m *MemStore) Count(kind string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.certs {
		if c.kind == kind {
			n++
		}
	}
	return n
}

// --- issuing ---

type issuer struct {
	ca  *x509.Certificate
	key crypto.Signer
	ttl time.Duration
}

func loadIssuer(certPath, keyPath string, ttl time.Duration) (*issuer, error) {
	cb, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	kb, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	cblk, _ := pem.Decode(cb)
	kblk, _ := pem.Decode(kb)
	if cblk == nil || kblk == nil {
		return nil, errors.New("CA certificate or key is not PEM")
	}
	ca, err := x509.ParseCertificate(cblk.Bytes)
	if err != nil {
		return nil, err
	}
	key, err := x509.ParseECPrivateKey(kblk.Bytes)
	if err != nil {
		return nil, err
	}
	if !key.PublicKey.Equal(ca.PublicKey) {
		return nil, errors.New("the CA key is not the key of the first CA certificate in the bundle (it signs)")
	}
	return &issuer{ca: ca, key: key, ttl: ttl}, nil
}

// errBadCSR is all an unauthenticated client learns about a certificate request the relay
// cannot use; the reason goes to the relay log.
var errBadCSR = errors.New("bad certificate request")

// parseCSR decodes a PEM certificate request and checks its signature (the agent holds the key).
func parseCSR(csrPEM []byte) (*x509.CertificateRequest, error) {
	blk, _ := pem.Decode(csrPEM)
	if blk == nil || blk.Type != "CERTIFICATE REQUEST" {
		return nil, errors.New("csr is not a PEM certificate request")
	}
	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	if err != nil {
		return nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("csr signature: %w", err)
	}
	return csr, nil
}

// agentID identifies one agent across its renewals: the serial number of the certificate it
// enrolled with, carried in the subject's serialNumber of every renewal. (A certificate
// issued before agent ids existed is its own id; its renewals inherit it.)
func agentID(c *x509.Certificate) string {
	if c.Subject.SerialNumber != "" {
		return c.Subject.SerialNumber
	}
	return c.SerialNumber.Text(16)
}

// sign issues a short-lived client certificate for org from a checked CSR. The subject the
// agent asked for is ignored: the organization comes from the token or the current
// certificate, the agent id from the current certificate ("" for an enrollment: a new agent).
func (is *issuer) sign(csr *x509.CertificateRequest, org, agent string) (certPEM []byte, serialHex string, notAfter time.Time, err error) {
	now := time.Now()
	sn := serial()
	if agent == "" {
		agent = sn.Text(16)
	}
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		Subject:      pkix.Name{CommonName: "org-" + org, SerialNumber: agent},
		NotBefore:    now.Add(-min(30*time.Second, is.ttl/10)), // clock skew allowance
		NotAfter:     now.Add(is.ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, is.ca, csr.PublicKey, is.key)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), tmpl.SerialNumber.Text(16), tmpl.NotAfter, nil
}
