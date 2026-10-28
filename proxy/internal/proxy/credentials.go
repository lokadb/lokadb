package proxy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// The organizations' credentials the proxy reads from the control plane (block 10, stage 9a,
// tasks/direct-profile.md): lokadb_cp.org_credentials, sealed with the key of credentials_key.
// They win over customers and customers_app of the config, which stay the way until an
// organization's credentials are stored (and the way for a proxy without control_plane).

// credentialsRefresh: how often the proxy reads the credentials again (a new organization's,
// a changed password).
const credentialsRefresh = 30 * time.Second

// credStore: the credentials last read (creds.Set), replaced whole. notified: when a session
// last saw NOTIFY lokadb_meta (unix ns); loadDone: when the last finished read began (a read
// that began after a notification has what the command wrote before it). A lokadb command
// that changes an organization's address (repoint) rewrites its credentials and notifies: the
// sessions read them again before their next statement (reloadIfNotified), not within
// credentialsRefresh.
type credStore struct {
	set atomic.Pointer[creds.Set]
	// kinds: for set, the databases that have credentials of each kind (has), made once per set
	// (third review of performance, perf3-23: every BEGIN went over the database's organizations)
	kinds atomic.Pointer[credKinds]
	// inst: the cloud's installation (its system_identifier, review of admin A14), learnt on
	// the first new connection to the cloud (learnInstallation); unknown, the rows of before
	// A14 alone are read. instTried: asked once per process.
	inst      atomic.Pointer[string]
	instTried atomic.Int64 // when it was last asked (unix ns): again after installationRetry while not known
	// legacySaid: the rows of before A14 last logged (by database), said again when they change
	legacySaid string
	keys       [][]byte
	notified   atomic.Int64
	loadDone   atomic.Int64
	mu         sync.Mutex // one read at a time
}

func (c *credStore) dsn(db, org, kind string) string {
	if p := c.set.Load(); p != nil {
		return p.DSN(db, org, kind)
	}
	return ""
}

type credKinds struct {
	set *creds.Set
	has map[string]map[string]bool // database → kind → some organization has it
}

func (c *credStore) has(db, kind string) bool {
	p := c.set.Load()
	if p == nil {
		return false
	}
	k := c.kinds.Load()
	if k == nil || k.set != p {
		k = &credKinds{set: p, has: map[string]map[string]bool{}}
		for d, orgs := range *p {
			k.has[d] = map[string]bool{}
			for _, kinds := range orgs {
				for kd, v := range kinds {
					if v != "" {
						k.has[d][kd] = true
					}
				}
			}
		}
		c.kinds.Store(k)
	}
	return k.has[db][kind]
}

// startCredentials loads the credentials once, then keeps them fresh. A key that cannot be
// read stops the proxy (the operator configured it); an unreachable control plane is logged and
// tried again, the config's credentials serving meanwhile.
func (s *Server) startCredentials() error {
	if s.cfg.ControlPlane == "" {
		return nil
	}
	key, err := creds.LoadKey(s.cfg.CredentialsKey)
	if err != nil {
		return err
	}
	s.creds.keys = [][]byte{key}
	if s.cfg.CredentialsKeyOld != "" {
		old, err := creds.LoadKey(s.cfg.CredentialsKeyOld)
		if err != nil {
			return err
		}
		s.creds.keys = append(s.creds.keys, old)
	}
	s.loadCredentials()
	go func() {
		for range time.Tick(credentialsRefresh) {
			s.loadCredentials()
		}
	}()
	s.log.Info("organizations' credentials from the control plane", "key_id", creds.KeyID(key))
	return nil
}

// loadCredentials reads the control plane's credentials again. The idle direct connections of
// an organization whose direct path DSN changed (repoint) are closed: they reach the old
// address.
func (s *Server) loadCredentials() {
	s.creds.mu.Lock()
	defer s.creds.mu.Unlock()
	s.loadCredentialsLocked()
}

// loadCredentialsLocked: loadCredentials with creds.mu held. A read that fails counts as done
// too: the sessions do not each try the control plane again; the ticker does.
func (s *Server) loadCredentialsLocked() {
	start := time.Now().UnixNano()
	defer s.creds.loadDone.Store(start)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cp, err := pgx.Connect(ctx, s.cfg.ControlPlane)
	if err != nil {
		s.log.Warn("organizations' credentials: control plane unreachable, keeping the last ones", "err", err)
		return
	}
	defer cp.Close(context.Background())
	set, errs, legacy, err := creds.LoadAll(ctx, cp, s.creds.installation(), s.creds.keys...)
	if err != nil {
		s.log.Warn("organizations' credentials: not read, keeping the last ones", "err", err)
		return
	}
	for _, e := range errs {
		s.log.Warn("organizations' credentials", "err", e)
	}
	if said := fmt.Sprint(legacy); len(legacy) > 0 && said != s.creds.legacySaid {
		// not used: which cloud's, the row does not say (second review of admin, R3-7)
		s.log.Warn("organizations' credentials stored before 26.10 name no cloud and are not used: lokadb org-credentials -claim-legacy -db <this cloud's DSN> -control-plane … -key … makes them this cloud's", "rows by database", said)
		s.creds.legacySaid = said
	}
	if old := s.creds.set.Swap(&set); old != nil && s.directPool != nil {
		for db, orgs := range *old {
			for org, kinds := range orgs {
				if was := kinds[creds.Direct]; was != "" && set.DSN(db, org, creds.Direct) != was {
					s.directPool.evictPrefix(directPoolKey(db, org, was, nil))
					s.log.Info("organization's direct path credentials changed: its idle direct connections closed", "db", db, "org", org)
				}
			}
		}
	}
}

// credentialsNotified records a NOTIFY lokadb_meta a session saw.
func (s *Server) credentialsNotified() {
	now := time.Now().UnixNano()
	for {
		n := s.creds.notified.Load()
		if n >= now || s.creds.notified.CompareAndSwap(n, now) {
			return
		}
	}
}

// reloadIfNotified reads the credentials again when a notification came after the last
// finished read began. The sessions that saw the same notification share one read: the others
// wait for it under the lock and find it done (review of block 10: one that went on while
// another's read ran used the old credentials).
func (s *Server) reloadIfNotified() {
	if s.cfg.ControlPlane == "" || s.creds.notified.Load() <= s.creds.loadDone.Load() {
		return
	}
	s.creds.mu.Lock()
	defer s.creds.mu.Unlock()
	if s.creds.notified.Load() > s.creds.loadDone.Load() {
		s.loadCredentialsLocked()
	}
}

// ownerDSNs: the DSN of each organization's owner role in db (DDL replay): the control plane's,
// over the config's customers.
func (s *Server) ownerDSNs(db string) map[string]string {
	out := map[string]string{}
	for org, dsn := range s.cfg.Customers[db] {
		out[org] = dsn
	}
	if p := s.creds.set.Load(); p != nil {
		for org, kinds := range (*p)[db] {
			if dsn := kinds[creds.Owner]; dsn != "" {
				out[org] = dsn
			}
		}
	}
	return out
}

// directDSNs: there are credentials of a direct path role for some organization of db.
func (s *Server) directDSNs(db string) bool {
	return len(s.cfg.CustomersApp[db]) > 0 || s.creds.has(db, creds.Direct)
}

func (c *credStore) installation() string {
	if p := c.inst.Load(); p != nil {
		return *p
	}
	return ""
}

// installationRetry: how long after a failed ask learnInstallation asks a new connection again.
var installationRetry = 30 * time.Second

// learnInstallation asks a new connection to the cloud its system_identifier until it is known
// (one exchange; after a failure, again on a new connection installationRetry later: third
// review of the protocol, 13), and reads the credentials again with it: they are sealed for
// it (review of admin, A14).
func (s *Server) learnInstallation(ctx context.Context, conn *pgconn.PgConn) {
	if s.cfg.ControlPlane == "" || s.creds.inst.Load() != nil {
		return
	}
	now, last := time.Now().UnixNano(), s.creds.instTried.Load()
	if last != 0 && now-last < int64(installationRetry) || !s.creds.instTried.CompareAndSwap(last, now) {
		return
	}
	res, err := conn.Exec(ctx, creds.InstallationSQL).ReadAll()
	if err != nil || len(res) != 1 || len(res[0].Rows) != 1 || len(res[0].Rows[0]) != 1 {
		s.log.Warn("the cloud's system_identifier not read: the organizations' credentials sealed for it are not used until it is (asked again on a new connection)", "err", err, "again in", installationRetry)
		return
	}
	inst := string(res[0].Rows[0][0])
	s.creds.inst.Store(&inst)
	s.log.Info("the cloud's installation", "system_identifier", inst)
	s.loadCredentials() // before this session's first statement: an organization on direct has no other way
}
