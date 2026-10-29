package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/check"
	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/creds"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/pgext"
	"github.com/lokadb/lokadb/proxy/internal/seal"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// Ops: what the jobs do to the SaaS database, its control plane and the relays. The
// self-serve MVP's service did the same itself (private/cloud/DESIGN-v2.md); here it runs at
// the SaaS, with the passwords that never leave it.
type Ops struct {
	Cfg *Config
	Tun *tunnel.PGStore
	Key *seal.Key // the runner's: the customers' role passwords come sealed to it
	Log *slog.Logger
}

func (o *Ops) cloudDB() (string, error) {
	cfg, err := pgx.ParseConfig(o.Cfg.DB)
	if err != nil {
		return "", err
	}
	return cfg.Database, nil
}

// customerDSN: the organization's database as this network reaches it, through the relay's port.
func (o *Ops) customerDSN(port int, database, role, password string) string {
	q := cloudproto.Quote
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s", q(o.Cfg.Relay.CloudHost), port,
		q(database), q(role), q(password), o.Cfg.Relay.SSLMode)
}

// Lookup: an organization by number or uuid.
func (o *Ops) Lookup(ctx context.Context, a cloudproto.LookupArgs) (cloudproto.LookupResult, error) {
	org, err := admin.RequireOrg(ctx, o.Cfg.DB, a.Org, a.Tenant)
	return cloudproto.LookupResult{Org: org}, err
}

// Check: lokadb check; with an organization, against its database as the cloud's role there
// (the role move-org will use: what it cannot do is what the move could not).
func (o *Ops) Check(ctx context.Context, a cloudproto.CheckArgs) (cloudproto.CheckResult, error) {
	if len(o.Cfg.spec.Tables) == 0 {
		return cloudproto.CheckResult{}, errNoSpec
	}
	spec := o.Cfg.spec
	opt := check.Options{DSN: o.Cfg.DB, Spec: &spec, Proxy: o.Cfg.proxyCfg, ProxyFile: o.Cfg.ProxyConfig}
	if a.Org != 0 {
		app, err := o.AppDSN(ctx, a.Org)
		if err != nil {
			return cloudproto.CheckResult{}, err
		}
		opt.CustomerDSN, opt.CustomerAppDSN, opt.Org = app, app, a.Org
	}
	rep, err := check.Run(ctx, opt)
	if err != nil {
		return cloudproto.CheckResult{}, err
	}
	b, err := json.Marshal(rep)
	if err != nil {
		return cloudproto.CheckResult{}, err
	}
	return cloudproto.CheckResult{Exit: rep.ExitCode(), Verdict: string(rep.Verdict), Report: b}, nil
}

// errNoSpec: a job that works on the chosen tables ran before any were chosen.
var errNoSpec = &cloudproto.JobError{Code: "no_spec", Message: "No tables are chosen yet to move with a customer.", Hint: "choose them in step 2 of the setup (what is one customer of yours)"}

// Enable: lokadb enable of the spec, when the person typed this database's name.
func (o *Ops) Enable(ctx context.Context, a cloudproto.EnableArgs, log *slog.Logger) error {
	if len(o.Cfg.spec.Tables) == 0 {
		return errNoSpec
	}
	db, err := o.cloudDB()
	if err != nil {
		return err
	}
	if a.ConfirmDB != db {
		return &cloudproto.JobError{Code: "confirm", Message: fmt.Sprintf("enable: the database typed (%q) is not this runner's (%q)", a.ConfirmDB, db)}
	}
	// Two phases, always: the preparation does the work that takes time, online (the tables keep
	// working, no long lock; resumable, and a no-op when it is done); the switch then only
	// changes the catalog, each wait for a lock limited by lock_timeout and tried again. The
	// phase is in every progress line, so the panel can show where it is.
	opt := admin.EnableOptions{DSN: o.Cfg.DB, Spec: o.Cfg.spec, Owner: o.Cfg.Owner, LockTimeout: o.Cfg.LockTimeout.Duration, Attempts: 5, Log: log.With("phase", "prepare")}
	prepare := func() error {
		log.Info("Enable, phase 1 of 2: preparing the tables while the application runs", "phase", "prepare")
		opt.Log = log.With("phase", "prepare")
		if err := admin.Prepare(ctx, opt); err != nil {
			return fmt.Errorf("preparing the tables (nothing was switched, and running enable again resumes where this stopped): %w", err)
		}
		return nil
	}
	// Already prepared to the end: no second pass over the rows (a pass can take hours); the
	// switch checks the tables itself, and if the application's writes left them out of step it
	// asks for the preparation again, once.
	if done, _ := admin.Prepared(ctx, opt); !done {
		if err := prepare(); err != nil {
			return err
		}
	} else {
		log.Info("Enable, phase 1 of 2: the tables are already prepared", "phase", "prepare")
	}
	opt.Log = log.With("phase", "switch")
	log.Info("Enable, phase 2 of 2: switching the tables (a short lock)", "phase", "switch")
	err = admin.EnableWith(ctx, opt)
	if err != nil && strings.Contains(err.Error(), "enable -prepare") {
		log.Warn("the tables changed since they were prepared: preparing them again", "why", err.Error())
		if err := prepare(); err != nil {
			return err
		}
		opt.Log = log.With("phase", "switch")
		err = admin.EnableWith(ctx, opt)
	}
	return err
}

// OrgTables: the candidates for the organizations table, or the spec for the chosen one (read-only).
func (o *Ops) OrgTables(ctx context.Context, a cloudproto.OrgTablesArgs) (cloudproto.OrgTablesResult, error) {
	var res cloudproto.OrgTablesResult
	conn, err := ownsql.Connect(ctx, o.Cfg.DB)
	if err != nil {
		return res, err
	}
	defer conn.Close(context.Background())
	if a.OrgTable == "" {
		c, err := check.OrgTableCandidates(ctx, conn)
		if err != nil {
			return res, err
		}
		if c == nil {
			c = []check.OrgCandidate{}
		}
		res.Candidates, err = json.Marshal(c)
		return res, err
	}
	d, err := check.DiscoverByOrgTable(ctx, conn, a.OrgTable)
	if err != nil {
		return res, err
	}
	res.Discovery, err = json.Marshal(d)
	return res, err
}

// Disable: turn LokaDB off again, when the person typed this database's name and no
// organization is in its own database. DryRun counts what would be copied and changes nothing.
func (o *Ops) Disable(ctx context.Context, a cloudproto.DisableArgs, log *slog.Logger) (cloudproto.DisableResult, error) {
	var res cloudproto.DisableResult
	db, err := o.cloudDB()
	if err != nil {
		return res, err
	}
	if a.ConfirmDB != db {
		return res, &cloudproto.JobError{Code: "confirm", Message: fmt.Sprintf("disable: the database typed (%q) is not this runner's (%q)", a.ConfirmDB, db)}
	}
	var rep admin.DisableReport
	err = admin.Disable(ctx, admin.DisableOptions{CloudDSN: o.Cfg.DB, Spec: o.Cfg.spec, DryRun: a.DryRun, Report: &rep,
		LockTimeout: o.Cfg.LockTimeout.Duration, Attempts: 5, Log: log})
	res.Rows, res.NoOrgRows, res.Pause = rep.Rows, rep.NoOrgRows, rep.Pause
	for _, t := range rep.Tables {
		res.Tables = append(res.Tables, cloudproto.DisableTable{Name: t.Name, Rows: t.Rows, NoOrgRows: t.NoOrgRows})
	}
	return res, err
}

// Prepare: what the installer needs: the relay port (lokadb org-endpoint), an enrollment
// token sealed to the installer (Fresh: the organization's earlier certificates revoked
// first, so one agent serves it) and the extensions the tables need.
func (o *Ops) Prepare(ctx context.Context, a cloudproto.PrepareArgs) (cloudproto.PrepareResult, error) {
	var res cloudproto.PrepareResult
	port, err := o.endpoint(ctx, a.Org, a.Port)
	if err != nil {
		return res, err
	}
	res.Port = port
	org := strconv.FormatInt(a.Org, 10)
	if a.Fresh {
		if a.InstallerKey == "" {
			return res, errors.New("agent.prepare: no installer key to seal the enrollment token to")
		}
		if _, _, err := o.Tun.Revoke(ctx, org, ""); err != nil {
			return res, err
		}
		tok, err := o.Tun.CreateToken(ctx, org, 15*time.Minute)
		if err != nil {
			return res, err
		}
		if res.EnrollTokenSealed, err = seal.Seal(a.InstallerKey, []byte(tok)); err != nil {
			return res, err
		}
	}
	if res.Relay, err = o.relayInfo(); err != nil {
		return res, err
	}
	res.Extensions, err = o.Extensions(ctx)
	return res, err
}

func (o *Ops) relayInfo() (cloudproto.RelayInfo, error) {
	ca, err := os.ReadFile(o.Cfg.Relay.CA)
	if err != nil {
		return cloudproto.RelayInfo{}, fmt.Errorf("relay.ca: %w", err)
	}
	return cloudproto.RelayInfo{Addr: o.Cfg.Relay.Agent, Enroll: o.Cfg.Relay.Enroll, ServerName: o.Cfg.Relay.ServerName, CAPEM: string(ca)}, nil
}

// endpoint gives the organization a port on the relays: the one it has (or the move holds),
// else the lowest free one of the range.
func (o *Ops) endpoint(ctx context.Context, orgN int64, held int) (int, error) {
	eps, err := o.Tun.Endpoints(ctx)
	if err != nil {
		return 0, err
	}
	org := strconv.FormatInt(orgN, 10)
	portOf := func(listen string) int {
		if i := strings.LastIndex(listen, ":"); i >= 0 {
			p, _ := strconv.Atoi(listen[i+1:])
			return p
		}
		return 0
	}
	port := held
	if ep, ok := eps[org]; ok && port == 0 {
		port = portOf(ep.Listen)
	}
	if port == 0 {
		used := map[int]bool{}
		for _, ep := range eps {
			used[portOf(ep.Listen)] = true
		}
		for p := o.Cfg.Relay.PortMin; p <= o.Cfg.Relay.PortMax; p++ {
			if !used[p] {
				port = p
				break
			}
		}
		if port == 0 {
			return 0, fmt.Errorf("no free relay port in %d-%d", o.Cfg.Relay.PortMin, o.Cfg.Relay.PortMax)
		}
	}
	want := tunnel.Endpoint{Listen: ":" + strconv.Itoa(port), TLS: o.Cfg.Relay.TLS}
	if ep, ok := eps[org]; !ok || ep != want {
		if err := o.Tun.SetEndpoint(ctx, org, want); err != nil {
			return 0, err
		}
	}
	return port, nil
}

// StoreCredentials: the installer's sealed role passwords, opened here and sealed again with
// the proxy's credentials key into the control plane (lokadb org-credentials): the proxy reads
// the direct one, move-org and the DDL replay the owner's. It connects with each through the
// relay, so it is the check that the tunnel is up, too: a failure is retryable.
func (o *Ops) StoreCredentials(ctx context.Context, a cloudproto.CredentialsArgs) error {
	plain, err := o.Key.Open(a.Sealed)
	if err != nil {
		return &cloudproto.JobError{Code: "rekeyed", Message: "the passwords were sealed to another runner key: " + err.Error(),
			Hint: "the installer gets the current key on its next claim: run its command again"}
	}
	var c cloudproto.Credentials
	if err := json.Unmarshal(plain, &c); err != nil {
		return err
	}
	for _, k := range []struct{ kind, role, pass string }{{creds.Owner, a.Roles.App, c.AppPassword}, {creds.Direct, a.Roles.DML, c.DMLPassword}} {
		if err := admin.StoreCredentials(ctx, admin.CredentialsOptions{CloudDSN: o.Cfg.DB, ControlPlane: o.Cfg.ControlPlane, KeyFile: o.Cfg.CredentialsKey,
			Org: a.Org, Kind: k.kind, DSN: o.customerDSN(a.Port, a.Database, k.role, k.pass), Log: o.Log}); err != nil {
			return &cloudproto.JobError{Code: "unreachable", Retryable: true, Message: fmt.Sprintf("%s: %s", k.role, Redact(err.Error())),
				Hint: "the agent may still be connecting; if it persists, check that pg_hba.conf lets " + a.Roles.App + " and " + a.Roles.DML + " in from the agent's address"}
		}
	}
	return nil
}

func (o *Ops) keys() ([][]byte, error) {
	var keys [][]byte
	for _, f := range []string{o.Cfg.CredentialsKey, o.Cfg.CredentialsKeyOld} {
		if f == "" {
			continue
		}
		k, err := creds.LoadKey(f)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// AppDSN: the owner role's DSN of the organization, opened from the control plane.
func (o *Ops) AppDSN(ctx context.Context, org int64) (string, error) {
	keys, err := o.keys()
	if err != nil {
		return "", err
	}
	inst, err := admin.CloudInstallation(ctx, o.Cfg.DB)
	if err != nil {
		return "", err
	}
	db, err := o.cloudDB()
	if err != nil {
		return "", err
	}
	cp, err := pgx.Connect(ctx, o.Cfg.ControlPlane)
	if err != nil {
		return "", fmt.Errorf("control plane: %w", err)
	}
	defer cp.Close(context.Background())
	set, _, err := creds.Load(ctx, cp, inst, keys...)
	if err != nil {
		return "", err
	}
	dsn := set.DSN(db, strconv.FormatInt(org, 10), creds.Owner)
	if dsn == "" {
		return "", fmt.Errorf("organization %d: no owner credentials in the control plane (the installer has not finished)", org)
	}
	return dsn, nil
}

func (o *Ops) dropCredentials(ctx context.Context, org int64) error {
	inst, err := admin.CloudInstallation(ctx, o.Cfg.DB)
	if err != nil {
		return err
	}
	db, err := o.cloudDB()
	if err != nil {
		return err
	}
	cp, err := pgx.Connect(ctx, o.Cfg.ControlPlane)
	if err != nil {
		return err
	}
	defer cp.Close(context.Background())
	_, err = cp.Exec(ctx, `DELETE FROM lokadb_cp.org_credentials WHERE installation = $1 AND db = $2 AND org = $3`, inst, db, strconv.FormatInt(org, 10))
	var pe interface{ SQLState() string }
	if errors.As(err, &pe) && pe.SQLState() == "42P01" { // nothing was ever stored
		return nil
	}
	return err
}

// Move: move-org of the organization (trigger mode, the cloud's role as the customer's
// admin), then its profile: fdw+direct when the direct credentials are in the control plane.
func (o *Ops) Move(ctx context.Context, a cloudproto.MoveArgs, log *slog.Logger) (cloudproto.MoveResult, error) {
	app, err := o.AppDSN(ctx, a.Org)
	if err != nil {
		return cloudproto.MoveResult{}, err
	}
	err = admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: o.Cfg.DB, CustomerDSN: app, CustomerAppDSN: app,
		Spec: o.Cfg.spec, Org: a.Org, Owner: o.Cfg.Owner, Mode: admin.ModeAuto, FetchSize: 1000, Attempts: 5,
		LockTimeout: o.Cfg.LockTimeout.Duration, MaxPause: o.Cfg.MaxPause.Duration, Log: log})
	if err != nil {
		return cloudproto.MoveResult{}, err
	}
	profile, warning, err := admin.SetMovedProfile(ctx, admin.MovedProfileOptions{OrgProfileOptions: admin.OrgProfileOptions{CloudDSN: o.Cfg.DB,
		Org: a.Org, LockTimeout: o.Cfg.LockTimeout.Duration, Log: log, CustomerAppDSN: app, DirectRole: a.Roles.DML,
		ControlPlane: o.Cfg.ControlPlane, KeyFile: o.Cfg.CredentialsKey, KeyFileOld: o.Cfg.CredentialsKeyOld}, ProxyConfig: true})
	if err != nil { // the move is done; the profile stays fdw
		return cloudproto.MoveResult{Profile: admin.ProfileFDW, Warning: "the profile was not set: " + Redact(err.Error())}, nil
	}
	return cloudproto.MoveResult{Profile: profile, Warning: Redact(warning)}, nil
}

// MoveBack: move-back of the organization with the cloud's role there.
func (o *Ops) MoveBack(ctx context.Context, a cloudproto.OrgArgs, log *slog.Logger) error {
	app, err := o.AppDSN(ctx, a.Org)
	if err != nil {
		return err
	}
	return admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: o.Cfg.DB, CustomerDSN: app, Spec: o.Cfg.spec, Org: a.Org,
		LockTimeout: o.Cfg.LockTimeout.Duration, MaxPause: o.Cfg.MaxPause.Duration, Attempts: 5, Bridge: 60 * time.Second, Log: log})
}

// Release: the organization leaves LokaDB Cloud: its agent revoked, its port closed, its
// credentials deleted.
func (o *Ops) Release(ctx context.Context, a cloudproto.OrgArgs) error {
	org := strconv.FormatInt(a.Org, 10)
	var errs []error
	if _, _, err := o.Tun.Revoke(ctx, org, ""); err != nil {
		errs = append(errs, fmt.Errorf("revoke: %w", err))
	}
	if err := o.Tun.SetEndpoint(ctx, org, tunnel.Endpoint{}); err != nil {
		errs = append(errs, fmt.Errorf("endpoint: %w", err))
	}
	if err := o.dropCredentials(ctx, a.Org); err != nil {
		errs = append(errs, fmt.Errorf("credentials: %w", err))
	}
	return errors.Join(errs...)
}

// Extensions: the extensions the spec's tables depend on in the SaaS database (types,
// functions, operator classes of their columns and indexes), with their version and whether
// a role without superuser may create them (trusted).
func (o *Ops) Extensions(ctx context.Context) ([]cloudproto.Extension, error) {
	conn, err := ownsql.Connect(ctx, o.Cfg.DB)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	seen := map[string]bool{}
	for _, t := range o.Cfg.spec.Tables {
		deps, err := admin.Dependencies(ctx, conn, t.Name)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", t.Name, err)
		}
		for _, d := range deps {
			if d.Ext != "" && d.Ext != "plpgsql" {
				seen[d.Ext] = true
			}
		}
	}
	out := []cloudproto.Extension{}
	for name := range seen {
		e := cloudproto.Extension{Name: name}
		if err := conn.QueryRow(ctx, `SELECT n.nspname::text, e.extversion,
			coalesce((SELECT v.trusted FROM pg_available_extension_versions v WHERE v.name = e.extname AND v.version = e.extversion), false)
			FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = $1`, name).Scan(&e.Schema, &e.Version, &e.Trusted); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// ExtSync: the organization's database gets the extensions its tables need now (one the SaaS
// added after the move): the trusted ones are created as the cloud's role there; the rest
// need the customer's administrator (DESIGN §3.3).
func (o *Ops) ExtSync(ctx context.Context, a cloudproto.OrgArgs) (cloudproto.ExtSyncResult, error) {
	var res cloudproto.ExtSyncResult
	need, err := o.Extensions(ctx)
	if err != nil {
		return res, err
	}
	app, err := o.AppDSN(ctx, a.Org)
	if err != nil {
		return res, err
	}
	conn, err := pgx.Connect(ctx, app)
	if err != nil {
		return res, &cloudproto.JobError{Code: "unreachable", Retryable: true, Message: Redact(err.Error())}
	}
	defer conn.Close(context.Background())
	res.Extensions = pgext.Sync(ctx, conn, need, false)
	return res, nil
}

// Inventory: the SaaS database as it is now; the service takes where organizations are from it.
func (o *Ops) Inventory(ctx context.Context) (cloudproto.Inventory, error) {
	var inv cloudproto.Inventory
	var err error
	if inv.Installation, err = admin.CloudInstallation(ctx, o.Cfg.DB); err != nil {
		return inv, err
	}
	if inv.Database, err = o.cloudDB(); err != nil {
		return inv, err
	}
	for _, t := range o.Cfg.spec.Tables {
		inv.Tables = append(inv.Tables, t.Name)
	}
	if inv.Relay, err = o.relayInfo(); err != nil {
		return inv, err
	}
	conn, err := ownsql.Connect(ctx, o.Cfg.DB)
	if err != nil {
		return inv, err
	}
	defer conn.Close(context.Background())
	if err := conn.QueryRow(ctx, `SELECT to_regclass('lokadb.tables') IS NOT NULL`).Scan(&inv.Enabled); err != nil {
		return inv, err
	}
	inv.Orgs = []cloudproto.OrgState{}
	if inv.Enabled {
		var hasOrgs bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass('lokadb.orgs') IS NOT NULL`).Scan(&hasOrgs); err != nil {
			return inv, err
		}
		if hasOrgs {
			rows, err := conn.Query(ctx, `SELECT org, coalesce(to_jsonb(o)->>'profile', '') FROM lokadb.orgs o ORDER BY org`)
			if err != nil {
				return inv, err
			}
			for rows.Next() {
				s := cloudproto.OrgState{Location: "customer"}
				if err := rows.Scan(&s.Org, &s.Profile); err != nil {
					rows.Close()
					return inv, err
				}
				inv.Orgs = append(inv.Orgs, s)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return inv, err
			}
		}
	}
	if inv.Extensions, err = o.Extensions(ctx); err != nil {
		return inv, err
	}
	b, _ := json.Marshal(inv)
	inv.Hash = hashOf(b)
	return inv, nil
}

func hashOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// Moved: the organization's rows are in its own database.
func (o *Ops) Moved(ctx context.Context, org int64) (bool, error) {
	inv, err := o.Inventory(ctx)
	if err != nil {
		return false, err
	}
	for _, s := range inv.Orgs {
		if s.Org == org {
			return true, nil
		}
	}
	return false, nil
}

// Cleanup: what interrupted moves left (lokadb move-cleanup), on start.
func (o *Ops) Cleanup(ctx context.Context) ([]string, error) {
	return admin.CleanupMoves(ctx, o.Cfg.DB, o.Log)
}

// Probe: the organization's database through the relay, as the cloud's role there: the path
// the proxy's statements take.
func (o *Ops) Probe(ctx context.Context, org int64) cloudproto.Probe {
	var p cloudproto.Probe
	app, err := o.AppDSN(ctx, org)
	if err != nil {
		p.Error = Redact(err.Error())
		return p
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, app)
	if err != nil {
		p.Error = Redact(err.Error())
		return p
	}
	defer conn.Close(context.Background())
	start := time.Now()
	if _, err := conn.Exec(ctx, `SELECT 1`); err != nil {
		p.Error = Redact(err.Error())
		return p
	}
	p.RTTMS = float64(time.Since(start).Microseconds()) / 1000
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version'), pg_database_size(current_database())`).Scan(&p.PGVersion, &p.DBBytes); err != nil {
		p.Error = Redact(err.Error())
		return p
	}
	p.OK = true
	return p
}
