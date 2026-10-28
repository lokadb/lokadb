// Command lokadb: a Postgres wire proxy that keeps selected tables of selected
// organizations in those organizations' own databases, plus admin commands.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/check"
	_ "github.com/lokadb/lokadb/proxy/check/ext" // -rtt, -suggest, -patch, the feed index advice
	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/creds"
	"github.com/lokadb/lokadb/proxy/internal/migrate"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
	"github.com/lokadb/lokadb/proxy/internal/runner"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// version: set at build time (-ldflags "-X main.version=0.9.0"); the runner reports it.
var version = "dev"

func main() {
	// time to the nanosecond, not slog's millisecond: logs of the proxy and of a lokadb command
	// are put in order by it (the direct path's pause, DP9), and a millisecond holds thousands
	// of statements on a fast machine
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level(),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.String(slog.TimeKey, a.Value.Time().Format(time.RFC3339Nano))
			}
			return a
		}}))
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "-h", "-help", "--help", "help":
		usage()
	}
	var err error
	switch os.Args[1] {
	case "proxy":
		fs := flag.NewFlagSet("proxy", flag.ExitOnError)
		cfgPath := fs.String("config", "lokadb.json", "config file")
		record := fs.String("record", "", "record the workload into this file for lokadb check (overrides \"record\" in the config)")
		parseArgs(fs)
		var cfg proxy.Config
		if err = readJSON(*cfgPath, &cfg); err == nil {
			if *record != "" {
				cfg.Record = *record
			}
			if err = cfg.Tenant.Validate(); err == nil { // block 10: the cooperative mode's config
				err = proxy.NewServer(cfg, log).ListenAndServe()
			}
		}
	case "check":
		// ContinueOnError: a mistyped flag exits 1, not 2, which is check's "needs review" (review 2, R14)
		fs := flag.NewFlagSet("check", flag.ContinueOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		specPath := fs.String("spec", "", "tables spec (JSON); without it the tables are discovered by -org-table or -tenant-column")
		orgTable := fs.String("org-table", "", "the organizations table the tables are discovered by: every table that points at its key (without -spec; recommended)")
		tenantCol := fs.String("tenant-column", "", "organization column the tables are discovered by (without -spec): every table with a column of this name")
		orgCands := fs.Bool("org-candidates", false, "print the tables that look like the organizations table, best first, as JSON, and exit")
		writeSpec := fs.String("write-spec", "", "save the discovered spec to this file")
		workload := fs.String("workload", "", "comma-separated workload recordings of the proxy (proxy -record)")
		appRoles := fs.String("app-role", "", "comma-separated roles whose pg_stat_statements are the application (default: all but the role running the check)")
		proxyCfg := fs.String("proxy-config", "", "the proxy's config: its fanout policy decides which queries are refused (default: reject)")
		proxyHosts := fs.String("proxy-hosts", "", "comma-separated IPs or CIDRs of the proxy and the relay: the only addresses the application's role should reach the cloud database from (cloud_direct_access)")
		customer := fs.String("customer", "", "DSN of the organization's own database: is it ready for move-org")
		customerApp := fs.String("customer-app", "", "DSN of the role move-org -customer-app gets (with -customer): can it own the moved tables")
		customerSchema := fs.String("customer-schema", "", "the schema move-org -customer-schema gets (default: public if it can take the tables, else lokadb)")
		org := fs.Int64("org", 0, "organization to be moved (with -customer)")
		tenant := fs.String("tenant", "", "the organization by its uuid instead of -org (block 18: its number from org-add)")
		asJSON := fs.Bool("json", false, "print the report as JSON")
		// block 11: CI of the customer (tasks/check-report-format.md §8)
		ci := fs.Bool("ci", false, "fail (exit 6) on what -baseline has not accepted; print only that")
		baseline := fs.String("baseline", "", "with -ci: the accepted items (JSON, made by -write-baseline)")
		writeBaseline := fs.String("write-baseline", "", "save every current item as accepted to this file (reasons of -baseline kept)")
		countLimit := fs.Int64("count-limit", 0, "data checks count tables of up to this many rows exactly, bigger ones from a sample (0: 1000000, -1: always exactly)")
		profile := fs.String("profile", "", "estimate this routing profile on the workload: fdw, fdw+direct, direct (with -org: against the organization's current one)")
		directForms := fs.String("direct-forms", "", "with -profile fdw+direct and -workload: save the recorded transaction forms as SQL for psql (lokadb.direct_learnt: direct from the first run; lokadb.direct_cloud_first: the cloud's way, no refusal)")
		for _, e := range check.Extensions() { // the extensions' flags (check/ext: -rtt, -suggest, -patch)
			e.Flags(fs)
		}
		if err = fs.Parse(os.Args[2:]); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				os.Exit(0)
			}
			os.Exit(1)
		}
		secretFlags(fs)
		if *db == "" { // without it pgx connects to libpq's defaults (PGHOST, PGDATABASE): a report on another database (review 3, L4)
			err = fmt.Errorf("check: give -db, the admin DSN of the cloud database")
			break
		}
		if fs.NArg() > 0 {
			err = fmt.Errorf("check: unexpected arguments %q (flags go before them, as -name value)", fs.Args())
			break
		}
		if *orgCands {
			var conn *pgx.Conn
			if conn, err = pgx.Connect(context.Background(), *db); err != nil {
				break
			}
			var cands []check.OrgCandidate
			cands, err = check.OrgTableCandidates(context.Background(), conn)
			conn.Close(context.Background())
			if err != nil {
				break
			}
			if cands == nil {
				cands = []check.OrgCandidate{}
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			err = enc.Encode(cands)
			if err == nil {
				os.Exit(0)
			}
			break
		}
		if *org, err = admin.LookupOrg(context.Background(), *db, *org, *tenant); err != nil {
			break
		}
		o := check.Options{DSN: *db, OrgTable: *orgTable, TenantColumn: *tenantCol, CustomerDSN: *customer, CustomerAppDSN: *customerApp, CustomerSchema: *customerSchema, Org: *org, CountLimit: *countLimit, Profile: *profile}
		if *specPath != "" {
			var spec admin.Spec
			if err = readJSON(*specPath, &spec); err != nil {
				break
			}
			spec = spec.Sorted()
			o.Spec = &spec
		}
		if *workload != "" {
			o.Workload = strings.Split(*workload, ",")
		}
		if *appRoles != "" {
			o.AppRoles = strings.Split(*appRoles, ",")
		}
		if *proxyHosts != "" {
			o.ProxyHosts = strings.Split(*proxyHosts, ",")
		}
		if *proxyCfg != "" {
			o.Proxy, o.ProxyFile = &proxy.Config{}, *proxyCfg
			if err = readJSON(*proxyCfg, o.Proxy); err != nil {
				break
			}
		}
		for _, e := range check.Extensions() {
			if c, ok := e.(check.Configurer); ok && err == nil {
				err = c.Configure(&o)
			}
		}
		if err != nil {
			break
		}
		var rep *check.Report
		if rep, err = check.Run(context.Background(), o); err != nil {
			break
		}
		for _, e := range check.Extensions() {
			if f, ok := e.(check.Finisher); ok && err == nil {
				err = f.Finish(rep)
			}
		}
		if err != nil {
			break
		}
		if *writeSpec != "" && rep.Spec != nil {
			var b []byte
			if b, err = json.MarshalIndent(rep.Spec, "", "  "); err != nil {
				break
			}
			if err = os.WriteFile(*writeSpec, append(b, '\n'), 0o644); err != nil {
				break
			}
		}
		if *directForms != "" {
			if rep.Profile == nil || *profile != admin.ProfileFDWDirect || len(*workload) == 0 {
				err = fmt.Errorf("-direct-forms needs -profile fdw+direct and -workload (pg_stat_statements has no transactions)")
				break
			}
			if err = os.WriteFile(*directForms, []byte(check.DirectFormsSQL(rep.Profile.Forms)), 0o644); err != nil {
				break
			}
		}
		var base *check.Baseline
		if *baseline != "" {
			if base, err = check.ReadBaseline(*baseline); err != nil {
				break
			}
		}
		if *writeBaseline != "" {
			if err = rep.MakeBaseline(base).Write(*writeBaseline); err != nil {
				break
			}
		}
		if *ci {
			res := rep.CompareBaseline(base, *baseline)
			if *asJSON {
				err = rep.WriteJSON(os.Stdout)
			} else {
				res.WriteText(os.Stdout)
			}
			if err == nil {
				os.Exit(res.ExitCode())
			}
			break
		}
		if *asJSON {
			err = rep.WriteJSON(os.Stdout)
		} else {
			rep.WriteText(os.Stdout)
		}
		if err == nil {
			os.Exit(rep.ExitCode())
		}
	case "relay":
		fs := flag.NewFlagSet("relay", flag.ExitOnError)
		cfgPath := fs.String("config", "relay.json", "config file")
		parseArgs(fs)
		var cfg tunnel.RelayConfig
		if err = readJSON(*cfgPath, &cfg); err != nil {
			break
		}
		cfg.ConfigPath = *cfgPath // "orgs" is re-read: a new organization needs no restart
		var store *tunnel.PGStore
		if store, err = tunnel.OpenPGStoreForRelay(context.Background(), cfg.ControlPlane, log); err == nil {
			rl := tunnel.NewRelay(cfg, store, log)
			go func() { // SIGTERM: drain (health check red, streams finish), then exit; a second one exits at once
				sig := make(chan os.Signal, 2)
				signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
				<-sig
				done := make(chan struct{})
				go func() { rl.Drain(); close(done) }()
				select {
				case <-done:
				case <-sig:
					log.Warn("second signal: exiting without waiting for the drain")
				}
				os.Exit(0)
			}()
			err = rl.Run()
		}
	case "issuer":
		fs := flag.NewFlagSet("issuer", flag.ExitOnError)
		cfgPath := fs.String("config", "issuer.json", "config file")
		parseArgs(fs)
		var cfg tunnel.IssuerConfig
		if err = readJSON(*cfgPath, &cfg); err == nil {
			err = tunnel.NewIssuer(cfg, log).Run()
		}
	case "runner": // LokaDB Cloud's runner at the SaaS (private/cloud/DESIGN.md §4)
		fs := flag.NewFlagSet("runner", flag.ExitOnError)
		cfgPath := fs.String("config", "/etc/lokadb/runner.json", "runner config")
		parseArgs(fs)
		var cfg *runner.Config
		if cfg, err = runner.LoadConfig(*cfgPath); err != nil {
			break
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		err = (&runner.Runner{Cfg: cfg, Log: log, Version: version}).Run(ctx)
		stop()
	case "agent":
		fs := flag.NewFlagSet("agent", flag.ExitOnError)
		var cfg tunnel.AgentConfig
		fs.StringVar(&cfg.Relay, "relay", "", "relay tunnel address host:port[,host:port...] (the only outbound connections; one session to each relay)")
		fs.StringVar(&cfg.Enroll, "enroll", "", "relay enrollment address host:port (default: relay host, port 7444)")
		fs.StringVar(&cfg.ServerName, "server-name", "", "name in the relay certificates (default: the host of each -relay address)")
		fs.StringVar(&cfg.CA, "ca", "", "CA certificate that signs the relay certificate")
		fs.StringVar(&cfg.StateDir, "state", "/var/lib/lokadb-agent", "directory for the agent's key and certificate")
		fs.StringVar(&cfg.TokenFile, "token-file", "", "file with a one-time enrollment token")
		fs.StringVar(&cfg.DB, "db", "", "organization database host:port (the only place cloud streams go)")
		fs.StringVar(&cfg.CloudListen, "cloud-listen", "", "local address forwarded to the cloud database (optional)")
		cloudAllow := fs.String("cloud-allow", "", "comma-separated IPs, CIDRs or host names allowed to use -cloud-listen (default: the -db host, loopback and this host's own addresses)")
		fs.BoolVar(&cfg.EnrollOnly, "enroll-only", false, "enroll if needed, then exit")
		fs.IntVar(&cfg.MaxConns, "max-conns", 0, "connections to -db open at once (default 200, negative: no limit)")
		fs.StringVar(&cfg.StatusListen, "status-listen", "", "local address that serves GET /status (state, relays, certificate, time to -db; counters only)")
		fs.StringVar(&cfg.StatusURL, "status-url", "", "LokaDB service URL that gets the status every 60 s (POST, with the secret of -status-token-file); LOKADB_AGENT_STATUS=off stops it")
		fs.StringVar(&cfg.StatusTokenFile, "status-token-file", "", "file with the agent's secret for -status-url")
		fs.IntVar(&cfg.Sessions, "sessions", 1, "sessions (TCP connections) to every relay, 1-8: the organization's connections spread over them")
		parseArgs(fs)
		if *cloudAllow != "" {
			cfg.CloudAllow = strings.Split(*cloudAllow, ",")
		}
		// several relays: -relay a:7443,b:7443 (or a name with an address per relay); the
		// certificate of each must carry its host name (or -server-name)
		if cfg.Enroll == "" {
			host, _, err := net.SplitHostPort(strings.Split(cfg.Relay, ",")[0])
			if err != nil { // a name without a port
				host = strings.Split(cfg.Relay, ",")[0]
			}
			cfg.Enroll = net.JoinHostPort(host, "7444") // [::1]:7443 -> [::1]:7444 (review of the tunnel, N-6)
		}
		ag := tunnel.NewAgent(cfg, log)
		go func() { // SIGTERM: the open connections finish (30 s), then exit; a second one exits at once
			sig := make(chan os.Signal, 2)
			signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
			<-sig
			done := make(chan struct{})
			go func() { ag.Drain(30 * time.Second); close(done) }()
			select {
			case <-done:
			case <-sig:
			}
			os.Exit(0)
		}()
		err = ag.Run()
	case "token", "revoke", "agents", "org-endpoint":
		fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
		cp := fs.String("cp", "", "control plane (registry) database DSN")
		org := fs.String("org", "", "organization id")
		ttl := fs.Duration("ttl", 15*time.Minute, "token lifetime (token)")
		serial := fs.String("serial", "", "revoke only the agent of this certificate: its whole renewal chain (revoke)")
		listen := fs.String("listen", "", "address the relays listen on for the organization, inside the cloud network (org-endpoint)")
		orgTLS := fs.String("tls", "", `"require": the relays let through only connections that negotiate TLS with the organization's database (org-endpoint)`)
		changePort := fs.Bool("change-port", false, "allow a new -listen for an organization that already has one (org-endpoint; update its FDW server too)")
		remove := fs.Bool("remove", false, "stop serving the organization: its port closes and its agent is disconnected (org-endpoint)")
		parseArgs(fs)
		ctx := context.Background()
		var store *tunnel.PGStore
		if store, err = tunnel.OpenPGStore(ctx, *cp); err != nil {
			break
		}
		defer store.Close()
		switch os.Args[1] {
		case "org-endpoint":
			switch {
			case *org == "":
				err = fmt.Errorf("org-endpoint: -org is required")
			case *remove:
				if err = store.SetEndpoint(ctx, *org, tunnel.Endpoint{}); err == nil {
					fmt.Printf("organization %s: no longer served by the relays (its port closes within seconds)\n", *org)
				}
			case *listen == "":
				err = fmt.Errorf("org-endpoint: -listen or -remove is required")
			default:
				// The cloud reaches a moved organization through its FDW server lokadb_org<N>
				// (host and port written by move-org -customer): a new port breaks it.
				var eps map[string]tunnel.Endpoint
				if eps, err = store.Endpoints(ctx); err != nil {
					break
				}
				if old, ok := eps[*org]; ok && old.Listen != *listen && !*changePort {
					err = fmt.Errorf("org-endpoint: organization %s already listens on %s; if it was moved, its FDW server lokadb_org%s points there: "+
						"pass -change-port and run ALTER SERVER lokadb_org%s OPTIONS (SET port '...') in each cloud database", *org, old.Listen, *org, *org)
					break
				}
				if err = store.SetEndpoint(ctx, *org, tunnel.Endpoint{Listen: *listen, TLS: *orgTLS}); err == nil {
					fmt.Printf("organization %s: relays listen on %s within seconds (tls %q)\n", *org, *listen, *orgTLS)
				}
			}
		case "token":
			var tok string
			if tok, err = store.CreateToken(ctx, *org, *ttl); err == nil {
				fmt.Println(tok) // shown once: only its hash is stored
			}
		case "revoke":
			var certs, tokens int64
			if certs, tokens, err = store.Revoke(ctx, *org, *serial); err == nil {
				fmt.Printf("revoked %d certificate(s), burned %d unused token(s) of organization %s\n", certs, tokens, *org)
			}
		case "agents":
			var rows [][]string
			if rows, err = store.List(ctx); err == nil {
				const row = "%-4s %-7s %-34s %-19s %-19s %s\n"
				fmt.Printf(row, "org", "kind", "serial", "issued", "expires", "status")
				for _, r := range rows {
					fmt.Printf(row, r[0], r[1], r[2], r[3], r[4], r[5])
				}
			}
		}
	case "certs":
		fs := flag.NewFlagSet("certs", flag.ExitOnError)
		dir := fs.String("dir", "certs", "output directory")
		relayHost := fs.String("relay-host", "relay", "relay host name")
		orgs := fs.String("orgs", "", "comma-separated organization ids for static client certificates (normally none: agents enroll)")
		force := fs.Bool("force", false, "replace an existing CA (every agent then needs a new certificate)")
		parseArgs(fs)
		if _, statErr := os.Stat(filepath.Join(*dir, "ca.crt")); statErr == nil && !*force {
			log.Info("certificates already present, keeping them", "dir", *dir)
			break
		}
		if err = tunnel.GenerateCerts(*dir, *relayHost, strings.Split(*orgs, ",")); err == nil {
			log.Info("certificates written", "dir", *dir)
		}
	case "move-cleanup":
		fs := flag.NewFlagSet("move-cleanup", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		parseArgs(fs)
		var left []string
		if left, err = admin.CleanupMoves(context.Background(), *db, log); err == nil {
			fmt.Printf("removed %d leftover(s) of interrupted moves\n", len(left))
		}
	case "schema-diff", "schema-sync": // block 7
		fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		orgs := fs.String("org", "", "comma-separated organizations (schema-diff: default every moved one)")
		all := fs.Bool("all", false, "schema-sync: every moved organization (without it -org is required: the sync changes the organizations' databases)")
		var customers multiFlag
		fs.Var(&customers, "customer", "N=DSN: organization N's database at this DSN instead of its FDW server's address and user mapping (repeatable)")
		proxyCfg := fs.String("proxy-config", "", "the proxy's config: its customers of this database, as -customer")
		dropExtra := fs.Bool("drop-extra", false, "schema-sync: also drop the columns, indexes and constraints the organization has and the cloud has not")
		asJSON := fs.Bool("json", false, "print the report as JSON")
		parseArgs(fs)
		o := migrate.Options{CloudDSN: *db, DSN: map[string]string{}, DropExtra: *dropExtra}
		if o.Orgs, err = orgList(*orgs, os.Args[1] == "schema-sync", *all); err != nil {
			break
		}
		if *proxyCfg != "" {
			var cfg proxy.Config
			if err = readJSON(*proxyCfg, &cfg); err != nil {
				break
			}
			if c, perr := pgconnConfig(*db); perr == nil {
				for org, dsn := range cfg.Customers[c] {
					o.DSN[org] = dsn
				}
			}
		}
		for _, c := range customers {
			org, dsn, ok := strings.Cut(c, "=")
			if !ok {
				err = fmt.Errorf("-customer %q: want N=DSN", c)
				break
			}
			o.DSN[org] = dsn
		}
		if err != nil {
			break
		}
		sync := os.Args[1] == "schema-sync"
		var res []*migrate.Org
		if sync {
			res, err = migrate.Sync(context.Background(), o)
		} else {
			res, err = migrate.DiffSchemas(context.Background(), o)
		}
		if err != nil {
			break
		}
		if *asJSON {
			b, _ := json.MarshalIndent(res, "", "  ")
			fmt.Println(string(b))
		} else {
			fmt.Print(migrate.Text(res, sync))
		}
		if !migrate.Clean(res) {
			os.Exit(2)
		}
	case "refs-diff", "refs-sync": // block 7
		fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		orgs := fs.String("org", "", "comma-separated organizations (refs-diff: default every moved one)")
		all := fs.Bool("all", false, "refs-sync: every moved organization (without it -org is required: the sync changes the cloud's refs tables)")
		deleteStale := fs.Bool("delete-stale", false, "refs-sync: also delete the ids of rows the organization no longer has")
		cloudOrgs := fs.Bool("cloud", false, "also every organization in the cloud (moved back, or never moved): its refs against its rows there (refs-sync: with -all)")
		maxJump := fs.Int64("max-sequence-jump", migrate.DefaultMaxSequenceJump, "refs-sync: the most it moves a cloud sequence on by itself (the organization's ids are its own data)")
		asJSON := fs.Bool("json", false, "print the report as JSON")
		parseArgs(fs)
		sync := os.Args[1] == "refs-sync"
		o := migrate.RefsOptions{CloudDSN: *db, Sync: sync, DeleteStale: *deleteStale, MaxSequenceJump: *maxJump, Cloud: *cloudOrgs}
		if o.Orgs, err = orgList(*orgs, sync, *all); err != nil {
			break
		}
		var res []migrate.RefsDiff
		if res, err = migrate.RefsCompare(context.Background(), o); err != nil {
			break
		}
		if *asJSON {
			b, _ := json.MarshalIndent(res, "", "  ")
			fmt.Println(string(b))
		} else {
			fmt.Print(migrate.RefsText(res, sync))
		}
		if migrate.RefsLeft(res, sync, *deleteStale) {
			os.Exit(2)
		}
	case "org-profile":
		// lokadb org-profile -db DSN -org N PROFILE: switch a moved organization's routing
		// profile under its pause (admin/profile.go).
		fs := flag.NewFlagSet("org-profile", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		org := fs.Int64("org", 0, "organization id")
		tenant := fs.String("tenant", "", "the organization by its uuid instead of -org (block 18: its number from org-add)")
		lockTimeout := fs.Duration("lock-timeout", 5*time.Second, "max wait for the organization's pause per attempt")
		custApp := fs.String("customer-app", "", "fdw+direct: DSN of the moved tables' owner in the organization's database (move-org's -customer-app)")
		directRole := fs.String("direct-role", "", "fdw+direct: the role the proxy's direct path uses there (customers_app); it gets DML on the organization's tables")
		cpDSN := fs.String("control-plane", "", "direct, and leaving it: DSN of the control plane holding the organization's credentials (lokadb org-credentials)")
		keyFile := fs.String("key", "", "direct, and leaving it: the proxy's credentials_key file")
		keyOld := fs.String("key-old", "", "during a key rotation: the proxy's credentials_key_old file (rows not resealed yet)")
		dropFDW := fs.Bool("drop-fdw-credentials", false, "direct: drop the cloud's user mappings of the organization's server (the cloud no longer reaches its database)")
		parseArgs(fs)
		if *org, err = admin.RequireOrg(context.Background(), *db, *org, *tenant); err != nil {
			break
		}
		if fs.NArg() != 1 || *org <= 0 {
			fmt.Fprintln(os.Stderr, "usage: lokadb org-profile -db DSN -org N|-tenant UUID [-customer-app DSN -direct-role ROLE] [-control-plane DSN -key FILE [-drop-fdw-credentials]] fdw|fdw+direct|direct")
			os.Exit(2)
		}
		var old string
		if old, err = admin.SetOrgProfile(context.Background(), admin.OrgProfileOptions{CloudDSN: *db, Org: *org, Profile: fs.Arg(0), LockTimeout: *lockTimeout, Log: log,
			CustomerAppDSN: *custApp, DirectRole: *directRole, ControlPlane: *cpDSN, KeyFile: *keyFile, KeyFileOld: *keyOld, DropFDWCredentials: *dropFDW}); err == nil {
			fmt.Printf("organization %d: profile %s (was %s)\n", *org, fs.Arg(0), old)
		}
	case "org-credentials":
		// lokadb org-credentials -db DSN -control-plane DSN -key FILE -org N -kind direct|owner
		// -dsn DSN|-: an organization's credentials for the proxy, sealed in the control plane
		// (stage 9a, admin/credentials.go); -list: what is stored, without opening it
		fs := flag.NewFlagSet("org-credentials", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database (its name is the proxy's database)")
		cpDSN := fs.String("control-plane", "", "DSN of the control plane database (the proxy's control_plane)")
		keyFile := fs.String("key", "", "the proxy's credentials_key file")
		org := fs.Int64("org", 0, "organization id")
		tenant := fs.String("tenant", "", "the organization by its uuid instead of -org")
		kind := fs.String("kind", "", "direct (the direct path's role, customers_app) or owner (the moved tables' owner, customers)")
		dsn := fs.String("dsn", "", "the credentials: a DSN of the organization's database, or - to read it from stdin")
		list := fs.Bool("list", false, "list what is stored (no secrets)")
		reseal := fs.Bool("reseal", false, "key rotation: seal with -key every row sealed with -key-old")
		claim := fs.Bool("claim-legacy", false, "the rows stored before 26.10 (no cloud named in them) of -db's database are this cloud's: seal them for it")
		keyOld := fs.String("key-old", "", "-reseal: the old key file (credentials_key_old)")
		parseArgs(fs)
		if *reseal || *claim {
			if *db == "" || *cpDSN == "" || *keyFile == "" || *reseal && *keyOld == "" {
				fmt.Fprintln(os.Stderr, "usage: lokadb org-credentials -reseal -db DSN -control-plane DSN -key NEW -key-old OLD [-claim-legacy]   (or -claim-legacy -db DSN -control-plane DSN -key FILE [-key-old OLD])")
				os.Exit(2)
			}
			var olds []string
			if *keyOld != "" {
				olds = []string{*keyOld}
			}
			var n int
			var left []error
			if n, left, err = admin.ResealCredentials(context.Background(), *db, *cpDSN, *keyFile, olds, *claim); err == nil {
				for _, e := range left {
					log.Warn("not resealed", "err", e)
				}
				fmt.Printf("%d credentials resealed with key %s; %d left as they are\n", n, mustKeyID(*keyFile), len(left))
				if len(left) > 0 {
					os.Exit(1)
				}
			}
			break
		}
		if *list {
			var rows [][]string
			if rows, err = admin.ListCredentials(context.Background(), *cpDSN); err == nil {
				for _, r := range rows {
					fmt.Println(strings.Join(r, "\t"))
				}
			}
			break
		}
		if *org, err = admin.RequireOrg(context.Background(), *db, *org, *tenant); err != nil {
			break
		}
		if *org <= 0 || *cpDSN == "" || *keyFile == "" || *kind == "" || *dsn == "" {
			fmt.Fprintln(os.Stderr, "usage: lokadb org-credentials -db DSN -control-plane DSN -key FILE -org N|-tenant UUID -kind direct|owner -dsn DSN|-   (or -control-plane DSN -list)")
			os.Exit(2)
		}
		if *dsn == "-" {
			var b []byte
			if b, err = io.ReadAll(os.Stdin); err != nil {
				break
			}
			*dsn = strings.TrimSpace(string(b))
		}
		if err = admin.StoreCredentials(context.Background(), admin.CredentialsOptions{CloudDSN: *db, ControlPlane: *cpDSN, KeyFile: *keyFile,
			Org: *org, Kind: *kind, DSN: *dsn, Log: log}); err == nil {
			fmt.Printf("organization %d: %s credentials stored\n", *org, *kind)
		}
	case "org-add":
		// lokadb org-add -db DSN -tenant UUID [-org N]: the number of an organization numbered
		// by uuid (block 18, admin/tenants.go), which every command with -org takes
		fs := flag.NewFlagSet("org-add", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		tenant := fs.String("tenant", "", "the organization's value in the organization column (a uuid)")
		org := fs.Int64("org", 0, "the number to give it (default: the next free one)")
		parseArgs(fs)
		if *tenant == "" {
			fmt.Fprintln(os.Stderr, "usage: lokadb org-add -db DSN -tenant UUID [-org N]")
			os.Exit(2)
		}
		var n int64
		if n, err = admin.OrgAdd(context.Background(), admin.OrgAddOptions{DSN: *db, Tenant: *tenant, Org: *org, Log: log}); err == nil {
			fmt.Printf("organization %d: %s\n", n, *tenant)
		}
	case "routing":
		// lokadb routing apply: the spec's routing declarations, after enable
		fs := flag.NewFlagSet("routing", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		specPath := fs.String("spec", "", "tables spec (JSON) with its routing declarations")
		patchPath := fs.String("patch", "", "routing declarations to add to the spec (as the extended lokadb check -suggest writes them)")
		if len(os.Args) < 3 || os.Args[2] != "apply" {
			usage()
		}
		_ = fs.Parse(os.Args[3:])
		var spec admin.Spec
		if spec, err = readSpec(*specPath, *patchPath); err != nil {
			break
		}
		if err = admin.ApplyRouting(context.Background(), *db, spec, log); err == nil {
			log.Info("routing applied")
		}
	case "enable", "move-org":
		fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		specPath := fs.String("spec", "", "tables spec (JSON)")
		owner := fs.String("owner", "", "role that owns the application tables")
		customer := fs.String("customer", "", "DSN of the organization's own database (move-org)")
		customerApp := fs.String("customer-app", "", "DSN of the role the cloud keeps for the organization's database: owns the moved tables, used by postgres_fdw and the proxy (move-org; default: -customer)")
		cp := fs.String("cp", "", "control plane DSN: opens the relay's window to the cloud database for the move (move-org)")
		publisher := fs.String("publisher", "", "cloud database DSN as seen from the customer database (move-org; default: -db)")
		org := fs.Int64("org", 0, "organization id (move-org)")
		tenant := fs.String("tenant", "", "the organization by its uuid instead of -org (move-org; block 18: its number from org-add)")
		lockTimeout := fs.Duration("lock-timeout", 5*time.Second, "max wait for the write lock at switch time (move-org; enable after -prepare, and -prepare's short catalog changes)")
		// block 3 (1.4): enable in two steps
		prepare := fs.Bool("prepare", false, "enable: prepare online (refs tables, organization columns of child tables, FKs, key indexes; can be stopped and run again); then enable without it switches in a short lock")
		maxPause := fs.Duration("max-pause", 10*time.Second, "max write pause while the last changes arrive (move-org)")
		retain := fs.Bool("retain-source", false, "keep the detached cloud partitions as <table>__o<N>_source (move-org)")
		mode := fs.String("mode", "auto", "how to copy the organization's rows (move-org): logical (subscription in its database), trigger (change capture in the cloud: no wal_level=logical, no subscription, no role that reads the change stream), auto (= trigger; logical needs -mode logical -allow-customer-replication)")
		custSchema := fs.String("customer-schema", "", "schema of the organization's tables in its database (move-org; default: public if it can take them, else lokadb, made by move-org and owned by -customer-app)")
		fetchSize := fs.Int("fetch-size", 1000, "rows postgres_fdw reads per round trip from the organization's database (move-org)")
		// move-org, large organizations (block 5)
		batchRows := fs.Int("batch-rows", admin.DefaultBatchRows, "rows per transaction when the organization is split out of DEFAULT (move-org)")
		maxSplitPause := fs.Duration("max-split-pause", admin.DefaultMaxSplitPause, "max pause of the organization while it is split out of DEFAULT; past it the split stops and its rows stay there (move-org)")
		copyTimeout := fs.Duration("copy-timeout", admin.DefaultCopyTimeout, "max time of the initial copy into the organization's database (move-org)")
		catchUpTimeout := fs.Duration("catchup-timeout", admin.DefaultCatchUpTimeout, "max time for the organization's database to catch up with the cloud after the copy (move-org)")
		catchUpStall := fs.Duration("catchup-stall", admin.DefaultCatchUpStall, "the catch-up gives up once the log left to apply has not got shorter for this long: changes come faster than they reach the organization's database (move-org; 0: only -catchup-timeout)")
		moveDry := fs.Bool("dry-run", false, "move-org: change nothing; estimate whether the trigger mode's catch-up keeps up with the organization's writes (round trips from here to both databases, the writes over -sample)")
		sample := fs.Duration("sample", 10*time.Second, "move-org -dry-run: how long to count the organization's writes")
		dropReturned := fs.Bool("drop-returned", false, "move-org: drop the copy an earlier move-back left of this organization in its database (DROP TABLE, no CASCADE); without it the copy is moved aside to a schema of its own, lokadb_returned_<org>_<date>, and the move says how to drop it")
		allowRepl := fs.Bool("allow-customer-replication", false, "with -mode logical: accept that the organization's database, for the time of the move, can read the change stream of the WHOLE cloud database (the subscription's role has the REPLICATION attribute); only if the cloud and the organization's database have one owner (move-org)")
		patchPath := fs.String("patch", "", "routing declarations to add to the spec (as the extended lokadb check -suggest writes them)")
		// move-org: the moved organization's profile (decision of 26.10: fdw+direct by default)
		profile := fs.String("profile", "", "the moved organization's routing profile (move-org; default: fdw+direct when the proxy has the direct path's credentials of it, see -proxy-config, else fdw with a warning)")
		proxyCfg := fs.String("proxy-config", "", "the proxy's config (move-org): its customers_app or control plane tell whether the direct path can be taken, so fdw+direct is set")
		directRole := fs.String("direct-role", "", "the role the proxy's direct path uses in the organization's database (move-org, fdw+direct: it gets DML on the moved tables)")
		parseArgs(fs)
		var spec admin.Spec
		if spec, err = readSpec(*specPath, *patchPath); err != nil {
			break
		}
		spec = spec.Sorted() // a child after its parent, whatever order the file has
		// Ctrl-C / SIGTERM cancel the work; move-org still cleans up what it created.
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if os.Args[1] == "enable" {
			o := admin.EnableOptions{DSN: *db, Spec: spec, Owner: *owner, LockTimeout: *lockTimeout, Attempts: 5, Log: log}
			if *prepare {
				err = admin.Prepare(ctx, o)
			} else {
				err = admin.EnableWith(ctx, o)
			}
		} else {
			if *org, err = admin.RequireOrg(ctx, *db, *org, *tenant); err != nil {
				break
			}
			if *moveDry {
				var est admin.MoveEstimate
				if est, err = admin.EstimateMove(ctx, admin.MoveOptions{CloudDSN: *db, CustomerDSN: *customer, Spec: spec, Org: *org}, *sample); err == nil {
					fmt.Println(est)
				}
				break
			}
			err = admin.MoveOrg(ctx, admin.MoveOptions{
				CloudDSN: *db, CustomerDSN: *customer, CustomerAppDSN: *customerApp, PublisherDSN: *publisher, ControlPlaneDSN: *cp,
				Spec: spec, Org: *org, Owner: *owner,
				LockTimeout: *lockTimeout, MaxPause: *maxPause, Attempts: 5, RetainSource: *retain, FetchSize: *fetchSize, Mode: *mode, CustomerSchema: *custSchema, Log: log,
				BatchRows: *batchRows, MaxSplitPause: *maxSplitPause, CopyTimeout: *copyTimeout, CatchUpTimeout: *catchUpTimeout, AllowCustomerReplication: *allowRepl, DropReturned: *dropReturned,
				CatchUpStall: stallOption(*catchUpStall),
			})
			if err == nil {
				err = setMovedProfile(ctx, *db, *org, *profile, *proxyCfg, *customerApp, *customer, *directRole, *lockTimeout, log)
			}
		}
		if err == nil {
			log.Info(os.Args[1] + " done")
		}
	// Block 6: the way back (tasks/rollback.md).
	case "repoint":
		fs := flag.NewFlagSet("repoint", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		org := fs.Int64("org", 0, "organization id")
		tenant := fs.String("tenant", "", "the organization by its uuid instead of -org (block 18: its number from org-add)")
		customerApp := fs.String("customer-app", "", "the organization's database at its new address, as the cloud's role there (postgres_fdw uses it)")
		custSchema := fs.String("customer-schema", "", "schema of the organization's tables there (default: unchanged)")
		lockTimeout := fs.Duration("lock-timeout", 5*time.Second, "max wait for the organization's pause")
		proxyCfg := fs.String("proxy-config", "", "the proxy's config (required): its customers_app, customers and control plane are the organization's other addresses, which must reach the new database too")
		key := fs.String("key", "", "the credentials key file (default: the proxy config's credentials_key)")
		keyOld := fs.String("key-old", "", "the previous key file (default: the proxy config's credentials_key_old)")
		directDSN := fs.String("direct", "", "the direct path role's DSN to store in the control plane (default: the stored one at the new host, port and database)")
		ownerDSN := fs.String("owner", "", "the owner's DSN to store in the control plane (default: the stored one at the new host, port and database)")
		parseArgs(fs)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		// the organization first: without -org every other check is about nobody (MV19)
		if *org, err = admin.RequireOrg(ctx, *db, *org, *tenant); err != nil {
			break
		}
		if *proxyCfg == "" {
			err = fmt.Errorf("repoint needs -proxy-config: the proxy reaches the organization's database by its customers_app, customers and control plane too, and repoint checks or moves them")
			break
		}
		var pcfg proxy.Config
		if err = readJSON(*proxyCfg, &pcfg); err != nil {
			break
		}
		var dbName string
		if dbName, err = pgconnConfig(*db); err != nil {
			break
		}
		ro := admin.RepointOptions{CloudDSN: *db, CustomerAppDSN: *customerApp, CustomerSchema: *custSchema,
			Org: *org, LockTimeout: *lockTimeout, Attempts: 5, Log: log,
			ConfigDirect: pcfg.CustomersApp[dbName][strconv.FormatInt(*org, 10)], ConfigOwner: pcfg.Customers[dbName][strconv.FormatInt(*org, 10)],
			ControlPlane: pcfg.ControlPlane, KeyFile: pcfg.CredentialsKey, KeyFileOld: pcfg.CredentialsKeyOld,
			DirectDSN: *directDSN, OwnerDSN: *ownerDSN}
		if *key != "" {
			ro.KeyFile = *key
		}
		if *keyOld != "" {
			ro.KeyFileOld = *keyOld
		}
		err = admin.Repoint(ctx, ro)
		if err == nil {
			log.Info("repoint done")
		}
	case "move-back":
		fs := flag.NewFlagSet("move-back", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		specPath := fs.String("spec", "", "tables spec (JSON)")
		org := fs.Int64("org", 0, "organization id")
		tenant := fs.String("tenant", "", "the organization by its uuid instead of -org (block 18: its number from org-add)")
		customer := fs.String("customer", "", "DSN of the organization's database: its admin, or the cloud's role there (then the copy may run under a pause of the organization: see -dry-run)")
		lockTimeout := fs.Duration("lock-timeout", 5*time.Second, "max wait for the write lock at switch time")
		maxPause := fs.Duration("max-pause", 10*time.Second, "max write pause while the last changes are applied")
		bridge := fs.Duration("bridge", 60*time.Second, "how long proxies with old metadata may still address the foreign partition (2 x their reload interval)")
		dryRun := fs.Bool("dry-run", false, "say which way back (capture or pause) and how many rows, change nothing")
		abandon := fs.Bool("abandon", false, "the organization's database is lost: bring the organization back EMPTY (needs -confirm-org)")
		confirm := fs.Int64("confirm-org", 0, "with -abandon: the organization id again")
		parseArgs(fs)
		var spec admin.Spec
		if err = readJSON(*specPath, &spec); err != nil {
			break
		}
		if *org, err = admin.RequireOrg(context.Background(), *db, *org, *tenant); err != nil {
			break
		}
		if *abandon && *confirm != *org {
			err = fmt.Errorf("-abandon gives up every row of organization %d: repeat its id with -confirm-org %d", *org, *org)
			break
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: *db, CustomerDSN: *customer, Spec: spec.Sorted(), Org: *org,
			LockTimeout: *lockTimeout, MaxPause: *maxPause, Attempts: 5, Bridge: *bridge, DryRun: *dryRun, Abandon: *abandon, Log: log})
		if err == nil {
			log.Info("move-back done")
		}
	case "disable":
		fs := flag.NewFlagSet("disable", flag.ExitOnError)
		db := fs.String("db", "", "admin DSN of the cloud database")
		specPath := fs.String("spec", "", "tables spec (JSON) enable was given")
		dryRun := fs.Bool("dry-run", false, "count the rows disable would copy (its pause grows with them), change nothing")
		grace := fs.Duration("grace", 60*time.Second, "how long the refs tables and schema lokadb stay for proxies with old metadata (2 x their reload interval)")
		lockTimeout := fs.Duration("lock-timeout", 5*time.Second, "max wait for a lock; the tables' lock is tried again (-attempts)")
		attempts := fs.Int("attempts", 5, "how many times disable tries for the tables' lock")
		parseArgs(fs)
		var spec admin.Spec
		if err = readJSON(*specPath, &spec); err != nil {
			break
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		err = admin.Disable(ctx, admin.DisableOptions{CloudDSN: *db, Spec: spec.Sorted(), DryRun: *dryRun, Grace: *grace, LockTimeout: *lockTimeout, Attempts: *attempts, Log: log})
		if err == nil {
			log.Info("disable done")
		}
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// orgList: the organizations of -org N[,N]: positive numbers, or uuids (block 18). A command that
// changes things (write) acts on every moved organization only when told so (-all): left out,
// -org would have sent it to all of them. Without write, no list means every one.
func orgList(flagOrg string, write, all bool) ([]string, error) {
	if flagOrg == "" {
		if write && !all {
			return nil, errors.New("give -org N[,N], or -all for every moved organization")
		}
		return nil, nil
	}
	if all {
		return nil, errors.New("-org and -all together: one of them")
	}
	orgs := strings.Split(flagOrg, ",")
	for _, o := range orgs {
		if n, err := strconv.ParseInt(strings.TrimSpace(o), 10, 64); err == nil && n <= 0 {
			return nil, fmt.Errorf("-org %s: %w", o, admin.ValidOrg(n))
		}
	}
	return orgs, nil
}

// multiFlag: a flag that may be given several times (block 7).
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// pgconnConfig: the database name of a DSN (block 7).
func pgconnConfig(dsn string) (string, error) {
	c, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	return c.Database, nil
}

func level() slog.Level {
	if os.Getenv("LOKADB_DEBUG") != "" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// readSpec reads a spec and adds the routing declarations of a patch ("" for none).
func readSpec(path, patchPath string) (admin.Spec, error) {
	var spec admin.Spec
	if err := readJSON(path, &spec); err != nil {
		return spec, err
	}
	if patchPath != "" {
		var patch admin.Spec
		if err := readJSON(patchPath, &patch); err != nil {
			return spec, err
		}
		spec.Merge(patch.Routing)
	}
	return spec, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  lokadb proxy -config lokadb.json [-record workload.json]
  lokadb check -db DSN (-spec spec.json | -org-table TABLE [-write-spec F] | -tenant-column COL [-write-spec F]) [-workload F[,F]] [-app-role R[,R]] [-proxy-config F]
              [-customer DSN [-customer-app DSN] [-customer-schema S] [-org N]] [-direct-forms forms.sql]
              [-profile fdw|fdw+direct|direct] [-json]
              exit code: 0 ready, 2 needs review, 3 blocked, 4 not supported by lokadb yet, 5 invalid spec, 1 the check failed
              [-ci [-baseline accepted.json]] [-write-baseline F]: exit 0 nothing new, 6 new items (CI of the application)
  lokadb relay -config relay.json
  lokadb runner -config runner.json   (LokaDB Cloud's runner: takes jobs from the service, runs them here)
  lokadb issuer -config issuer.json   (signs agent certificates for the relays: the CA key lives only here)
  lokadb agent -relay HOST:PORT -ca F -state DIR -token-file F -db HOST:PORT [-cloud-listen ADDR [-cloud-allow A[,A]]] [-status-listen ADDR] [-status-url URL -status-token-file F] [-enroll-only]
  lokadb token  -cp DSN -org N [-ttl 15m]
  lokadb revoke -cp DSN -org N [-serial S]
  lokadb agents -cp DSN
  lokadb org-endpoint -cp DSN -org N (-listen ADDR [-tls require] [-change-port] | -remove)   (relays serve the organization without a restart)
  lokadb certs -dir DIR -relay-host relay
  lokadb org-profile -db DSN -org N [-customer-app DSN -direct-role ROLE] [-control-plane DSN -key FILE [-drop-fdw-credentials]] fdw|fdw+direct|direct   (the organization's routing profile, switched under its pause)
  lokadb org-credentials -db DSN -control-plane DSN -key FILE -org N -kind direct|owner -dsn DSN|-   (the organization's credentials for the proxy, sealed in the control plane)
  lokadb org-add -db DSN -tenant UUID [-org N]   (the number of an organization numbered by uuid: every command with -org takes it, or -tenant UUID)
  lokadb enable -db DSN -spec spec.json [-patch patch.json] -owner ROLE [-prepare] [-lock-timeout 5s]
               (-prepare first on large tables: online; enable then switches in a short lock)
  lokadb routing apply -db DSN -spec spec.json [-patch patch.json]   (routing declarations after enable)
  lokadb move-cleanup -db DSN   (what killed moves left: capture triggers, log, slots; refused while a move runs)
  lokadb move-org -db DSN -spec spec.json -owner ROLE -org N -customer DSN [-customer-app DSN] [-customer-schema S] [-publisher DSN] [-cp DSN]
                 [-lock-timeout 5s] [-max-pause 10s] [-retain-source] [-mode auto|logical|trigger] [-fetch-size 1000]
                 [-batch-rows 10000] [-max-split-pause 10m] [-copy-timeout 24h] [-catchup-timeout 2h] [-catchup-stall 10m] [-drop-returned] [-allow-customer-replication] [-dry-run [-sample 10s]]
  lokadb repoint -db DSN -org N -customer-app DSN -proxy-config F [-customer-schema S] [-direct DSN] [-owner DSN] [-key F] [-lock-timeout 5s]
                 (the organization's database moved: same rows, new address or credentials)
  lokadb move-back -db DSN -spec spec.json -org N -customer DSN [-dry-run] [-lock-timeout 5s] [-max-pause 10s] [-bridge 60s]
  lokadb move-back -db DSN -spec spec.json -org N -abandon -confirm-org N   (its database is lost: back EMPTY)
  lokadb disable -db DSN -spec spec.json [-dry-run] [-grace 60s] [-lock-timeout 5s] [-attempts 5]   (undo enable; every organization moved back first)
  lokadb schema-diff -db DSN [-org N[,N]] [-customer N=DSN]... [-proxy-config F] [-json]
  lokadb schema-sync -db DSN (-org N[,N] | -all) [-customer N=DSN]... [-proxy-config F] [-drop-extra] [-json]
              the organizations' copies of the LokaDB tables against the cloud's; exit code 0 same, 2 differences left, 1 failed
  lokadb refs-diff -db DSN [-org N[,N]] [-cloud] [-json]
  lokadb refs-sync -db DSN (-org N[,N] | -all [-cloud]) [-delete-stale] [-max-sequence-jump 100000] [-json]
              the refs tables against the rows the organizations have (rows written there past the cloud)
  lokadb COMMAND -h   (the flags of one command)`)
	os.Exit(2)
}

// DSN flags may hold a password, which ps and the shell's history show (review 2, check R15 and
// admin A13). Besides a DSN, they take "-" (read it from stdin, one flag per command) or
// env:NAME (from the environment variable NAME); a DSN without a password takes it from
// PGPASSFILE (~/.pgpass) or PGPASSWORD, as libpq does. A password given on the command line
// still works, with a warning.
var dsnFlags = map[string]bool{"db": true, "customer": true, "customer-app": true, "cp": true, "publisher": true, "dsn": true, "proxy-db": true,
	"direct": true, "owner": true, // repoint -direct, -owner (a role name elsewhere: no password in it)
	"control-plane": true} // org-profile, org-credentials (review 3 of admin, R3-11)

// parseArgs parses a command's flags (os.Args[2:]) and resolves its DSN flags.
func parseArgs(fs *flag.FlagSet) {
	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(1) // ExitOnError sets exit themselves; this is ContinueOnError's
	}
	secretFlags(fs)
}

var passwordInDSN = regexp.MustCompile(`(?i)(^|\s)password\s*=|^postgres(ql)?://[^/@]*:[^/@]*@|^postgres(ql)?://[^?]*\?(.*&)?password=`)

func secretFlags(fs *flag.FlagSet) {
	stdin := ""
	fs.Visit(func(f *flag.Flag) {
		if !dsnFlags[f.Name] || fmt.Sprintf("%T", f.Value) != "*flag.stringValue" {
			return
		}
		v := f.Value.String()
		switch {
		case v == "-":
			if stdin != "" {
				fmt.Fprintf(os.Stderr, "error: -%s -: stdin is already -%s's\n", f.Name, stdin)
				os.Exit(1)
			}
			stdin = f.Name
			b, err := io.ReadAll(os.Stdin)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error: reading -"+f.Name+" from stdin:", err)
				os.Exit(1)
			}
			_ = f.Value.Set(strings.TrimSpace(string(b)))
		case strings.HasPrefix(v, "env:"):
			name := strings.TrimPrefix(v, "env:")
			if os.Getenv(name) == "" {
				fmt.Fprintf(os.Stderr, "error: -%s %s: the environment variable %s is empty\n", f.Name, v, name)
				os.Exit(1)
			}
			_ = f.Value.Set(os.Getenv(name))
		case passwordInDSN.MatchString(v):
			fmt.Fprintf(os.Stderr, "warning: -%s holds a password on the command line (ps and the shell's history show it): give -%s - (stdin) or -%s env:NAME, or leave the password out and keep it in PGPASSFILE (~/.pgpass)\n", f.Name, f.Name, f.Name)
		}
	})
}

// mustKeyID: the key_id of a key file already read (printed, never the key).
func mustKeyID(path string) string {
	k, err := creds.LoadKey(path)
	if err != nil {
		return "?"
	}
	return creds.KeyID(k)
}

// setMovedProfile: move-org's choice of the moved organization's profile (admin.SetMovedProfile):
// the proxy config's customers_app and control plane say whether the direct path can be taken.
func setMovedProfile(ctx context.Context, db string, org int64, profile, proxyCfg, customerApp, customer, directRole string, lockTimeout time.Duration, log *slog.Logger) error {
	if customerApp == "" {
		customerApp = customer
	}
	o := admin.MovedProfileOptions{OrgProfileOptions: admin.OrgProfileOptions{CloudDSN: db, Org: org, LockTimeout: lockTimeout, Log: log,
		CustomerAppDSN: customerApp, DirectRole: directRole}, Explicit: profile}
	if proxyCfg != "" {
		var cfg proxy.Config
		if err := readJSON(proxyCfg, &cfg); err != nil {
			return err
		}
		dbName, err := pgconnConfig(db)
		if err != nil {
			return err
		}
		o.ProxyConfig = true
		o.ProxyCustomersApp = cfg.CustomersApp[dbName][strconv.FormatInt(org, 10)]
		o.ControlPlane, o.KeyFile, o.KeyFileOld = cfg.ControlPlane, cfg.CredentialsKey, cfg.CredentialsKeyOld
	}
	got, warning, err := admin.SetMovedProfile(ctx, o)
	if err != nil {
		return err
	}
	if warning != "" {
		log.Warn(warning)
		return nil
	}
	log.Info("the moved organization's profile", "org", org, "profile", got)
	return nil
}

// stallOption: -catchup-stall 0 turns the stall check off (MoveOptions: negative; 0 is its default).
func stallOption(d time.Duration) time.Duration {
	if d == 0 {
		return -1
	}
	return d
}
