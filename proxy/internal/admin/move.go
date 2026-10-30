package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pg "github.com/pganalyze/pg_query_go/v6"
)

var setNullRe = regexp.MustCompile(`ON DELETE SET (NULL|DEFAULT)(\s*\([^)]*\))?`)

// moveLockKey: the advisory lock a running move-org holds in the cloud database ("lokadb").
const moveLockKey int64 = 0x62796f6462

// fkLocalRe splits an FK onto a refs table: its columns, the refs columns it references
// (id, or tenant and id), what follows.
var fkLocalRe = regexp.MustCompile(`^FOREIGN KEY \(([^)]+)\) REFERENCES .+?\(([^)]+)\)(.*)$`)

// MoveOptions configures MoveOrg.
type MoveOptions struct {
	dbID        string // the organization's database id, in the mark on its tables there (dbid.go)
	CloudDSN    string // admin DSN of the cloud database
	CustomerDSN string // admin DSN of the organization's own database
	// CustomerAppDSN: the role the cloud uses in the organization's database afterwards
	// (postgres_fdw, the proxy's DDL replay). It becomes the owner of the moved tables and
	// needs no other right. "" = the admin role of CustomerDSN (not recommended).
	CustomerAppDSN string
	PublisherDSN   string // the cloud database as the customer database reaches it (subscription)
	// ControlPlaneDSN: when set, the relay's window that lets the organization's agent
	// reach the cloud database is opened for the move and closed after it.
	ControlPlaneDSN string
	Spec            Spec
	Org             int64
	Owner           string
	LockTimeout     time.Duration // how long the switch may wait for the write lock
	MaxPause        time.Duration // how long writes may be paused while the last changes arrive
	Attempts        int
	RetainSource    bool // keep the detached cloud partition instead of dropping it
	// Mode: ModeLogical (subscription in the organization's database), ModeTrigger (change
	// capture by triggers in the cloud, applied by move-org), ModeAuto ("": logical when
	// both sides can, see MoveMode).
	Mode      string
	FetchSize int // postgres_fdw fetch_size of the organization's server
	// CustomerSchema: the schema of the organization's tables in its database ("": public if
	// it can take them, else DefaultCustomerSchema; see ChooseCustomerSchema).
	CustomerSchema string
	Log            *slog.Logger

	// Large organizations (block 5).
	// BatchRows: rows per transaction of a copy in batches (the split out of DEFAULT; 0:
	// DefaultBatchRows).
	BatchRows int
	// MaxSplitPause: how long the organization may pause while it is split out of DEFAULT;
	// past it the split stops and the rows stay in DEFAULT (0: DefaultMaxSplitPause).
	MaxSplitPause time.Duration
	// CopyTimeout: how long the initial copy may take (0: DefaultCopyTimeout).
	CopyTimeout time.Duration
	// CatchUpTimeout: how long the organization's database may take to catch up with the
	// cloud after the initial copy (0: DefaultCatchUpTimeout).
	CatchUpTimeout time.Duration
	// AllowCustomerReplication: -mode logical is allowed. Its subscription connects as a role
	// with the REPLICATION attribute, which reads the change stream of the whole cloud database.
	AllowCustomerReplication bool
	// DropReturned: the copy an earlier move-back left of this organization in this database is
	// dropped (no CASCADE) instead of moved aside to a schema of its own (returned.go).
	DropReturned bool

	// CatchUpStall: the catch-up gives up once the log left has not got shorter for this long
	// (0: DefaultCatchUpStall; negative: never, only CatchUpTimeout).
	CatchUpStall time.Duration

	tenant Tenant // the organization by its value too (block 18: resolve, org)
	// replica: whether the role may turn the FK checks off in the organization's database, asked
	// once per move by the first round that applies the log (nil: asked by every round)
	replica *replicaRight
}

type moveTable struct {
	name, tenant string
	key          string     // the column refs maps to the organization (lokadb.tables.key_column)
	cols         [][]string // name, type, notnull, generated expression, own collation, attgenerated
	checks       [][]string // CHECK constraints of the parent: name, definition
	org          int64
	val          string // the organization's value as an SQL literal (Tenant.Lit, block 18)
	// replace: where the log is applied (t.local()) has no unique index on the key alone, so an
	// entry replaces the row (DELETE, INSERT) instead of an upsert (the split's copy in the cloud)
	replace bool
}

// colDef declares a column the way the parent has it. A partition must repeat the parent's
// NOT NULL and generated columns of the same kind (ATTACH refuses it otherwise); the
// collation keeps sorting and unique checks in the organization's database as in the cloud.
func colDef(c []string) string {
	d := q(c[0]) + " " + c[1]
	if c[4] != "" {
		d += " COLLATE " + c[4]
	}
	switch {
	case c[3] != "" && c[5] == "v": // PostgreSQL 18+: computed when read
		d += " GENERATED ALWAYS AS (" + c[3] + ") VIRTUAL"
	case c[3] != "":
		d += " GENERATED ALWAYS AS (" + c[3] + ") STORED"
	}
	if c[2] == "true" {
		d += " NOT NULL"
	}
	return d
}

// local is the organization's partition in the cloud database, and also the name the
// customer copy has while it is being replicated (logical replication matches by name).
func (t moveTable) local() string { return hn(t.name, fmt.Sprintf("__o%d", t.org)) }

func (t moveTable) foreign() string { return hn(t.name, fmt.Sprintf("__org%d", t.org)) }

// likeParent: a partitioned table made only to give the foreign table the parent's columns and
// CHECK constraints, dropped right after.
func (t moveTable) likeParent() string { return hn(t.name, fmt.Sprintf("__org%d_like", t.org)) }

// foreignTableDDL: the foreign table that stands for the organization's table, to be attached to
// the parent at the switch. ATTACH wants the parent's CHECK constraints on it, compared by their
// stored expression (conbin), not by meaning: the text of pg_get_constraintdef parsed again can
// give another expression (a Laravel enum, a varchar column = ANY (ARRAY['a'::character
// varying]::text[]), came back with other casts and ATTACH refused it: "has different definition
// for check constraint"). CREATE FOREIGN TABLE takes no LIKE, so it is made a partition of a
// table made LIKE the parent (which copies the expressions, not their text) and detached from it:
// the constraints stay, with the parent's expressions. Postgres does not check them on a foreign
// table: the organization's database does (see describe).
func foreignTableDDL(t moveTable, key, server, custSchema string) []string {
	return []string{
		"DROP FOREIGN TABLE IF EXISTS " + qr(t.foreign()),
		"DROP TABLE IF EXISTS " + qr(t.likeParent()),
		fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING CONSTRAINTS INCLUDING GENERATED) PARTITION BY LIST (%s)",
			qr(t.likeParent()), qr(t.name), key), // the parent's partition key (tenantKey)
		fmt.Sprintf("CREATE FOREIGN TABLE %s PARTITION OF %s FOR VALUES IN (%s) SERVER %s OPTIONS (schema_name %s, table_name %s)",
			qr(t.foreign()), qr(t.likeParent()), t.val, q(server), lit(custSchema), lit(nameOf(t.name))),
		fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", qr(t.likeParent()), qr(t.foreign())),
		"DROP TABLE " + qr(t.likeParent()),
	}
}

func (t moveTable) retired() string { return hn(t.name, fmt.Sprintf("__o%d_source", t.org)) }

var errRetry = errors.New("switch not possible right now")

// MoveOrg moves one organization's rows into its own database without stopping writes:
//
//  1. the organization's partitions are published and the customer database subscribes:
//     consistent initial copy, then every change (logical replication);
//  2. once both sides match (row count + md5 of every row), writes to these tables are
//     paused, a marker row proves the replica has every earlier commit, and the
//     organization's partitions are swapped for foreign tables: DETACH + ATTACH, no row
//     is copied or deleted, and a validated CHECK on the DEFAULT partition spares ATTACH
//     its scan, so the pause does not grow with the amount of data;
//  3. the detached partitions are compared with the customer copy and dropped.
func MoveOrg(ctx context.Context, o MoveOptions) (err error) {
	if err := ValidOrg(o.Org); err != nil {
		return fmt.Errorf("move-org: %w", err)
	}
	log := o.Log.With("org", o.Org)
	o.replica = &replicaRight{}
	if o.PublisherDSN == "" {
		o.PublisherDSN = o.CloudDSN
	}
	cc, err := pgconn.ParseConfig(o.CustomerDSN)
	if err != nil {
		return err
	}
	// The credentials the cloud keeps for the organization's database.
	app := cc
	if o.CustomerAppDSN != "" {
		if app, err = pgconn.ParseConfig(o.CustomerAppDSN); err != nil {
			return fmt.Errorf("customer app DSN: %w", err)
		}
	} else {
		log.Warn("no -customer-app: the cloud keeps the admin credentials of the organization's database (postgres_fdw, DDL replay)")
	}
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer cloud.Close(ctx)
	marker, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer marker.Close(ctx)
	cust, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	defer cust.Close(ctx)

	// Two moves at once (of one organization, or of two sharing the DEFAULT partitions) would
	// drop each other's subscription, tables and partitions. The lock lives as long as cloud.
	var mine bool
	if err := cloud.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", moveLockKey).Scan(&mine); err != nil {
		return err
	}
	if !mine {
		return errors.New("another move-org is running against this cloud database")
	}
	// block 7: from the lock on, the proxy refuses DDL on the LokaDB tables; one it is replaying
	// right now may not be committed yet. Waiting for it here, describe reads the new schema.
	if err := waitDDL(ctx, cloud, o); err != nil {
		return err
	}
	// block 18: an organization numbered by uuid goes by its value (lokadb.tenants, org-add)
	if err := o.resolve(ctx, cloud); err != nil {
		return err
	}
	if ValueKind(o.tenant.Kind) {
		log = log.With("tenant", o.tenant.Value)
	}
	pub := fmt.Sprintf("lokadb_org%d", o.Org)
	sub, server := pub, pub
	// Replication slots and roles belong to the whole cloud cluster, which may hold several
	// application databases moving the same organization number: the database's OID tells
	// them apart.
	var dbOID uint32
	if err := cloud.QueryRow(ctx, "SELECT oid FROM pg_database WHERE datname = current_database()").Scan(&dbOID); err != nil {
		return err
	}
	slot := fmt.Sprintf("lokadb_%d_org%d", dbOID, o.Org)
	// What earlier moves left (killed): this organization's own slot is dropped in step 2,
	// another's keeps the cloud's WAL until move-cleanup.
	if left, err := MoveLeftovers(ctx, cloud); err == nil && len(left) > 0 {
		log.Warn("an interrupted move-org left these behind: run lokadb move-cleanup", "left", strings.Join(left, "; "))
	}
	repl := fmt.Sprintf("lokadb_repl_%d_org%d", dbOID, o.Org)
	switched := false
	var fullIdentity []string // partitions given REPLICA IDENTITY FULL for the move
	var tables []moveTable
	// Where the tables go in the organization's database: public if it can take them, else
	// schema lokadb (a table of the customer's has one of their names, or the roles may not
	// create there). Every unqualified name of the move resolves there first.
	var names []string
	for _, st := range o.Spec.Tables {
		if schemaOf(st.Name) == "public" { // the others go to their own schema there
			names = append(names, st.Name, hn(st.Name, fmt.Sprintf("__o%d", o.Org)))
		}
	}
	choice, err := ChooseCustomerSchema(ctx, cust, o.CustomerSchema, app.User, names, o.Org)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	if choice.Schema == "" {
		return fmt.Errorf("the organization's database cannot take the tables: %s", choice.Refusals())
	}
	var keys []string
	for _, st := range o.Spec.Tables {
		keys = append(keys, st.Name)
	}
	if clash, err := OtherSchemaClashes(ctx, cust, keys, o.Org); err != nil {
		return fmt.Errorf("customer database: %w", err)
	} else if len(clash) > 0 {
		return fmt.Errorf("the organization's database cannot take the tables: %s; it is left alone: rename it, or use another database",
			SchemaChoice{Refused: clash}.Refusals())
	}
	if choice.Schema != "public" {
		log.Info("the organization's tables go to schema "+choice.Schema, "public", choice.Refusals())
	}
	// block 7: logical replication writes to public, which only a GRANT of the organization's opens
	if g := choice.GrantFor(); g != "" && o.Mode == ModeLogical {
		return fmt.Errorf("-mode logical: the organization's tables cannot go to public (%s), and logical replication writes to the cloud's schema, public. The organization's database allows it with: %s; or use -mode auto or trigger", choice.Refusals(), g)
	}
	o.CustomerSchema = choice.Schema
	// public's tables go to that schema there, another schema's to their own: a table of public
	// and one of schema S of the same name would be one table there when S is it
	for _, st := range o.Spec.Tables {
		if s, n := SplitKey(st.Name); s == "public" {
			for _, ot := range o.Spec.Tables {
				if ts, tn := SplitKey(ot.Name); ts == choice.Schema && ts != "public" && tn == n {
					return fmt.Errorf("the organization's tables of public go to schema %s, where %s's table %s goes too: they would be one table. Choose another schema for them (-customer-schema), or leave one of them in the cloud",
						choice.Schema, ot.Name, n)
				}
			}
		}
	}
	if err := setCustomerSchema(ctx, cust, o.CustomerSchema); err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	// The extensions the tables use, before anything changes: at the cloud's version, or a
	// refusal with the command for the organization's administrator (extensions.go).
	if err := preflightExtensions(ctx, cloud, cust, o, log); err != nil {
		return err
	}
	mode, err := chooseMode(ctx, cloud, cust, o, log)
	if err != nil {
		return err
	}
	log.Info("move mode", "mode", mode)
	// Whatever happens, nothing of the move outlives it: the subscription unless the switch
	// dropped it, its slot, the publication, the replication role, the relay window.
	// It runs on fresh connections and its own context: the move may end because its context
	// was cancelled (Ctrl-C), which also closes the connections it was using.
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ccloud, cerr := connect(cctx, o.CloudDSN)
		if cerr != nil {
			log.Warn("cleanup: cloud database unreachable: subscription, slot, publication or role may be left", "err", cerr)
			return
		}
		defer ccloud.Close(cctx)
		var steps []string
		if switched && mode != ModeTrigger {
			// the switch dropped the subscription without its slot
			if serr := dropSlot(cctx, ccloud, slot); serr != nil {
				log.Warn("cleanup: replication slot left in the cloud database", "slot", slot, "err", serr)
			}
		}
		if !switched {
			dropGuards(ccloud, o, log) // a split that failed: its rows are in DEFAULT
			ccust, cerr := connect(cctx, o.CustomerDSN)
			if cerr == nil {
				cerr = dropSubscription(cctx, ccust, ccloud, sub, slot)
				ccust.Close(cctx)
			}
			if cerr != nil {
				// The slot would keep the cloud's WAL forever: dropped here; the subscription
				// left there cannot connect any more (its role is dropped below).
				log.Warn("cleanup: subscription left in the customer database, dropping its slot in the cloud", "subscription", sub, "err", cerr)
				if serr := dropSlot(cctx, ccloud, slot); serr != nil {
					log.Warn("cleanup: replication slot left in the cloud database", "slot", slot, "err", serr)
				}
			}
			steps = append(steps, "DROP PUBLICATION IF EXISTS "+q(pub))
			if mode == ModeTrigger {
				steps = append(steps, dropCapture(o.Org, tables, moveTable.local)...) // the triggers and the log of this move
			}
			for _, p := range fullIdentity {
				// logs whole old rows of every UPDATE and DELETE otherwise
				steps = append(steps, fmt.Sprintf("ALTER TABLE IF EXISTS %s REPLICA IDENTITY DEFAULT", q(p)))
			}
		}
		steps = append(steps, dropRole(repl)...)
		if cerr := execAll(cctx, ccloud, steps); cerr != nil {
			log.Warn("cleanup: replication role or publication left in the cloud database", "role", repl, "err", cerr)
		}
		if o.ControlPlaneDSN != "" {
			if cerr := cloudWindow(cctx, o, 0, ""); cerr != nil {
				log.Warn("cleanup: the relay window to the cloud database stays open until it expires", "err", cerr)
			}
		}
	}()
	// A database enabled by an older version has older lokadb functions (partition_org), and
	// the organizations it moved lack the triggers of the FKs onto cloud tables.
	if err := execAll(ctx, cloud, []string{baseSQL, "SELECT lokadb.fk_cloud_sync()", "SELECT lokadb.fk_cascade_sync()"}); err != nil {
		return err
	}
	moved, err := rowsOf(ctx, cloud, `SELECT count(*)::text FROM lokadb.orgs WHERE org = $1`, o.Org)
	if err != nil {
		return err
	}
	if moved[0][0] != "0" {
		switched = true // nothing of this run to clean up: leave the moved organization alone
		return fmt.Errorf("organization %d is already in its own database", o.Org)
	}

	log.Info("step 1/6: organization partitions")
	// enable leaves every organization in DEFAULT: the organization's rows move to a partition
	// of its own while the organization pauses (split.go).
	if err := splitOrg(ctx, cloud, o, log); err != nil {
		return err
	}
	tables, custDDL, err := describe(ctx, cloud, o.Spec, o.org())
	if err != nil {
		return err
	}
	o.dbID = newDBID() // in the mark on the tables and in lokadb.orgs (dbid.go)
	custDDL = withDBID(custDDL, o.Org, o.dbID)

	log.Info("step 2/6: customer schema", "host", cc.Host, "db", cc.Database)
	if err := dropSubscription(ctx, cust, cloud, sub, slot); err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	if err := dropSlot(ctx, cloud, slot); err != nil { // left by an attempt that could not clean up
		return err
	}
	if choice.Create {
		create := "CREATE SCHEMA IF NOT EXISTS " + q(o.CustomerSchema)
		if app.User != cc.User {
			create += " AUTHORIZATION " + q(app.User) // the cloud's role there owns its tables' schema
		}
		if err := runCustomer(ctx, cust, []string{create}, log); err != nil {
			return err
		}
	}
	// A table of a schema other than public goes to the same schema there.
	if err := runCustomer(ctx, cust, otherSchemas(o.Spec, app.User, cc.User), log); err != nil {
		return err
	}
	if err := customerDeps(ctx, cloud, cust, tables, app.User, o.CustomerSchema, log); err != nil {
		return err
	}
	var markerDDL []string
	if mode == ModeLogical { // the switch's marker travels through the subscription
		markerDDL = []string{
			"CREATE SCHEMA IF NOT EXISTS lokadb",
			"CREATE TABLE IF NOT EXISTS lokadb.move_marker (org bigint, token text, PRIMARY KEY (org, token))",
		}
	}
	if err := checkReturned(ctx, cloud, cust, tables, o.CustomerSchema); err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	if err := customerSchema(ctx, cust, tables, app.User, o.CustomerSchema, append(markerDDL, custDDL...), o.DropReturned, log); err != nil {
		return fmt.Errorf("customer database: %w", err)
	}

	log.Info("step 3/6: publication, foreign tables, CHECK on DEFAULT")
	var prep, pubTables, locked []string
	for _, t := range tables {
		// The whole partition is published: no row filter, no replica identity changes
		// for anybody else. Without a primary key its UPDATE and DELETE would fail while it
		// is published: it identifies rows by all columns then (the partition goes away with
		// the switch; a failed move resets it, see fullIdentity).
		pubTables = append(pubTables, qr(t.local()))
		full, err := needsFullIdentity(ctx, cloud, t.local())
		if err != nil {
			return err
		}
		if full && mode == ModeLogical {
			locked = append(locked, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", qr(t.local())))
			fullIdentity = append(fullIdentity, t.local())
		}
		// ATTACH skips scanning DEFAULT when a valid CHECK proves it has no row of this
		// organization (it cannot have any: the organization has its own partition).
		// VALIDATE takes SHARE UPDATE EXCLUSIVE: reads and writes continue meanwhile.
		chk := fmt.Sprintf("lokadb_not_org%d", o.Org)
		valid, err := rowsOf(ctx, cloud, `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2 AND convalidated)::text`,
			qr(hn(t.name, "__cloud")), chk)
		if err != nil {
			return err
		}
		if valid[0][0] == "true" { // the split left it validated (split.go)
			continue
		}
		locked = append(locked,
			fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s", qr(hn(t.name, "__cloud")), q(chk)),
			// on the partition key itself, or it proves nothing to ATTACH
			fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s <> %s) NOT VALID", qr(hn(t.name, "__cloud")), q(chk), o.org().Key(t.tenant), t.val),
			fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qr(hn(t.name, "__cloud")), q(chk)))
	}
	// The TLS of the organization's DSNs (sslmode, sslrootcert...) on the server too: postgres_fdw
	// takes only the server's options (review 3, R3-5). -customer-app first: the cloud's role
	// there is what postgres_fdw connects as.
	ssl, err := FDWSSLOptions(o.CustomerAppDSN, o.CustomerDSN)
	if err != nil {
		return fmt.Errorf("the organization's DSN: %w", err)
	}
	sslOpts := ""
	if len(ssl) > 0 {
		sslOpts = ", " + strings.Join(ssl, ", ")
	}
	prep = append(prep,
		// fetch_size: rows per round trip when reading (postgres_fdw's default is 100, which
		// makes a full read of an organization cost hundreds of round trips over a WAN).
		// A server left by an earlier attempt may carry an old address or password: it is
		// recreated. It cannot be in use: the organization is not moved (checked above), so
		// no foreign table of it is attached.
		"CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		"DROP SERVER IF EXISTS "+q(server)+" CASCADE",
		fmt.Sprintf("CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %s, port %s, dbname %s, async_capable 'true', connect_timeout '3', fetch_size '%d'%s)",
			q(server), lit(cc.Host), lit(fmt.Sprint(cc.Port)), lit(cc.Database), o.FetchSize, sslOpts),
	)
	// Only the roles that read the LokaDB tables get a user mapping (the owner, roles granted
	// privileges on them, and this admin), and none gets USAGE on the server: a role with
	// USAGE could create foreign tables of its own and read whatever the mapped role reads.
	roles, err := mappedRoles(ctx, cloud, tables, o.Owner)
	if err != nil {
		return err
	}
	for _, r := range roles {
		prep = append(prep, fmt.Sprintf("CREATE USER MAPPING FOR %s SERVER %s OPTIONS (user %s, password %s)", r, q(server), lit(app.User), lit(app.Password)))
	}
	for _, t := range tables {
		prep = append(prep, foreignTableDDL(t, o.org().Key(t.tenant), server, custSchemaOf(t.name, o.CustomerSchema))...)
	}
	// A marker row committed after the write lock proves, once it reaches the customer,
	// that every earlier commit has been applied there (changes apply in commit order).
	pubTables = append(pubTables, fmt.Sprintf("lokadb.move_marker WHERE (org = %d)", o.Org))
	replFrom := len(prep) // from here on: logical replication only
	// The subscription connects as a role made for this move, which expires by itself and is
	// dropped after the move, so the customer never gets the cloud's own credentials. Its
	// SELECT is on the published tables only, but a logical subscription needs the REPLICATION
	// attribute, and with it the role can create a logical slot and read the change stream of
	// the WHOLE cloud database (the rows of every organization, whatever SELECT it has).
	// Hence -mode logical only with -allow-customer-replication, and -mode auto never.
	replPassword, err := randomPassword()
	if err != nil {
		return err
	}
	prep = append(prep, dropRole(repl)...)
	prep = append(prep,
		// it outlives the move by a margin only: the move gives up by then (its own limits)
		fmt.Sprintf("CREATE ROLE %s LOGIN REPLICATION CONNECTION LIMIT 8 VALID UNTIL %s PASSWORD %s",
			q(repl), lit(time.Now().Add(o.moveWindow()).UTC().Format(time.RFC3339)), lit(replPassword)),
		"CREATE TABLE IF NOT EXISTS lokadb.move_marker (org bigint, token text, PRIMARY KEY (org, token))",
		"GRANT USAGE ON SCHEMA lokadb TO "+q(repl),
		"GRANT SELECT ON lokadb.move_marker TO "+q(repl))
	usage := map[string]bool{"public": true} // the tables of another schema: their schema too
	for _, t := range tables {
		if s := schemaOf(t.name); !usage[s] {
			usage[s] = true
			prep = append(prep, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", q(s), q(repl)))
		}
		prep = append(prep, fmt.Sprintf("GRANT SELECT ON %s TO %s", qr(t.local()), q(repl)))
	}
	prep = append(prep,
		"DROP PUBLICATION IF EXISTS "+q(pub),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')",
			q(pub), strings.Join(pubTables, ", ")))
	if mode == ModeTrigger { // no publication, no replication role: nothing connects to the cloud
		prep = prep[:replFrom]
	}
	if err := lockedDDL(ctx, cloud, o, locked); err != nil {
		return err
	}
	if err := execAll(ctx, cloud, prep); err != nil {
		return err
	}

	if mode == ModeTrigger {
		log.Info("step 4/6: change capture, initial copy")
		start := time.Now()
		if err := installCapture(ctx, cloud, o, tables, moveTable.local); err != nil {
			return err
		}
		hbCtx, stopBeat := context.WithCancel(ctx)
		defer stopBeat()
		go beat(hbCtx, o.CloudDSN, o.Org, log)
		dial := func(ctx context.Context) (*pgx.Conn, error) {
			c, err := connect(ctx, o.CustomerDSN)
			if err == nil {
				if err = setCustomerSchema(ctx, c, o.CustomerSchema); err != nil {
					c.Close(context.Background())
				}
			}
			if err != nil {
				return nil, fmt.Errorf("customer database: %w", err)
			}
			return c, nil
		}
		order, err := copyOrder(ctx, cloud, tables)
		if err != nil {
			return err
		}
		if err := initialCopy(ctx, cloud, dial, order, moveTable.local, moveTable.local, o.batchRows(), log); err != nil {
			return err
		}
		log.Info("initial copy done", "took", time.Since(start).Round(time.Millisecond))
		log.Info("step 5/6: catching up (writes continue)")
		if err := catchUp(ctx, cloud, cust, o, tables, log); err != nil {
			return err
		}
		return finishMove(ctx, cloud, marker, cust, o, tables, mode, pub, sub, server, &switched, log)
	}

	log.Info("step 4/6: subscription, initial copy")
	if o.ControlPlaneDSN != "" {
		if err := cloudWindow(ctx, o, o.moveWindow(), repl); err != nil {
			return fmt.Errorf("control plane: %w", err)
		}
	}
	conninfo, err := withCredentials(o.PublisherDSN, repl, replPassword)
	if err != nil {
		return fmt.Errorf("publisher DSN: %w", err)
	}
	start := time.Now()
	if err := execAll(ctx, cust, []string{fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION %s PUBLICATION %s WITH (slot_name = %s)",
		q(sub), lit(conninfo), q(pub), lit(slot))}); err != nil {
		return fmt.Errorf("customer database: %w%s", err, publisherHint(err))
	}
	want := fmt.Sprint(len(tables) + 1) // + lokadb.move_marker
	watch, err := newCopyWatch(ctx, cloud, tables, log)
	if err != nil {
		return err
	}
	for {
		rows, err := rowsOf(ctx, cust, `SELECT count(*)::text, count(*) FILTER (WHERE r.srsubstate = 'r')::text,
			count(*) FILTER (WHERE r.srsubstate <> 'i')::text
			FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = $1`, sub)
		if err != nil {
			return err
		}
		if rows[0][0] == want && rows[0][1] == want {
			break
		}
		// No table copy started: the subscription cannot reach the cloud database.
		if rows[0][2] == "0" && time.Since(start) > 90*time.Second {
			return errors.New("the subscription has not started copying in 90 s: it cannot reach the cloud database " +
				"(is -publisher reachable from the organization's database, is the relay's window open: -cp? see the customer database's log)")
		}
		if time.Since(start) > o.copyTimeout() {
			return fmt.Errorf("initial copy did not finish in %s (-copy-timeout)", o.copyTimeout())
		}
		watch.poll(ctx, cust, sub)
		time.Sleep(200 * time.Millisecond)
	}
	watch.finish()
	log.Info("initial copy done", "took", time.Since(start).Round(time.Millisecond))

	// Catching up: a marker's round trip through replication is the replica's lag. Comparing
	// data here would not converge under steady writes once a checksum takes longer than the
	// gap between writes; the full comparison runs after the switch, on a frozen snapshot.
	log.Info("step 5/6: catching up (writes continue)")
	start = time.Now()
	var lags []time.Duration
	for {
		lag, err := markerLag(ctx, marker, cust, o.Org, time.Minute)
		if err != nil {
			return err
		}
		lags = append(lags, lag)
		if ok, why := caughtUp(lags, o.MaxPause); ok {
			log.Info("replica caught up", "lag", lag.Round(time.Millisecond), "why", why, "took", time.Since(start).Round(time.Millisecond))
			break
		}
		if time.Since(start) > o.catchUpTimeout() {
			return fmt.Errorf("replica did not catch up in %s (-catchup-timeout): %s", o.catchUpTimeout(), catchUpHint(lags, o.MaxPause))
		}
		log.Info("replica still behind", "lag", lag.Round(time.Millisecond))
	}

	return finishMove(ctx, cloud, marker, cust, o, tables, mode, pub, sub, server, &switched, log)
}

// finishMove switches the organization over to its database, verifies, drops the source.
func finishMove(ctx context.Context, cloud, marker, cust *pgx.Conn, o MoveOptions, tables []moveTable, mode, pub, sub, server string, switched *bool, log *slog.Logger) error {
	log.Info("step 6/6: switch")
	snap, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	defer snap.Close(ctx)
	if err := setCustomerSchema(ctx, snap, o.CustomerSchema); err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	var snapTx pgx.Tx
	for attempt := 1; ; attempt++ {
		var p pause
		p, snapTx, err = switchOver(ctx, cloud, marker, cust, snap, o, tables, mode, pub, sub, server)
		if err == nil {
			*switched = true
			log.Info("switched", "writes_paused", p.total, "wait_last_changes", p.catchUp, "drop_subscription", p.dropSub, "swap_partitions", p.swap, "attempt", attempt)
			break
		}
		if !errors.Is(err, errRetry) || attempt >= o.Attempts {
			return err
		}
		log.Warn("switch attempt failed, retrying", "attempt", attempt, "err", err)
		time.Sleep(time.Second)
	}

	// The detached partitions must equal the customer copy as it was at the switch. Writes
	// resumed since, so the customer side is read in a snapshot taken while they were paused.
	same, err := compare(ctx, cloud, snapTx, tables, func(t moveTable) string { return t.retired() }, func(t moveTable) string { return t.name })
	_ = snapTx.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("verify after switch: %w", err)
	}
	if !same {
		return fmt.Errorf("verify after switch: the customer copy differs from the detached source; source kept as *_source tables")
	}
	log.Info("verified: customer copy equals the detached source partitions")
	analyzeForeign(ctx, cloud, tables, log)
	if o.RetainSource {
		log.Info("source partitions retained", "suffix", "_source")
		return nil
	}
	var drops []string
	for i := len(tables) - 1; i >= 0; i-- {
		drops = append(drops, "DROP TABLE "+qr(tables[i].retired()))
	}
	if err := execAll(ctx, cloud, drops); err != nil {
		return err
	}
	log.Info("source partitions dropped from the cloud database")
	return nil
}

// analyzeForeign gives the organization's new foreign partitions their statistics. Postgres
// keeps none for a foreign table until it is analyzed by name (autovacuum and a database-wide
// ANALYZE skip it), and without them the planner takes the organization for a handful of rows:
// postgres_fdw then sends neither ORDER BY nor LIMIT to the organization's database, and a feed
// (ORDER BY id DESC LIMIT 20) fetched every row of the organization (Z2 of block 14's trial). A
// failure is only logged: the move is done, the statistics can be made later (ANALYZE <table>).
func analyzeForeign(ctx context.Context, cloud *pgx.Conn, tables []moveTable, log *slog.Logger) {
	for _, t := range tables {
		t0 := time.Now()
		if _, err := cloud.Exec(ctx, "ANALYZE "+qr(t.foreign())); err != nil {
			log.Warn("ANALYZE of the foreign partition failed: the planner has no statistics of it until ANALYZE "+t.name, "table", t.foreign(), "err", err)
			continue
		}
		log.Info("statistics of the foreign partition", "table", t.foreign(), "took", time.Since(t0).Round(time.Millisecond))
	}
}

// waitDDL waits for the DDL running on the spec's tables to commit (ACCESS SHARE waits only
// for ACCESS EXCLUSIVE), each attempt at most LockTimeout.
func waitDDL(ctx context.Context, cloud *pgx.Conn, o MoveOptions) error {
	var names []string
	for _, st := range o.Spec.Tables {
		names = append(names, qr(st.Name))
	}
	if len(names) == 0 {
		return nil
	}
	for attempt := 1; ; attempt++ {
		err := pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.LockTimeout.Milliseconds())); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "LOCK TABLE "+strings.Join(names, ", ")+" IN ACCESS SHARE MODE")
			return err
		})
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || (pgErr.Code != "55P03" && pgErr.Code != "40P01") || attempt >= max(o.Attempts, 1) {
			if err != nil {
				return fmt.Errorf("wait for the DDL running on the tables: %w", err)
			}
			return nil
		}
		o.Log.Warn("DDL running on the tables, waiting", "attempt", attempt)
		time.Sleep(time.Second)
	}
}

// needsFullIdentity: the partition has no replica identity, so a publication would make
// Postgres refuse its UPDATE and DELETE.
func needsFullIdentity(ctx context.Context, cloud *pgx.Conn, part string) (bool, error) {
	r, err := rowsOf(ctx, cloud, `SELECT (c.relreplident = 'n' OR c.relreplident = 'd'
		AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary))::text
		FROM pg_class c WHERE c.oid = $1::regclass`, qr(part))
	if err != nil {
		return false, err
	}
	return r[0][0] == "true", nil
}

// lockedDDL runs statements that lock tables every organization shares (DEFAULT, the
// partitions), each waiting at most LockTimeout and retried: an exclusive lock queued behind
// a long transaction would stop everybody queued behind it.
func lockedDDL(ctx context.Context, cloud *pgx.Conn, o MoveOptions, stmts []string) error {
	if len(stmts) == 0 {
		return nil
	}
	if _, err := cloud.Exec(ctx, fmt.Sprintf("SET lock_timeout = '%dms'", o.LockTimeout.Milliseconds())); err != nil {
		return err
	}
	defer cloud.Exec(context.Background(), "RESET lock_timeout") //nolint:errcheck
	for _, st := range stmts {
		for attempt := 1; ; attempt++ {
			_, err := cloud.Exec(ctx, st)
			if err == nil {
				break
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || (pgErr.Code != "55P03" && pgErr.Code != "40P01") || attempt >= o.Attempts {
				return fmt.Errorf("%w\n  in: %s", err, mask(st))
			}
			o.Log.Warn("table busy, retrying", "statement", mask(st), "attempt", attempt, "err", err)
			time.Sleep(time.Second)
		}
	}
	return nil
}

// splitOut gives the organization its own partition of table (lokadb.partition_org), waiting
// at most LockTimeout for the DEFAULT partition each attempt: everybody queues behind a
// waiting exclusive lock, so it must not wait long.
func splitOut(ctx context.Context, cloud *pgx.Conn, o MoveOptions, table string) error {
	for attempt := 1; ; attempt++ {
		err := pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.LockTimeout.Milliseconds())); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "SELECT lokadb.partition_org($1, $2, $3)", table, o.Org, o.org().Value)
			return err
		})
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || (pgErr.Code != "55P03" && pgErr.Code != "40P01") || attempt >= o.Attempts {
			return err
		}
		o.Log.Warn("DEFAULT partition busy, retrying", "table", table, "attempt", attempt, "err", err)
		time.Sleep(time.Second)
	}
}

// markerLag commits a marker row in the cloud database and returns how long it took to
// appear in the customer database.
func markerLag(ctx context.Context, marker, cust *pgx.Conn, org int64, limit time.Duration) (time.Duration, error) {
	start := time.Now()
	token := fmt.Sprintf("lag-%d", start.UnixNano())
	if _, err := marker.Exec(ctx, "INSERT INTO lokadb.move_marker VALUES ($1, $2)", org, token); err != nil {
		return 0, err
	}
	for {
		rows, err := rowsOf(ctx, cust, "SELECT count(*)::text FROM lokadb.move_marker WHERE org = $1 AND token = $2", org, token)
		if err != nil {
			return 0, fmt.Errorf("customer database: %w", err)
		}
		if rows[0][0] == "1" {
			return time.Since(start), nil
		}
		if time.Since(start) > limit {
			return time.Since(start), nil
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// steadyRounds: how many marker lags in a row tell the lag of delivery from a backlog.
const steadyRounds = 3

// caughtUp tells from the marker lags measured so far (oldest first) whether the replica has
// caught up: the switch then waits one more marker with writes paused, about the last lag.
// Under 500 ms it has. Above, the lag is a backlog, which falls round after round as the
// replica applies it (or grows when writes outpace it), or what delivery takes on this
// machine (a loaded one, a distant database), which stays: the last steadyRounds within a
// quarter of each other, and short enough for the switch, under half of maxPause.
func caughtUp(lags []time.Duration, maxPause time.Duration) (bool, string) {
	last := lags[len(lags)-1]
	if last < 500*time.Millisecond {
		return true, "lag under 500ms"
	}
	if len(lags) < steadyRounds {
		return false, ""
	}
	lo, hi := lagRange(lags[len(lags)-steadyRounds:])
	if hi <= lo+lo/4+50*time.Millisecond && last < maxPause/2 {
		return true, fmt.Sprintf("lag steady over %d rounds (%s-%s): delivery, not a backlog", steadyRounds, lo.Round(time.Millisecond), hi.Round(time.Millisecond))
	}
	return false, ""
}

func lagRange(lags []time.Duration) (lo, hi time.Duration) {
	lo, hi = lags[0], lags[0]
	for _, l := range lags {
		lo, hi = min(lo, l), max(hi, l)
	}
	return lo, hi
}

// catchUpHint says why the replica did not catch up, from the marker lags measured.
func catchUpHint(lags []time.Duration, maxPause time.Duration) string {
	last := lags[len(lags)-1]
	if len(lags) < steadyRounds {
		return fmt.Sprintf("lag %s", last.Round(time.Millisecond))
	}
	w := lags[len(lags)-steadyRounds:]
	lo, hi := lagRange(w)
	switch {
	case hi <= lo+lo/4+50*time.Millisecond:
		return fmt.Sprintf("the lag is steady at %s, more than half of -max-pause %s: the switch would pause writes about that long; "+
			"raise -max-pause, or move when the databases are less loaded", last.Round(time.Millisecond), maxPause)
	case w[len(w)-1] > w[0]:
		return fmt.Sprintf("the lag grows (%s, last %s): the organization's writes outpace replication to its database; "+
			"move when it writes less, or check the organization's database (its load, its log)", w[0].Round(time.Millisecond), last.Round(time.Millisecond))
	}
	return fmt.Sprintf("the lag still falls (last %s): a longer -catchup-timeout lets the replica finish", last.Round(time.Millisecond))
}

// switchLockTry: how long one try of the switch waits for its locks, under deadlock_timeout
// (a quarter of it: 250 ms by default) and at most lockTimeout.
func switchLockTry(lockTimeout, deadlockTimeout time.Duration) time.Duration {
	try := max(deadlockTimeout/4, 10*time.Millisecond)
	if lockTimeout > 0 && lockTimeout < try {
		try = lockTimeout
	}
	return try
}

type pause struct{ total, catchUp, dropSub, swap time.Duration }

// switchOver pauses writes to the LokaDB tables, waits until the replica has every change,
// drops the subscription and swaps the organization's partitions for foreign tables.
//
// On success it also returns a REPEATABLE READ transaction on the customer database whose
// snapshot was taken while writes were paused: the customer copy exactly as switched.
func switchOver(ctx context.Context, cloud, marker, cust, snap *pgx.Conn, o MoveOptions, tables []moveTable, mode, pub, sub, server string) (pause, pgx.Tx, error) {
	var p pause
	tx, err := cloud.Begin(ctx)
	if err != nil {
		return p, nil, err
	}
	defer tx.Rollback(ctx)
	var lock []string
	for _, t := range tables {
		lock = append(lock, "ONLY "+qr(t.name))
	}
	// FK actions from cloud tables (ON DELETE CASCADE / SET NULL) write the organization's
	// partition directly, without the parent: locked too, after every parent (the order
	// writers use), or such a change could commit after the marker and miss the replica.
	for _, t := range tables {
		lock = append(lock, "ONLY "+qr(t.local()))
	}
	for _, t := range tables {
		lock = append(lock, "ONLY "+qr(hn(t.name, "__cloud"))) // ATTACH and DETACH lock DEFAULT too
	}
	// The switch yields to the application. A writer holding an organization's partition may
	// next need a parent the switch holds (a cascade): a lock cycle, and Postgres cancels
	// whoever waits deadlock_timeout first, the writer when the switch waits longer. So each
	// try waits less than deadlock_timeout, gives back what it took and tries again, up to
	// LockTimeout in all.
	var deadlockMs int64
	if err := tx.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name = 'deadlock_timeout'").Scan(&deadlockMs); err != nil {
		return p, nil, err
	}
	try := switchLockTry(o.LockTimeout, time.Duration(deadlockMs)*time.Millisecond)
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", try.Milliseconds())); err != nil {
		return p, nil, err
	}
	var pid uint32
	if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		return p, nil, err
	}
	// Tell the operator who holds the switch back if the lock does not come quickly.
	locked, diagDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(diagDone)
		select {
		case <-locked:
		case <-time.After(time.Second):
			rows, err := rowsOf(ctx, marker, `SELECT pid::text, backend_type, coalesce(state, ''), query
				FROM pg_stat_activity WHERE pid = ANY(pg_blocking_pids($1))`, pid)
			if err == nil {
				for _, r := range rows {
					// the application's query, its literals left out: they are its data (review 3, R3-12)
					o.Log.Warn("switch waits for", "pid", r[0], "backend", r[1], "state", r[2], "query", redactSQL(r[3], 100))
				}
			}
		}
	}()
	// ACCESS EXCLUSIVE from the start, what DETACH needs: reads wait too, for the marker's
	// round trip and the swap. A weaker lock raised later deadlocks with a transaction that
	// read these tables and then writes them (it waits for our lock, we for its read lock),
	// and Postgres cancels one of the two; raised by DETACH after DROP SUBSCRIPTION, it may
	// be the switch, with no replica left to retry with. Waiting for the lock instead, such a
	// transaction is let ahead of us (Postgres reorders the queue) and finishes first.
	waitFrom := time.Now()
	for tries := 1; ; tries++ {
		if _, err = tx.Exec(ctx, "SAVEPOINT lokadb_switch_lock"); err != nil {
			break
		}
		_, err = tx.Exec(ctx, "LOCK TABLE "+strings.Join(lock, ", ")+" IN ACCESS EXCLUSIVE MODE")
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "55P03" || time.Since(waitFrom)+try > o.LockTimeout {
			break
		}
		// what this try took goes back: the writer it waited for goes on
		if _, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_switch_lock"); err != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err == nil { // the statements under the lock wait as long as before
		_, err = tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.LockTimeout.Milliseconds()))
	}
	close(locked)
	<-diagDone // the marker connection is free again
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01") {
			return p, nil, fmt.Errorf("%w: %v", errRetry, err)
		}
		return p, nil, err
	}
	paused := time.Now()

	if mode == ModeTrigger {
		// No writer is left: what the log holds is everything; applied to the end, the
		// organization's copy equals the partitions.
		if err := drainLog(ctx, tx, cust, o, tables, paused); err != nil {
			return p, nil, err
		}
		p.catchUp = time.Since(paused)
		return finishSwitch(ctx, tx, cust, snap, o, tables, mode, pub, server, paused, p)
	}
	// Every write to these tables committed before the lock; the marker commits after it.
	token := fmt.Sprintf("%d", paused.UnixNano())
	if _, err := marker.Exec(ctx, "INSERT INTO lokadb.move_marker VALUES ($1, $2)", o.Org, token); err != nil {
		return p, nil, err
	}
	for {
		rows, err := rowsOf(ctx, cust, "SELECT count(*)::text FROM lokadb.move_marker WHERE org = $1 AND token = $2", o.Org, token)
		if err != nil {
			return p, nil, fmt.Errorf("customer database: %w", err)
		}
		if rows[0][0] == "1" {
			break
		}
		if time.Since(paused) > o.MaxPause {
			return p, nil, fmt.Errorf("%w: replica still behind after %s", errRetry, o.MaxPause)
		}
		time.Sleep(2 * time.Millisecond)
	}
	p.catchUp = time.Since(paused)

	// Point of no return for the replica: from here the customer copy is final. Detached
	// from its slot, the subscription drops without a word to the publisher: several round
	// trips over a slow link, with writes paused. The slot goes once they resume (the cleanup).
	if err := execAll(ctx, cust, []string{
		"ALTER SUBSCRIPTION " + q(sub) + " DISABLE",
		"ALTER SUBSCRIPTION " + q(sub) + " SET (slot_name = NONE)",
		"DROP SUBSCRIPTION " + q(sub),
	}); err != nil {
		return p, nil, fmt.Errorf("drop subscription: %w", err)
	}
	return finishSwitch(ctx, tx, cust, snap, o, tables, mode, pub, server, paused, p)
}

// finishSwitch renames the organization's copy, takes the snapshot the verification reads,
// and swaps the partitions for foreign tables (switchOver's transaction tx, still locked).
func finishSwitch(ctx context.Context, tx pgx.Tx, cust, snap *pgx.Conn, o MoveOptions, tables []moveTable, mode, pub, server string, paused time.Time, p pause) (pause, pgx.Tx, error) {
	renames := []string{"BEGIN"}
	for _, t := range tables {
		renames = append(renames, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qr(t.local()), q(nameOf(t.name))))
	}
	if err := execAll(ctx, cust, append(renames, "COMMIT")); err != nil {
		return p, nil, fmt.Errorf("customer database: %w", err)
	}
	p.dropSub = time.Since(paused) - p.catchUp
	snapTx, err := snap.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return p, nil, fmt.Errorf("customer database: %w", err)
	}
	if _, err := snapTx.Exec(ctx, "SELECT 1"); err != nil { // the snapshot is taken here
		return p, nil, fmt.Errorf("customer database: %w", err)
	}
	if err := verifyCounts(ctx, tx, snapTx, tables, o.Log, "nothing switched, the organization stays in the cloud database; run move-org again (it copies from the start)"); err != nil {
		return p, nil, err
	}
	var steps []string
	if mode == ModeTrigger {
		steps = append(steps, dropCapture(o.Org, tables, moveTable.local)...)
	}
	for _, t := range tables {
		steps = append(steps,
			fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", qr(t.name), qr(t.local())),
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qr(t.local()), q(nameOf(t.retired()))))
		// The retired copy keeps its FKs to the refs and cloud tables: an ON DELETE action
		// would change it, a RESTRICT would refuse deleting the organization's rows.
		fks, err := rowsOf(ctx, tx, `SELECT conname::text FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'f'`, qr(t.local()))
		if err != nil {
			return p, nil, err
		}
		for _, f := range fks {
			steps = append(steps, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", qr(t.retired()), q(f[0])))
		}
		steps = append(steps,
			fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (%s)", qr(t.name), qr(t.foreign()), t.val),
			// The customer controls its database: rows it returns are trusted only for its own
			// tenant. lokadb_tenant checks it for every foreign partition (lokadb.tenant_policy).
			fmt.Sprintf("ALTER FOREIGN TABLE %s OWNER TO %s", qr(t.foreign()), q(o.Owner)),
			// the parent's privileges on the partition: the proxy addresses it directly
			fmt.Sprintf("SELECT lokadb.sync_grants(%s)", lit(t.name)))
	}
	// the FKs onto cloud tables, on the new foreign partitions (fkcloud.go)
	steps = append(steps, "SELECT lokadb.fk_cloud_sync()")
	// the children's __refs on a parent's cascade, for the new foreign partitions (fkcascade.go)
	steps = append(steps, "SELECT lokadb.fk_cascade_sync()")
	steps = append(steps, fmt.Sprintf("INSERT INTO lokadb.orgs (org, server, schema, db_id) VALUES (%d, %s, %s, %s)", o.Org, lit(server), lit(o.CustomerSchema), nullLit(o.dbID)))
	// a user mapping for every role that may read the tables, also through a role it is a
	// member of (mappedRoles has the roles granted directly)
	steps = append(steps, "SELECT lokadb.sync_mappings()")
	if mode == ModeLogical {
		steps = append(steps, "DROP PUBLICATION "+q(pub), fmt.Sprintf("DELETE FROM lokadb.move_marker WHERE org = %d", o.Org))
	}
	for _, s := range steps {
		if _, err := tx.Exec(ctx, s); err != nil {
			return p, nil, fmt.Errorf("%w\n  in: %s\n  the organization stays in the cloud database; run move-org again", err, s)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return p, nil, fmt.Errorf("%w (the organization stays in the cloud database; run move-org again)", err)
	}
	p.total = time.Since(paused)
	p.swap = p.total - p.catchUp - p.dropSub
	return p, snapTx, nil
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// compare checks the row count and a digest of every row of a cloud table and a customer
// table, for each LokaDB table.
func compare(ctx context.Context, cloud, cust querier, tables []moveTable, src, dst func(moveTable) string) (bool, error) {
	for _, t := range tables {
		// A sum of 64-bit row hashes: order-free and streamed (md5 of string_agg of every row
		// needed the whole table as one value, and fails past 1 GB).
		digest := fmt.Sprintf("SELECT count(*)::text, coalesce(sum(hashtextextended(%s, 0)::numeric), 0)::text FROM %%s t", rowDigest(t))
		a, err := rowsOf(ctx, cloud, fmt.Sprintf(digest, qr(src(t))))
		if err != nil {
			return false, err
		}
		b, err := rowsOf(ctx, cust, fmt.Sprintf(digest, qr(dst(t))))
		if err != nil {
			return false, fmt.Errorf("customer database: %w", err)
		}
		if a[0][0] != b[0][0] || a[0][1] != b[0][1] {
			return false, nil
		}
	}
	return true, nil
}

// rowDigest: the text a row of t is hashed by, the same on both sides whatever their
// versions (2d). The whole row's text (t::text) depended on how each side prints its types:
// a float by the version (shortest form since PostgreSQL 12), an interval or a bytea by the
// session (connect fixes IntervalStyle and bytea_output). Floats go by their binary form.
func rowDigest(t moveTable) string {
	var cols []string
	for _, c := range t.cols {
		col := "t." + q(c[0])
		switch c[1] {
		case "real", "double precision":
			cols = append(cols, "float8send("+col+"::float8)")
		case "real[]", "double precision[]":
			cols = append(cols, "(SELECT array_agg(float8send(x::float8)) FROM unnest("+col+") x)")
		default:
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return "t::text"
	}
	return "ROW(" + strings.Join(cols, ", ") + ")::text"
}

// tableColumns: the columns of table t as moveTable.cols has them.
func tableColumns(ctx context.Context, cloud querier, t string) ([][]string, error) {
	return rowsOf(ctx, cloud, `SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.attnotnull::text,
		CASE WHEN a.attgenerated <> '' THEN pg_get_expr(d.adbin, d.adrelid) ELSE '' END,
		CASE WHEN a.attcollation <> 0 AND a.attcollation <> ty.typcollation THEN a.attcollation::regcollation::text ELSE '' END,
		a.attgenerated::text
		FROM pg_attribute a JOIN pg_type ty ON ty.oid = a.atttypid
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`, qr(t))
}

// describe reads the enabled tables and builds the customer database DDL (staging names).
// The FKs come after every table: the spec may list a table before the one it references
// (two tables with their own organization column are in any order).
func describe(ctx context.Context, cloud *pgx.Conn, spec Spec, ten Tenant) ([]moveTable, []string, error) {
	org := ten.Org
	var tables []moveTable
	var ddl, fkDDL []string
	for _, st := range spec.Tables {
		t := st.Name
		tc, err := rowsOf(ctx, cloud, `SELECT tenant_column, key_column FROM lokadb.tables WHERE table_name = $1`, t)
		if err != nil { // a failed read is not "not enabled" (a closed connection said so once)
			return nil, nil, fmt.Errorf("reading lokadb.tables for %s: %w", t, err)
		}
		if len(tc) == 0 {
			return nil, nil, fmt.Errorf("%s is not enabled (run enable first)", t)
		}
		cols, err := tableColumns(ctx, cloud, t)
		if err != nil {
			return nil, nil, err
		}
		checks, err := rowsOf(ctx, cloud, `SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint
			WHERE conrelid = $1::regclass AND contype = 'c' ORDER BY conname`, qr(t))
		if err != nil {
			return nil, nil, err
		}
		mt := moveTable{name: t, tenant: tc[0][0], key: tc[0][1], cols: cols, checks: checks, org: org, val: ten.Lit()}
		tables = append(tables, mt)

		var defs []string
		for _, c := range cols {
			defs = append(defs, colDef(c))
		}
		// the key (a composite one: its columns, compkey.go); (tenant, key) for the widened FKs
		// of children, unless the key holds the organization column already
		defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", keyList(mt)))
		if !slices.Contains(keyColsOf(mt), mt.tenant) {
			defs = append(defs, fmt.Sprintf("UNIQUE (%s, %s)", q(mt.tenant), keyList(mt)))
		}
		defs = append(defs, fmt.Sprintf("CONSTRAINT lokadb_owner CHECK (%s = %s)", q(mt.tenant), mt.val))
		ddl = append(ddl, fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", qr(mt.local()), strings.Join(defs, ",\n  ")),
			fmt.Sprintf("COMMENT ON TABLE %s IS %s", qr(mt.local()), lit(orgMark(org, ""))))
		// The organization's database enforces the CHECK constraints of the moved rows:
		// Postgres does not on the cloud's foreign partition.
		for _, c := range checks {
			ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", qr(mt.local()), q(c[0]), c[1]))
		}

		// UNIQUE and EXCLUDE constraints of the DEFAULT partition stay constraints: as an index
		// (pg_get_indexdef) an exclusion constraint becomes a plain index that accepts
		// overlaps, and a deferrable unique one is checked at once instead of at COMMIT.
		cons, err := rowsOf(ctx, cloud, `SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint
			WHERE conrelid = $1::regclass AND contype IN ('u', 'x') ORDER BY conname`, qr(hn(t, "__cloud")))
		if err != nil {
			return nil, nil, err
		}
		for _, c := range cons {
			ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", qr(mt.local()), q(c[0]), c[1]))
		}
		// The application's other indexes, with their original names, from the DEFAULT
		// partition, put on the organization's table: renamed in SQL, which quotes names as
		// pg_get_indexdef does (only where needed).
		idx, err := rowsOf(ctx, cloud, `SELECT replace(pg_get_indexdef(i.indexrelid), ' ON ' || quote_ident($4) || '.' || quote_ident($2) || ' ',
				' ON ' || quote_ident($4) || '.' || quote_ident($3) || ' ')
			FROM pg_index i WHERE i.indrelid = $1::regclass AND NOT i.indisprimary
			  -- an invalid index (a failed CREATE INDEX CONCURRENTLY) holds nothing in the cloud:
			  -- made valid at the organization, a unique one would refuse rows or change behavior
			  AND i.indisvalid
			  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid = i.indrelid AND c.conindid = i.indexrelid AND c.contype IN ('u', 'x'))`,
			qr(hn(t, "__cloud")), nameOf(hn(t, "__cloud")), nameOf(mt.local()), schemaOf(t))
		if err != nil {
			return nil, nil, err
		}
		for _, row := range idx {
			ddl = append(ddl, row[0])
		}
		// FKs between LokaDB tables become real local FKs in the customer database,
		// widened with the tenant column so a row can only point inside its own tenant.
		// (Logical replication applies changes with FK triggers off, so table sync order does not matter.)
		// The parent comes from the catalog (the refs table the FK points at), not from the
		// constraint's text, where a name that needs quotes is printed quoted.
		// An FK onto a unique column's map (block 4, item 2) becomes one onto that column of
		// the organization's table, which has its unique index: not widened, as declared.
		fks, err := rowsOf(ctx, cloud, `SELECT pg_get_constraintdef(c.oid), coalesce(bt.table_name, ur.table_name, ''),
				coalesce(bt.key_column, ur.column_name, ''), coalesce(bt.tenant_column, ''), (ur.table_name IS NOT NULL)::text
			FROM pg_constraint c LEFT JOIN lokadb.tables bt ON c.confrelid = to_regclass(lokadb.rel(bt.refs_table))
			LEFT JOIN lokadb.unique_refs ur ON c.confrelid = to_regclass(lokadb.rel(ur.refs_table))
			WHERE c.contype = 'f' AND c.conrelid = $1::regclass`, qr(hn(t, "__cloud")))
		if err != nil {
			return nil, nil, err
		}
		for _, row := range fks {
			m := fkLocalRe.FindStringSubmatch(row[0])
			if m == nil || row[1] == "" {
				continue // FK to a cloud table: not enforceable in the customer database
			}
			parent := qr(hn(row[1], fmt.Sprintf("__o%d", org)))
			if row[4] == "true" {
				fkDDL = append(fkDDL, fmt.Sprintf("ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s (%s)%s",
					qr(mt.local()), m[1], parent, q(row[2]), m[3]))
				continue
			}
			if m[2] != "id" {
				// A tenant-scoped FK (the organization column and the key, as the application
				// declared it): the same columns of the organization's table.
				// A composite key (compkey.go): its refs columns id_1 ... id_n are its columns.
				onto := map[string]string{"tenant": q(row[3])}
				for i, c := range RefsKeyCols(row[2]) {
					onto[c] = q(KeyCols(row[2])[i])
				}
				var refs []string
				for _, c := range strings.Split(m[2], ", ") {
					refs = append(refs, onto[strings.Trim(c, `"`)])
				}
				fkDDL = append(fkDDL, fmt.Sprintf("ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s (%s)%s",
					qr(mt.local()), m[1], parent, strings.Join(refs, ", "), m[3]))
				continue
			}
			// SET NULL / SET DEFAULT must not touch the tenant column: only the reference is
			// reset. ($ in a column name is not a group reference.)
			action := setNullRe.ReplaceAllString(m[3], "ON DELETE SET $1 ("+strings.ReplaceAll(m[1], "$", "$$")+")")
			fkDDL = append(fkDDL, fmt.Sprintf("ALTER TABLE %s ADD FOREIGN KEY (%s, %s) REFERENCES %s (%s, %s)%s",
				qr(mt.local()), q(mt.tenant), m[1], parent, q(row[3]), q(row[2]), action))
		}
	}
	return tables, append(ddl, fkDDL...), nil
}

// TableMark starts the comment move-org puts on the tables it creates in an organization's
// database: only tables carrying it are ever dropped there.
const TableMark = "lokadb:"

// customerSchema (re)creates the staging tables in the customer database. The organization
// is still served from the cloud at this point, so leftovers of a failed attempt are dropped.
// A table of the same name without TableMark belongs to the customer and stops the move.
func customerSchema(ctx context.Context, cust *pgx.Conn, tables []moveTable, owner, schema string, ddl []string, dropReturned bool, log *slog.Logger) error {
	tx, err := cust.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var returned []string // the copy an earlier move-back left of this organization: dropped below
	for _, t := range tables {
		for _, name := range []string{t.name, t.local()} {
			var mark string
			err := tx.QueryRow(ctx, `SELECT coalesce(obj_description(oid, 'pg_class'), '') FROM pg_class
				WHERE relnamespace = to_regnamespace($2) AND relname = $1`, nameOf(name), q(custSchemaOf(name, schema))).Scan(&mark)
			if err == nil && isReturnedMark(mark, t.org) {
				returned = append(returned, fmt.Sprintf("%s.%s", q(custSchemaOf(name, schema)), q(nameOf(name))))
				continue
			}
			if err == nil && !strings.HasPrefix(mark, TableMark) {
				return fmt.Errorf("table %s already exists in schema %s and was not created by lokadb: it is left alone; rename it or use another database", nameOf(name), custSchemaOf(name, schema))
			}
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
	}
	if len(returned) > 0 {
		if err := setReturnedAside(ctx, tx, returned, tables[0].org, dropReturned, log); err != nil {
			return err
		}
	}
	for i := len(tables) - 1; i >= 0; i-- {
		t := tables[i]
		// qualified: the search path also has public, where the customer's own tables live
		ts := q(custSchemaOf(t.name, schema))
		if _, err := tx.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s, %s.%s CASCADE", ts, q(nameOf(t.name)), ts, q(nameOf(t.local())))); err != nil {
			return err
		}
	}
	for _, s := range ddl {
		// the indexes come from the cloud's partitions, qualified with public (those of another
		// schema keep it: the organization's tables are in the same schema there)
		if strings.HasPrefix(s, "CREATE INDEX ") || strings.HasPrefix(s, "CREATE UNIQUE INDEX ") {
			s = strings.Replace(s, " ON public.", " ON "+q(schema)+".", 1)
		}
		if strings.HasPrefix(s, "CREATE INDEX ") {
			// A plain index whose operator class or function is not there (an extension that
			// could not be installed, see customerDeps): the table answers the same without it.
			if _, err := tx.Exec(ctx, "SAVEPOINT lokadb_index"); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, s)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && (pgErr.Code == "42704" || pgErr.Code == "42883") { // undefined object, function
				if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_index"); err != nil {
					return err
				}
				log.Warn("index not created in the organization's database: what it uses is not there (its queries get slower, results stay the same)",
					"index", mask(s), "err", pgErr.Message)
				continue
			}
			if err != nil {
				return fmt.Errorf("%w\n  in: %s", err, mask(s))
			}
			continue
		}
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("%w\n  in: %s", err, mask(s))
		}
	}
	// The cloud's role there owns the moved tables (DML, the application's DDL) and nothing else.
	for _, t := range tables {
		if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(t.local()), q(owner))); err != nil {
			return fmt.Errorf("%w (the role %s must exist in the organization's database and have CREATE on its schema %s: it owns the moved tables)", err, owner, schema)
		}
	}
	return tx.Commit(ctx)
}

// connect opens an admin connection with a fixed text format, so row checksums agree.
func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["TimeZone"] = "UTC"
	cfg.RuntimeParams["DateStyle"] = "ISO"
	cfg.RuntimeParams["extra_float_digits"] = "3"
	// how values print: the row comparison reads them as text on both sides (rowDigest)
	cfg.RuntimeParams["IntervalStyle"] = "postgres"
	cfg.RuntimeParams["bytea_output"] = "hex"
	// lit() doubles a quote and nothing else: a backslash before one (a text key) is an escape
	// with the setting off (a database of an old application can have it so), review A18
	cfg.RuntimeParams["standard_conforming_strings"] = "on"
	ownsql.Configure(cfg) // the command's statements are LokaDB's own (check skips them)
	return pgx.ConnectConfig(ctx, cfg)
}

// connectPlain: a connection of the tool's commands that run SQL of their own (enable,
// org-add, org-profile, routing) with standard_conforming_strings on, like connect: lokadb's
// functions and literals take a backslash as it is, and a database of an old application can
// have the setting off (review A18).
func connectPlain(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["standard_conforming_strings"] = "on"
	ownsql.Configure(cfg) // the command's statements are LokaDB's own (check skips them)
	return pgx.ConnectConfig(ctx, cfg)
}

func execAll(ctx context.Context, conn *pgx.Conn, stmts []string) error {
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("%w\n  in: %s", err, mask(s))
		}
	}
	return nil
}

var (
	secretKV  = regexp.MustCompile(`(?i)(password\s*=\s*)(''[^']*''|'(?:\\.|[^'\\])*'|\S+)`) // conninfo: password=x, password='a b' (\' inside), or in a SQL literal password=''a b''
	secretSQL = regexp.MustCompile(`(?i)(password\s+)'(?:[^']|'')*'`)                        // SQL: PASSWORD 'x'
)

// mask hides passwords in a statement shown in an error or a log.
func mask(s string) string {
	return secretSQL.ReplaceAllString(secretKV.ReplaceAllString(s, "${1}***"), "${1}'***'")
}

func randomPassword() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// dropRole: statements that drop role (and its privileges in this database) if it exists.
func dropRole(role string) []string {
	return []string{
		// the name by quote_ident: one with a quote in it ended the string (R3-14)
		doTag(fmt.Sprintf(`DO $$ BEGIN IF EXISTS (SELECT FROM pg_roles WHERE rolname = %s) THEN EXECUTE 'DROP OWNED BY ' || quote_ident(%s); END IF; END $$`, lit(role), lit(role))),
		"DROP ROLE IF EXISTS " + q(role),
	}
}

// withCredentials returns dsn with user and password replaced (keyword/value or URI form).
// publisherHint: what to check when the subscription cannot reach -publisher (the agent's
// -cloud-listen): its refusal by -cloud-allow, or a bare disconnect (an older agent refused
// that way, block 14's stand).
func publisherHint(err error) string {
	switch msg := err.Error(); {
	case strings.Contains(msg, "-cloud-allow"):
		return "\n  hint: the agent refused the subscription's address: add the address the organization's database connects from to the agent's -cloud-allow (a database on the agent's host that reaches -publisher by the host's external address comes from that address)"
	case strings.Contains(msg, "pg_hba.conf rejects") && strings.Contains(msg, "lokadb_repl_"):
		return "\n  hint: the cloud database's pg_hba.conf refuses the move's role from the relay's address: add, above the general reject, host all /^lokadb_repl_ RELAY_ADDR/32 scram-sha-256 (docs/limits.md: \"Installation: direct access to the cloud database is closed\")"
	case strings.Contains(msg, "could not connect to the publisher") && strings.Contains(msg, "closed the connection unexpectedly"):
		return "\n  hint: the connection to -publisher was closed without an answer: an agent older than this one closes connections -cloud-allow does not admit that way (see the agent's log: \"connection refused, not allowed\"); add the address the organization's database connects from to -cloud-allow"
	}
	return ""
}

func withCredentials(dsn, user, password string) (string, error) {
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		u.User = url.UserPassword(user, password)
		return u.String(), nil
	}
	quote := func(s string) string { return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'" }
	return dsn + " user=" + quote(user) + " password=" + quote(password), nil // a later keyword wins
}

// dropSubscription removes the subscription and its replication slot. When the publisher
// cannot be reached (or refuses the subscription's role), the subscription is detached from
// its slot and dropped, and the slot is dropped in the cloud database directly.
func dropSubscription(ctx context.Context, cust, cloud *pgx.Conn, sub, slot string) error {
	exists, err := rowsOf(ctx, cust, `SELECT count(*)::text FROM pg_subscription WHERE subname = $1 AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())`, sub)
	if err != nil {
		return err
	}
	if exists[0][0] == "0" {
		return nil
	}
	if err := execAll(ctx, cust, []string{"DROP SUBSCRIPTION " + q(sub)}); err == nil {
		return nil
	}
	if err := execAll(ctx, cust, []string{
		"ALTER SUBSCRIPTION " + q(sub) + " DISABLE",
		"ALTER SUBSCRIPTION " + q(sub) + " SET (slot_name = NONE)",
		"DROP SUBSCRIPTION " + q(sub),
	}); err != nil {
		return err
	}
	return dropSlot(ctx, cloud, slot)
}

// dropSlot drops the replication slot in the cloud database if it exists, ending the
// walsender that holds it first.
func dropSlot(ctx context.Context, cloud *pgx.Conn, slot string) error {
	for i := 0; ; i++ {
		rows, err := rowsOf(ctx, cloud, `SELECT coalesce(active_pid, 0)::text FROM pg_replication_slots WHERE slot_name = $1`, slot)
		if err != nil {
			return err
		}
		switch {
		case len(rows) == 0:
			return nil
		case rows[0][0] == "0":
			return execAll(ctx, cloud, []string{"SELECT pg_drop_replication_slot(" + lit(slot) + ")"})
		case i >= 20:
			return fmt.Errorf("replication slot %s is still in use by pid %s", slot, rows[0][0])
		}
		if err := execAll(ctx, cloud, []string{"SELECT pg_terminate_backend(" + rows[0][0] + ")"}); err != nil {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// mappedRoles: the roles that get a user mapping for the organization's server.
func mappedRoles(ctx context.Context, cloud *pgx.Conn, tables []moveTable, owner string) ([]string, error) {
	var names []string
	for _, t := range tables {
		names = append(names, qr(t.name)) // regclass input: quoted
	}
	// Never FOR PUBLIC: every role with USAGE on the server would see the organization role's
	// password in pg_user_mappings. A grant to PUBLIC gets a mapping per role from
	// lokadb.sync_mappings, in the switch's transaction.
	rows, err := rowsOf(ctx, cloud, `SELECT DISTINCT quote_ident(g.rolname) FROM pg_class c
		CROSS JOIN LATERAL aclexplode(c.relacl) a JOIN pg_roles g ON g.oid = a.grantee
		WHERE c.oid = ANY($1::regclass[]) AND a.grantee <> c.relowner
		UNION SELECT quote_ident($2) UNION SELECT quote_ident(current_user::text) ORDER BY 1`, names, owner)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range rows {
		out = append(out, r[0])
	}
	return out, nil
}

// cloudWindow opens (ttl > 0) or closes the relay's window that lets the organization's
// agent reach the cloud database: for role, to the database of -publisher, replication only
// (the relay reads the startup of each connection: review 3 of the tunnel, R3-13).
func cloudWindow(ctx context.Context, o MoveOptions, ttl time.Duration, role string) error {
	cp, err := tunnel.OpenPGStore(ctx, o.ControlPlaneDSN)
	if err != nil {
		return err
	}
	defer cp.Close()
	if ttl <= 0 {
		return cp.OpenCloud(ctx, fmt.Sprint(o.Org), 0)
	}
	pc, err := pgconn.ParseConfig(o.PublisherDSN)
	if err != nil {
		return fmt.Errorf("publisher DSN: %w", err)
	}
	return cp.OpenCloudFor(ctx, fmt.Sprint(o.Org), ttl, role, pc.Database)
}

func rowsOf(ctx context.Context, conn querier, sql string, args ...any) ([][]string, error) {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%w\n  in: %s", err, mask(sql))
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			if v != nil {
				row[i] = fmt.Sprint(v)
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// setCustomerSchema makes the organization's schema the first of the connection's search
// path: the move's unqualified names (CREATE TABLE, COPY, the log's writes, the comparison)
// resolve there; public stays for the types and functions the tables use.
func setCustomerSchema(ctx context.Context, cust *pgx.Conn, schema string) error {
	if schema == "" || schema == "public" {
		return nil
	}
	_, err := cust.Exec(ctx, "SET search_path TO "+q(schema)+", public")
	return err
}

// custSchemaOf: the schema of table t (a key) in the organization's database: the
// organization's schema (lokadb.orgs.schema) for a table of public, its own for another.
func custSchemaOf(t, orgSchema string) string {
	if s := schemaOf(t); s != "public" {
		return s
	}
	if orgSchema == "" {
		return "public"
	}
	return orgSchema
}

// otherSchemas: the statements that make, in the organization's database, the schemas other
// than public of the spec's tables, owned by the cloud's role there (app) when it is not the admin.
func otherSchemas(spec Spec, app, admin string) []string {
	seen := map[string]bool{}
	var out []string
	for _, st := range spec.Tables {
		s := schemaOf(st.Name)
		if s == "public" || seen[s] {
			continue
		}
		seen[s] = true
		create := "CREATE SCHEMA IF NOT EXISTS " + q(s)
		if app != admin {
			create += " AUTHORIZATION " + q(app)
		}
		out = append(out, create)
	}
	return out
}

// redactSQL: a query of another session as the log may show it: its literals replaced by $n
// (pg_query normalization), cut to n bytes; one that does not parse by its size only.
func redactSQL(sql string, n int) string {
	norm, err := pg.Normalize(sql)
	if err != nil {
		return fmt.Sprintf("<%d bytes that do not parse>", len(sql))
	}
	norm = strings.Join(strings.Fields(norm), " ")
	if len(norm) > n {
		norm = norm[:n] + "..."
	}
	return norm
}
