// Command defects reproduces, against real Postgres, each way LokaDB could lose or misplace
// an organization's rows or let somebody reach what they should not (research/ANALYSIS.md,
// 5.1 and 5.2), and checks that it no longer happens. Every check gets fresh databases; the
// arguments are lokadb binaries to check
// (several: the same checks for each, e.g. before and after a fix).
//
// Exit code: 0 when every check of every binary passed.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func dsn(host, db, user string) string {
	pw := user // the checks' roles have their name as password
	if user == "postgres" {
		pw = "pw"
	}
	return fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable", host, db, user, pw)
}

// env is one check run: a lokadb binary and a database of the same name in the cloud
// and in the customer Postgres.
type env struct {
	ctx  context.Context
	bin  string
	db   string
	spec string // tables spec; "" = documents + document_versions
	port int    // the proxy's port, when the check starts one
	// cloudHost: the cloud Postgres of the check ("cloud", wal_level = logical; "cloudplain",
	// wal_level = replica)
	cloudHost string
	// customerHost: the organization's Postgres of the check ("customer"; "customervec" and
	// "customergis" has PostGIS installed as a package: movepgext.go)
	customerHost string
	// proxyProc: the proxy the check started last (proxyCfg), to signal it (PR7: SIGHUP)
	proxyProc *os.Process
}

// checkTimeout: how long a check may take (DEFECTS_TIMEOUT, a Go duration; 3 minutes).
var checkTimeout = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("DEFECTS_TIMEOUT")); err == nil && d > 0 {
		return d
	}
	return 3 * time.Minute
}()

// moveMode: DEFECTS_MODE (auto, logical, trigger) runs every move-org in that mode.
var moveMode = os.Getenv("DEFECTS_MODE")

// runCheck: a check that panics (a helper that cannot connect, under load) is an ERROR of that check,
// not the end of the run.
func runCheck(run func(*env) (bool, string, error), e *env) (ok bool, detail string, err error) {
	defer func() {
		if r := recover(); r != nil {
			ok, detail, err = false, "", fmt.Errorf("panic: %v", r)
		}
	}()
	return run(e)
}

// shard: DEFECTS_SHARD=k/n runs every n-th check starting at k (0 <= k < n), so n stands share one run.
func shard() (k, n int, ok bool) {
	s := os.Getenv("DEFECTS_SHARD")
	if s == "" {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(s, "%d/%d", &k, &n); err != nil || n < 1 || k < 0 || k >= n {
		fmt.Fprintf(os.Stderr, "DEFECTS_SHARD=%q: want k/n with 0 <= k < n\n", s)
		os.Exit(2)
	}
	return k, n, true
}

func (e *env) cloud() string    { return dsn(e.cloudHost, e.db, "postgres") }
func (e *env) customer() string { return dsn(e.customerHost, e.db, "postgres") }

type check struct {
	cloudHost    string // "" = cloud
	customerHost string // "" = customer
	id, name     string
	run          func(e *env) (ok bool, detail string, err error)
}

var checks = []check{
	{id: "D1", name: "rows inserted while an organization created after enable gets its own partition", run: func(e *env) (bool, string, error) { return checkSplitRace(e, false) }},
	{id: "D1", name: "rows updated while an organization created after enable gets its own partition", run: func(e *env) (bool, string, error) { return checkSplitRace(e, true) }},
	{id: "D2", name: "BEGIN; INSERT child; COMMIT in one query string lands in the parent's organization", run: checkTxString},
	{id: "D3", name: "a failed refs lookup is an error, not an empty result", run: checkLookupFailure},
	{id: "D3", name: "a child row with an unknown organization (-1) is refused by the database", run: checkUnknownOrg},
	{id: "D4", name: "an FK cascade from a cloud table during the switch reaches the customer", run: checkCascadeAtSwitch},
	{id: "D4", name: "the same with documents the only LokaDB table", run: func(e *env) (bool, string, error) {
		// versions stay a cloud table: nothing else of the cascade passes a locked table
		e.spec = `{"tables": [{"name": "documents", "tenant_column": "organization_id"}]}`
		return checkCascadeAtSwitch(e)
	}},
	{id: "D5", name: "move-org replaces a FDW server left by an earlier attempt", run: checkStaleServer},
	{id: "D6", name: "two move-org at once: one runs, the other is refused", run: checkConcurrentMoves},
	{id: "D7", name: "ON DELETE SET NULL in the customer database clears only the reference", run: checkSetNull},
	{id: "D8", name: "enable keeps other roles' privileges and never reuses ids", run: checkEnableKeeps},
	{id: "D8", name: "enable keeps a table's trigger: it fires once, through the new parent", run: checkEnableTrigger},
	{id: "D9", name: "deleting a moved row leaves the retained *_source copy alone", run: checkRetainedSource},
}

func main() {
	ctx := context.Background()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: defects LOKADB_BINARY...")
		os.Exit(2)
	}
	if err := waitUp(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	failed := 0
	type result struct{ bin, id, name, verdict, detail string }
	var results []result
	for bi, bin := range os.Args[1:] {
		for ci, c := range checks {
			if only := os.Getenv("DEFECTS_ONLY"); only != "" && !strings.Contains(","+only+",", ","+c.id+",") {
				continue
			}
			if k, n, ok := shard(); ok && ci%n != k { // defects_par.sh: the other shards run on stands of their own
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, checkTimeout) // a check that waits on itself fails, not hangs
			e := &env{ctx: cctx, bin: bin, db: fmt.Sprintf("dl_%d_%d", bi, ci), port: 6500 + bi*50 + ci, cloudHost: "cloud", customerHost: "customer"}
			if c.cloudHost != "" {
				e.cloudHost = c.cloudHost
			}
			if c.customerHost != "" {
				e.customerHost = c.customerHost
			}
			fmt.Printf("--- %s %s: %s\n", filepath.Base(bin), c.id, c.name)
			start := time.Now()
			ok, detail, err := false, "", fresh(e)
			if err == nil {
				ok, detail, err = runCheck(c.run, e)
			}
			if cctx.Err() != nil {
				why := detail
				if err != nil {
					why = strings.TrimSpace(why + " " + err.Error())
				}
				ok, detail, err = false, fmt.Sprintf("timed out after %s: %s", checkTimeout, why), nil
			}
			cancel()
			verdict := "PASS"
			if err != nil {
				verdict, detail = "ERROR", err.Error()
			} else if !ok {
				verdict = "FAIL"
			}
			if verdict != "PASS" {
				failed++
				// cleanup.go: what it left must not fail the next checks
				if left := cleanupAfter(bin, e.cloudHost, e.db); left != "" {
					detail += " [cleaned up: " + left + "]"
				}
			}
			fmt.Printf("    %s: %s [took %s]\n", verdict, detail, time.Since(start).Round(time.Second))
			results = append(results, result{filepath.Base(bin), c.id, c.name, verdict, detail})
		}
	}
	fmt.Println("\n=== summary")
	for _, r := range results {
		fmt.Printf("%-12s %-3s %-5s %s\n      %s\n", r.bin, r.id, r.verdict, r.name, oneLine(r.detail))
	}
	if failed > 0 {
		fmt.Printf("%d check(s) did not pass\n", failed)
		os.Exit(1)
	}
	fmt.Println("all checks passed")
}

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

func waitUp(ctx context.Context) error {
	for _, host := range []string{"cloud", "cloudplain", "customer", "customergis"} {
		for i := 0; ; i++ {
			c, err := pgx.Connect(ctx, dsn(host, "postgres", "postgres"))
			if err == nil {
				c.Close(ctx)
				break
			}
			if i > 120 {
				return fmt.Errorf("%s is not up: %w", host, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	for _, host := range []string{"cloud", "cloudplain", "customer", "customergis"} {
		if err := execSQL(ctx, dsn(host, "postgres", "postgres"),
			`DO $$ BEGIN CREATE ROLE app LOGIN PASSWORD 'app'; EXCEPTION WHEN duplicate_object THEN END $$`,
			`DO $$ BEGIN CREATE ROLE reader; EXCEPTION WHEN duplicate_object THEN END $$`,
			// security checks: a role that may read documents but owns nothing, and the
			// unprivileged role the cloud keeps in the organization's database
			`DO $$ BEGIN CREATE ROLE intruder LOGIN PASSWORD 'intruder'; EXCEPTION WHEN duplicate_object THEN END $$`,
			`DO $$ BEGIN CREATE ROLE lokadb_app LOGIN PASSWORD 'lokadb_app'; EXCEPTION WHEN duplicate_object THEN END $$`,
			// the direct path's role in the organization's database (customers_app): DML alone,
			// granted by org-profile -direct-role
			`DO $$ BEGIN CREATE ROLE lokadb_dml LOGIN PASSWORD 'lokadb_dml'; EXCEPTION WHEN duplicate_object THEN END $$`); err != nil {
			return err
		}
	}
	return nil
}

// fresh (re)creates the check's databases. The cloud one gets the application schema:
// users (a cloud table), documents (tenant column organization_id, FK to users ON DELETE
// CASCADE) and document_versions (tenant derived through document_id).
func fresh(e *env) error {
	for _, host := range []string{e.cloudHost, e.customerHost} {
		if err := execSQL(e.ctx, dsn(host, "postgres", "postgres"),
			"DROP DATABASE IF EXISTS "+e.db+" WITH (FORCE)", "CREATE DATABASE "+e.db+" OWNER app"); err != nil {
			return err
		}
	}
	// block 7: the organization allows a logical move (its tables in public, where the role
	// the cloud keeps creates the indexes of later migrations); M7 checks the move without it
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
		return err
	}
	return schema(e, "SET NULL")
}

func schema(e *env, versionsOnDelete string) error {
	return execSQL(e.ctx, e.cloud(),
		"CREATE EXTENSION postgres_fdw",
		"CREATE TABLE users (id bigserial PRIMARY KEY, name text)",
		"CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, user_id bigint REFERENCES users ON DELETE CASCADE, title text)",
		"CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE "+versionsOnDelete+", content text)",
		"ALTER TABLE users OWNER TO app", "ALTER TABLE documents OWNER TO app", "ALTER TABLE document_versions OWNER TO app",
		"INSERT INTO users (name) SELECT 'u' || g FROM generate_series(1, 10) g",
		// user 1 (g % 10 = 0, so g even) owns documents of organization 1 only
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1 + g % 2, 1 + g % 10, 'd' || g FROM generate_series(1, 200) g",
		"INSERT INTO document_versions (document_id, content) SELECT id, 'v' || id FROM documents")
}

// orgInCloud counts organization 1's documents still in the cloud database: in its own
// partition if a move split it out, in DEFAULT if not (enable leaves every organization there).
const orgInCloud = `SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid
	WHERE d.organization_id = 1 AND c.relkind = 'r'`

const spec = `{"tables": [
  {"name": "documents", "tenant_column": "organization_id"},
  {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}
]}`

func (e *env) lokadb(args ...string) (string, error) {
	args = withReplConsent(args)
	start := time.Now()
	out, err := command(e.ctx, e.bin, args...).CombinedOutput() // cleanup.go: Ctrl-C at the deadline
	if os.Getenv("DEFECTS_TRACE") != "" {                       // what lokadb said, with its timestamps: where a slow check waits
		fmt.Printf("      lokadb %s took %s, err %v\n", args[0], time.Since(start).Round(time.Millisecond), err)
		for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			fmt.Println("        " + l)
		}
	}
	if err != nil {
		return string(out), fmt.Errorf("lokadb %s: %w\n%s", args[0], err, tail(string(out)))
	}
	return string(out), nil
}

func tail(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > 6 {
		lines = lines[len(lines)-6:]
	}
	return strings.Join(lines, "\n")
}

// specFile writes the spec to a file named by its content and replaces it atomically: two
// move-org running at once (D6) must never read a half-written spec of each other.
func (e *env) specFile(body string) (string, error) {
	sum := sha256.Sum256([]byte(body))
	p := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%x.json", e.db, sum[:6]))
	f, err := os.CreateTemp(os.TempDir(), e.db+"-*.tmp")
	if err != nil {
		return p, err
	}
	_, err = f.WriteString(body)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), p)
	}
	if err != nil {
		os.Remove(f.Name())
	}
	return p, err
}

func (e *env) enable() error {
	if e.spec != "" {
		return e.enableSpec(e.spec)
	}
	return e.enableSpec(spec)
}

func (e *env) enableSpec(body string) error {
	p, err := e.specFile(body)
	if err != nil {
		return err
	}
	_, err = e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app")
	return err
}

func (e *env) moveArgs(org int, extra ...string) []string {
	body := spec
	if e.spec != "" {
		body = e.spec
	}
	p, _ := e.specFile(body)
	args := []string{"move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", fmt.Sprint(org),
		"-customer", e.customer(), "-publisher", e.cloud()}
	if moveMode != "" {
		args = append(args, "-mode", moveMode)
	}
	return withReplConsent(append(args, extra...))
}

// withReplConsent: a move-org in -mode logical (the last -mode given) says it accepts the
// subscription's role (-allow-customer-replication), unless the check gives the flag itself,
// also to refuse it (-allow-customer-replication=false).
func withReplConsent(args []string) []string {
	if len(args) == 0 || args[0] != "move-org" {
		return args
	}
	mode := ""
	for i := 1; i+1 < len(args); i++ {
		if args[i] == "-mode" {
			mode = args[i+1]
		}
	}
	for _, a := range args {
		if strings.HasPrefix(a, "-allow-customer-replication") {
			return args
		}
	}
	if mode != "logical" {
		return args
	}
	return append(args[:len(args):len(args)], "-allow-customer-replication")
}

func (e *env) move(org int, extra ...string) (string, error) {
	out, err := e.lokadb(e.moveArgs(org, extra...)...)
	// a version without -customer-app (before the security fixes): the same move without it
	if err != nil && strings.Contains(out, "flag provided but not defined: -customer-app") {
		var rest []string
		for i := 0; i < len(extra); i++ {
			if extra[i] == "-customer-app" {
				i++
				continue
			}
			rest = append(rest, extra[i])
		}
		return e.lokadb(e.moveArgs(org, rest...)...)
	}
	return out, err
}

// moveWatched runs move-org and calls on(line) for every line it prints.
func (e *env) moveWatched(org int, on func(string)) error {
	cmd := command(e.ctx, e.bin, e.moveArgs(org)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return err
	}
	var all strings.Builder
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		on(sc.Text())
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("move-org: %w\n%s", err, tail(all.String()))
	}
	return nil
}

func execSQL(ctx context.Context, d string, stmts ...string) error {
	c, err := pgx.Connect(ctx, d)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	for _, s := range stmts {
		if _, err := c.Exec(ctx, s); err != nil {
			return fmt.Errorf("%w\n  in: %s", err, s)
		}
	}
	return nil
}

func one(ctx context.Context, d, sql string, args ...any) (string, error) {
	c, err := pgx.Connect(ctx, d)
	if err != nil {
		return "", err
	}
	defer c.Close(ctx)
	var v any
	if err := c.QueryRow(ctx, sql, args...).Scan(&v); err != nil {
		return "", fmt.Errorf("%w\n  in: %s", err, sql)
	}
	if v == nil {
		return "NULL", nil
	}
	return fmt.Sprint(v), nil
}

// proxy starts the proxy for the check's database (fanout: log) and returns a stop function.
func (e *env) proxy() (func(), error) { return e.proxyWith(map[string]string{}) }

// proxyWith starts the proxy with customers: organization -> DSN of its database (DDL replay).
func (e *env) proxyWith(customers map[string]string) (func(), error) {
	return e.proxyCfg(customers, nil)
}

// proxyCfg starts the proxy with customers and extra config settings.
func (e *env) proxyCfg(customers map[string]string, extra map[string]any) (func(), error) {
	c := map[string]any{
		"listen":    fmt.Sprintf(":%d", e.port),
		"upstream":  "host=" + e.cloudHost + " port=5432 sslmode=disable",
		"customers": map[string]any{e.db: customers},
		"fanout":    map[string]any{e.db: map[string]string{"mode": "log"}},
		"route_log": true, // the checks read the line "route" (off by default since 26.10)
	}
	for k, v := range extra {
		c[k] = v
	}
	cfg, _ := json.Marshal(c)
	p := filepath.Join(os.TempDir(), e.db+"-proxy.json")
	if err := os.WriteFile(p, cfg, 0o644); err != nil {
		return nil, err
	}
	log, err := os.Create(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(e.bin, "proxy", "-config", p)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	e.proxyProc = cmd.Process
	stop := func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); log.Close() }
	// 60 s, not 10: with three defects stands and e2e on 4 CPUs the proxy has taken longer than
	// 10 s to open its port. The tail of its log says why it did not start, if it did not.
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", e.port)); err == nil {
			c.Close()
			return stop, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	out, _ := os.ReadFile(log.Name())
	if len(out) > 400 {
		out = out[len(out)-400:]
	}
	return nil, fmt.Errorf("proxy did not start in 60 s: %s", strings.TrimSpace(string(out)))
}

// viaProxy connects to the proxy as the application (simple query protocol is used below).
func (e *env) viaProxy() (*pgconn.PgConn, error) {
	return pgconn.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
}

func simple(ctx context.Context, c *pgconn.PgConn, sql string) ([]*pgconn.Result, error) {
	return c.Exec(ctx, sql).ReadAll()
}

// D1. The organization's rows in DEFAULT are copied to its new partition and deleted from
// DEFAULT; a row written in between was deleted without being copied. The application keeps
// inserting (updates: keeps updating) organization 3 during the whole move.
func checkSplitRace(e *env, updates bool) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// organization 3 is created after enable: its rows are in DEFAULT, many of them so the split takes a while
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT 3, 2, 'x' || g FROM generate_series(1, 300000) g"); err != nil {
		return false, "", err
	}
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	rows, err := w.Query(e.ctx, "SELECT id FROM documents WHERE organization_id = 3 ORDER BY id LIMIT 50")
	if err != nil {
		return false, "", err
	}
	upd, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return false, "", err
	}
	var (
		stop     = make(chan struct{})
		done     = make(chan struct{})
		inserted []int64
		last     = map[int64]string{}
		failures int
	)
	go func() { // the application keeps writing organization 3 all along
		defer close(done)
		// at an application's pace: a writer flat out loads the stand itself, and the move
		// under test then waits on it (the subscription's copy of the table above all)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			if !updates {
				var id int64
				if err := w.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (3, 2, $1) RETURNING id",
					fmt.Sprint("w", i)).Scan(&id); err == nil {
					inserted = append(inserted, id)
				} else {
					failures++
				}
				continue
			}
			title := fmt.Sprint("u", i)
			if tag, err := w.Exec(e.ctx, "UPDATE documents SET title = $1 WHERE organization_id = 3 AND id = $2", title, upd[i%len(upd)]); err == nil && tag.RowsAffected() == 1 {
				last[upd[i%len(upd)]] = title
			} else if err != nil {
				failures++
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)
	_, moveErr := e.move(3)
	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-done
	if moveErr != nil {
		return false, "move-org failed: " + oneLine(moveErr.Error()), nil
	}
	got := map[int64]string{}
	r, err := w.Query(e.ctx, "SELECT id, title FROM documents WHERE organization_id = 3")
	if err != nil {
		return false, "", err
	}
	for r.Next() {
		var id int64
		var t string
		if err := r.Scan(&id, &t); err != nil {
			return false, "", err
		}
		got[id] = t
	}
	if err := r.Err(); err != nil {
		return false, "", err
	}
	lostRows, lostUpd := 0, 0
	for _, id := range inserted {
		if _, ok := got[id]; !ok {
			lostRows++
		}
	}
	for id, t := range last {
		if got[id] != t {
			lostUpd++
		}
	}
	return lostRows == 0 && lostUpd == 0, fmt.Sprintf("during the move %d rows inserted, %d of them lost; %d rows updated, %d updates lost (%d writes failed or waited out)",
		len(inserted), lostRows, len(last), lostUpd, failures), nil
}

// D2. Statements of a query string that also has BEGIN/COMMIT ran under the organization
// set by the connection's previous query.
func checkTxString(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 1"); err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, "BEGIN; INSERT INTO document_versions (document_id, content) VALUES ("+doc2+", 'd2'); COMMIT"); err != nil {
		return false, "", err
	}
	org, err := one(e.ctx, e.cloud(), "SELECT organization_id FROM document_versions WHERE content = 'd2'")
	if err != nil {
		return false, "", err
	}
	// a ROLLBACK before the statement would undo lokadb.org set before the string
	if _, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 1"); err != nil {
		return false, "", err
	}
	_, rbErr := simple(e.ctx, c, "ROLLBACK; INSERT INTO document_versions (document_id, content) VALUES ("+doc2+", 'd2b')")
	wrong, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content = 'd2b' AND organization_id <> 2")
	if err != nil {
		return false, "", err
	}
	rb := "ran"
	if rbErr != nil {
		rb = "refused: " + rbErr.Error()
	}
	return org == "2" && wrong == "0", fmt.Sprintf("version of a document of organization 2 stored under organization %s; ROLLBACK + INSERT %s, rows in a wrong organization: %s", org, rb, wrong), nil
}

// D3. A refs lookup that fails (here: the lock it needs is not granted in time) used to
// route the statement to "no organization": a read silently returned nothing.
func checkLookupFailure(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc1, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(e.ctx)
	if _, err := tx.Exec(e.ctx, "LOCK TABLE documents__refs IN ACCESS EXCLUSIVE MODE"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, "SET lock_timeout = '300ms'"); err != nil {
		return false, "", err
	}
	res, qErr := simple(e.ctx, c, "SELECT count(*) FROM document_versions WHERE document_id = "+doc1)
	if qErr != nil {
		return true, "the query failed with an error: " + qErr.Error(), nil
	}
	n := "?"
	if len(res) == 1 && len(res[0].Rows) == 1 {
		n = string(res[0].Rows[0][0])
	}
	return false, "the query returned count = " + n + " without an error (the document has 1 version)", nil
}

// D3. lokadb.org = -1 means "the parent was not found"; a child row inserted then was
// stored under organization -1 in the cloud.
func checkUnknownOrg(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	c, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	_, insErr := c.Exec(e.ctx, "SELECT set_config('lokadb.org', '-1', false)")
	if insErr == nil {
		_, insErr = c.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) SELECT min(id), 'orphan' FROM documents")
	}
	n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE organization_id = -1")
	if err != nil {
		return false, "", err
	}
	if insErr != nil {
		return n == "0", "insert refused: " + insErr.Error(), nil
	}
	return false, "insert accepted: rows under organization -1: " + n, nil
}

// D4. ON DELETE CASCADE from a cloud table writes the organization's partition directly,
// not through the parent the switch locks: committed after the marker, it missed the replica.
func checkCascadeAtSwitch(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// enable leaves every organization in DEFAULT; the cascade's transaction below holds the
	// organization's own partition, which move-org's first step would create
	if err := execSQL(e.ctx, e.cloud(), "SELECT lokadb.partition_org(table_name, 1) FROM lokadb.tables"); err != nil {
		return false, "", err
	}
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	// The cascade's transaction already holds the partition (as a cascade does) when the
	// switch begins, but writes nothing before it: a transaction with an xid would hold back
	// the replication slot of the initial copy.
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(e.ctx)
	// ACCESS SHARE from the start, in every mode: it keeps the switch's ACCESS EXCLUSIVE waiting
	// as the cascade's ROW EXCLUSIVE does, and does not keep the capture's CREATE TRIGGER (SHARE ROW
	// EXCLUSIVE) from installing; taken after the copy instead, it raced the move, which on a fast
	// machine was over before the lock was asked for ("relation documents__o1 does not exist")
	if _, err := tx.Exec(e.ctx, "LOCK TABLE ONLY documents__o1 IN ACCESS SHARE MODE"); err != nil {
		return false, "", err
	}
	var once6 sync.Once
	var txErr error
	committed := make(chan struct{})
	moveErr := e.moveWatched(1, func(line string) {
		if strings.Contains(line, "step 6/6") {
			once6.Do(func() {
				go func() { // delete user 1: cascades into organization 1's partition, committed 1.5 s into the switch
					defer close(committed)
					if txErr != nil {
						return
					}
					// after the switch the cascade into the organization's database is lokadb's
					// trigger (fkcloud.go): users is not in routing.cloud_tables, so it runs in
					// every moved organization only if the fanout policy lets it
					if _, txErr = tx.Exec(e.ctx, "SET LOCAL lokadb.fanout = 'log'"); txErr != nil {
						return
					}
					if _, txErr = tx.Exec(e.ctx, "DELETE FROM users WHERE id = 1"); txErr != nil {
						return
					}
					time.Sleep(1500 * time.Millisecond)
					txErr = tx.Commit(e.ctx)
				}()
			})
		}
	})
	select {
	case <-committed:
	case <-time.After(10 * time.Second):
	}
	cascade := "committed"
	if txErr != nil {
		cascade = "failed: " + oneLine(txErr.Error())
		var pe *pgconn.PgError
		if errors.As(txErr, &pe) && pe.Detail != "" { // a deadlock: who waited for what
			cascade += " (" + oneLine(pe.Detail) + ")"
		}
	}
	if moveErr != nil && !strings.Contains(moveErr.Error(), "verify after switch") {
		return false, "move-org failed: " + moveErr.Error(), nil
	}
	left, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE user_id = 1")
	if err != nil {
		return false, "", err
	}
	if moveErr != nil {
		return false, "move-org failed: " + oneLine(moveErr.Error()) + "; cascade " + cascade + "; documents of the deleted user in the customer database: " + left, nil
	}
	return left == "0" && txErr == nil, "move-org done; cascade " + cascade + "; documents of the deleted user left in the customer database: " + left, nil
}

// D5. CREATE SERVER IF NOT EXISTS kept the options of a server left by an earlier attempt.
func checkStaleServer(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE SERVER lokadb_org1 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'old-address.invalid', dbname 'gone', connect_timeout '2')",
		"CREATE USER MAPPING FOR PUBLIC SERVER lokadb_org1 OPTIONS (user 'postgres', password 'old')"); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "move-org failed: " + oneLine(err.Error()), nil
	}
	n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "after the move the organization cannot be read: " + oneLine(err.Error()), nil
	}
	return n == "100", "after the move the organization reads " + n + " documents (want 100)", nil
}

// D6. Nothing stopped two move-org of one organization from running at once.
func checkConcurrentMoves(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 50 * time.Millisecond)
			_, errs[i] = e.move(1)
		}(i)
	}
	wg.Wait()
	n, readErr := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	state := "reads " + n + " documents"
	if readErr != nil {
		state = "cannot be read: " + oneLine(readErr.Error())
	}
	refused := 0
	var msgs []string
	for i, err := range errs {
		if err == nil {
			msgs = append(msgs, fmt.Sprintf("run %d: done", i+1))
			continue
		}
		if strings.Contains(err.Error(), "another move-org is running") {
			refused++
		}
		msgs = append(msgs, fmt.Sprintf("run %d: %s", i+1, oneLine(err.Error())))
	}
	ok := (errs[0] == nil) != (errs[1] == nil) && refused == 1 && n == "100"
	return ok, strings.Join(msgs, "; ") + "; the organization " + state, nil
}

// D7. The customer FK is (tenant, document_id): SET NULL nulled the tenant column too.
func checkSetNull(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM documents WHERE organization_id = 1 AND id = "+doc); err != nil {
		return false, "deleting a document of the moved organization failed: " + oneLine(err.Error()), nil
	}
	v, err := one(e.ctx, e.customer(), "SELECT organization_id::text || '/' || coalesce(document_id::text, 'NULL') FROM document_versions WHERE content = 'v' || "+doc)
	if err != nil {
		return false, "", err
	}
	return v == "1/NULL", "document deleted; its version now has organization/document " + v, nil
}

// D8. enable recreates the table: other roles' privileges were lost, and an identity id
// restarted at max(id) + 1, handing out ids of deleted rows again.
func checkEnableKeeps(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE notes (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"ALTER TABLE notes OWNER TO app",
		"INSERT INTO notes (organization_id, body) SELECT 1, 'n' || g FROM generate_series(1, 10) g",
		"DELETE FROM notes WHERE id > 5", // ids 6..10 were handed out already
		"GRANT SELECT, INSERT ON notes TO reader"); err != nil {
		return false, "", err
	}
	if err := e.enableSpec(`{"tables": [{"name": "notes", "tenant_column": "organization_id"}]}`); err != nil {
		return false, "", err
	}
	c, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var id int64
	readerErr := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
		if _, err := tx.Exec(e.ctx, "SET LOCAL ROLE reader"); err != nil {
			return err
		}
		if err := tx.QueryRow(e.ctx, "INSERT INTO notes (organization_id, body) VALUES (1, 'by reader') RETURNING id").Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(e.ctx, "SELECT count(*) FROM notes")
		return err
	})
	if readerErr != nil {
		if err := c.QueryRow(e.ctx, "INSERT INTO notes (organization_id, body) VALUES (1, 'by owner') RETURNING id").Scan(&id); err != nil {
			return false, "", err
		}
		return false, fmt.Sprintf("role reader can no longer use the table: %s; next id %d (ids up to 10 were used)", oneLine(readerErr.Error()), id), nil
	}
	return id > 10, fmt.Sprintf("role reader still reads and inserts; next id %d (ids up to 10 were used)", id), nil
}

// D8. enable dropped a table's triggers silently (it recreated the table); then it refused
// such a table. Now the table becomes the parent's DEFAULT partition and its triggers move
// to the parent: they must fire, once, for an update through the parent.
func checkEnableTrigger(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.body := NEW.body || ' (touched)'; RETURN NEW; END $$",
		"CREATE TABLE notes (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"CREATE TRIGGER notes_touch BEFORE UPDATE ON notes FOR EACH ROW EXECUTE FUNCTION touch()",
		"INSERT INTO notes (organization_id, body) VALUES (1, 'n')",
		"ALTER TABLE notes OWNER TO app"); err != nil {
		return false, "", err
	}
	if err := e.enableSpec(`{"tables": [{"name": "notes", "tenant_column": "organization_id"}]}`); err != nil {
		return false, "enable refused: " + oneLine(err.Error()), nil
	}
	if err := execSQL(e.ctx, e.cloud(), "UPDATE notes SET body = 'm' WHERE organization_id = 1"); err != nil {
		return false, "", err
	}
	body, err := one(e.ctx, e.cloud(), "SELECT body FROM notes WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	return body == "m (touched)", "after an update through the parent the row holds: " + body, nil
}

// D9. The retained source partition kept its FKs: deleting a moved document cascaded
// into it (or, with RESTRICT, was refused).
func checkRetainedSource(e *env) (bool, string, error) {
	// versions follow their document with ON DELETE CASCADE here (with SET NULL, D7 would
	// interfere); the FK changed in place: a database made again costs a loaded stand tens of seconds
	if err := execSQL(e.ctx, e.cloud(), "ALTER TABLE document_versions DROP CONSTRAINT document_versions_document_id_fkey, "+
		"ADD FOREIGN KEY (document_id) REFERENCES documents ON DELETE CASCADE"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-retain-source"); err != nil {
		return false, "", err
	}
	before, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__o1_source")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM documents WHERE organization_id = 1 AND id = (SELECT min(id) FROM documents WHERE organization_id = 1)"); err != nil {
		return false, "deleting a document of the moved organization failed: " + oneLine(err.Error()), nil
	}
	after, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__o1_source")
	if err != nil {
		return false, "", err
	}
	return before == after, fmt.Sprintf("document deleted; versions in the retained source: %s before, %s after", before, after), nil
}
