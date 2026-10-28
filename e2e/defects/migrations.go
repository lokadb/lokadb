package main

// Migrations of LokaDB tables through the proxy (tasks/BACKLOG.md, block 7).

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "M1", name: "an organization moved while the proxy runs gets the next migration, no restart, no customers in the config", run: checkReplayNewOrg},
		check{id: "M3", name: "schema-diff finds a damaged organization's schema, schema-sync repairs it", run: checkSchemaDiffSync},
		check{id: "M4", name: "DDL through the proxy while move-org runs is refused, nowhere applied; after it, it is replayed", run: checkDDLDuringMove},
		check{id: "M2", name: "the organization's credentials of the DDL replay: not to clients through the proxy, not in its log or recording", run: checkReplayCredentials},
	)
}

// M1 (1.5). The proxy replayed DDL only in the databases of its config (customers): an
// organization moved after the proxy started did not get the migration, and its queries then
// failed on the missing column.
func checkReplayNewOrg(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy() // no customers at all
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy() // a session open before the move
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 1"); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	_, altErr := simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN m1_new text")
	got, err := e.bothSides("m1_new")
	if err != nil {
		return false, "", err
	}
	_, insErr := simple(e.ctx, c, "INSERT INTO documents (organization_id, title, m1_new) VALUES (1, 'm1', 'v')")
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE m1_new = 'v'")
	if err != nil {
		n = "error: " + oneLine(err.Error())
	}
	return altErr == nil && insErr == nil && got == "cloud m1_new, organization m1_new" && n == "1",
		fmt.Sprintf("ALTER error: %v; %s; insert of the new column: error %v, rows at the organization %s", errText(altErr), got, errText(insErr), n), nil
}

func errText(err error) string {
	if err == nil {
		return "none"
	}
	return oneLine(err.Error())
}

// M2. The proxy reads the credentials of the organizations' roles from the cloud
// (lokadb.replay_targets: the FDW server and its user mapping). A client of the proxy must not
// get them, and they must not reach the proxy's log or its recording of the workload.
func checkReplayCredentials(e *env) (bool, string, error) {
	if moveMode == "logical" { // its role may not create what a subscription's move needs (schema lokadb)
		return true, "DEFECTS_MODE=logical: the check moves by triggers, with a role that may not create schema lokadb", nil
	}
	const secret = "m2-secret-7f3a"
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		`DO $$ BEGIN CREATE ROLE m2app LOGIN; EXCEPTION WHEN duplicate_object THEN END $$`,
		"ALTER ROLE m2app PASSWORD '"+secret+"'"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	app := fmt.Sprintf("host=customer dbname=%s user=m2app password=%s sslmode=disable", e.db, secret)
	if _, err := e.move(1, "-customer-app", app); err != nil {
		return false, "", err
	}
	rec := filepath.Join(os.TempDir(), e.db+"-workload.json")
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"record": rec})
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		stop()
		return false, "", err
	}
	ok := true
	var notes []string
	for _, q := range []string{
		"SELECT * FROM lokadb.replay_targets()",
		"SELECT umoptions FROM pg_user_mappings",
		"SELECT * FROM information_schema.user_mapping_options",
		"SELECT srvoptions FROM pg_foreign_server",
	} {
		res, qerr := simple(e.ctx, c, q)
		if qerr == nil {
			got := ""
			for _, r := range res {
				for _, row := range r.Rows {
					for _, v := range row {
						got += string(v) + " "
					}
				}
			}
			ok = false
			notes = append(notes, fmt.Sprintf("%s ran (password shown: %v)", q, strings.Contains(got, secret)))
		}
	}
	_, altErr := simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN m2_col text")
	c.Close(e.ctx)
	time.Sleep(3 * time.Second) // the proxy writes its recording every 2 s
	stop()
	got, err := e.bothSides("m2_col")
	if err != nil {
		return false, "", err
	}
	if altErr != nil || got != "cloud m2_col, organization m2_col" {
		ok = false
		notes = append(notes, fmt.Sprintf("replay as the organization's role: error %s, %s", errText(altErr), got))
	}
	for _, f := range []string{filepath.Join(os.TempDir(), e.db+"-proxy.log"), rec} {
		b, err := os.ReadFile(f)
		if err != nil || len(b) == 0 {
			ok = false
			notes = append(notes, "nothing to look at in "+filepath.Base(f))
		}
		if strings.Contains(string(b), secret) {
			ok = false
			notes = append(notes, "the password is in "+filepath.Base(f))
		}
	}
	// and the function is there to the owner in the cloud, outside the proxy
	n, err := one(e.ctx, dsn(e.cloudHost, e.db, "app"), "SELECT count(*) FROM lokadb.replay_targets()")
	if err != nil {
		n = "error: " + oneLine(err.Error())
	}
	if len(notes) == 0 {
		notes = append(notes, "refused to the client: lokadb.replay_targets(), pg_user_mappings, user_mapping_options, pg_foreign_server; replayed; no password in the log or the recording")
	}
	return ok, strings.Join(notes, "; ") + "; replay_targets() for the owner in the cloud: " + n, nil
}

// schemaCmd runs schema-diff or schema-sync: its output and exit code.
func (e *env) schemaCmd(args ...string) (string, int, error) {
	// the sync commands act on organizations named, or on every moved one by -all
	if (args[0] == "schema-sync" || args[0] == "refs-sync") && !slices.Contains(args, "-org") && !slices.Contains(args, "-all") {
		args = append(args[:len(args):len(args)], "-all")
	}
	out, err := exec.CommandContext(e.ctx, e.bin, append(args[:1:1], append([]string{"-db", e.cloud()}, args[1:]...)...)...).CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode(), nil
	}
	return string(out), 0, err
}

// M3. Nothing compared the organizations' copies with the cloud: a replay that failed
// half-way, a change by hand there or a migration that bypassed the proxy left an
// organization's table behind, and only its queries found out.
func checkSchemaDiffSync(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := e.script("CREATE INDEX documents_title_m3 ON documents (title)"); err != nil {
		return false, "", err
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	rel := sch + ".documents"
	// the damage: by hand in the organization's database, and a migration that bypassed the proxy
	if err := execSQL(e.ctx, e.customer(),
		"ALTER TABLE "+rel+" ALTER COLUMN title TYPE varchar(50)",
		"ALTER TABLE "+rel+" ADD COLUMN m3_junk int",
		"DROP INDEX "+sch+".documents_title_m3"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "ALTER TABLE documents ADD COLUMN m3_missed text NOT NULL DEFAULT 'x'"); err != nil {
		return false, "", err
	}
	diff, code, err := e.schemaCmd("schema-diff")
	if err != nil {
		return false, "", err
	}
	var notes []string
	ok := code == 2
	for _, want := range []string{"documents.title column_type", "documents.m3_junk extra_column", "documents.documents_title_m3 missing_index", "documents.m3_missed missing_column"} {
		if !strings.Contains(diff, want) {
			ok = false
			notes = append(notes, "schema-diff misses "+want)
		}
	}
	notes = append(notes, fmt.Sprintf("schema-diff exit %d", code))
	sync, scode, err := e.schemaCmd("schema-sync")
	if err != nil {
		return false, "", err
	}
	// without -drop-extra the organization's own column stays, reported
	if scode != 2 || !strings.Contains(sync, "m3_junk extra_column") {
		ok = false
		notes = append(notes, fmt.Sprintf("schema-sync without -drop-extra: exit %d: %s", scode, tail(sync)))
	}
	if sync, scode, err = e.schemaCmd("schema-sync", "-drop-extra"); err != nil {
		return false, "", err
	}
	after, acode, err := e.schemaCmd("schema-diff")
	if err != nil {
		return false, "", err
	}
	if scode != 0 || acode != 0 {
		ok = false
		notes = append(notes, fmt.Sprintf("schema-sync -drop-extra exit %d, schema-diff after it exit %d: %s", scode, acode, tail(sync+after)))
	}
	// and the organization works with the cloud's schema
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	long := strings.Repeat("t", 80)
	_, insErr := simple(e.ctx, c, "INSERT INTO documents (organization_id, title) VALUES (1, '"+long+"')")
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM "+rel+" WHERE m3_missed = 'x'")
	if err != nil {
		n = "error: " + oneLine(err.Error())
	}
	if insErr != nil || n != "101" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("after sync: insert of an 80-character title error %s, rows with m3_missed at the organization %s", errText(insErr), n))
	return ok, strings.Join(notes, "; "), nil
}

// M4. A migration while move-org ran: the moving organization's tables had been made from
// the schema read at the start, and it was not in the replay targets before the switch: its
// database never got the change. Here the move is held at its lock (the same lock move-org
// takes: a real move-org is refused meanwhile, so the key is the one move-org uses).
func checkDDLDuringMove(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	mover, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer mover.Close(e.ctx)
	if _, err := mover.Exec(e.ctx, "SELECT pg_advisory_lock($1)", int64(0x62796f6462)); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	if out, err := e.move(2); err == nil || !strings.Contains(out, "another move-org is running") {
		ok = false
		notes = append(notes, "a real move-org was not held by the lock: "+tail(out))
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	_, altErr := simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN m4_during text")
	got, err := e.bothSides("m4_during")
	if err != nil {
		return false, "", err
	}
	if altErr == nil || !strings.Contains(altErr.Error(), "move-org is running") || got != "cloud -, organization -" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("during the move: error %s; %s", errText(altErr), got))
	if _, err := mover.Exec(e.ctx, "SELECT pg_advisory_unlock($1)", int64(0x62796f6462)); err != nil {
		return false, "", err
	}
	_, altErr = simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN m4_after text")
	if got, err = e.bothSides("m4_after"); err != nil {
		return false, "", err
	}
	if altErr != nil || got != "cloud m4_after, organization m4_after" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("after it: error %s; %s", errText(altErr), got))
	return ok, strings.Join(notes, "; "), nil
}
