package admin

// R01, part 2, live: the organization's role gets a new password while the SaaS database reads
// it through postgres_fdw: the open transaction goes on, the next one connects with the new
// mapping, the old password opens nothing, the control plane holds the new one; a rotation cut
// after the customer's ALTER ROLE is finished by the next run.
//   LOKADB_LIVE_DSN=postgres://u:pw@host:5432 go test ./internal/admin -run RotateLive

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

func TestRotateLive(t *testing.T) {
	base := os.Getenv("LOKADB_LIVE_DSN")
	if base == "" {
		t.Skip("LOKADB_LIVE_DSN not set")
	}
	ctx := context.Background()
	u, _ := url.Parse(base)
	host, port := u.Hostname(), u.Port()
	su, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer su.Close(ctx)
	for _, q := range []string{"DROP DATABASE IF EXISTS rot_cust WITH (FORCE)", "DROP ROLE IF EXISTS lokadb_app_rot", "CREATE ROLE lokadb_app_rot LOGIN PASSWORD 'p0'", "CREATE DATABASE rot_cust OWNER lokadb_app_rot"} {
		if _, err := su.Exec(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}
	defer func() {
		_, _ = su.Exec(ctx, "DROP DATABASE IF EXISTS rot_cust WITH (FORCE)")
		_, _ = su.Exec(ctx, "DROP ROLE IF EXISTS lokadb_app_rot")
	}()
	appDSN := fmt.Sprintf("host=%s port=%s dbname=rot_cust user=lokadb_app_rot password=p0 sslmode=disable", host, port)
	app, err := pgx.Connect(ctx, appDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.Exec(ctx, "CREATE TABLE docs (id int); INSERT INTO docs SELECT generate_series(1, 50)"); err != nil {
		t.Fatal(err)
	}
	app.Close(ctx)

	cloud := liveConn(t, "rot_cloud", "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		fmt.Sprintf("CREATE SERVER lokadb_org7 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%s', dbname 'rot_cust')", host, port),
		"CREATE USER MAPPING FOR CURRENT_USER SERVER lokadb_org7 OPTIONS (user 'lokadb_app_rot', password 'p0')",
		"CREATE FOREIGN TABLE docs (id int) SERVER lokadb_org7")
	liveConn(t, "rot_cp")
	cpDSN := base + "/rot_cp"
	keyFile := filepath.Join(t.TempDir(), "key")
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	if err := os.WriteFile(keyFile, k, 0o600); err != nil {
		t.Fatal(err)
	}
	cloudDSN := base + "/rot_cloud"
	inst, err := CloudInstallation(ctx, cloudDSN)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := pgx.Connect(ctx, cpDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close(ctx)
	if err := creds.Put(ctx, cp, k, inst, "rot_cloud", "7", creds.Owner, appDSN); err != nil {
		t.Fatal(err)
	}
	count := func(q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}) (int, error) {
		var n int
		err := q.QueryRow(ctx, "SELECT count(*) FROM docs").Scan(&n)
		return n, err
	}

	// a transaction of the SaaS database is reading the organization when its password changes
	tx, err := cloud.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := count(tx); err != nil || n != 50 {
		t.Fatal(n, err)
	}
	o := RotateOptions{CloudDSN: cloudDSN, ControlPlane: cpDSN, KeyFile: keyFile, Org: 7}
	res, err := RotateCredentials(ctx, o)
	if err != nil || len(res.Rotated) != 1 || res.Rotated[0] != "lokadb_app_rot" {
		t.Fatalf("rotate: %+v %v", res, err)
	}
	if n, err := count(tx); err != nil || n != 50 {
		t.Fatalf("the open transaction after the rotation: %d %v", n, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := count(cloud); err != nil || n != 50 {
		t.Fatalf("the next transaction (a new connection with the new mapping): %d %v", n, err)
	}
	if c, err := pgx.Connect(ctx, appDSN); err == nil {
		c.Close(ctx)
		t.Fatal("the old password still opens the customer's database")
	}
	set, _, err := creds.Load(ctx, cp, inst, k)
	if err != nil {
		t.Fatal(err)
	}
	now := set.DSN("rot_cloud", "7", creds.Owner)
	if now == appDSN || !strings.Contains(now, "lokadb_app_rot") {
		t.Fatalf("the control plane: %q", now)
	}
	if c, err := pgx.Connect(ctx, now); err != nil {
		t.Fatalf("the control plane's DSN: %v", err)
	} else {
		c.Close(ctx)
	}
	var maps string
	if err := cloud.QueryRow(ctx, "SELECT array_to_string(umoptions, ',') FROM pg_user_mappings WHERE srvname = 'lokadb_org7'").Scan(&maps); err != nil || strings.Contains(maps, "password=p0") {
		t.Fatalf("the user mapping: %q %v", maps, err)
	}

	// a rotation cut after the customer's ALTER ROLE: the role has the next password, the control
	// plane and the mapping the old one; the next run finishes it
	next, _ := withCredentials(now, "lokadb_app_rot", "cut-in-the-middle")
	blob, _ := creds.Seal(k, inst, "rot_cloud", "7", creds.Owner+".next", next)
	if _, err := cp.Exec(ctx, `UPDATE lokadb_cp.cred_rotations SET next = $1 WHERE org = 7 AND kind = 'owner'`, blob); err != nil {
		t.Fatal(err)
	}
	if _, err := su.Exec(ctx, "ALTER ROLE lokadb_app_rot PASSWORD 'cut-in-the-middle'"); err != nil {
		t.Fatal(err)
	}
	if res, err := RotateCredentials(ctx, o); err != nil || len(res.Rotated) != 1 {
		t.Fatalf("the rotation after a cut one: %+v %v", res, err)
	}
	if _, err := cloud.Exec(ctx, "SELECT postgres_fdw_disconnect_all()"); err != nil {
		t.Fatal(err)
	}
	if n, err := count(cloud); err != nil || n != 50 {
		t.Fatalf("after the cut rotation was finished: %d %v", n, err)
	}
	at, err := Rotated(ctx, cpDSN)
	if err != nil || at["7"].IsZero() {
		t.Fatalf("rotated at: %v %v", at, err)
	}
}
