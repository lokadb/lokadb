package admin

// R4 review (enable area): a rotation whose ALTER ROLE at the customer committed, but whose
// answer was lost (the agent's connection dropped), clears cred_rotations.next: the new
// password is lost, the old one opens nothing.

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// dropAfterAlter: a TCP relay to target that, the first time it sees ALTER ROLE from the
// client, passes it on, waits for the server to run it, then closes both sides without the answer.
func dropAfterAlter(t *testing.T, target string) (addr string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var fired atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				s, err := net.Dial("tcp", target)
				if err != nil {
					c.Close()
					return
				}
				go func() { io.Copy(s, c); s.Close() }()
				buf := make([]byte, 64*1024)
				for {
					n, err := s.Read(buf)
					if n > 0 {
						// the server's CommandComplete of ALTER ROLE: it ran (and commits); the client never hears it
						if bytes.Contains(buf[:n], append([]byte("ALTER ROLE"), 0)) && fired.CompareAndSwap(false, true) {
							time.Sleep(300 * time.Millisecond)
							s.Close()
							c.Close()
							return
						}
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						c.Close()
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestR4RotateLostAnswer(t *testing.T) {
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
	for _, q := range []string{"DROP DATABASE IF EXISTS r4rot_cust WITH (FORCE)", "DROP ROLE IF EXISTS lokadb_app_r4rot", "CREATE ROLE lokadb_app_r4rot LOGIN PASSWORD 'p0'", "CREATE DATABASE r4rot_cust OWNER lokadb_app_r4rot"} {
		if _, err := su.Exec(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}
	relay := dropAfterAlter(t, net.JoinHostPort(host, port))
	rh, rp, _ := net.SplitHostPort(relay)
	appDSN := fmt.Sprintf("host=%s port=%s dbname=r4rot_cust user=lokadb_app_r4rot password=p0 sslmode=disable", rh, rp)
	liveConn(t, "r4rot_cloud", "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		fmt.Sprintf("CREATE SERVER lokadb_org7 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%s', dbname 'r4rot_cust')", host, port),
		"CREATE USER MAPPING FOR CURRENT_USER SERVER lokadb_org7 OPTIONS (user 'lokadb_app_r4rot', password 'p0')")
	liveConn(t, "r4rot_cp")
	cpDSN := base + "/r4rot_cp"
	keyFile := filepath.Join(t.TempDir(), "key")
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	if err := os.WriteFile(keyFile, k, 0o600); err != nil {
		t.Fatal(err)
	}
	cloudDSN := base + "/r4rot_cloud"
	inst, err := CloudInstallation(ctx, cloudDSN)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := pgx.Connect(ctx, cpDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close(ctx)
	if err := creds.Put(ctx, cp, k, inst, "r4rot_cloud", "7", creds.Owner, appDSN); err != nil {
		t.Fatal(err)
	}
	o := RotateOptions{CloudDSN: cloudDSN, ControlPlane: cpDSN, KeyFile: keyFile, Org: 7}
	_, err = RotateCredentials(ctx, o)
	t.Logf("first rotation (its ALTER ROLE ran, the answer was lost): %v", err)
	var next []byte
	_ = cp.QueryRow(ctx, `SELECT next FROM lokadb_cp.cred_rotations WHERE org = 7 AND kind = 'owner'`).Scan(&next)
	if next == nil {
		t.Errorf("cred_rotations.next was cleared although the answer was only lost: the new password is gone")
	}
	if c, err := pgx.Connect(ctx, appDSN); err == nil {
		c.Close(ctx)
		t.Logf("the old password still opens the customer's database")
	} else {
		t.Logf("the old password (the one the control plane holds) opens nothing: %v", err)
	}
	if _, err = RotateCredentials(ctx, o); err != nil {
		t.Errorf("the next rotation does not recover the role: %v", err)
	}
	_, _ = su.Exec(ctx, "DROP DATABASE IF EXISTS r4rot_cust WITH (FORCE)")
	_, _ = su.Exec(ctx, "DROP ROLE IF EXISTS lokadb_app_r4rot")
}
