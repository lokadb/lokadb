package admin

// R01, live: a copy of the SaaS database (its organization's FDW server and user mapping, as a
// restored dump has them) in another cluster reaches the customer's database through the relay,
// until lokadb pin proves the SaaS database's address; then the copy's DELETE is refused and the
// customer's rows stay, while the SaaS database goes on.
//   LOKADB_LIVE_DSN=postgres://u:pw@saas-host:5432 LOKADB_LIVE_COPY_DSN=postgres://u:pw@other-host:5432 go test ./internal/admin -run PinLive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

func TestPinLive(t *testing.T) {
	base, copyBase := os.Getenv("LOKADB_LIVE_DSN"), os.Getenv("LOKADB_LIVE_COPY_DSN")
	if base == "" || copyBase == "" {
		t.Skip("LOKADB_LIVE_DSN and LOKADB_LIVE_COPY_DSN not set (two clusters)")
	}
	ctx := context.Background()
	u, _ := url.Parse(base)
	pw, _ := u.User.Password()
	me := ownAddr(t) // the address the clusters reach this test (and its relay) at

	// the customer's database, the control plane, the SaaS database: on the first cluster
	cust := liveConn(t, "pin_cust", "CREATE TABLE docs (id int PRIMARY KEY)", "INSERT INTO docs SELECT generate_series(1, 300)")
	_ = cust
	liveConn(t, "pin_cp")
	cp, err := tunnel.OpenPGStore(ctx, base+"/pin_cp")
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()

	// the relay and the organization's agent, which reaches the customer's database
	dir := t.TempDir()
	if err := tunnel.GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	free := func() string {
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return l.Addr().String()
	}
	relayAddr, enroll, orgAddr := free(), free(), free()
	_, orgPort, _ := net.SplitHostPort(orgAddr)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	rl := tunnel.NewRelay(tunnel.RelayConfig{Listen: relayAddr, EnrollListen: enroll, Orgs: map[string]string{"7": orgAddr}, OrgAllow: []string{"0.0.0.0/0", "::/0"},
		Cert: filepath.Join(dir, "relay.crt"), Key: filepath.Join(dir, "relay.key"), CA: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key")}, cp, log)
	go func() { _ = rl.Run() }()
	tok, err := cp.CreateToken(ctx, "7", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tf := filepath.Join(dir, "token")
	_ = os.WriteFile(tf, []byte(tok), 0o600)
	ag := tunnel.NewAgent(tunnel.AgentConfig{Relay: strings.Replace(relayAddr, "0.0.0.0", "127.0.0.1", 1), Enroll: strings.Replace(enroll, "0.0.0.0", "127.0.0.1", 1), ServerName: "localhost",
		CA: filepath.Join(dir, "ca.crt"), StateDir: filepath.Join(dir, "agent"), TokenFile: tf, DB: u.Host}, log)
	go func() { _ = ag.Run() }()

	// the organization's server, as move-org makes it, in the SaaS database and in its copy (what
	// a restored dump has); the customer's password is in both user mappings
	ddl := []string{"CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		fmt.Sprintf("CREATE SERVER lokadb_org7 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%s', dbname 'pin_cust', connect_timeout '5')", me, orgPort),
		fmt.Sprintf("CREATE USER MAPPING FOR CURRENT_USER SERVER lokadb_org7 OPTIONS (user '%s', password '%s')", u.User.Username(), pw),
		"CREATE FOREIGN TABLE docs (id int) SERVER lokadb_org7 OPTIONS (table_name 'docs')"}
	saas := liveConn(t, "pin_saas", ddl...)
	cu, _ := url.Parse(copyBase)
	cpw, _ := cu.User.Password()
	adm, err := pgx.Connect(ctx, copyBase+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = adm.Exec(ctx, "DROP DATABASE IF EXISTS pin_copy WITH (FORCE)")
	if _, err := adm.Exec(ctx, "CREATE DATABASE pin_copy"); err != nil {
		t.Fatal(err)
	}
	adm.Close(ctx)
	_ = cpw
	cop, err := pgx.Connect(ctx, copyBase+"/pin_copy")
	if err != nil {
		t.Fatal(err)
	}
	defer cop.Close(ctx)
	for _, q := range ddl {
		if _, err := cop.Exec(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}
	count := func(c *pgx.Conn) (int, error) {
		var n int
		err := c.QueryRow(ctx, "SELECT count(*) FROM docs").Scan(&n)
		return n, err
	}
	waitLive(t, "the agent connected", func() bool { n, err := count(saas); return err == nil && n == 300 })

	// before the pin: R01 as found, the copy reaches the customer
	if n, err := count(cop); err != nil || n != 300 {
		t.Fatalf("before the pin the copy should reach the customer (the finding): %d %v", n, err)
	}
	res, err := Pin(ctx, saas, cp)
	if err != nil || len(res.Relays) != 1 || len(res.Failed) != 0 {
		t.Fatalf("pin: %+v %v", res, err)
	}
	proven := false
	for _, s := range res.Sources {
		proven = proven || s.How == "probe" && s.Valid
	}
	if !proven {
		t.Fatalf("no address proven: %+v", res.Sources)
	}

	// the copy's DELETE is refused, with what it means; the customer keeps every row
	_, err = cop.Exec(ctx, "DELETE FROM docs WHERE id < 28")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || !strings.Contains(pe.Detail, "it is not the SaaS database organization 7 was moved from") || !strings.Contains(pe.Detail, "lokadb defuse") {
		t.Fatalf("the copy's DELETE: %v", err)
	}
	var left int
	if err := cust.QueryRow(ctx, "SELECT count(*) FROM docs").Scan(&left); err != nil || left != 300 {
		t.Fatalf("the customer's rows: %d %v", left, err)
	}
	// the SaaS database goes on (a new connection of it too)
	if _, err := saas.Exec(ctx, "SELECT postgres_fdw_disconnect_all()"); err != nil {
		t.Fatal(err)
	}
	if n, err := count(saas); err != nil || n != 300 {
		t.Fatalf("the SaaS database after the pin: %d %v", n, err)
	}
	refused, err := cp.RefusedSources(ctx, time.Hour)
	if err != nil || len(refused) == 0 || refused[0].Org != "7" {
		t.Fatalf("the refusal is not recorded: %+v %v", refused, err)
	}

	// lokadb defuse on the copy: no password left in it, its server leads nowhere
	if res, err := Defuse(ctx, cop, "wrong"); err != nil || res.Done || len(res.Servers) != 1 {
		t.Fatalf("defuse without the name: %+v %v", res, err)
	}
	if res, err := Defuse(ctx, cop, "pin_copy"); err != nil || !res.Done {
		t.Fatalf("defuse: %+v %v", res, err)
	}
	var maps int
	if err := cop.QueryRow(ctx, "SELECT count(*) FROM pg_user_mappings WHERE srvname = 'lokadb_org7'").Scan(&maps); err != nil || maps != 0 {
		t.Fatalf("user mappings left: %d %v", maps, err)
	}
	if _, err := count(cop); err == nil {
		t.Fatal("the defused copy still reads the customer")
	}

	// LokaDB's own components (move-org, the proxy's direct profile) from another address: refused
	// until they prove it, through the registry, as they do before they connect
	direct := fmt.Sprintf("host=127.0.0.1 port=%s dbname=pin_cust user=%s password=%s sslmode=disable connect_timeout=10", orgPort, u.User.Username(), pw)
	if c, err := connect(ctx, direct); err == nil {
		c.Close(ctx)
		t.Fatal("a component that did not prove its address got through")
	}
	tunnel.ProveSelfWith(cp)
	defer tunnel.ProveSelfWith(nil)
	waitLive(t, "the relay published its ports", func() bool {
		c, err := connect(ctx, direct)
		if err != nil {
			return false
		}
		c.Close(ctx)
		return true
	})
}

func waitLive(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 150; i++ {
		if ok() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}

// ownAddr: this machine's first address other than loopback.
func ownAddr(t *testing.T) string {
	as, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range as {
		if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && n.IP.To4() != nil {
			return n.IP.String()
		}
	}
	t.Fatal("no address")
	return ""
}
