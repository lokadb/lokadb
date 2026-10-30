package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// lokadb pin: the relays take the organizations' connections from the SaaS database only (R01: a
// restored copy of it reached a customer's database). With -db it proves the database's address
// by a probe from it (move-org and the runner do it by themselves); -confirm and -forget change the
// list by hand (an address the relays saw while nothing was proven, a NAT the probe cannot show).
func runPin() error {
	fs := flag.NewFlagSet("pin", flag.ExitOnError)
	cp := fs.String("cp", "", "control plane (registry) database DSN")
	db := fs.String("db", "", "the SaaS database (an administrator's DSN): prove its address with a probe from it")
	confirm := fs.String("confirm", "", "an address (IP) that is the SaaS database's: only the confirmed and proven ones pass")
	forget := fs.String("forget", "", "an address that is not the SaaS database's any more")
	parseArgs(fs)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	store, err := tunnel.OpenPGStore(ctx, *cp)
	if err != nil {
		return err
	}
	defer store.Close()
	switch {
	case *confirm != "":
		if net.ParseIP(*confirm) == nil {
			return fmt.Errorf("pin: -confirm takes an IP address, not %q", *confirm)
		}
		if err := store.ProveSource(ctx, *confirm, "manual"); err != nil {
			return err
		}
		fmt.Printf("%s is the SaaS database's: the relays take the organizations' connections from it, and from the other proven addresses only\n", *confirm)
	case *forget != "":
		if err := store.ForgetSource(ctx, *forget); err != nil {
			return err
		}
		fmt.Printf("%s forgotten\n", *forget)
	case *db != "":
		conn, err := pgx.Connect(ctx, *db)
		if err != nil {
			return err
		}
		defer conn.Close(context.Background())
		res, err := admin.Pin(ctx, conn, store)
		if err != nil {
			return err
		}
		if len(res.Relays) == 0 {
			fmt.Println("no organization of this database is moved: nothing to pin yet")
		}
		for _, r := range res.Relays {
			if why, bad := res.Failed[r]; bad {
				fmt.Printf("relay %s: the probe did not go through: %s\n", r, why)
			} else {
				fmt.Printf("relay %s: this database's address proven\n", r)
			}
		}
		if res.SystemID != "" {
			fmt.Printf("database system identifier %s\n", res.SystemID)
		}
	}
	src, err := store.Sources(ctx)
	if err != nil {
		return err
	}
	if len(src) == 0 {
		fmt.Println("no address known: the relays let org_allow in (run lokadb pin -db from the SaaS database)")
	}
	for _, s := range src {
		state := "passes"
		switch {
		case s.How == "seen":
			state = "trusted, NOT confirmed (seen first " + s.First.UTC().Format(time.RFC3339) + "): lokadb pin -confirm " + s.Addr + " if it is your SaaS database"
		case !s.Valid:
			state = "proven earlier, refused now (a newer address is proven)"
		}
		fmt.Printf("%-40s %-7s %s\n", s.Addr, s.How, state)
	}
	refused, err := store.RefusedSources(ctx, 7*24*time.Hour)
	if err == nil && len(refused) > 0 {
		fmt.Println("refused lately (not the SaaS database: a copy of it?):")
		for _, r := range refused {
			fmt.Printf("  %-38s organization %s, %d time(s), last %s\n", r.Addr, r.Org, r.Count, r.Last.UTC().Format(time.RFC3339))
		}
	}
	return nil
}

// proveWith: this process proves its address to the relays before it connects to one of their
// organizations' ports (tunnel/self.go); dsn is the control plane ("": it does not).
func proveWith(dsn string) {
	if dsn == "" {
		return
	}
	if cp, err := tunnel.OpenRegistry(context.Background(), dsn); err == nil {
		tunnel.ProveSelfWith(cp) // for the process's life
	}
}

// lokadb defuse: a copy of the SaaS database (a dump restored for staging, a laptop, analytics)
// carries every moved organization's FDW server and the customer's password in its user
// mappings. This takes them out of the copy: the user mappings go (the passwords with them) and
// each server leads nowhere; the organizations' partitions stay (their rows are at the
// customers), and reading them gives an error. Run it on the copy only: on the SaaS database
// itself it cuts every moved customer off.
func runDefuse() error {
	fs := flag.NewFlagSet("defuse", flag.ExitOnError)
	db := fs.String("db", "", "the COPY of the SaaS database (an administrator's DSN)")
	yes := fs.String("this-is-a-copy", "", "the copy's database name, typed again: defuse runs only when it matches")
	parseArgs(fs)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, *db)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	res, err := admin.Defuse(ctx, conn, *yes)
	if err != nil {
		return err
	}
	if len(res.Servers) == 0 {
		fmt.Printf("database %s has no LokaDB organization servers: nothing to defuse\n", res.Database)
		return nil
	}
	if !res.Done {
		fmt.Printf("database %s has %d organization server(s) that lead to customers' databases:\n", res.Database, len(res.Servers))
		for _, s := range res.Servers {
			fmt.Printf("  %s (user mappings: %s)\n", s.Name, strings.Join(s.Roles, ", "))
		}
		return fmt.Errorf("defuse: run again with -this-is-a-copy %s if this is a COPY (on the SaaS database itself it cuts every moved customer off)", res.Database)
	}
	fmt.Printf("database %s defused: %d organization server(s) lead nowhere and keep no password; the organizations' partitions give an error when read\n", res.Database, len(res.Servers))
	return nil
}
