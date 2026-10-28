package creds

// The rows of before A14 against a real Postgres (second review of admin, R3-7). Needs
// LOKADB_REVIEW_DSN (a disposable server, a superuser): skipped without it.

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// A row of before A14 names no cloud: neither of two clouds with a database of one name is
// served it, Reseal and Put of one cloud leave it alone, and ClaimLegacy gives it to the cloud
// the operator names, unless that cloud has its own (then it is kept and reported).
func TestLegacyRowsNoCloudsUntilClaimed(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS creds_legacy_cp")
	if _, err := admin.Exec(ctx, "CREATE DATABASE creds_legacy_cp"); err != nil {
		t.Fatal(err)
	}
	admin.Close(ctx)
	u, _ := url.Parse(base)
	u.Path = "/creds_legacy_cp"
	cp, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close(ctx)
	k := key(1)
	const a, b = "7000000000000000001", "7000000000000000002"
	for _, org := range []string{"5", "6"} { // before A14: no installation
		if err := Put(ctx, cp, k, "", "app", org, Direct, "host=org"+org+"-legacy"); err != nil {
			t.Fatal(err)
		}
	}
	for _, inst := range []string{a, b} {
		set, _, legacy, err := LoadAll(ctx, cp, inst, k)
		if err != nil {
			t.Fatal(err)
		}
		if set.DSN("app", "5", Direct) != "" || legacy["app"] != 2 {
			t.Errorf("cloud %s is served a row of before A14 (%q) or does not count them (%v)", inst, set.DSN("app", "5", Direct), legacy)
		}
	}
	// cloud A: a rotation and a row of its own for organization 6; the rows of before A14 stay
	if _, _, err := Reseal(ctx, cp, key(2), a, k); err != nil {
		t.Fatal(err)
	}
	if err := Put(ctx, cp, key(2), a, "app", "6", Direct, "host=org6-of-A"); err != nil {
		t.Fatal(err)
	}
	if _, _, legacy, _ := LoadAll(ctx, cp, b, k); legacy["app"] != 2 {
		t.Fatalf("cloud A's Reseal or Put took or deleted a row of before A14: %v left", legacy)
	}
	// the operator says the rows are A's: organization 5's becomes A's, 6's stays (A has its own)
	n, left, err := ClaimLegacy(ctx, cp, key(2), a, "app", k)
	if err != nil {
		t.Fatal(err)
	}
	setA, _, legacy, _ := LoadAll(ctx, cp, a, key(2))
	if n != 1 || len(left) != 1 || setA.DSN("app", "5", Direct) != "host=org5-legacy" || setA.DSN("app", "6", Direct) != "host=org6-of-A" || legacy["app"] != 1 {
		t.Fatalf("claim: %d claimed, left %v; A has 5 %q, 6 %q; %v of before A14 left (want 1, 1 left, the legacy row of 5, its own of 6, 1)",
			n, left, setA.DSN("app", "5", Direct), setA.DSN("app", "6", Direct), legacy)
	}
	if setB, _, _, _ := LoadAll(ctx, cp, b, k, key(2)); setB.DSN("app", "5", Direct) != "" {
		t.Fatalf("cloud B opens what A claimed: %q", setB.DSN("app", "5", Direct))
	}
}
