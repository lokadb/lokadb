package main

import (
	"flag"
	"testing"
)

// DSN flags take env:NAME, and a password written in a DSN is recognized (to warn of it).
func TestSecretFlags(t *testing.T) {
	t.Setenv("LOKADB_TEST_DSN", "host=c user=app password=s3cret")
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	app := fs.String("customer-app", "", "")
	other := fs.String("note", "", "")
	if err := fs.Parse([]string{"-customer-app", "env:LOKADB_TEST_DSN", "-note", "env:LOKADB_TEST_DSN"}); err != nil {
		t.Fatal(err)
	}
	secretFlags(fs)
	if *app != "host=c user=app password=s3cret" || *other != "env:LOKADB_TEST_DSN" {
		t.Errorf("-customer-app %q, -note %q: want the variable's value for the DSN flag only", *app, *other)
	}
	for dsn, has := range map[string]bool{
		"host=c user=app password=x":       true,
		"host=c password = x":              true,
		"postgres://app:x@c/db":            true,
		"postgresql://app:x@c:5432/db":     true,
		"host=c user=app":                  false,
		"postgres://app@c/db":              false,
		"host=c user=app dbname=passwords": false,
	} {
		if passwordInDSN.MatchString(dsn) != has {
			t.Errorf("%q: password found %v, want %v", dsn, !has, has)
		}
	}
}

// R3-11 of review 3 of admin: -control-plane (org-profile, org-credentials) is a DSN flag too.
func TestSecretFlagsControlPlane(t *testing.T) {
	t.Setenv("LOKADB_TEST_CP", "host=cp user=reg password=x")
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	cp := fs.String("control-plane", "", "")
	if err := fs.Parse([]string{"-control-plane", "env:LOKADB_TEST_CP"}); err != nil {
		t.Fatal(err)
	}
	secretFlags(fs)
	if *cp != "host=cp user=reg password=x" {
		t.Errorf("-control-plane %q: want the variable's value", *cp)
	}
}
