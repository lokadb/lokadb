package main

import "testing"

// Review 3 of lokadb check (tasks/review3/check.md), C9: a password in a URL's query
// (postgres://app@db/x?password=..., which pgconn takes) is on the command line without the
// warning secretFlags gives for user:password@ and password=.
func TestReview3PasswordInURLQuery(t *testing.T) {
	for _, dsn := range []string{
		"postgres://app@db:5432/app?password=s3cret",
		"postgres://app@db:5432/app?sslmode=require&password=s3cret",
	} {
		if !passwordInDSN.MatchString(dsn) {
			t.Errorf("%s: no warning for the password on the command line", dsn)
		}
	}
}
