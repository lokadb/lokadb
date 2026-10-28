package admin

import (
	"strings"
	"testing"
)

// The organization's DSN's TLS settings reach its FDW server (review 3, R3-5).
func TestFDWSSLOptions(t *testing.T) {
	got, err := FDWSSLOptions(
		`host=relay port=15001 dbname=d user=app password='a b' sslmode=verify-full sslrootcert='/etc/lokadb/org 7.crt'`,
		`host=relay dbname=d user=postgres sslmode=require sslcrl=/etc/crl.pem`)
	if err != nil {
		t.Fatal(err)
	}
	want := "sslmode 'verify-full', sslrootcert '/etc/lokadb/org 7.crt', sslcrl '/etc/crl.pem'"
	if strings.Join(got, ", ") != want {
		t.Errorf("got %q, want %q", strings.Join(got, ", "), want)
	}
	got, err = FDWSSLOptions("postgres://app:pw@relay:15001/d?sslmode=require&connect_timeout=5")
	if err != nil || strings.Join(got, ", ") != "sslmode 'require'" {
		t.Errorf("URL: %q, %v", got, err)
	}
	if got, err = FDWSSLOptions("host=h dbname=d"); err != nil || len(got) != 0 {
		t.Errorf("no TLS settings: %q, %v", got, err)
	}
	if _, err = FDWSSLOptions("host=h password='open"); err == nil {
		t.Error("an unterminated quote: no error")
	}
}
