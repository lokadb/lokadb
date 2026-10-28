package admin

import (
	"strings"
	"testing"
)

// mask hides a password whether it is bare, quoted (with spaces) or quoted with an escaped quote.
func TestMaskQuotedPassword(t *testing.T) {
	for _, in := range []string{
		`host=h user=u password=s3cret dbname=d`,
		`host=h password='a b c' dbname=d`,
		`host=h password = 'it\'s a secret' dbname=d`,
		`CREATE SUBSCRIPTION s CONNECTION 'host=h password=''two words'' dbname=d' PUBLICATION p`,
		`CREATE ROLE r PASSWORD 'x y'`,
	} {
		out := mask(in)
		for _, leak := range []string{"s3cret", "a b c", "secret", "two words", "x y", "b c", "words"} {
			if strings.Contains(out, leak) {
				t.Errorf("mask(%q) = %q: %q left", in, out, leak)
			}
		}
		if !strings.Contains(out, "***") {
			t.Errorf("mask(%q) = %q: nothing masked", in, out)
		}
	}
}
