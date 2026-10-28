package ext

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestInstallAndHint(t *testing.T) {
	if got := Install("vector", "lokadb"); got != `CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "lokadb"` {
		t.Errorf("Install: %q", got)
	}
	if got := Install("uuid-ossp", ""); got != `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"` {
		t.Errorf("Install: %q", got)
	}
	cases := []struct {
		code, op string
		want     []string
	}{
		{"42501", "create", []string{"cannot be installed", `CREATE EXTENSION IF NOT EXISTS "postgis" WITH SCHEMA "public"`, "organization 7"}},
		{"58P01", "create", []string{"is not available", "package", `CREATE EXTENSION IF NOT EXISTS "postgis"`}},
		{"22023", "alter", []string{"no such version to update to", `ALTER EXTENSION "postgis" UPDATE`}},
		{"2BP01", "drop", []string{"still used", "the cloud did not drop it either"}},
	}
	for _, c := range cases {
		err := fmt.Errorf("organization 7: %w", &pgconn.PgError{Code: c.code, Message: "msg", Detail: "detail"})
		got := Hint(err, c.op, "postgis", "public", "7")
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s %s: %q lacks %q", c.code, c.op, got, w)
			}
		}
	}
	if Hint(errors.New("boom"), "create", "x", "", "1") != "" || Hint(&pgconn.PgError{Code: "40001"}, "create", "x", "", "1") != "" {
		t.Error("an error no administrator fixes got a hint")
	}
	if Hint(&pgconn.PgError{Code: "58P01"}, "drop", "x", "", "1") != "" {
		t.Error("DROP of an extension that is not there is no error for an administrator")
	}
	if !CloudOnly("postgres_fdw") || !CloudOnly("PG_STAT_STATEMENTS") || CloudOnly("vector") {
		t.Error("CloudOnly")
	}
}
