package admin

import (
	"strings"
	"testing"
)

// R3-12 of review 3 of admin: move-org logs the queries that block its switch without their
// literals (the application's data).
func TestRedactSQL(t *testing.T) {
	got := redactSQL("UPDATE accounts SET email = 'anna@example.com' WHERE id = 42", 100)
	if strings.Contains(got, "anna@example.com") || strings.Contains(got, "42") || !strings.Contains(got, "UPDATE accounts") {
		t.Errorf("redactSQL: %q", got)
	}
	if got := redactSQL("selec 'secret' frm x", 100); strings.Contains(got, "secret") {
		t.Errorf("a query that does not parse: %q", got)
	}
}
