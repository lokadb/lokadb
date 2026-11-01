package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Third review R4-26: the policy log is reject once an organization has moved out of the cloud,
// as its log line promises; read once at the start, it ran a statement without a customer in
// every organization's database, the moved one's too, until someone restarted the proxy.
func TestR4LogAfterFirstMove(t *testing.T) {
	const write = "UPDATE documents SET title = 'x' WHERE slug = 'a'" // names no organization
	const mystery = "SELECT reports.mystery()"                        // the analyzer sees no LokaDB table
	for _, moved := range []bool{false, true} {
		m := testMeta()
		if moved {
			m.Foreign = map[string]map[string]string{"documents": {"1": "documents__org1"}}
		}
		f := &fakeUpstream{}
		fe, _, s := protoSessionS(t, f, m)
		pol, err := Config{Fanout: map[string]FanoutPolicy{"db": {Mode: FanoutLog}}}.FanoutFor("db")
		if err != nil {
			t.Fatal(err)
		}
		s.fanout = pol
		for _, q := range []string{write, mystery} {
			sendAll(t, fe, &pgproto3.Query{String: q})
			errorCode(t, fe)
		}
		log := strings.Join(f.log(), "\n")
		ranWrite := strings.Contains(log, write)
		org, _ := orgsBefore(f.log(), mystery)
		if moved && (ranWrite || org != "SELECT set_config('lokadb.org', '-1', false)") {
			t.Errorf("an organization moved, policy log: the write ran %v, %s ran after %q (want refused and -1); upstream ran:\n%s", ranWrite, mystery, org, log)
		}
		if !moved && (!ranWrite || org != "SELECT set_config('lokadb.org', '', false)") {
			t.Errorf("nothing moved, policy log: the write ran %v, %s ran after %q (want run and ''); upstream ran:\n%s", ranWrite, mystery, org, log)
		}
	}
}
