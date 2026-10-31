package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// CT1: a ctid is unique only inside one table, and after enable a LokaDB table has a partition for
// each organization (a moved one lives in another database). Laravel's MassPrunable and every
// ->limit()->delete() send
//
//	delete from "documents" where "ctid" in (select "documents"."ctid" from "documents" where … limit n)
//
// which, passed on as it is, deletes the rows of every partition that have the ctid of a row the
// subquery picked: other organizations' rows, lost. The proxy rewrites that shape to the key and
// refuses every other use of ctid, whatever the fanout policy.
func init() {
	checks = append(checks,
		check{id: "CT1", name: "ctid on a LokaDB table: the limit-delete of Laravel's MassPrunable deletes only what it picked, other uses of ctid are refused, under every fanout policy", run: checkCtid},
	)
}

func checkCtid(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	// organization 1 lives in its own database, organization 2 in the cloud's partition; both tables were
	// filled alike, so the rows of the two sit at the same ctids
	count := func(dsn, where string) int {
		n, _ := one(e.ctx, dsn, "SELECT count(*) FROM documents WHERE "+where)
		var v int
		fmt.Sscan(n, &v)
		return v
	}
	org1, org2 := count(e.customer(), "organization_id = 1"), count(e.cloud(), "organization_id = 2")
	if org1 < 100 || org2 < 100 {
		return false, fmt.Sprintf("too few documents to prune: organization 1 %d, organization 2 %d", org1, org2), nil
	}
	for _, mode := range []string{"log", "allow", "reject"} {
		stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": mode}}})
		if err != nil {
			return false, "", err
		}
		c, err := e.viaProxy()
		if err != nil {
			stop()
			return false, "", err
		}
		// what the proxy answers: the message when it refused
		run := func(sql string, args ...string) (refusal string) {
			var params [][]byte
			for _, a := range args {
				params = append(params, []byte(a))
			}
			res := c.ExecParams(e.ctx, sql, params, nil, nil, nil).Read()
			if res.Err != nil {
				var pe *pgconn.PgError
				if errors.As(res.Err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
					return pe.Message
				}
				return "error: " + firstErrLine(res.Err)
			}
			return ""
		}
		// other uses of ctid: refused with the reason, never run
		for _, sql := range []string{
			`DELETE FROM documents WHERE ctid = '(0,1)'`,
			`DELETE FROM documents WHERE organization_id = 2 AND ctid = '(0,1)'`,
			`SELECT ctid, id FROM documents WHERE organization_id = 2`,
			`UPDATE documents SET title = title WHERE organization_id = 2 AND id < 0 RETURNING ctid`,
		} {
			why := run(sql)
			note(strings.Contains(why, "ctid") && strings.Contains(why, "primary key"), mode+": "+sql+" -> "+short80(why))
		}
		// the prune (what Laravel sends): 20 rows of organization 1 are picked by their ctid
		why := run(`DELETE FROM "documents" WHERE "ctid" IN (SELECT "documents"."ctid" FROM "documents" WHERE "organization_id" = $1 LIMIT $2)`, "1", "20")
		now1, now2 := count(e.customer(), "organization_id = 1"), count(e.cloud(), "organization_id = 2")
		// rewritten to the key, the outer DELETE finds its organization through the subquery that names it, so
		// even "reject" lets it run, in organization 1's database only
		note(why == "" && now1 == org1-20, fmt.Sprintf("%s: the 20 picked rows of organization 1 are gone (%d of %d left): %s", mode, now1, org1, short80(why)))
		note(now2 == org2, fmt.Sprintf("%s: organization 2 keeps its %d documents (now %d)", mode, org2, now2))
		org1 = now1
		// the same with the organization named by both: it goes to organization 1 only
		why = run(`DELETE FROM documents WHERE organization_id = 1 AND ctid IN (SELECT ctid FROM documents WHERE organization_id = 1 LIMIT 5)`)
		now1, now2 = count(e.customer(), "organization_id = 1"), count(e.cloud(), "organization_id = 2")
		note(why == "" && now1 == org1-5 && now2 == org2, fmt.Sprintf("%s: with the organization named: organization 1 %d (was %d), organization 2 %d (was %d): %s", mode, now1, org1, now2, org2, short80(why)))
		org1 = now1
		c.Close(e.ctx)
		stop()
	}
	return ok, strings.Join(notes, "; "), nil
}

func short80(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 80 {
		return s[:80] + "…"
	}
	return s
}
