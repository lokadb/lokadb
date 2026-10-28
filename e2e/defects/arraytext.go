package main

// The second review of the routing (tasks/review/routing.md), R8: a text array whose element
// holds a comma or a quote.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "RA8", name: "a text array with a comma or an escaped quote in an element (a literal, a text parameter) finds the organization of those keys, not of the pieces", run: checkArrayTextElements},
	)
}

// RA8. parseArrayText cut {"p,q"} at the comma and kept the backslash of {"a\"b"}: the proxy
// looked up the keys p and q (organization 2's) and a\"b, and ran the statement in the wrong
// organization, which saw nothing. The keys are organization 1's.
func checkArrayTextElements(e *env) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO utags (code, organization_id, label) VALUES ('p,q', 1, 'comma key'), ('p', 2, 'p of 2'), ('q', 2, 'q of 2'), ('a\"b', 1, 'quote key'), ('a', 2, 'a of 2')"); err != nil {
		return false, "", err
	}
	e.spec = uuidSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x := &expects{ok: true}
	rows := func(res []*pgconn.Result, err error) string {
		switch {
		case err != nil:
			return "error: " + firstErrLine(err)
		case len(res) > 0 && len(res[0].Rows) > 0:
			var out []string
			for _, r := range res[0].Rows {
				out = append(out, string(r[0]))
			}
			return strings.Join(out, ",")
		}
		return "no rows"
	}
	// literals, by the simple query protocol
	x.eq("literal {\"p,q\"}", rows(simple(e.ctx, c, `SELECT label FROM utags WHERE code = ANY('{"p,q"}'::varchar[])`)), "comma key")
	x.eq(`literal {"a\"b"}`, rows(simple(e.ctx, c, `SELECT label FROM utags WHERE code = ANY('{"a\"b"}'::varchar[])`)), "quote key")
	x.eq(`literal {"p,q",tag-2}: both organization 1's`, rows(simple(e.ctx, c, `SELECT label FROM utags WHERE code = ANY('{"p,q",tag-2}'::varchar[]) ORDER BY 1`)), "comma key,label 2")
	// a text parameter (format 0), as psycopg sends an array
	param := func(v string) string {
		res := c.ExecParams(e.ctx, "SELECT label FROM utags WHERE code = ANY($1)", [][]byte{[]byte(v)}, []uint32{1015}, []int16{0}, nil).Read()
		if res.Err != nil {
			return "error: " + firstErrLine(res.Err)
		}
		var out []string
		for _, r := range res.Rows {
			out = append(out, string(r[0]))
		}
		if len(out) == 0 {
			return "no rows"
		}
		return strings.Join(out, ",")
	}
	x.eq("text parameter {\"p,q\"}", param(`{"p,q"}`), "comma key")
	x.eq(`text parameter {"a\"b"}`, param(`{"a\"b"}`), "quote key")
	x.eq("text parameter of the pieces {p,q}: organization 2's", param(`{p,q}`), "p of 2,q of 2")
	if !x.ok {
		x.notes = append(x.notes, fmt.Sprint("proxy: ", e.proxyLines("msg=route")))
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}
