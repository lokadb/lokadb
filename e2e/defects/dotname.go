package main

// Block 4, item 4: a table of schema public whose name has a dot ("a.b"). Its key is
// public.a.b (package rel); a bare name "a.b" was read as schema a, table b.

import (
	"encoding/json"
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X20", name: `a table of public named "acct.docs" (a dot in its name): check names it schema public, table acct.docs; enable, a move and the proxy route it`, run: checkDotName},
	)
}

const dotSpec = `{"tables": [{"name": "public.acct.docs", "tenant_column": "org"}]}`

// X20.
func checkDotName(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE "acct.docs" (id bigserial PRIMARY KEY, org bigint NOT NULL, title text, code int)`,
		`ALTER TABLE "acct.docs" OWNER TO app`,
		`CREATE FUNCTION x20_touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.title := coalesce(NEW.title, ''); RETURN NEW; END $$`,
		`CREATE TRIGGER x20_touch BEFORE INSERT ON "acct.docs" FOR EACH ROW EXECUTE FUNCTION x20_touch()`,
		`INSERT INTO "acct.docs" (org, title) SELECT 1 + g % 2, 'd' || g FROM generate_series(1, 40) g`); err != nil {
		return false, "", err
	}
	e.spec = dotSpec
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	// the report names the table by schema and name (subject), as any other
	js, code := e.checkWith(dotSpec, "-json")
	var rep struct {
		Tables []struct {
			Name   string `json:"name"`
			Schema string `json:"schema"`
		} `json:"tables"`
		Findings []struct {
			Code    string                                `json:"code"`
			Subject *struct{ Kind, Schema, Table string } `json:"subject"`
		} `json:"findings"`
	}
	i := max(strings.Index(js, "{"), 0)
	if err := json.NewDecoder(strings.NewReader(js[i:])).Decode(&rep); err != nil {
		return false, "check -json: " + err.Error(), nil
	}
	var subj []string
	for _, f := range rep.Findings {
		if f.Subject != nil && f.Subject.Kind == "table" {
			subj = append(subj, f.Code+"="+f.Subject.Schema+"|"+f.Subject.Table)
		}
	}
	table := "-"
	if len(rep.Tables) == 1 {
		table = rep.Tables[0].Schema + "|" + rep.Tables[0].Name
	}
	wrong := false
	for _, s := range subj {
		wrong = wrong || !strings.HasSuffix(s, "=public|acct.docs")
	}
	note(code != 3 && code != 5 && table == "public|public.acct.docs" && len(subj) > 0 && !wrong,
		fmt.Sprintf("check (exit %d): table %s; table subjects %v", code, table, subj))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if mv, err := e.move(1); err != nil {
		note(false, "move-org failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	at := val(e.customer(), `SELECT count(*) FROM "acct.docs"`)
	note(at == "20", "after the move: organization 1's rows there "+at)
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
	run := func(sql string) string {
		res, err := simple(e.ctx, c, sql)
		if err != nil {
			if msg := firstErrLine(err); strings.Contains(msg, "lokadb") {
				return "refused"
			} else {
				return "error: " + msg
			}
		}
		if len(res) > 0 && len(res[0].Rows) > 0 {
			return string(res[0].Rows[0][0])
		}
		return ""
	}
	one1 := run(`SELECT count(*) FROM "acct.docs" WHERE org = 1`)
	all := run(`SELECT count(*) FROM "acct.docs"`)
	qual := run(`SELECT count(*) FROM public."acct.docs" WHERE org = 2`)
	upd := run(`UPDATE "acct.docs" SET code = 7 WHERE org = 1 AND id = 2`)
	note(one1 == "20" && all == "refused" && qual == "20" && !strings.HasPrefix(upd, "error") && upd != "refused",
		fmt.Sprintf(`through the proxy (reject): organization 1's rows %s, all rows %s, public."acct.docs" of 2 %s, an UPDATE of 1 %q`, one1, all, qual, upd))
	return ok, strings.Join(notes, "; "), nil
}
