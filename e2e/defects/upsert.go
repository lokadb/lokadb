package main

// The rewritten upsert (research/ANALYSIS.md 5.4: P11, P12), for an organization in its own
// database (1) and one in the cloud (2).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "P11", name: "ON CONFLICT DO UPDATE … WHERE false leaves the row alone instead of inserting it again", run: checkUpsertWhere},
		check{id: "P12", name: "an upsert of a value too long for varchar(n) fails instead of cutting it", run: checkUpsertTooLong},
	)
}

func checkUpsertWhere(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, org := range []string{"1", "2"} {
		id, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = "+org)
		if err != nil {
			return false, "", err
		}
		before, _ := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE organization_id = "+org+" AND id = "+id)
		_, errFalse := simple(e.ctx, c, "INSERT INTO documents (id, organization_id, title) VALUES ("+id+", "+org+", 'p11') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title WHERE documents.title = 'no such title'")
		afterFalse, _ := one(e.ctx, e.cloud(), "SELECT title || ' x' || count(*) OVER () FROM documents WHERE organization_id = "+org+" AND id = "+id)
		_, errTrue := simple(e.ctx, c, "INSERT INTO documents (id, organization_id, title) VALUES ("+id+", "+org+", 'p11') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title WHERE documents.title <> 'no such title'")
		afterTrue, _ := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE organization_id = "+org+" AND id = "+id)
		if errFalse != nil || afterFalse != before+" x1" || errTrue != nil || afterTrue != "p11" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("organization %s: WHERE false → %s (error %v), WHERE true → %s (error %v)", org, afterFalse, short(errFalse), afterTrue, short(errTrue)))
	}
	return ok, strings.Join(notes, "; "), nil
}

func short(err error) string {
	if err == nil {
		return "none"
	}
	s := oneLine(err.Error())
	if len(s) > 90 {
		s = s[:90] + "…"
	}
	return s
}

func checkUpsertTooLong(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := e.script("ALTER TABLE documents ADD COLUMN code varchar(5)"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, org := range []string{"1", "2"} {
		id, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = "+org)
		if err != nil {
			return false, "", err
		}
		_, upErr := simple(e.ctx, c, "INSERT INTO documents (id, organization_id, title, code) VALUES ("+id+", "+org+", 't', 'much-too-long') ON CONFLICT (id) DO UPDATE SET code = EXCLUDED.code")
		code, _ := one(e.ctx, e.cloud(), "SELECT coalesce(code, 'NULL') FROM documents WHERE organization_id = "+org+" AND id = "+id)
		if upErr == nil || !strings.Contains(upErr.Error(), "too long") || code != "NULL" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("organization %s: error %s, stored %s", org, short(upErr), code))
	}
	return ok, strings.Join(notes, "; "), nil
}
