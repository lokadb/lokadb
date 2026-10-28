package main

// Block 2, item 7 (tasks/QUESTIONS.md): lokadb.same_org(new, old) lets an UPDATE write the
// organization a row already has (Django save(), Rails) and refuses a real change with a
// clear error. The proxy's rewritten UPDATE calls it (block 2).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "SO1", name: "lokadb.same_org passes the same organization and refuses another one", run: checkSameOrg},
		check{id: "SO2", name: "through the proxy, an UPDATE that writes a row's own organization goes through and one that changes it gets the lokadb error (block 2, item 7)", run: checkSameOrgProxy},
	)
}

func checkSameOrg(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	same, err := one(e.ctx, e.cloud(), "UPDATE documents SET organization_id = lokadb.same_org(1, organization_id) WHERE organization_id = 1 RETURNING 'updated'")
	if err != nil {
		return false, "writing the same organization: " + firstErrLine(err), nil
	}
	_, err = one(e.ctx, e.cloud(), "UPDATE documents SET organization_id = lokadb.same_org(2, organization_id) WHERE organization_id = 1 RETURNING 'moved'")
	if err == nil || !strings.Contains(err.Error(), "moving a row to another organization is not supported") {
		got := "accepted"
		if err != nil {
			got = firstErrLine(err)
		}
		return false, "same organization: " + same + "; another organization: " + got, nil
	}
	return true, "same organization: " + same + "; another organization: refused (" + firstErrLine(err) + ")", nil
}

// SO2 (block 2, item 7). Django's save() and Rails write the organization column on every
// UPDATE: the RLS policy refused that for the cloud and the organization's CHECK for a moved
// one only when the value really changes, and then without saying why. The proxy wraps the
// new value in lokadb.same_org. Both kinds of organization, both protocols.
func checkSameOrgProxy(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	const refusal = "moving a row to another organization is not supported"
	ok := true
	var notes []string
	for _, o := range []struct {
		org, other, db, where string
	}{
		{"1", "2", e.customer(), "moved"},
		{"2", "3", e.cloud(), "cloud"},
	} {
		id, err := one(e.ctx, o.db, "SELECT min(id) FROM documents WHERE organization_id = "+o.org)
		if err != nil {
			return false, "", err
		}
		for _, p := range []struct {
			name string
			run  func(sql string, args ...string) error
		}{
			{"simple", func(sql string, args ...string) error {
				for i, a := range args {
					sql = strings.Replace(sql, fmt.Sprintf("$%d", i+1), "'"+a+"'", 1)
				}
				_, err := simple(e.ctx, c, sql)
				return err
			}},
			{"bound", func(sql string, args ...string) error {
				ps := make([][]byte, len(args))
				for i, a := range args {
					ps[i] = []byte(a)
				}
				return c.ExecParams(e.ctx, sql, ps, nil, nil, nil).Read().Err
			}},
		} {
			title := "so2-" + o.where + "-" + p.name
			errSame := p.run("UPDATE documents SET organization_id = $1, title = $2 WHERE id = $3", o.org, title, id)
			same, _ := one(e.ctx, o.db, "SELECT count(*) FROM documents WHERE id = "+id+" AND title = '"+title+"' AND organization_id = "+o.org)
			errOther := p.run("UPDATE documents SET organization_id = $1 WHERE id = $2", o.other, id)
			still, _ := one(e.ctx, o.db, "SELECT count(*) FROM documents WHERE id = "+id+" AND organization_id = "+o.org)
			if errSame != nil || same != "1" || errOther == nil || !strings.Contains(errOther.Error(), refusal) || still != "1" {
				ok = false
			}
			notes = append(notes, fmt.Sprintf("%s organization %s, %s: its own organization %s (row updated %s); organization %s %s",
				o.where, o.org, p.name, short(errSame), same, o.other, short(errOther)))
		}
	}
	return ok, strings.Join(notes, "; "), nil
}
