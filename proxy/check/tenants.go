package check

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// Organizations with a uuid number (block 18, tasks/uuid-tenant.md).

// reservedTenant: lokadb keeps values of its own in the organization column's range: the nil
// uuid for rows of no organization in a table numbered by uuid, and for integers -2 (no
// organization) and -1 (nothing found), so that the proxy takes any negative number for
// "no organization". Rows that hold such a value would be taken for rows without one, and
// their organization could never move. A blocker while there are any.
func (c *checker) reservedTenant(t, tc, kind string, mark func(string)) error {
	var cond, why string
	switch kind {
	case admin.TenantUUID:
		cond = fmt.Sprintf("c.%s = '%s'::uuid", q(tc), admin.NilUUID)
		why = fmt.Sprintf("have %s = %s: lokadb keeps rows of no organization under the nil uuid", tc, admin.NilUUID)
	case admin.TenantInt:
		cond = fmt.Sprintf("c.%s < 0", q(tc))
		why = fmt.Sprintf("have a negative %s: lokadb keeps -2 for rows of no organization and -1 for \"no organization found\", and takes any negative number for no organization", tc)
	default:
		return nil
	}
	m, err := c.present(qk(t), "c", cond) // a blocker: a sample that finds none proves nothing (review 2, R8)
	if err != nil {
		return err
	}
	if m.Rows > 0 {
		mark(StateBlocked)
		c.addFinding(Finding{Severity: Blocker, Code: "tenant_value_reserved", Object: t + "." + tc, Subject: subjectOf("tenant_value_reserved", t+"."+tc), Measure: m,
			Message: fmt.Sprintf("%s of %s %s, so these rows would be taken for rows without an organization, and no organization could take them along. Give them a real organization (or NULL, if they have none) before enable",
				counted(m, "row", "rows"), t, why)})
	}
	return nil
}

// mixedTenants: the LokaDB tables hold the numbers of the same organizations, so their
// organization columns are of one kind (integers or uuids); a spec that mixes them is wrong.
func (c *checker) mixedTenants(kinds map[string]string) {
	by := map[string][]string{}
	for t, k := range kinds {
		if k != "" {
			by[k] = append(by[k], t)
		}
	}
	if len(by) < 2 {
		return
	}
	var parts, examples []string
	for k, ts := range by {
		sort.Strings(ts)
		parts = append(parts, fmt.Sprintf("%s: %s", k, strings.Join(ts, ", ")))
		examples = append(examples, ts...)
	}
	sort.Strings(parts)
	sort.Strings(examples)
	c.add(SpecError, "tenant_kind_mixed", "", fmt.Sprintf("the organization columns of the spec's tables are of different kinds (%s): they hold the numbers of the same organizations, so they must be all integers or all uuids", strings.Join(parts, "; ")), examples)
}
