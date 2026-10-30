package main

// Block 5 (moving an organization): a second organization moved into the database of the first
// must not take the first one's tables (risk hunt R38: customerSchema dropped any table marked
// "lokadb:", whichever organization's, and the first organization's foreign partition in the
// cloud then read the second one's rows).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV41", name: "a second organization moved into the first one's database is refused before anything changes: the first one's tables are not dropped, its rows stay where they were, in the database and through the cloud", run: checkSecondOrgSameDatabase},
	)
}

func checkSecondOrgSameDatabase(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if out, err := e.move(1); err != nil {
		return false, "moving organization 1 failed: " + lastErr(out), nil
	}
	count := func(d string, org int) string {
		n, err := one(e.ctx, d, fmt.Sprintf("SELECT count(*) FROM documents WHERE organization_id = %d", org))
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return n
	}
	inCloud2 := count(e.cloud(), 2)
	first, firstCloud := count(e.customer(), 1), count(e.cloud(), 1)
	out, err := e.move(2)
	afterDB, afterCloud, afterCloud2 := count(e.customer(), 1), count(e.cloud(), 1), count(e.cloud(), 2)
	refused := err != nil && strings.Contains(out, "already holds another organization") && strings.Contains(out, "organization 1") && strings.Contains(out, "nothing was changed")
	part2, _ := one(e.ctx, e.cloud(), "SELECT (to_regclass('documents__o2') IS NOT NULL)::text")
	// refused before anything is changed: organization 2 has no partition of its own and its rows are where they were
	ok := first != "0" && afterDB == first && afterCloud == firstCloud && firstCloud == first && refused && part2 == "false" && afterCloud2 == inCloud2
	return ok, fmt.Sprintf("organization 1 before: %s rows at its database, %s through the cloud; second move-org: %s; partition of organization 2 made: %s (want false); organization 1 after: %s at its database, %s through the cloud (want the same); organization 2 in the cloud: %s (was %s)",
		first, firstCloud, errOr(err, true)+" "+lastLine(out), part2, afterDB, afterCloud, afterCloud2, inCloud2), nil
}
