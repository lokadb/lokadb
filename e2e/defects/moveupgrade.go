package main

// Block 5 (moving an organization): a database enabled by an older version gets the current
// functions of LokaDB when move-org runs, so that the split's DELETE does not run the old triggers
// (refs_sync and key_sync that call lokadb.moving() for every row cost 15 s of 27 for 600 000 rows).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV43", name: "a database enabled by an older version: move-org replaces the functions of the refs and key triggers with the current ones", run: checkFunctionsUpgraded},
	)
}

func checkFunctionsUpgraded(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// The older bodies: the same functions reading another setting (an older version differs in
	// the text of the same check; this one is told apart by the name).
	for _, fn := range []string{"refs_sync", "key_sync"} {
		def, err := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT pg_get_functiondef('lokadb.%s'::regproc)", fn))
		if err != nil {
			return false, "", err
		}
		if err := execSQL(e.ctx, e.cloud(), strings.ReplaceAll(def, "lokadb.moving", "lokadb.moving_stale")); err != nil {
			return false, "", err
		}
	}
	stale := func() string {
		n, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM pg_proc WHERE pronamespace = 'lokadb'::regnamespace
			AND proname IN ('refs_sync', 'key_sync') AND prosrc LIKE '%moving_stale%'`)
		return n
	}
	before := stale()
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	after := stale()
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	return before == "2" && after == "0" && n != "0", fmt.Sprintf("functions with the older body before move-org: %s (want 2), after: %s (want 0); organization 1 moved: %s rows", before, after, n), nil
}
