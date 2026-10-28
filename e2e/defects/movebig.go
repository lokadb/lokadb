package main

// Block 5 (moving an organization), A5: a large value written while the trigger mode
// captures changes.

import (
	"fmt"
	"strings"
	"sync"
)

func init() {
	checks = append(checks,
		check{id: "MV10", name: "trigger mode: a 40 MB bytea written during the move does not fail the application and arrives", run: checkBigRow},
	)
}

// MV10. The capture logged every change as to_jsonb(NEW): a bytea of more than about 134 MB
// (twice that as hex) passed jsonb's limit, and the application's UPDATE failed because of
// the move. Now a row whose large values pass 32 MB is logged without its image and read from
// the cloud when applied. 40 MB takes that path (140 MB reproduced the old failure, but cost
// about 1 GB per backend, and the shared test server killed the stand for memory).
func checkBigRow(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), "ALTER TABLE documents ADD COLUMN blob bytea"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	id, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	var once sync.Once
	var werr error
	// the application's write of 40 MB holds the partition for long on a loaded machine: the
	// switch yields to it (each try under deadlock_timeout) and may wait up to -lock-timeout
	out, _, merr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "step 5/6") {
			once.Do(func() {
				// 40 MB, written by the application: past bigRow (32 MB), logged without its image
				_, werr = one(e.ctx, e.cloud(), `WITH u AS (UPDATE documents SET blob = decode(repeat('ab', 40 * 1024 * 1024), 'hex')
					WHERE id = `+id+` RETURNING 1) SELECT count(*) FROM u`)
			})
		}
	}, "-mode", "trigger", "-lock-timeout", "60s")
	if werr != nil {
		return false, "the application's write during the move failed: " + firstErrLine(werr), nil
	}
	if merr != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	want, err := one(e.ctx, e.cloud(), "SELECT md5(decode(repeat('ab', 40 * 1024 * 1024), 'hex'))")
	if err != nil {
		return false, "", err
	}
	got, err := one(e.ctx, e.customer(), "SELECT coalesce(md5(blob), 'NULL') || ' ' || coalesce(length(blob), 0) FROM documents WHERE id = "+id)
	if err != nil {
		return false, "", err
	}
	ok := strings.HasPrefix(got, want+" ")
	return ok, fmt.Sprintf("the application wrote a 40 MB bytea during the catch-up; at the organization: md5 and length %s (want md5 %s)", got, want), nil
}
