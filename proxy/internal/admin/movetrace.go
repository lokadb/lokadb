package admin

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// The move's trace (RB3 failed twice under load with one row behind, and the log entries are
// deleted once applied): with LOKADB_MOVE_TRACE naming a file, every log entry applyLog applies
// is written there, in the order applied, with the round it came in. What a failed
// verification is examined by; off otherwise.

var (
	traceMu   sync.Mutex
	traceFile *os.File
	traceOnce sync.Once
)

func moveTraceFile() *os.File {
	traceOnce.Do(func() {
		if p := os.Getenv("LOKADB_MOVE_TRACE"); p != "" {
			traceFile, _ = os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		}
	})
	return traceFile
}

// traceApplied writes the entries of one applyLog round: phase (catch-up, or the switch's
// drain), the cursor it read after (last, how many open transactions), then one line per entry:
// seq, table, op, the old key, the new key, fresh or late (an open transaction's).
func traceApplied(phase string, last int64, snap string, entries []logEntry, tables []moveTable) {
	f := moveTraceFile()
	if f == nil || len(entries) == 0 {
		return
	}
	byName := map[string]moveTable{}
	for _, t := range tables {
		byName[t.name] = t
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	fmt.Fprintf(f, "%s round %s after seq %d, snapshot %s, %d entries\n", time.Now().UTC().Format("15:04:05.000"), phase, last, snap, len(entries))
	for _, e := range entries {
		old, nw := "-", "-"
		if e.oldID != nil {
			old = *e.oldID
		}
		if e.newID != nil {
			nw = *e.newID
		} else if e.row != nil {
			var m map[string]any
			if json.Unmarshal([]byte(*e.row), &m) == nil {
				nw = fmt.Sprint(m[keyOf(byName[e.tbl])])
			}
		}
		kind := "late"
		if e.fresh {
			kind = "fresh"
		}
		fmt.Fprintf(f, "  %d %s %s old=%s new=%s %s\n", e.seq, e.tbl, e.op, old, nw, kind)
	}
}
