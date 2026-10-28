package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
)

// The relay finishes an expect while the session closes the queue with it still in the queue
// (the client went away mid-statement): finish and close both reported the outcome, writing the
// expect's onOK and notify (-race: DATA RACE in (*queue).close against (*expect).finish). The
// outcome is now reported once, by whoever comes first: onOK runs at most once, and not at all
// when the queue closed first.
func TestQueueCloseWhileFinishing(t *testing.T) {
	for i := 0; i < 300; i++ {
		q := newQueue()
		var ran atomic.Int32
		got := make(chan bool, 1)
		e := &expect{kind: kExec, notify: got, onOK: func() { ran.Add(1) }}
		q.push([]*expect{e})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); e.finish(true); q.pop() }()
		go func() { defer wg.Done(); q.close() }()
		wg.Wait()
		if n := ran.Load(); n > 1 {
			t.Fatalf("onOK ran %d times", n)
		}
		select {
		case <-got:
		default:
			t.Fatal("the waiter got no outcome")
		}
	}
}
