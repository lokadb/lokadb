package admin

import (
	"strings"
	"testing"
	"time"
)

// The catch-up ends on a short lag, or on a steady one (delivery on a loaded machine), not on
// a backlog that still falls or grows.
func TestCaughtUp(t *testing.T) {
	ms := func(v ...int) []time.Duration {
		var d []time.Duration
		for _, x := range v {
			d = append(d, time.Duration(x)*time.Millisecond)
		}
		return d
	}
	for _, c := range []struct {
		lags []time.Duration
		want bool
		hint string
	}{
		{ms(120), true, ""},
		{ms(9000, 400), true, ""},
		{ms(1100, 1200), false, "lag 1.2s"},                           // too few rounds to tell
		{ms(9000, 1150, 1100, 1250), true, ""},                        // steady at delivery's 1.1-1.25 s
		{ms(9000, 6000, 3000), false, "still falls"},                  // a backlog applied
		{ms(1000, 2000, 4000), false, "grows"},                        // writes outpace replication
		{ms(6000, 6100, 6050), false, "more than half of -max-pause"}, // steady, too long for the switch
	} {
		ok, why := caughtUp(c.lags, 10*time.Second)
		if ok != c.want {
			t.Errorf("%v: caught up %v (%s), want %v", c.lags, ok, why, c.want)
		}
		if !ok {
			if h := catchUpHint(c.lags, 10*time.Second); !strings.Contains(h, c.hint) {
				t.Errorf("%v: hint %q, want %q in it", c.lags, h, c.hint)
			}
		}
	}
}

// A try of the switch's lock waits less than deadlock_timeout.
func TestSwitchLockTry(t *testing.T) {
	for _, c := range []struct{ lock, deadlock, want time.Duration }{
		{5 * time.Second, time.Second, 250 * time.Millisecond},
		{100 * time.Millisecond, time.Second, 100 * time.Millisecond},
		{5 * time.Second, 10 * time.Millisecond, 10 * time.Millisecond},
		{0, 2 * time.Second, 500 * time.Millisecond},
	} {
		if got := switchLockTry(c.lock, c.deadlock); got != c.want {
			t.Errorf("lock %s, deadlock %s: %s, want %s", c.lock, c.deadlock, got, c.want)
		}
	}
}
