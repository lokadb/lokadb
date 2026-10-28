package check

import (
	"fmt"
	"strconv"
	"time"
)

// prepareProgress reports where lokadb enable -prepare is (block 3, 1.4): lokadb.prepare holds
// a row per table from enable -prepare until enable switches it.
func (c *checker) prepareProgress() error {
	ok, err := c.rows(`SELECT (to_regclass('lokadb.prepare') IS NOT NULL)::text`)
	if err != nil || ok[0][0] != "true" {
		return err
	}
	rows, err := c.rows(`SELECT table_name, stage, done::text, total::text,
		greatest(extract(epoch FROM updated - started), 0)::bigint::text, greatest(extract(epoch FROM now() - updated), 0)::bigint::text
		FROM lokadb.prepare ORDER BY table_name`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		t, stage := r[0], r[1]
		done, _ := strconv.ParseInt(r[2], 10, 64)
		total, _ := strconv.ParseInt(r[3], 10, 64)
		ran, _ := strconv.ParseInt(r[4], 10, 64)
		idle, _ := strconv.ParseInt(r[5], 10, 64)
		since := (time.Duration(idle) * time.Second).String()
		switch {
		case stage == "ready":
			c.add(Info, "enable_prepare_done", t, fmt.Sprintf("enable -prepare is done for %s: lokadb enable switches it in a short lock (it checks the preparation first)", t), nil)
		case idle > 600:
			c.add(Warning, "enable_prepare_stalled", t, fmt.Sprintf("enable -prepare of %s stopped at stage %s, %d of about %d rows, %s ago: run lokadb enable -prepare again (it goes on from there); lokadb enable refuses until it is done", t, stage, done, total, since), nil)
		default:
			msg := fmt.Sprintf("enable -prepare of %s is at stage %s: %d of about %d rows", t, stage, done, total)
			if ran > 0 && done > 0 && total > done {
				left := time.Duration(float64(total-done)/(float64(done)/float64(ran))) * time.Second
				msg += fmt.Sprintf(", about %s left at its rate", left.Round(time.Second))
			}
			c.add(Info, "enable_prepare", t, msg+"; lokadb enable refuses until it is done", nil)
		}
	}
	return nil
}
