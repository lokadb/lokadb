package admin

// Copies in batches and their progress: the split out of DEFAULT and the initial copy of the
// trigger mode stream one COPY out and commit it BatchRows rows at a time on the other side.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DefaultBatchRows: rows per transaction of a copy in batches (MoveOptions.BatchRows).
const DefaultBatchRows = 10000

func (o MoveOptions) batchRows() int {
	if o.BatchRows > 0 {
		return o.BatchRows
	}
	return DefaultBatchRows
}

// DefaultMaxSplitPause: how long the organization may pause while it is split out of DEFAULT
// (MoveOptions.MaxSplitPause).
const DefaultMaxSplitPause = 10 * time.Minute

func (o MoveOptions) maxSplitPause() time.Duration {
	if o.MaxSplitPause > 0 {
		return o.MaxSplitPause
	}
	return DefaultMaxSplitPause
}

// copyChunked streams srcSQL (COPY ... TO STDOUT, text format) from one connection into
// dstSQL (COPY ... FROM STDIN) on to, batch rows per transaction there; setup runs at the start
// of each one. In the text format a row is one line (a newline in a value is written \n).
// onBatch gets the rows and bytes of every committed batch. It returns the rows copied.
func copyChunked(ctx context.Context, from *pgconn.PgConn, srcSQL string, to *pgx.Conn, dstSQL string, batch int, setup []string, onBatch func(rows, bytes int64)) (int64, error) {
	pr, pw := io.Pipe()
	readErr := make(chan error, 1)
	go func() {
		_, err := from.CopyTo(ctx, pw, srcSQL)
		pw.CloseWithError(err)
		readErr <- err
	}()
	fail := func(err error) (int64, error) {
		pr.CloseWithError(err) // the reader stops writing
		if rerr := <-readErr; rerr != nil && !errors.Is(rerr, io.ErrClosedPipe) && !errors.Is(rerr, err) {
			return 0, fmt.Errorf("%w (reading: %v)", err, rerr)
		}
		return 0, err
	}
	br := bufio.NewReaderSize(pr, 1<<20)
	var total int64
	for {
		if _, err := br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fail(err)
		}
		chunk := &lines{r: br, left: batch}
		err := pgx.BeginFunc(ctx, to, func(tx pgx.Tx) error {
			for _, s := range setup {
				if _, err := tx.Exec(ctx, s); err != nil {
					return err
				}
			}
			_, err := tx.Conn().PgConn().CopyFrom(ctx, chunk, dstSQL)
			return err
		})
		if err != nil {
			if chunk.err != nil { // the source failed: that is the cause
				err = chunk.err
			}
			return fail(err)
		}
		total += chunk.rows
		if onBatch != nil {
			onBatch(chunk.rows, chunk.bytes)
		}
	}
	if err := <-readErr; err != nil {
		return total, err
	}
	return total, nil
}

// lines reads whole lines of r, at most left of them, then ends (io.EOF).
type lines struct {
	r           *bufio.Reader
	left        int
	rows, bytes int64
	pending     []byte
	err         error
}

func (l *lines) Read(p []byte) (int, error) {
	if len(l.pending) == 0 {
		if l.left <= 0 {
			return 0, io.EOF
		}
		line, err := l.r.ReadBytes('\n')
		if len(line) > 0 {
			l.pending = line
			l.rows++
			l.bytes += int64(len(line))
			l.left--
		}
		if err != nil {
			l.left = 0
			if !errors.Is(err, io.EOF) {
				l.err = err
				return 0, err
			}
			if len(line) == 0 {
				return 0, io.EOF
			}
		}
	}
	n := copy(p, l.pending)
	l.pending = l.pending[n:]
	return n, nil
}

// progress reports a long step of the move as it goes, at most every progressEvery, in one
// line format (msg "progress"):
//
//	step=split|copy|catch-up table=T rows=N rows_total=M bytes=B [bytes_total=S] elapsed=D [eta=D] [state=started|waiting|done]
//
// rows_total is what the step started with (0: unknown), bytes what went over the wire,
// bytes_total the table's size in the cloud (an estimate of it).
type progress struct {
	log               *slog.Logger
	step, table       string
	total, totalBytes int64
	rows, bytes       int64
	start, last       time.Time
	finished          bool
}

const progressEvery = 2 * time.Second

func newProgress(log *slog.Logger, step, table string, total, done int64) *progress {
	now := time.Now()
	p := &progress{log: log, step: step, table: table, total: total, rows: done, start: now, last: now}
	p.report("started")
	return p
}

func (p *progress) add(rows, bytes int64) {
	p.rows += rows
	p.bytes += bytes
	if time.Since(p.last) >= progressEvery {
		p.report("")
	}
}

func (p *progress) done() { p.report("done") }

func (p *progress) report(state string) {
	p.last = time.Now()
	elapsed := time.Since(p.start)
	args := []any{"step", p.step, "table", p.table, "rows", p.rows, "rows_total", p.total, "bytes", p.bytes}
	if p.totalBytes > 0 {
		args = append(args, "bytes_total", p.totalBytes)
	}
	args = append(args, "elapsed", elapsed.Round(time.Second))
	if state != "" {
		args = append(args, "state", state)
	}
	if state == "" && p.total > p.rows && p.rows > 0 {
		eta := time.Duration(float64(elapsed) * float64(p.total-p.rows) / float64(p.rows))
		args = append(args, "eta", eta.Round(time.Second))
	}
	p.log.Info("progress", args...)
}

// Limits of the copy and of the catch-up (A4): flags of move-org.
const (
	DefaultCopyTimeout    = 24 * time.Hour
	DefaultCatchUpTimeout = 2 * time.Hour
	// DefaultCatchUpStall: the catch-up gives up once the log left has not got shorter for this
	// long (block 14, 27.10: a move 43 ms away fell behind 8 writers for 65 minutes and would not
	// have caught up in any time)
	DefaultCatchUpStall = 10 * time.Minute
)

func (o MoveOptions) catchUpStall() time.Duration {
	switch {
	case o.CatchUpStall < 0:
		return 0
	case o.CatchUpStall > 0:
		return o.CatchUpStall
	}
	return DefaultCatchUpStall
}

func (o MoveOptions) copyTimeout() time.Duration {
	if o.CopyTimeout > 0 {
		return o.CopyTimeout
	}
	return DefaultCopyTimeout
}

func (o MoveOptions) catchUpTimeout() time.Duration {
	if o.CatchUpTimeout > 0 {
		return o.CatchUpTimeout
	}
	return DefaultCatchUpTimeout
}

// moveWindow: how long what the move opens for the organization's database lives (the
// replication role, the relay's window): the move's own limits and a margin for the switch.
func (o MoveOptions) moveWindow() time.Duration {
	return o.copyTimeout() + o.catchUpTimeout() + 2*time.Hour
}

// copyWatch reports the initial copy of the logical mode, table by table, from the
// subscription's table synchronization in the organization's database (pg_stat_progress_copy).
type copyWatch struct {
	log    *slog.Logger
	tables map[string]*progress // by the name in the organization's database (t.local())
	last   time.Time
}

func newCopyWatch(ctx context.Context, cloud *pgx.Conn, tables []moveTable, log *slog.Logger) (*copyWatch, error) {
	w := &copyWatch{log: log, tables: map[string]*progress{}} // the first poll reports at once
	for _, t := range tables {
		// rows: the planner's estimate, or a count when there is none yet (a new partition)
		r, err := rowsOf(ctx, cloud, fmt.Sprintf(`SELECT CASE WHEN c.reltuples > 0 THEN c.reltuples::bigint ELSE (SELECT count(*) FROM %s) END::text,
			pg_table_size(c.oid)::text FROM pg_class c WHERE c.oid = $1::regclass`, qr(t.local())), qr(t.local()))
		if err != nil {
			return nil, err
		}
		p := &progress{log: log, step: "copy", table: t.name, start: time.Now()}
		_, _ = fmt.Sscan(r[0][0], &p.total)
		_, _ = fmt.Sscan(r[0][1], &p.totalBytes)
		w.tables[t.local()] = p
	}
	return w, nil
}

// poll reports every table not done yet, at most every 5 seconds.
func (w *copyWatch) poll(ctx context.Context, cust *pgx.Conn, sub string) {
	if time.Since(w.last) < 5*time.Second {
		return
	}
	w.last = time.Now()
	rows, err := rowsOf(ctx, cust, `SELECT c.relname::text, r.srsubstate::text, coalesce(p.tuples_processed, 0)::text, coalesce(p.bytes_processed, 0)::text,
		c.relnamespace::regnamespace::text
		FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid JOIN pg_class c ON c.oid = r.srrelid
		LEFT JOIN pg_stat_progress_copy p ON p.relid = r.srrelid AND p.datid = s.subdbid
		WHERE s.subname = $1`, sub)
	if err != nil {
		w.log.Warn("initial copy: no progress from the organization's database", "err", err)
		return
	}
	for _, r := range rows {
		p := w.tables[r[0]]
		if p == nil { // a table of a schema other than public
			p = w.tables[RelKey(strings.Trim(r[4], `"`), r[0])]
		}
		if p == nil || p.finished {
			continue
		}
		switch r[1] {
		case "f", "s", "r": // copied: finished, synchronized, ready
			p.rows = max(p.rows, p.total)
			p.finished = true
			p.report("done")
		case "d": // copying
			_, _ = fmt.Sscan(r[2], &p.rows)
			_, _ = fmt.Sscan(r[3], &p.bytes)
			p.report("")
		default:
			p.report("waiting")
		}
	}
}

// finish reports every table not reported done yet: the copy is over.
func (w *copyWatch) finish() {
	for _, p := range w.tables {
		if !p.finished {
			p.rows = max(p.rows, p.total)
			p.finished = true
			p.report("done")
		}
	}
}
