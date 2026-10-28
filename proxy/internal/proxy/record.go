package proxy

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// Workload recording (proxy -record FILE): while the proxy forwards traffic, LokaDB enabled
// or not, it notes which statements each database runs and which of them share a
// transaction. `lokadb check` reads the file to find queries that name no organization and
// transactions that write cloud and LokaDB tables together. Only normalized SQL is kept
// (constants become $n); parameter values never reach the recorder.

// Workload is the recording file.
type Workload struct {
	Version   int                    `json:"version"`
	Updated   time.Time              `json:"updated"`
	Databases map[string]*WorkloadDB `json:"databases"`
}

type WorkloadDB struct {
	Statements   []*RecordedStatement `json:"statements"`
	Transactions []*RecordedTx        `json:"transactions"`
	Dropped      int64                `json:"dropped,omitempty"` // not kept: the limits below were reached
}

type RecordedStatement struct {
	SQL   string `json:"sql"`
	Count int64  `json:"count"`
}

// RecordedTx is a transaction shape: its distinct statements in first-seen order (transaction
// control left out) and how many transactions had that shape. A statement outside BEGIN is a
// transaction of its own, as are the statements of one simple query or one Sync.
type RecordedTx struct {
	Statements []string `json:"statements"`
	Count      int64    `json:"count"`
	// Repeats: how many times each statement (by fingerprint, as Statements) ran in the
	// RepeatsTx transactions of this shape, summed; its average per transaction is
	// Repeats[i] / RepeatsTx. Recordings made before it was kept have RepeatsTx 0.
	Repeats   []int64 `json:"repeats,omitempty"`
	RepeatsTx int64   `json:"repeats_tx,omitempty"`
	// Form: how the client sent the shape: "block" (BEGIN … COMMIT), "batch" (the Executes of
	// one Sync of the extended protocol outside a block: a pipeline, pgx.Batch), "script"
	// (several statements of one simple query outside a block). "" in recordings made before
	// it was kept. Shapes of different forms are different shapes.
	Form string `json:"form,omitempty"`
}

const (
	maxRecordedStatements = 20000 // per database
	maxRecordedTxs        = 5000  // per database
	maxTxStatements       = 100
	recordEvery           = 2 * time.Second
)

type recorder struct {
	path  string
	log   *slog.Logger
	mu    sync.Mutex
	dbs   map[string]*recDB
	dirty bool
}

type recDB struct {
	stmts   map[string]*RecordedStatement // by normalized text
	txs     map[string]*RecordedTx        // by statement fingerprints
	dropped int64
}

// newRecorder continues an existing recording file, so a proxy restart loses at most the last interval.
func newRecorder(path string, log *slog.Logger) *recorder {
	r := &recorder{path: path, log: log, dbs: map[string]*recDB{}}
	var w Workload
	if b, err := os.ReadFile(path); err == nil && json.Unmarshal(b, &w) == nil {
		for name, wd := range w.Databases {
			d := r.db(name)
			d.dropped = wd.Dropped
			for _, s := range wd.Statements {
				d.stmts[s.SQL] = s
			}
			for _, tx := range wd.Transactions {
				var fps []string
				for _, s := range tx.Statements {
					if k := statementKeys(s); len(k) > 0 {
						fps = append(fps, k[0].fp)
					}
				}
				if len(tx.Repeats) != len(tx.Statements) { // recorded before Repeats was kept
					tx.Repeats, tx.RepeatsTx = make([]int64, len(tx.Statements)), 0
				}
				d.txs[tx.Form+"|"+strings.Join(fps, " ")] = tx
			}
		}
	}
	go func() {
		for range time.Tick(recordEvery) {
			func() {
				defer func() { // a bug in the recording must not take down the proxy
					if p := recover(); p != nil {
						log.Error("proxy bug: workload recording skipped", "panic", p, "stack", string(debug.Stack()))
					}
				}()
				if err := r.flush(); err != nil {
					log.Warn("workload recording not written", "path", path, "err", err)
				}
			}()
		}
	}()
	return r
}

func (r *recorder) db(name string) *recDB {
	d := r.dbs[name]
	if d == nil {
		d = &recDB{stmts: map[string]*RecordedStatement{}, txs: map[string]*RecordedTx{}}
		r.dbs[name] = d
	}
	return d
}

// record adds one finished transaction: its distinct statements and how often each ran.
func (r *recorder) record(db string, order []stmtKey, runs map[string]int64, form string) {
	if len(order) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dirty = true
	d := r.db(db)
	var fps, texts []string
	seen := map[string]bool{}
	runsFP := map[string]int64{} // texts that differ only in list lengths share a fingerprint, and its runs
	for _, k := range order {
		runsFP[k.fp] += runs[k.norm]
		if s := d.stmts[k.norm]; s != nil {
			s.Count += runs[k.norm]
		} else if len(d.stmts) < maxRecordedStatements {
			d.stmts[k.norm] = &RecordedStatement{SQL: k.norm, Count: runs[k.norm]}
		} else {
			d.dropped++
		}
		// Statements that differ only in list lengths (IN, VALUES) share a fingerprint.
		if !seen[k.fp] && len(fps) < maxTxStatements {
			seen[k.fp] = true
			fps = append(fps, k.fp)
			texts = append(texts, k.norm)
		}
	}
	key := form + "|" + strings.Join(fps, " ")
	if d.txs[key] == nil { // a shape of a recording made before Form was kept takes the form of the first new run
		if old := d.txs["|"+strings.Join(fps, " ")]; old != nil && form != "" {
			delete(d.txs, "|"+strings.Join(fps, " "))
			old.Form = form
			d.txs[key] = old
		}
	}
	if tx := d.txs[key]; tx != nil {
		tx.Count++
		tx.RepeatsTx++
		for i, fp := range fps {
			tx.Repeats[i] += runsFP[fp]
		}
	} else if len(d.txs) < maxRecordedTxs {
		reps := make([]int64, len(fps))
		for i, fp := range fps {
			reps[i] = runsFP[fp]
		}
		d.txs[key] = &RecordedTx{Statements: texts, Count: 1, Repeats: reps, RepeatsTx: 1, Form: form}
	} else {
		d.dropped++
	}
}

func (r *recorder) flush() error {
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return nil
	}
	r.dirty = false
	w := Workload{Version: 1, Updated: time.Now().UTC(), Databases: map[string]*WorkloadDB{}}
	for name, d := range r.dbs {
		wd := &WorkloadDB{Dropped: d.dropped}
		for _, s := range d.stmts {
			c := *s
			wd.Statements = append(wd.Statements, &c)
		}
		for _, tx := range d.txs {
			c := *tx
			c.Repeats = append([]int64(nil), tx.Repeats...) // record() changes it after the lock is released
			wd.Transactions = append(wd.Transactions, &c)
		}
		w.Databases[name] = wd
	}
	r.mu.Unlock()
	for _, wd := range w.Databases {
		sort.Slice(wd.Statements, func(i, j int) bool {
			a, b := wd.Statements[i], wd.Statements[j]
			return a.Count > b.Count || a.Count == b.Count && a.SQL < b.SQL
		})
		sort.Slice(wd.Transactions, func(i, j int) bool {
			a, b := wd.Transactions[i], wd.Transactions[j]
			return a.Count > b.Count || a.Count == b.Count && strings.Join(a.Statements, ";") < strings.Join(b.Statements, ";")
		})
	}
	b, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// stmtKey is one statement as the recorder sees it.
type stmtKey struct {
	fp, norm string
	ctl      byte // 'B' opens a transaction block, 'E' ends it, 'C' ends it and opens the next (AND CHAIN), 'S' other transaction control
	// tenantParams: set_config($1, $2, $3): it sets lokadb.tenant when its Bind says so
	// (tenantBindKey, record_tenant.go)
	tenantParams bool
}

// unparsedRe: what the recording keeps of a statement that does not parse.
var unparsedRe = regexp.MustCompile(`^<[0-9]+ bytes that do not parse>$`)

// unparsed: a statement the parser (PostgreSQL 17's grammar) did not take, or that could not be
// normalized, is recorded by its size only: its literals are the clients' data, and the recording
// keeps constants out (PR-12 of the protocol review). check lists it as parse_error.
func unparsed(text string) string {
	if unparsedRe.MatchString(text) { // read back from a recording
		return text
	}
	return fmt.Sprintf("<%d bytes that do not parse>", len(text))
}

func statementKeys(sql string) []stmtKey {
	tree, err := pg.Parse(sql)
	if err != nil {
		text := unparsed(strings.TrimSpace(sql))
		return []stmtKey{{fp: "!" + text, norm: text}}
	}
	var out []stmtKey
	for _, raw := range tree.Stmts {
		start := int(raw.StmtLocation)
		end := len(sql)
		if raw.StmtLen > 0 {
			end = start + int(raw.StmtLen)
		}
		text := strings.TrimSpace(sql[start:end])
		k := stmtKey{fp: "!" + text, norm: text}
		k.fp = "!" + unparsed(text) // until it is fingerprinted: not the literals
		if ts := raw.Stmt.GetTransactionStmt(); ts != nil {
			switch ts.Kind {
			case pg.TransactionStmtKind_TRANS_STMT_BEGIN, pg.TransactionStmtKind_TRANS_STMT_START:
				k.ctl = 'B'
			case pg.TransactionStmtKind_TRANS_STMT_COMMIT, pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
				k.ctl = 'E'
				if ts.Chain {
					k.ctl = 'C'
				}
			case pg.TransactionStmtKind_TRANS_STMT_PREPARE:
				k.ctl = 'E'
			default:
				k.ctl = 'S'
			}
			out = append(out, k)
			continue
		}
		if n, err := pg.Normalize(text); err == nil {
			k.norm = n
		} else {
			k.norm = unparsed(text)
			k.fp = "!" + k.norm
		}
		if fp, err := pg.Fingerprint(text); err == nil {
			k.fp = fp
		}
		k, _ = tenantRecordKey(raw.Stmt, k) // lokadb.tenant: '*' kept apart (record_tenant.go)
		out = append(out, k)
	}
	return out
}

// txTracker follows one client connection's messages and hands finished transactions to the recorder.
type txTracker struct {
	rec      *recorder
	db       string
	cache    map[string][]stmtKey
	prepared map[string][]stmtKey // prepared statement name -> its statement
	portals  map[string][]stmtKey
	block    bool // inside BEGIN ... COMMIT
	order    []stmtKey
	runs     map[string]int64
}

func newTxTracker(rec *recorder, db string) *txTracker {
	return &txTracker{rec: rec, db: db, cache: map[string][]stmtKey{}, prepared: map[string][]stmtKey{},
		portals: map[string][]stmtKey{}, runs: map[string]int64{}}
}

func (t *txTracker) observe(msg pgproto3.FrontendMessage) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		for _, k := range t.keys(m.String) {
			t.exec(k)
		}
		if !t.block { // a simple query outside BEGIN is one implicit transaction
			t.end("script")
		}
	case *pgproto3.Parse:
		t.prepared[m.Name] = t.keys(m.Query)
	case *pgproto3.Bind:
		t.portals[m.DestinationPortal] = tenantBindKey(t.prepared[m.PreparedStatement], m.Parameters)
	case *pgproto3.Execute:
		for _, k := range t.portals[m.Portal] {
			t.exec(k)
		}
	case *pgproto3.Sync:
		if !t.block { // so are the Executes before a Sync
			t.end("batch")
		}
	case *pgproto3.Close:
		if m.ObjectType == 'S' {
			delete(t.prepared, m.Name)
		} else {
			delete(t.portals, m.Name)
		}
	}
}

func (t *txTracker) keys(sql string) []stmtKey {
	if k, ok := t.cache[sql]; ok {
		return k
	}
	if len(t.cache) > 2000 {
		t.cache = map[string][]stmtKey{}
	}
	k := statementKeys(sql)
	t.cache[sql] = k
	return k
}

func (t *txTracker) exec(k stmtKey) {
	switch k.ctl {
	case 'B': // statements already run in this implicit transaction become part of the block
		t.block = true
	case 'E':
		t.end("block")
		t.block = false
	case 'C':
		t.end("block")
	case 'S':
	default:
		if t.runs[k.norm] == 0 && len(t.order) < 10*maxTxStatements {
			t.order = append(t.order, k)
		}
		t.runs[k.norm]++
	}
}

func (t *txTracker) end(form string) {
	if len(t.order) == 0 {
		return
	}
	t.rec.record(t.db, t.order, t.runs, form)
	t.order = nil
	t.runs = map[string]int64{}
}
