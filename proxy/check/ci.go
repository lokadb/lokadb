package check

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// CI of the customer (tasks/routing-declarations.md §8): `check -ci -baseline accepted.json`
// fails on what the baseline has not accepted yet, typically a query pattern the application's
// new code sends and the proxy cannot confine to one organization.

// Baseline: what the application's owner has reviewed and accepted, with the reason.
type Baseline struct {
	SchemaVersion int        `json:"schema_version"`
	Accepted      []Accepted `json:"accepted"`
}

// Accepted is matched by Key; the rest is for people.
type Accepted struct {
	Key         string `json:"key"`
	Code        string `json:"code"`
	Fingerprint string `json:"fingerprint,omitempty"`
	SQL         string `json:"sql,omitempty"`
	Object      string `json:"object,omitempty"`
	Reason      string `json:"reason"`
}

// CIItem is one item -ci judges: a query pattern of a finding about patterns, otherwise the
// finding itself. Findings of severity info take no part.
type CIItem struct {
	Key      string   `json:"key"`
	Severity Severity `json:"severity"`
	Code     string   `json:"code"`
	Object   string   `json:"object,omitempty"`
	Pattern  *Pattern `json:"pattern,omitempty"`
	Hint     string   `json:"hint"`
}

type CIResult struct {
	Baseline string     `json:"baseline"` // the file, "" without -baseline
	New      []CIItem   `json:"new"`      // not in the baseline: -ci fails on them
	Stale    []Accepted `json:"stale"`    // in the baseline but no longer found
	Accepted int        `json:"accepted"` // items found and accepted
	NoReason int        `json:"accepted_without_reason"`
}

// Items lists what -ci judges, in the report's order.
func (r *Report) Items() []CIItem {
	var out []CIItem
	for _, f := range r.Findings {
		if f.Severity == Info {
			continue
		}
		if len(f.Patterns) > 0 {
			for i := range f.Patterns {
				p := f.Patterns[i]
				out = append(out, CIItem{Key: patternKey(f.Code, p), Severity: f.Severity, Code: f.Code, Pattern: &p, Hint: ciHint(f)})
			}
			continue
		}
		if len(f.Transactions) > 0 && f.Subject == nil {
			// a finding about transaction shapes: each shape is an item, keyed by the
			// fingerprints of its statements (review 3, M3)
			for _, tx := range f.Transactions {
				out = append(out, CIItem{Key: f.Key + ":tx:" + txKey(tx), Severity: f.Severity, Code: f.Code, Object: txObject(tx), Hint: ciHint(f)})
			}
			continue
		}
		if exampleKeyed[f.Code] && len(f.Examples) > 0 {
			// one finding lists many objects (FKs, publications, slots): each is an item of its
			// own, so a new one is new to the baseline (review 2, R6)
			for _, ex := range f.Examples {
				out = append(out, CIItem{Key: f.Key + ":ex:" + ex, Severity: f.Severity, Code: f.Code, Object: ex, Hint: ciHint(f)})
			}
			continue
		}
		out = append(out, CIItem{Key: f.Key, Severity: f.Severity, Code: f.Code, Object: f.Object, Hint: ciHint(f)})
	}
	return out
}

// txKey: a transaction shape's statement fingerprints (their text when one does not parse), sorted.
func txKey(tx TxShape) string {
	var fps []string
	for _, s := range tx.Statements {
		if s.Fingerprint != "" {
			fps = append(fps, s.Fingerprint)
		} else {
			fps = append(fps, s.SQL)
		}
	}
	sort.Strings(fps)
	return strings.Join(fps, "+")
}

// txObject: a transaction shape for people: its first statement and how many follow.
func txObject(tx TxShape) string {
	if len(tx.Statements) == 0 {
		return ""
	}
	o := short(tx.Statements[0].SQL)
	if n := len(tx.Statements) - 1; n > 0 {
		o += fmt.Sprintf(" (+%d)", n)
	}
	return o
}

// txShapeOf: a recorded transaction shape as findings carry it.
func txShapeOf(t *proxy.RecordedTx) TxShape {
	sh := TxShape{CloudTables: []string{}, LokadbTables: []string{}, Shapes: 1, Count: t.Count, Statements: []TxStatement{}}
	for _, s := range t.Statements {
		fp, err := pg.Fingerprint(analyzable(s))
		if err != nil {
			fp = ""
		}
		sh.Statements = append(sh.Statements, TxStatement{Fingerprint: fp, SQL: s})
	}
	return sh
}

// exampleKeyed: findings whose examples are the objects they are about, one per line and
// complete; -ci judges each example as an item (key code:ex:<example>).
var exampleKeyed = map[string]bool{
	"fk_to_cloud": true, "cloud_references": true, "cdc_publication": true, "logical_slots": true,
	"move_leftovers": true, "profile_direct_fk_cloud": true,
}

// ciHints: what to do about a new item, by code; other codes take the finding's message.
var ciHints = map[string]string{
	"query_refused": `the proxy refuses it after enable: restrict it by the organization (its column, id or a parent id, = or IN), ` +
		`or, if it is meant to read every organization, accept it here and add it to "allow" of the proxy config`,
	"query_without_organization": `it does not say which organization it is for, so it covers many at once and runs separately in your database and in each moved organization's database (each looks at its own rows only): restrict it by the organization (its column, id or a parent id, = or IN), ` +
		`or accept it here with the reason (a report, an admin page, a background job)`,
	"query_forbidden":         "the proxy refuses it whatever its policy: use the LokaDB table itself, not lokadb's own objects",
	"ddl_rejected":            "the proxy refuses this schema change after enable: change the migration",
	"parse_error":             "the proxy's parser does not understand it; it names no LokaDB table, so the proxy sends it to the cloud as it is",
	"mixed_write_transaction": "a transaction writing cloud and LokaDB tables: move the cloud table to LokaDB too, or accept the two commits here",
}

func ciHint(f Finding) string {
	if h, ok := ciHints[f.Code]; ok {
		return h
	}
	return f.Message
}

// CompareBaseline compares the report with a baseline (nil: an empty one) and keeps the result in r.CI.
func (r *Report) CompareBaseline(b *Baseline, file string) *CIResult {
	res := &CIResult{Baseline: file, New: []CIItem{}, Stale: []Accepted{}}
	acc := map[string]Accepted{}
	if b != nil {
		for _, a := range b.Accepted {
			acc[a.Key] = a
		}
	}
	found := map[string]bool{}
	for _, it := range r.Items() {
		if found[it.Key] {
			continue
		}
		found[it.Key] = true
		a, ok := acc[it.Key]
		if !ok {
			res.New = append(res.New, it)
			continue
		}
		res.Accepted++
		if a.Reason == "" {
			res.NoReason++
		}
	}
	if b != nil {
		for _, a := range b.Accepted {
			if !found[a.Key] {
				res.Stale = append(res.Stale, a)
			}
		}
	}
	r.CI = res
	return res
}

// MakeBaseline: every item of the report accepted, the reasons left for the owner to fill.
// Reasons already given in old are kept.
func (r *Report) MakeBaseline(old *Baseline) Baseline {
	reasons := map[string]string{}
	if old != nil {
		for _, a := range old.Accepted {
			reasons[a.Key] = a.Reason
		}
	}
	b := Baseline{SchemaVersion: SchemaVersion, Accepted: []Accepted{}}
	seen := map[string]bool{}
	for _, it := range r.Items() {
		if seen[it.Key] {
			continue
		}
		seen[it.Key] = true
		a := Accepted{Key: it.Key, Code: it.Code, Object: it.Object, Reason: reasons[it.Key]}
		if it.Pattern != nil {
			a.Fingerprint, a.SQL = it.Pattern.Fingerprint, it.Pattern.SQL
		}
		b.Accepted = append(b.Accepted, a)
	}
	sort.Slice(b.Accepted, func(i, j int) bool { return b.Accepted[i].Key < b.Accepted[j].Key })
	return b
}

func ReadBaseline(path string) (*Baseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if b.SchemaVersion > SchemaVersion {
		return nil, fmt.Errorf("%s: baseline of report schema %d, this lokadb knows %d: update lokadb", path, b.SchemaVersion, SchemaVersion)
	}
	return &b, nil
}

func (b Baseline) Write(path string) error {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// CIExitCode: 0 nothing new, 6 something the baseline has not accepted.
func (res *CIResult) ExitCode() int {
	if len(res.New) > 0 {
		return 6
	}
	return 0
}

// WriteText prints the result of -ci for the CI log.
func (res *CIResult) WriteText(w io.Writer) {
	base := res.Baseline
	if base == "" {
		base = "no baseline (-baseline FILE)"
	}
	if len(res.New) == 0 {
		fmt.Fprintf(w, "lokadb check -ci: OK, nothing new against %s (%d accepted)\n", base, res.Accepted)
	} else {
		fmt.Fprintf(w, "lokadb check -ci: FAILED, %s not accepted in %s (%d accepted)\n", plural(len(res.New), "new item", "new items"), base, res.Accepted)
		for _, it := range res.New {
			fmt.Fprintf(w, "\n  [%s] %s\n", it.Code, it.Severity)
			if it.Pattern != nil {
				fmt.Fprintf(w, "      x%d  %s\n", it.Pattern.Calls, short(it.Pattern.SQL))
			} else if it.Object != "" {
				fmt.Fprintf(w, "      %s\n", it.Object)
			}
			fmt.Fprintf(w, "      key: %s\n", it.Key)
			fmt.Fprintf(w, "      %s\n", it.Hint)
		}
		fmt.Fprintf(w, "\n  To accept them: lokadb check ... -write-baseline FILE, then give each new entry its reason.\n")
	}
	if res.NoReason > 0 {
		fmt.Fprintf(w, "  %s in the baseline without a reason\n", plural(res.NoReason, "accepted item has", "accepted items have"))
	}
	if len(res.Stale) > 0 {
		fmt.Fprintf(w, "  %s no longer found (can be removed):\n", plural(len(res.Stale), "baseline entry is", "baseline entries are"))
		for _, a := range res.Stale {
			fmt.Fprintf(w, "      %s\n", a.Key)
		}
	}
}
