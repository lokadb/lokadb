package check

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

var verdictText = map[Verdict]string{
	Ready:        "READY: nothing found that stops the migration",
	Review:       "NEEDS REVIEW: the migration can run, the changes and warnings below need a decision",
	NotSupported: "NOT SUPPORTED YET: nothing in the schema stops the migration, but lokadb cannot move some of its tables yet",
	Blocked:      "BLOCKED: the migration is blocked until the blockers below are fixed",
	InvalidSpec:  "INVALID SPEC: the spec does not match the database; fix it and run the check again (the tables it names wrongly are not checked)",
}

var sectionText = map[Severity]string{
	SpecError:   "Spec errors (the spec does not match the database)",
	Blocker:     "Blockers",
	Unsupported: "Not supported by lokadb yet (limitations of lokadb, not problems of the schema; enable or move-org would refuse or mishandle these for now)",
	Change:      "Changes after the migration (the application works, but these behave differently)",
	Warning:     "Warnings",
	Info:        "Info",
}

// WriteText prints the report for people.
func (r *Report) WriteText(w io.Writer) {
	s := r.Summary
	fmt.Fprintf(w, "Database compatibility report: %s\n", r.Database)
	fmt.Fprintf(w, "Verdict: %s\n\n", verdictText[r.Verdict])
	fmt.Fprintf(w, "  %d of %s ready for tenant partitioning\n", s.TablesReady, plural(s.Tables, "table", "tables"))
	if s.TablesUnsupported > 0 {
		fmt.Fprintf(w, "  %s not supported by lokadb yet\n", plural(s.TablesUnsupported, "table", "tables"))
	}
	fmt.Fprintf(w, "  %s per organization after the move\n", uniqueLine(s.UniquePerOrg))
	if len(r.Sources) > 0 {
		fmt.Fprintf(w, "  %s the organization (of %d on LokaDB tables), %d refused by the proxy\n",
			plural(s.UnroutedPatterns, "query pattern does not name", "query patterns do not name"), s.QueryPatterns, s.RefusedPatterns)
	} else {
		fmt.Fprintf(w, "  queries not checked (no pg_stat_statements, no proxy recording)\n")
	}
	if s.TxPatternsChecked {
		fmt.Fprintf(w, "  %s cloud and LokaDB tables together (of %d writing LokaDB tables)\n",
			plural(s.MixedTxPatterns, "transaction shape writes", "transaction shapes write"), s.TxPatterns)
	} else {
		fmt.Fprintf(w, "  transactions not checked (no proxy recording)\n")
	}
	for _, src := range r.Sources {
		fmt.Fprintf(w, "  source: %s\n", src)
	}
	if r.FanoutPolicy != "" {
		fmt.Fprintf(w, "  proxy policy for queries without the organization: %s\n", r.FanoutPolicy)
	}
	for _, e := range extensions { // the extensions' sections (the latency of -rtt)
		if t, ok := e.(TextSection); ok {
			t.WriteText(w, r)
		}
	}

	if len(r.Tables) > 0 {
		fmt.Fprintf(w, "\nTables\n")
		for _, t := range r.Tables {
			state := map[string]string{StateReady: "ready", StateReview: "ready, see changes", StateUnsupported: "not supported yet", StateBlocked: "blocked", StateSpecError: "spec error"}[t.State]
			rows := "rows: ?"
			if t.Rows >= 0 {
				rows = fmt.Sprintf("rows: ~%d", t.Rows)
			}
			fmt.Fprintf(w, "  %-28s %-36s %-16s %s\n", t.Name, t.Tenant, rows, state)
		}
	}
	for _, sev := range []Severity{SpecError, Blocker, Unsupported, Change, Warning, Info} {
		var fs []Finding
		for _, f := range r.Findings {
			if f.Severity == sev {
				fs = append(fs, f)
			}
		}
		if len(fs) == 0 {
			continue
		}
		fmt.Fprintf(w, "\n%s (%d)\n", sectionText[sev], len(fs))
		for _, f := range fs {
			fmt.Fprintf(w, "  [%s] %s\n", f.Code, f.Message)
			for _, e := range f.Examples {
				fmt.Fprintf(w, "      %s\n", e)
			}
		}
	}
	if d := r.Discovery; d != nil {
		fmt.Fprintf(w, "\nTables by the organizations table %s (key %s):\n", d.OrgTable, d.Key)
		for _, t := range d.Tables {
			fmt.Fprintf(w, "  %-12s %s", t.Role, t.Table)
			switch t.Role {
			case RoleRoot:
				fmt.Fprintf(w, "  tenant_column %s", t.Column)
			case RoleChild:
				fmt.Fprintf(w, "  via %s", strings.Join(t.Path, " -> "))
			}
			if t.Basis == "name" {
				fmt.Fprint(w, "  (by name, verified on the data)")
			}
			fmt.Fprintln(w)
			if t.Reason != "" {
				fmt.Fprintf(w, "      %s\n", t.Reason)
			}
		}
	}
	if r.Spec != nil {
		b, _ := json.MarshalIndent(r.Spec, "  ", "  ")
		fmt.Fprintf(w, "\nSuggested spec (save it with -write-spec FILE):\n  %s\n", b)
	}
	fmt.Fprintf(w, "\nThe report covers the schema and the queries seen so far. Queries nobody has run yet (rare code paths,\n"+
		"dynamic SQL) are not covered: test the application through the proxy before moving an organization.\n")
}

func uniqueLine(n int) string {
	if n == 1 {
		return "1 global unique constraint becomes"
	}
	return fmt.Sprintf("%d global unique constraints become", n)
}

// ExitCode: 0 ready, 2 needs review, 3 blocked, 4 not supported by lokadb yet (1 is left for errors).
func (r *Report) ExitCode() int {
	switch r.Verdict {
	case InvalidSpec:
		return 5
	case Review:
		return 2
	case Blocked:
		return 3
	case NotSupported:
		return 4
	}
	return 0
}

func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
