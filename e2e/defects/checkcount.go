package main

// lokadb check counts rows without an organization exactly on small tables and estimates them
// from a sample on big ones (block 11, item 5), and says which it did.

import (
	"encoding/json"
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "CK1", name: "check estimates orphan rows from a sample above -count-limit, counts them exactly below, and says which", run: checkOrphanEstimate},
	)
}

// orphanMeasure: the measure of the orphan_rows finding in a JSON report.
func orphanMeasure(out string) (rows int64, method string, ok bool) {
	i := strings.Index(out, "{")
	if i < 0 {
		return 0, "", false
	}
	var rep struct {
		Findings []struct {
			Code    string `json:"code"`
			Measure *struct {
				Rows   int64  `json:"rows"`
				Method string `json:"method"`
			} `json:"measure"`
		} `json:"findings"`
	}
	if err := json.Unmarshal([]byte(out[i:]), &rep); err != nil {
		return 0, "", false
	}
	for _, f := range rep.Findings {
		if f.Code == "orphan_rows" && f.Measure != nil {
			return f.Measure.Rows, f.Measure.Method, true
		}
	}
	return 0, "", false
}

// CK1. 300000 more versions, every tenth without a document: 30000 orphans; the sample reads about a third.
func checkOrphanEstimate(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO document_versions (document_id, content) SELECT CASE WHEN g % 10 = 0 THEN NULL ELSE 1 + g % 200 END, 'o' || g FROM generate_series(1, 300000) g",
		"ANALYZE document_versions"); err != nil {
		return false, "", err
	}
	exactOut, _ := e.checkWith(spec, "-json")
	sampleOut, _ := e.checkWith(spec, "-json", "-count-limit", "5000")
	forcedOut, _ := e.checkWith(spec, "-json", "-count-limit", "-1")
	er, em, eok := orphanMeasure(exactOut)
	sr, sm, sok := orphanMeasure(sampleOut)
	fr, fm, fok := orphanMeasure(forcedOut)
	ok := eok && sok && fok && er == 30000 && em == "exact" && fr == 30000 && fm == "exact" &&
		sm == "sample" && sr > 20000 && sr < 40000 && sr != 30000
	detail := fmt.Sprintf("default: %d (%s); -count-limit 5000: %d (%s); -count-limit -1: %d (%s)", er, em, sr, sm, fr, fm)
	if !sok {
		detail += "; -count-limit 5000: " + headLine(sampleOut)
	}
	return ok, detail, nil
}

func headLine(s string) string {
	l, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	if len(l) > 200 {
		l = l[:200]
	}
	return l
}
