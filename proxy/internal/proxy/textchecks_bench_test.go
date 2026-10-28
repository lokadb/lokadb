package proxy

import (
	"strings"
	"testing"
)

// The cheap tests every statement's text goes through before any parse (BACKLOG block 9, item
// 14; third review of performance, perf3-17): on a simple-protocol INSERT of a thousand rows of
// literals, each copied the text to lower case.
func BenchmarkTextChecks(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO documents (organization_id, title, content) VALUES ")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(1, 'Title number one of the many', 'Some content of the document, a sentence or two long')")
	}
	sql := sb.String()
	b.SetBytes(int64(len(sql)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = effectOnPath(sql)
		_, _ = deallocates(sql)
		_ = dropsListen(sql)
		_ = mentionsTenant(sql, "")
		_, _, _ = executed(sql)
		_ = declaresHold(sql)
		_ = declared(sql)
		_ = fetched(sql)
		_, _ = directSetting(sql)
	}
}
