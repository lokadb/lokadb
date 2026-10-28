// Package ext: the parts of lokadb check built on its extension point (check.Extension): the
// routing suggestions (-suggest, -patch), the feed index advice and the latency estimate
// (-rtt). lokadb's main imports it, so every lokadb binary and image has them; a program that
// runs check.Run itself imports it the same way (tasks/check-split.md).
package ext

import "github.com/lokadb/lokadb/proxy/check"

func init() {
	// in the order check made them before the split: the suggestions with the workload, the
	// feed index after it, the latency after the profile
	check.Register(&routing{})
	check.Register(feeds{})
	check.Register(&latency{})
}
