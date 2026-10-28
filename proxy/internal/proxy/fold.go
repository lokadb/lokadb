package proxy

import "strings"

// hasFold: s contains sub, ASCII letters in any case; sub is lower case. The cheap tests before
// a parse used strings.ToLower(sql) each, a copy of the whole text per test (BACKLOG block 9,
// item 14; third review of performance, perf3-17: 8-10 copies of a simple query's text, tens of
// megabytes of garbage for an INSERT of megabytes of VALUES).
func hasFold(s, sub string) bool {
	n := len(sub)
	if n == 0 {
		return true
	}
	if len(s) < n {
		return false
	}
	lo, up := sub[0], sub[0]
	if 'a' <= lo && lo <= 'z' {
		up = lo - 'a' + 'A'
	}
	last := len(s) - n // the last place sub may start at
	// next: the next place of b at or after from (IndexByte is the fast scan), -1 for none
	next := func(b byte, from int) int {
		if k := strings.IndexByte(s[from:last+1], b); k >= 0 {
			return from + k
		}
		return -1
	}
	nl, nu := next(lo, 0), -1
	if up != lo {
		nu = next(up, 0)
	}
	for nl >= 0 || nu >= 0 {
		i := nl
		if i < 0 || nu >= 0 && nu < i {
			i = nu
		}
		j := 1
		for j < n && lowerByte(s[i+j]) == sub[j] {
			j++
		}
		if j == n {
			return true
		}
		if i == nl { // each position is searched past once
			if nl = -1; i < last {
				nl = next(lo, i+1)
			}
		} else {
			if nu = -1; i < last {
				nu = next(up, i+1)
			}
		}
	}
	return false
}

// hasPrefixFold: s begins with prefix (lower case), ASCII letters in any case.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && hasFold(s[:len(prefix)], prefix)
}

func lowerByte(b byte) byte {
	if 'A' <= b && b <= 'Z' {
		return b + 'a' - 'A'
	}
	return b
}
