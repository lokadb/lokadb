package proxy

import (
	"reflect"
	"testing"
)

// parseArrayText reads a one-dimensional array in Postgres text form by the rules of array_in
// (review 2 of the routing, R8: a quoted element with a comma was split at it, and a backslash
// escape was kept).
func TestParseArrayText(t *testing.T) {
	const u1, u2 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "B0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A12"
	for _, c := range []struct {
		in   string
		want []string // nil with ok false: refused
		ok   bool
	}{
		{`{1,2,3}`, []string{"1", "2", "3"}, true},
		{`{ 1 , 2 }`, []string{"1", "2"}, true},
		{`{"1","2"}`, []string{"1", "2"}, true},
		{`{"a,b",c}`, []string{"a,b", "c"}, true},             // R8
		{`{"a\"b"}`, []string{`a"b`}, true},                   // R8
		{`{"a\\b"}`, []string{`a\b`}, true},                   // a quoted backslash
		{`{a\,b,c}`, []string{"a,b", "c"}, true},              // an escaped comma, unquoted
		{`{"a b", c d}`, []string{"a b", "c d"}, true},        // spaces inside stay
		{`{" a "}`, []string{" a "}, true},                    // quoted: as written
		{`{a\ }`, []string{"a "}, true},                       // an escaped trailing space is the element's
		{`{"{x}","y}"}`, []string{"{x}", "y}"}, true},         // braces in quotes
		{`{` + u1 + `,"` + u2 + `"}`, []string{u1, u2}, true}, // uuids
		{`{"NULL"}`, []string{"NULL"}, true},                  // the string, not a NULL
		{`{\NULL}`, []string{"NULL"}, true},                   // an escaped one too
		{`{}`, nil, true},
		{`{ }`, nil, true},
		{` {1} `, []string{"1"}, true},
		{`{NULL}`, nil, false},
		{`{1,null}`, nil, false},
		{`{ Null , 1}`, nil, false},
		{`{{1,2}}`, nil, false}, // a nested array
		{`{1,{2}}`, nil, false},
		{`{1,,2}`, nil, false}, // an empty element
		{`{1,}`, nil, false},
		{`{,1}`, nil, false},
		{`{"a}`, nil, false},        // an unterminated quote
		{`{"a"b}`, nil, false},      // something after a closing quote
		{`{a"b}`, nil, false},       // a quote inside an element
		{`{a\}`, nil, false},        // a backslash at the end
		{`[1:2]={1,2}`, nil, false}, // a dimension prefix
		{`1,2`, nil, false},
		{``, nil, false},
		{`{`, nil, false},
	} {
		got, ok := parseArrayText(c.in)
		if ok != c.ok || ok && !reflect.DeepEqual(got, c.want) && !(len(got) == 0 && len(c.want) == 0) {
			t.Errorf("parseArrayText(%s) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}
