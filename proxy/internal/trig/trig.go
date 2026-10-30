// Package trig: the names a row trigger of a LokaDB table sees (risk hunt R04). Postgres clones
// a row trigger of a partitioned table to every partition and fires it there: TG_TABLE_NAME,
// TG_TABLE_SCHEMA and TG_RELID name the partition (documents__cloud, documents__org1), not the
// table the application created the trigger on. Audit tables keyed by the table's name,
// pg_notify(TG_TABLE_NAME, …) listeners and dynamic SQL on TG_RELID then see another table.
//
// Rewrite replaces those variables in a PL/pgSQL body with calls of lokadb.tg_name,
// lokadb.tg_schema and lokadb.tg_relid, which give the LokaDB parent of a partition and the
// relation itself otherwise (a function shared with other tables keeps working for them).
// Restore undoes it (disable). Only the variables are replaced: string literals, quoted
// identifiers, comments and dollar-quoted strings in the body are left as they are.
package trig

import "strings"

const (
	nameCall   = "lokadb.tg_name(TG_RELID, TG_TABLE_NAME)"
	schemaCall = "lokadb.tg_schema(TG_RELID, TG_TABLE_SCHEMA)"
	relidCall  = "lokadb.tg_relid(TG_RELID)"
)

// replacement: what a trigger variable becomes ("" = none).
func replacement(ident string) string {
	switch strings.ToLower(ident) {
	case "tg_table_name", "tg_relname":
		return nameCall
	case "tg_table_schema":
		return schemaCall
	case "tg_relid":
		return relidCall
	}
	return ""
}

// Mentions: body reads a trigger variable Rewrite replaces (a quick test on the text).
func Mentions(body string) bool {
	l := strings.ToLower(body)
	return strings.Contains(l, "tg_table_name") || strings.Contains(l, "tg_relname") ||
		strings.Contains(l, "tg_table_schema") || strings.Contains(l, "tg_relid")
}

// Rewritten: body has been through Rewrite.
func Rewritten(body string) bool {
	return strings.Contains(body, "lokadb.tg_name(") || strings.Contains(body, "lokadb.tg_schema(") || strings.Contains(body, "lokadb.tg_relid(")
}

// Rewrite returns body with the trigger variables replaced; body itself when it has none or
// was rewritten already.
func Rewrite(body string) string {
	if !Mentions(body) || Rewritten(body) {
		return body
	}
	var b strings.Builder
	n := len(body)
	for i := 0; i < n; {
		c := body[i]
		switch {
		case c == '\'': // a string literal ('' inside; E'…' takes backslash escapes)
			esc := i > 0 && (body[i-1] == 'E' || body[i-1] == 'e') && (i < 2 || !isIdent(body[i-2]))
			j := i + 1
			for j < n {
				if esc && body[j] == '\\' {
					j += 2
					continue
				}
				if body[j] == '\'' {
					if j+1 < n && body[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			j = min(j+1, n)
			b.WriteString(body[i:j])
			i = j
		case c == '"': // a quoted identifier
			j := i + 1
			for j < n {
				if body[j] == '"' {
					if j+1 < n && body[j+1] == '"' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			j = min(j+1, n)
			b.WriteString(body[i:j])
			i = j
		case c == '-' && i+1 < n && body[i+1] == '-': // a comment to the end of the line
			j := strings.IndexByte(body[i:], '\n')
			if j < 0 {
				j = n - i
			}
			b.WriteString(body[i : i+j])
			i += j
		case c == '/' && i+1 < n && body[i+1] == '*': // a block comment (they nest)
			depth, j := 1, i+2
			for j < n && depth > 0 {
				switch {
				case strings.HasPrefix(body[j:], "/*"):
					depth, j = depth+1, j+2
				case strings.HasPrefix(body[j:], "*/"):
					depth, j = depth-1, j+2
				default:
					j++
				}
			}
			b.WriteString(body[i:j])
			i = j
		case c == '$' && (i == 0 || !isIdent(body[i-1])): // a dollar-quoted string inside the body
			if tag, ok := dollarTag(body[i:]); ok {
				end := strings.Index(body[i+len(tag):], tag)
				j := n
				if end >= 0 {
					j = i + len(tag) + end + len(tag)
				}
				b.WriteString(body[i:j])
				i = j
				continue
			}
			b.WriteByte(c)
			i++
		case isIdentStart(c):
			j := i + 1
			for j < n && isIdent(body[j]) {
				j++
			}
			word := body[i:j]
			// a field (rec.tg_relid) or a qualified name is not the variable
			if r := replacement(word); r != "" && !afterDot(body, i) {
				b.WriteString(r)
			} else {
				b.WriteString(word)
			}
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// Restore undoes Rewrite (TG_RELNAME comes back as TG_TABLE_NAME, its synonym).
func Restore(body string) string {
	return strings.NewReplacer(nameCall, "TG_TABLE_NAME", schemaCall, "TG_TABLE_SCHEMA", relidCall, "TG_RELID").Replace(body)
}

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

func isIdent(c byte) bool { return isIdentStart(c) || c >= '0' && c <= '9' || c == '$' }

func afterDot(s string, i int) bool {
	for j := i - 1; j >= 0; j-- {
		switch s[j] {
		case ' ', '\t', '\n', '\r':
			continue
		case '.':
			return true
		}
		return false
	}
	return false
}

// dollarTag: the opening tag ($$ or $name$) s starts with.
func dollarTag(s string) (string, bool) {
	for j := 1; j < len(s); j++ {
		if s[j] == '$' {
			return s[:j+1], true
		}
		if !isIdent(s[j]) || s[j] == '$' || j == 1 && s[j] >= '0' && s[j] <= '9' {
			return "", false
		}
	}
	return "", false
}
