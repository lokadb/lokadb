// Package rel names tables by key (shared by admin, the proxy, check and migrate).
package rel

import (
	"crypto/md5"
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

// A table is named in lokadb's metadata (lokadb.tables, lookups, routing, the spec) by its key:
// the bare name for a table of schema public, as before schemas other than public were
// supported, and schema.name for any other schema. The schema part is in double quotes when
// it holds a dot or a quote ("a.b".t); the name part is the name as it is, so that a helper
// table's key is the table's key with a suffix: app.documents__cloud is in schema app.

// SplitKey: the schema ("public" for a bare key) and the name of the table a key names.
func SplitKey(k string) (schema, name string) {
	if strings.HasPrefix(k, `"`) {
		for i := 1; i < len(k); i++ {
			if k[i] != '"' {
				continue
			}
			if i+1 < len(k) && k[i+1] == '"' {
				i++
				continue
			}
			if i+1 < len(k) && k[i+1] == '.' {
				return strings.ReplaceAll(k[1:i], `""`, `"`), k[i+2:]
			}
			break
		}
		return "public", k
	}
	if s, n, ok := strings.Cut(k, "."); ok {
		return s, n
	}
	return "public", k
}

// RelKey: the key of table name of schema schema.
func RelKey(schema, name string) string {
	if schema == "" {
		schema = "public" // unqualified: public's (a name with a dot is qualified all the same)
	}
	if schema == "public" && !strings.Contains(name, ".") && !strings.HasPrefix(name, `"`) {
		return name
	}
	if strings.ContainsAny(schema, `."`) {
		schema = `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	}
	return schema + "." + name
}

// KeyOfSQL: the key of a relation in SQL, from text expressions for its schema and its name
// (as lokadb.key_of, inline: a database enabled by an older version has no such function).
func KeyOfSQL(nsp, name string) string {
	return "(CASE WHEN " + nsp + " = 'public' AND strpos(" + name + ", '.') = 0 AND left(" + name + ", 1) <> '\"' THEN " + name +
		" ELSE CASE WHEN " + nsp + " ~ '[.\"]' THEN '\"' || replace(" + nsp + ", '\"', '\"\"') || '\"' ELSE " + nsp + " END || '.' || " + name + " END)"
}

// QuotedSQL: the table key e (a text expression) as its quoted, qualified name in SQL, for
// to_regclass and the like (lokadb.rel inline: a database enabled by an older version has no
// such function). A bare key of public with a dot ("acct.docs__refs") is not schema acct.
func QuotedSQL(e string) string {
	return strings.NewReplacer("{e}", e).Replace(`(CASE WHEN strpos({e}, '.') = 0 THEN quote_ident({e})
		WHEN left({e}, 1) = '"' THEN quote_ident(replace((regexp_match({e}, '^"((?:[^"]|"")*)"\.', 's'))[1], '""', '"'))
			|| '.' || quote_ident((regexp_match({e}, '^"(?:[^"]|"")*"\.(.*)$', 's'))[1])
		ELSE quote_ident(split_part({e}, '.', 1)) || '.' || quote_ident(substr({e}, strpos({e}, '.') + 1)) END)`)
}

// NameMax: the bytes of a Postgres name (NAMEDATALEN - 1). A longer one is cut silently.
const NameMax = 63

// HelperName: the name of a helper of a table named name (its partitions, refs, sequences,
// indexes: name + suffix). Postgres cuts a name to NameMax bytes, so two helpers of a long
// name (__org1 and __org12, or a table and its __cloud) could get the same name: a name that
// does not fit keeps the suffix whole and cuts the table's name, marked by the first 8 hex
// digits of its md5. One that fits is name + suffix, as before.
func HelperName(name, suffix string) string {
	if len(name)+len(suffix) <= NameMax {
		return name + suffix
	}
	keep := NameMax - len(suffix) - 9
	if keep < 0 {
		keep = 0
	}
	cut := name
	for len(cut) > keep { // whole characters, as Postgres' left()
		_, size := utf8.DecodeLastRuneInString(cut)
		cut = cut[:len(cut)-size]
	}
	sum := md5.Sum([]byte(name))
	return cut + "_" + hex.EncodeToString(sum[:])[:8] + suffix
}

// Helper: the key of the helper suffix of the table of key k (in the table's schema).
func Helper(k, suffix string) string {
	s, n := SplitKey(k)
	if n == k {
		return HelperName(n, suffix)
	}
	return RelKey(s, HelperName(n, suffix))
}

// HelperNameSQL is HelperName in SQL, of text expressions name and suffix.
func HelperNameSQL(name, suffix string) string {
	return "(CASE WHEN octet_length(" + name + " || " + suffix + ") <= 63 THEN " + name + " || " + suffix +
		" ELSE (SELECT left(" + name + ", i) FROM generate_series(length(" + name + "), 0, -1) i WHERE octet_length(left(" + name + ", i)) <= greatest(63 - octet_length(" + suffix + ") - 9, 0) LIMIT 1)" +
		" || '_' || left(md5(" + name + "), 8) || " + suffix + " END)"
}

// HelperSQL is Helper in SQL, of a text expression for the key and one for the suffix. It
// needs no function of lokadb (a database enabled by an older version has none).
func HelperSQL(key, suffix string) string {
	prefix := "coalesce(substring(" + key + ` from '^((?:"(?:[^"]|"")*"|[^."]*)\.)'), '')`
	name := "substr(" + key + ", length(" + prefix + ") + 1)"
	return "(" + prefix + " || " + HelperNameSQL(name, suffix) + ")"
}

// HelperSuffix: the suffix of h if it is a helper of the table of key k (Helper(k, suffix)),
// as built or shortened; ok false when it is not one.
func HelperSuffix(k, h string) (suffix string, ok bool) {
	ks, kn := SplitKey(k)
	hs, hn := SplitKey(h)
	if ks != hs {
		return "", false
	}
	if s, ok := strings.CutPrefix(hn, kn); ok && strings.HasPrefix(s, "__") {
		return s, true
	}
	sum := md5.Sum([]byte(kn))
	mark := "_" + hex.EncodeToString(sum[:])[:8] + "__"
	if i := strings.Index(hn, mark); i >= 0 && strings.HasPrefix(kn, hn[:i]) {
		return hn[i+len(mark)-2:], true
	}
	return "", false
}
