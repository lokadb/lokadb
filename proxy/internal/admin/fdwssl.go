package admin

// The TLS settings of the organization's DSNs on its FDW server (review 3, R3-5). postgres_fdw
// connects with the server's options and the user mapping's user and password only: a DSN with
// sslmode=verify-full gave the server nothing of it, and postgres_fdw connected with libpq's
// default (prefer: no check of the certificate, plain text if the server offers no TLS).

import (
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

// fdwSSLKeys: the libpq options of a DSN that the FDW server takes over. Paths (sslrootcert,
// sslcrl, sslcrldir) are read by the cloud's Postgres, on its own file system.
var fdwSSLKeys = []string{"sslmode", "sslrootcert", "sslcrl", "sslcrldir", "sslsni", "ssl_min_protocol_version", "ssl_max_protocol_version"}

// FDWSSLOptions: "key 'value'" server options for the TLS settings of dsns, the first DSN that
// sets a key giving its value (move-org: -customer-app, then -customer).
func FDWSSLOptions(dsns ...string) ([]string, error) {
	var params []map[string]string
	for _, d := range dsns {
		if d == "" {
			continue
		}
		p, err := conninfoParams(d)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	var out []string
	for _, k := range fdwSSLKeys {
		for _, p := range params {
			if v, ok := p[k]; ok && v != "" {
				out = append(out, fmt.Sprintf("%s %s", k, lit(v)))
				break
			}
		}
	}
	return out, nil
}

// conninfoParams: the keywords of a DSN, a URL (postgres://...?sslmode=...) or keyword=value
// pairs (values quoted with ' and \ as libpq takes them).
func conninfoParams(dsn string) (map[string]string, error) {
	out := map[string]string{}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, err
		}
		for k, v := range u.Query() {
			if len(v) > 0 {
				out[k] = v[len(v)-1]
			}
		}
		return out, nil
	}
	s := dsn
	for {
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		if s == "" {
			return out, nil
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("DSN: a keyword without =value near %q", s)
		}
		key := strings.TrimSpace(s[:eq])
		s = strings.TrimLeftFunc(s[eq+1:], unicode.IsSpace)
		var val strings.Builder
		if strings.HasPrefix(s, "'") {
			i, closed := 1, false
			for ; i < len(s); i++ {
				switch c := s[i]; {
				case c == '\\' && i+1 < len(s):
					i++
					val.WriteByte(s[i])
				case c == '\'':
					closed = true
				default:
					val.WriteByte(c)
				}
				if closed {
					break
				}
			}
			if !closed {
				return nil, fmt.Errorf("DSN: an unterminated quoted value of %s", key)
			}
			s = s[i+1:]
		} else {
			i := 0
			for ; i < len(s) && !unicode.IsSpace(rune(s[i])); i++ {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				val.WriteByte(s[i])
			}
			s = s[i:]
		}
		out[key] = val.String() // a later keyword wins, as in libpq
	}
}
