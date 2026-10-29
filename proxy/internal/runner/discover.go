package runner

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Found: the database the runner found in the application's environment. The DSN holds the
// password and stays in this process; Describe is what may leave it.
type Found struct {
	DSN    string // key=value, with the password
	Host   string
	Port   string
	DBName string
	User   string
	SSL    string
	Source string // which variable(s) it came from, for the panel
}

// Describe: "pgsql:5432/coders_talk as forge" (from DATABASE_URL): no password.
func (f Found) Describe() string {
	return fmt.Sprintf("%s:%s/%s as %s", f.Host, f.Port, f.DBName, f.User)
}

// Upstream: the proxy's upstream: the server, not the application's user or password.
func (f Found) Upstream() string {
	return fmt.Sprintf("host=%s port=%s sslmode=%s", dsnValue(f.Host), f.Port, f.SSL)
}

// DiscoverDB finds the application's database in its environment (get reads one variable):
// LOKADB_DB wins, then DATABASE_URL (Rails, Heroku-style, most frameworks), then Laravel's
// DB_HOST/DB_PORT/DB_DATABASE/DB_USERNAME/DB_PASSWORD, Django-style DB_NAME/DB_USER, and last
// libpq's PG* variables. A host that is a bare name (a docker-compose service) gets
// sslmode=disable unless the environment says otherwise: a stock Postgres container has no SSL.
func DiscoverDB(get func(string) string) (Found, error) {
	first := func(names ...string) (string, string) {
		for _, n := range names {
			if v := strings.TrimSpace(get(n)); v != "" {
				return v, n
			}
		}
		return "", ""
	}
	if v := strings.TrimSpace(get("LOKADB_DB")); v != "" {
		return fromDSN(v, "LOKADB_DB")
	}
	if v, n := first("DATABASE_URL", "DB_URL", "POSTGRES_URL", "DATABASE_DSN"); v != "" {
		return fromDSN(v, n)
	}
	host, hn := first("DB_HOST", "POSTGRES_HOST", "PGHOST", "DATABASE_HOST")
	if host == "" {
		return Found{}, fmt.Errorf("no database in the environment: the runner looks for LOKADB_DB, DATABASE_URL, DB_HOST with DB_DATABASE/DB_USERNAME/DB_PASSWORD, or the PG* variables. Pass the application's env_file to this container, or set LOKADB_DB")
	}
	port, _ := first("DB_PORT", "POSTGRES_PORT", "PGPORT", "DATABASE_PORT")
	name, _ := first("DB_DATABASE", "DB_NAME", "POSTGRES_DB", "PGDATABASE", "DATABASE_NAME")
	user, _ := first("DB_USERNAME", "DB_USER", "POSTGRES_USER", "PGUSER", "DATABASE_USER")
	pass, _ := first("DB_PASSWORD", "POSTGRES_PASSWORD", "PGPASSWORD", "DATABASE_PASSWORD")
	ssl, _ := first("DB_SSLMODE", "PGSSLMODE", "DATABASE_SSLMODE")
	if port == "" {
		port = "5432"
	}
	if name == "" || user == "" {
		return Found{}, fmt.Errorf("the environment names the database host (%s) but not %s: add it to the environment the container gets",
			hn, map[bool]string{true: "the database name", false: "the user"}[name == ""])
	}
	f := Found{Host: host, Port: port, DBName: name, User: user, SSL: ssl, Source: hn + ", DB_*"}
	f.finish(pass)
	return f, nil
}

func (f *Found) finish(pass string) {
	if f.SSL == "" {
		switch {
		case !strings.Contains(f.Host, ".") && !strings.Contains(f.Host, ":") && f.Host != "localhost":
			f.SSL = "disable" // a compose service name
		case ManagedProvider(f.Host) != "":
			f.SSL = "require" // a managed service: never in clear text over the internet
		default:
			f.SSL = "prefer"
		}
	}
	f.DSN = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		dsnValue(f.Host), f.Port, dsnValue(f.DBName), dsnValue(f.User), dsnValue(pass), f.SSL)
}

// fromDSN reads a postgres:// URL or a key=value DSN.
func fromDSN(v, source string) (Found, error) {
	f := Found{Source: source, Port: "5432"}
	pass := ""
	if strings.HasPrefix(v, "postgres://") || strings.HasPrefix(v, "postgresql://") {
		u, err := url.Parse(v)
		if err != nil {
			return Found{}, fmt.Errorf("%s is not a valid postgres:// URL", source)
		}
		f.Host = u.Hostname()
		if p := u.Port(); p != "" {
			f.Port = p
		}
		f.DBName = strings.TrimPrefix(u.Path, "/")
		f.User = u.User.Username()
		pass, _ = u.User.Password()
		f.SSL = u.Query().Get("sslmode")
		if f.Host == "" && u.Query().Get("host") != "" { // host in the query: a socket path or a name
			f.Host = u.Query().Get("host")
		}
	} else {
		kv, err := splitKV(v)
		if err != nil {
			return Found{}, fmt.Errorf("%s is not a key=value DSN or a postgres:// URL", source)
		}
		f.Host, f.DBName, f.User, pass, f.SSL = kv["host"], kv["dbname"], kv["user"], kv["password"], kv["sslmode"]
		if p := kv["port"]; p != "" {
			f.Port = p
		}
	}
	if f.Host == "" || f.DBName == "" {
		return Found{}, fmt.Errorf("%s names no host or no database", source)
	}
	if _, _, err := net.SplitHostPort(net.JoinHostPort(f.Host, f.Port)); err != nil {
		return Found{}, fmt.Errorf("%s: bad host or port", source)
	}
	f.finish(pass)
	return f, nil
}

// splitKV: a libpq key=value string, values optionally in single quotes.
func splitKV(s string) (map[string]string, error) {
	out := map[string]string{}
	for i := 0; i < len(s); {
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			break
		}
		j := strings.IndexByte(s[i:], '=')
		if j < 0 {
			return nil, fmt.Errorf("no =")
		}
		key := s[i : i+j]
		i += j + 1
		var val strings.Builder
		if i < len(s) && s[i] == '\'' {
			i++
			for i < len(s) && s[i] != '\'' {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				val.WriteByte(s[i])
				i++
			}
			i++
		} else {
			for i < len(s) && s[i] != ' ' {
				val.WriteByte(s[i])
				i++
			}
		}
		out[key] = val.String()
	}
	return out, nil
}

// dsnValue: one value of a key=value DSN, quoted.
func dsnValue(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}
