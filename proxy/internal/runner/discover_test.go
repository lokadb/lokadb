package runner

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

// TestDiscoverDB: the application's own environment names the database, in the shapes
// frameworks use; the password is in the DSN and never in what is described.
func TestDiscoverDB(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string // host:port/db as user, sslmode
	}{
		{"url", map[string]string{"DATABASE_URL": "postgres://forge:p%40ss@pgsql:5432/coders_talk"}, "pgsql:5432/coders_talk as forge disable"},
		{"url with ssl", map[string]string{"DATABASE_URL": "postgresql://u:p@db.example.com/shop?sslmode=require"}, "db.example.com:5432/shop as u require"},
		{"laravel", map[string]string{"DB_CONNECTION": "pgsql", "DB_HOST": "pgsql", "DB_PORT": "5433", "DB_DATABASE": "coders", "DB_USERNAME": "forge", "DB_PASSWORD": "s3 cr'et"}, "pgsql:5433/coders as forge disable"},
		{"django", map[string]string{"DB_HOST": "10.0.0.5", "DB_NAME": "app", "DB_USER": "django", "DB_PASSWORD": "x"}, "10.0.0.5:5432/app as django prefer"},
		{"libpq", map[string]string{"PGHOST": "db.internal", "PGDATABASE": "d", "PGUSER": "u", "PGPASSWORD": "x"}, "db.internal:5432/d as u prefer"},
		{"lokadb_db wins", map[string]string{"LOKADB_DB": "host=admin-pg dbname=x user=admin password=a sslmode=disable", "DATABASE_URL": "postgres://forge:p@other/y"}, "admin-pg:5432/x as admin disable"},
	} {
		f, err := DiscoverDB(env(tc.env))
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got := f.Host + ":" + f.Port + "/" + f.DBName + " as " + f.User + " " + f.SSL; got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
		// the DSN works as a DSN, with the password intact
		cfg, err := pgconn.ParseConfig(f.DSN)
		if err != nil || cfg.Host != f.Host || cfg.User != f.User {
			t.Errorf("%s: DSN %v %+v", tc.name, err, cfg)
		}
		if strings.Contains(f.Describe(), cfg.Password) && cfg.Password != "" && len(cfg.Password) > 2 {
			t.Errorf("%s: the description holds the password: %s", tc.name, f.Describe())
		}
		if strings.Contains(f.Upstream(), "password") || strings.Contains(f.Upstream(), "user") {
			t.Errorf("%s: the upstream holds a login: %s", tc.name, f.Upstream())
		}
	}
	if f, _ := DiscoverDB(env(map[string]string{"DB_HOST": "pgsql", "DB_DATABASE": "d", "DB_USERNAME": "u", "DB_PASSWORD": "s3 cr'et"})); true {
		if cfg, err := pgconn.ParseConfig(f.DSN); err != nil || cfg.Password != "s3 cr'et" {
			t.Errorf("a password with a space and a quote: %v %q", err, cfg.Password)
		}
	}
	for _, bad := range []map[string]string{{}, {"DB_HOST": "pgsql"}, {"DATABASE_URL": "postgres://:::"}, {"DATABASE_URL": "mysql://u:p@h/d"}} {
		if _, err := DiscoverDB(env(bad)); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
}
