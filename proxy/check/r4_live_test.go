package check

// Review 3 (R4) on live servers of LOKADB_PG_VERSIONS (version_live_test.go).

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// R4-27: pg_stat_statements keeps some statements as the application sent them (DECLARE ...
// CURSOR before 16, COPY (SELECT ...) TO): the values in them do not reach the report, which
// the runner sends to Cloud; the statements are still judged (query_refused).
func TestLiveR4ValuesNotInReport(t *testing.T) {
	srv := versionServers(t)
	for v := 13; v <= 18; v++ {
		base, ok := srv[v]
		if !ok {
			continue
		}
		t.Run(strconv.Itoa(v), func(t *testing.T) {
			ctx := context.Background()
			dsn := freshDB(t, base, "b11_r4_values",
				`CREATE EXTENSION pg_stat_statements`,
				`CREATE TABLE orgs (id bigint PRIMARY KEY)`,
				`CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES orgs, title text)`,
				`DO $$BEGIN CREATE ROLE b11app LOGIN PASSWORD 'pw'; EXCEPTION WHEN duplicate_object THEN END$$`,
				`GRANT SELECT ON orgs, documents TO b11app`,
				`SELECT pg_stat_statements_reset()`)
			u, rest, _ := strings.Cut(dsn, "@")
			app, err := pgx.Connect(ctx, u[:strings.Index(u, "://")+3]+"b11app:pw@"+rest)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range []string{"BEGIN", "DECLARE c1 CURSOR FOR SELECT * FROM documents WHERE title = 'ann.secret@example.com'", "CLOSE c1", "COMMIT"} {
				if _, err := app.Exec(ctx, s); err != nil {
					t.Fatal(s, err)
				}
			}
			if _, err := app.PgConn().CopyTo(ctx, io.Discard, "COPY (SELECT * FROM documents WHERE title = 'bob.secret@example.com') TO STDOUT"); err != nil {
				t.Fatal(err)
			}
			app.Close(ctx)
			spec := admin.Spec{Tables: []admin.SpecTable{{Name: "documents", TenantColumn: "organization_id"}}}.Sorted()
			rep, err := Run(ctx, Options{DSN: dsn, Spec: &spec})
			if err != nil {
				t.Fatal(err)
			}
			b, _ := json.Marshal(rep)
			for _, secret := range []string{"ann.secret", "bob.secret"} {
				if strings.Contains(string(b), secret) {
					t.Errorf("PostgreSQL %d: the report carries the application's value %s", v, secret)
				}
			}
			refused := false
			for _, f := range rep.Findings {
				for _, p := range f.Patterns {
					refused = refused || f.Code == "query_refused" && strings.Contains(p.SQL, "DECLARE c1 CURSOR")
				}
			}
			if !refused {
				t.Errorf("PostgreSQL %d: the DECLARE without the organization is not query_refused", v)
			}
		})
	}
}

// R4-40: Laravel Jetstream declares no foreign keys (foreignId() without constrained()): its
// membership table team_user is kept in the cloud as a link table by its columns' names, as it
// is with the foreign keys.
func TestLiveR4OrgTableJetstream(t *testing.T) {
	for name, fk := range map[string]string{"r4_jetstream": "", "r4_jetstream_fk": " REFERENCES teams"} {
		users := ""
		if fk != "" {
			users = " REFERENCES users"
		}
		_, conn := orgDB(t, name,
			`CREATE TABLE users (id bigserial PRIMARY KEY, name text, email text, password text, current_team_id bigint)`,
			`CREATE TABLE teams (id bigserial PRIMARY KEY, user_id bigint NOT NULL`+users+`, name text, personal_team boolean)`,
			`CREATE TABLE team_user (id bigserial PRIMARY KEY, team_id bigint NOT NULL`+fk+`, user_id bigint NOT NULL`+users+`, role text, created_at timestamp, updated_at timestamp)`,
			`CREATE TABLE team_invitations (id bigserial PRIMARY KEY, team_id bigint NOT NULL REFERENCES teams ON DELETE CASCADE, email text, role text)`,
			`INSERT INTO users (name) VALUES ('a'), ('b')`,
			`INSERT INTO teams (user_id, name) VALUES (1, 'a'), (2, 'b')`,
			`INSERT INTO team_user (team_id, user_id, role) VALUES (1, 2, 'editor'), (2, 1, 'admin')`,
		)
		d := orgDiscover(t, conn, "teams")
		if r := roleOf(t, d, "team_user"); r.Role != RoleCloud || r.Code != CodeLink {
			t.Errorf("%s: team_user is %s %s (%s), not a link table kept in the cloud", name, r.Role, r.Code, r.Reason)
		}
		declared := false
		if d.Spec.Routing == nil {
			d.Spec.Routing = &admin.Routing{}
		}
		for _, ct := range d.Spec.Routing.CloudTables {
			declared = declared || ct.Table == "public.team_user" || ct.Table == "team_user"
		}
		for _, st := range d.Spec.Tables {
			if st.Name == "team_user" || st.Name == "public.team_user" {
				t.Errorf("%s: team_user is in the spec's tables: %+v", name, st)
			}
		}
		if !declared {
			t.Errorf("%s: team_user is not declared as routing.cloud_tables: %+v", name, d.Spec.Routing)
		}
	}
}

// R4-38: pg_hba (LOKADB_R4_HBA_DSN's server) rejects every address for database otherdb only,
// then lets every role in from everywhere: the application's role reaches the cloud past the
// proxy, and check says so.
func TestLiveR4HbaOtherDatabase(t *testing.T) {
	base := os.Getenv("LOKADB_R4_HBA_DSN")
	if base == "" {
		t.Skip("LOKADB_R4_HBA_DSN not set")
	}
	ctx := context.Background()
	dsn := freshDB(t, base, "b11_r4_hba",
		`DO $$BEGIN CREATE ROLE b11app LOGIN PASSWORD 'pw'; EXCEPTION WHEN duplicate_object THEN END$$`,
		`CREATE TABLE orgs (id bigint PRIMARY KEY)`,
		`CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES orgs, title text)`,
		`GRANT SELECT ON orgs, documents TO b11app`)
	var rules int
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRow(ctx, `SELECT count(*) FROM pg_hba_file_rules WHERE 'otherdb' = ANY (database) AND auth_method = 'reject'`).Scan(&rules); err != nil || rules == 0 {
		t.Fatalf("the server's pg_hba has no reject for otherdb (%d, %v)", rules, err)
	}
	c.Close(ctx)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "documents", TenantColumn: "organization_id"}}}.Sorted()
	rep, err := Run(ctx, Options{DSN: dsn, Spec: &spec, AppRoles: []string{"b11app"}, ProxyHosts: []string{"10.99.99.99"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range rep.Findings {
		if f.Code == "cloud_direct_access" && f.Object == "b11app" {
			return
		}
	}
	t.Errorf("no cloud_direct_access for b11app: %+v", rep.Findings)
}
