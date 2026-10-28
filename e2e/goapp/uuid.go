package main

// The uuid mode (block 18, tasks/uuid-tenant.md): an application whose organizations are
// numbered by uuid (accounts.id, projects.account_id; tasks under projects), as Rails, Django
// and Laravel applications with UUID keys have them. It talks pgx, the uuids binary, to its own
// database (DATABASE_URL names uuidapp): goapp uuid migrate|seed|load|r1|coop|offline.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	accountA = mustUUID("aaaaaaaa-0000-4000-8000-00000000000a")
	accountB = mustUUID("bbbbbbbb-0000-4000-8000-00000000000b")
)

func mustUUID(s string) pgtype.UUID {
	var u pgtype.UUID
	if err := u.Scan(s); err != nil {
		panic(err)
	}
	return u
}

func uuidText(u pgtype.UUID) string {
	v, _ := u.Value()
	return fmt.Sprint(v)
}

const uuidSchema = `
CREATE TABLE accounts (id uuid PRIMARY KEY, name text NOT NULL);
CREATE TABLE projects (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), account_id uuid REFERENCES accounts, name text NOT NULL);
CREATE TABLE tasks (id bigserial PRIMARY KEY, project_id uuid NOT NULL REFERENCES projects ON DELETE CASCADE, title text NOT NULL);
CREATE INDEX projects_account ON projects (account_id);
CREATE INDEX tasks_project ON tasks (project_id);`

func uuidMain(args []string) int {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}
	defer conn.Close(ctx)
	switch args[0] {
	case "migrate":
		_, err = conn.Exec(ctx, uuidSchema)
	case "seed":
		err = uuidSeed(ctx, conn)
	case "load":
		err = uuidLoad(ctx, conn, args[1:])
	case "r1":
		uuidR1(ctx, conn)
	case "offline":
		uuidOffline(ctx, conn)
	case "coop":
		uuidCoop(ctx, conn)
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}
	if fails > 0 {
		return 1
	}
	return 0
}

// uuidSeed: accounts A and B, 20 projects each and one of no account (a template), 3 tasks
// under every project. Row by row, as the application writes: every statement names its
// account, by itself or by its project (check sees them before enable, the proxy after it).
func uuidSeed(ctx context.Context, conn *pgx.Conn) error {
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO accounts VALUES ($1, 'Account A'), ($2, 'Account B')", accountA, accountB); err != nil {
			return err
		}
		project := func(account pgtype.UUID, name string) error {
			var id pgtype.UUID
			if err := tx.QueryRow(ctx, "INSERT INTO projects (account_id, name) VALUES ($1, $2) RETURNING id", account, name).Scan(&id); err != nil {
				return err
			}
			for g := 1; g <= 3; g++ {
				if _, err := tx.Exec(ctx, "INSERT INTO tasks (project_id, title) VALUES ($1, $2)", id, fmt.Sprintf("task %d", g)); err != nil {
					return err
				}
			}
			return nil
		}
		for _, a := range []pgtype.UUID{accountA, accountB} {
			for g := 1; g <= 20; g++ {
				if err := project(a, fmt.Sprintf("seed %d", g)); err != nil {
					return err
				}
			}
		}
		return project(pgtype.UUID{}, "template") // no account: NULL
	})
}

// uuidLoad writes projects (a task under each) of both accounts through the proxy until
// organization 1 is in its own database (-until-moved) or back in the cloud (-until-back),
// then reads every committed one back: nothing may be lost or fail.
func uuidLoad(ctx context.Context, conn *pgx.Conn, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	untilMoved := fs.Bool("until-moved", false, "write until organization 1 is in its own database, then 3s more")
	untilBack := fs.Bool("until-back", false, "write until organization 1 is back in the cloud, then 3s more")
	dur := fs.Duration("duration", 10*time.Minute, "at most")
	tag := fs.String("tag", "load", "the projects' name prefix")
	fs.Parse(args)
	type written struct {
		id      pgtype.UUID
		account pgtype.UUID
		name    string
	}
	var rows []written
	writeErrs := 0
	end := time.Now().Add(*dur)
	var doneAt time.Time
	for i := 0; ; i++ {
		if doneAt.IsZero() && i%10 == 0 && (*untilMoved || *untilBack) {
			// moved: its partition is a foreign table; back: a local one again
			var n int
			q := `SELECT count(*) FROM pg_class c JOIN pg_foreign_table f ON f.ftrelid = c.oid WHERE c.relname = 'projects__org1'`
			if *untilBack {
				q = `SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid WHERE c.relname = 'projects__o1' AND c.relkind = 'r'`
			}
			if conn.QueryRow(ctx, q).Scan(&n) == nil && n == 1 {
				doneAt = time.Now()
				fmt.Printf("organization 1 switched (after %d writes)\n", i)
			}
		}
		if !doneAt.IsZero() && time.Since(doneAt) > 3*time.Second {
			break
		}
		if time.Now().After(end) {
			if *untilMoved || *untilBack {
				return errors.New("LOAD FAIL organization 1 did not switch in time")
			}
			break
		}
		w := written{account: []pgtype.UUID{accountA, accountB}[i%2], name: fmt.Sprintf("%s %d", *tag, i)}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, "INSERT INTO projects (account_id, name) VALUES ($1, $2) RETURNING id", w.account, w.name).Scan(&w.id); err != nil {
				return err
			}
			// the child by its FK: the proxy finds its organization by the project's uuid
			_, err := tx.Exec(ctx, "INSERT INTO tasks (project_id, title) VALUES ($1, $2)", w.id, w.name)
			return err
		})
		if err != nil {
			writeErrs++
			if writeErrs <= 5 {
				fmt.Println("write error:", err)
			}
		} else {
			rows = append(rows, w)
		}
		time.Sleep(10 * time.Millisecond)
	}
	missing, perAccount := 0, map[string]int{}
	for _, w := range rows {
		var name string
		var account pgtype.UUID
		var tasks int
		// each table by its own key (a correlated subquery names no organization for the proxy)
		err := conn.QueryRow(ctx, "SELECT name, account_id FROM projects WHERE id = $1", w.id).Scan(&name, &account)
		if err == nil {
			tasks, err = countOf(ctx, conn, "SELECT count(*) FROM tasks WHERE project_id = $1", w.id)
		}
		if err != nil || name != w.name || account != w.account || tasks != 1 {
			missing++
			if missing <= 5 {
				fmt.Printf("project %s (%s): name %q, account %s, tasks %d, %v\n", uuidText(w.id), w.name, name, uuidText(account), tasks, err)
			}
		}
		perAccount[uuidText(w.account)]++
	}
	fmt.Printf("load: committed A=%d B=%d write_errors=%d missing=%d\n", perAccount[uuidText(accountA)], perAccount[uuidText(accountB)], writeErrs, missing)
	if writeErrs > 0 || missing > 0 || perAccount[uuidText(accountA)] == 0 {
		return errors.New("LOAD FAIL")
	}
	fmt.Println("LOAD OK  every committed write survived")
	return nil
}

// countOf: the one integer sql answers.
func countOf(ctx context.Context, conn *pgx.Conn, sql string, args ...any) (int, error) {
	var n int
	err := conn.QueryRow(ctx, sql, args...).Scan(&n)
	return n, err
}

// uuidR1: the application's requests with account A in its own database, B in the cloud.
func uuidR1(ctx context.Context, conn *pgx.Conn) {
	var nA int
	check("uuid: A's projects by account_id (binary uuid parameter)", func() (err error) {
		nA, err = countOf(ctx, conn, "SELECT count(*) FROM projects WHERE account_id = $1", accountA)
		return expect(err == nil && nA >= 20, "%d projects, %v", nA, err)
	})
	check("uuid: account_id = ANY($1) of a uuid[] parameter", func() error {
		n, err := countOf(ctx, conn, "SELECT count(*) FROM projects WHERE account_id = ANY($1)", []pgtype.UUID{accountA})
		return expect(err == nil && n == nA, "%d projects (want %d), %v", n, nA, err)
	})
	var pa pgtype.UUID
	check("uuid: a project of A by its key, its tasks by their FK", func() error {
		if err := conn.QueryRow(ctx, "SELECT id FROM projects WHERE account_id = $1 AND name = 'seed 1'", accountA).Scan(&pa); err != nil {
			return err
		}
		var name string
		if err := conn.QueryRow(ctx, "SELECT name FROM projects WHERE id = $1", pa).Scan(&name); err != nil {
			return err
		}
		n, err := countOf(ctx, conn, "SELECT count(*) FROM tasks WHERE project_id = $1", pa)
		return expect(err == nil && name == "seed 1" && n == 3, "name %q, %d tasks, %v", name, n, err)
	})
	check("uuid: tasks joined to A's projects named by A", func() error {
		n, err := countOf(ctx, conn, "SELECT count(*) FROM tasks t JOIN projects p ON p.id = t.project_id WHERE p.account_id = $1", accountA)
		return expect(err == nil && n >= 60, "%d tasks, %v", n, err)
	})
	check("uuid: a project and its task inserted in one transaction, the task updated, both deleted", func() error {
		var id pgtype.UUID
		var taskID int64
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, "INSERT INTO projects (account_id, name) VALUES ($1, 'r1 new') RETURNING id", accountA).Scan(&id); err != nil {
				return err
			}
			return tx.QueryRow(ctx, "INSERT INTO tasks (project_id, title) VALUES ($1, 'r1 task') RETURNING id", id).Scan(&taskID)
		})
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, "UPDATE tasks SET title = 'r1 done' WHERE id = $1", taskID); err != nil {
			return err
		}
		var title string
		var account pgtype.UUID
		if err := conn.QueryRow(ctx, "SELECT title, account_id FROM tasks WHERE id = $1", taskID).Scan(&title, &account); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, "DELETE FROM projects WHERE id = $1", id); err != nil {
			return err
		}
		n, err := countOf(ctx, conn, "SELECT count(*) FROM tasks WHERE project_id = $1", id)
		return expect(err == nil && title == "r1 done" && account == accountA && n == 0, "task %q of %s, %d tasks after the delete, %v", title, uuidText(account), n, err)
	})
	for _, c := range []struct {
		who     string
		account pgtype.UUID
	}{{"A (its own database)", accountA}, {"B (the cloud)", accountB}} {
		check("uuid: an upsert of a project of "+c.who, func() error {
			var id pgtype.UUID
			if err := conn.QueryRow(ctx, "SELECT id FROM projects WHERE account_id = $1 AND name = 'seed 2'", c.account).Scan(&id); err != nil {
				return err
			}
			var name string
			if err := conn.QueryRow(ctx, "INSERT INTO projects (id, account_id, name) VALUES ($1, $2, 'upserted') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING name",
				id, c.account).Scan(&name); err != nil {
				return err
			}
			_, err := conn.Exec(ctx, "UPDATE projects SET name = 'seed 2' WHERE id = $1", id)
			return expect(err == nil && name == "upserted", "name %q, %v", name, err)
		})
	}
	check("uuid: a statement that names no account is refused (fanout reject)", func() error {
		_, err := countOf(ctx, conn, "SELECT count(*) FROM projects WHERE name LIKE 'seed%'")
		return expect(err != nil && strings.Contains(err.Error(), "lokadb"), "error %v", err)
	})
}

// uuidCoop: the cooperative mode, the middleware naming the account by its uuid in the
// transaction (set_config('lokadb.tenant', …, true)).
func uuidCoop(ctx context.Context, conn *pgx.Conn) {
	check("uuid: the cooperative mode names A by its uuid", func() error {
		var n int
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT set_config('lokadb.tenant', $1, true)", uuidText(accountA)); err != nil {
				return err
			}
			return tx.QueryRow(ctx, "SELECT count(*) FROM projects WHERE name LIKE 'seed%'").Scan(&n)
		})
		return expect(err == nil && n == 20, "%d projects, %v", n, err)
	})
}

// uuidOffline: A's database (or its agent) is down: B works, A fails instead of falling back.
func uuidOffline(ctx context.Context, conn *pgx.Conn) {
	check("uuid: B works while A's database is down (count, by key, insert)", func() error {
		n, err := countOf(ctx, conn, "SELECT count(*) FROM projects WHERE account_id = $1", accountB)
		if err != nil {
			return err
		}
		var id pgtype.UUID
		if err := conn.QueryRow(ctx, "SELECT id FROM projects WHERE account_id = $1 AND name = 'seed 1'", accountB).Scan(&id); err != nil {
			return err
		}
		var name string
		if err := conn.QueryRow(ctx, "SELECT name FROM projects WHERE id = $1", id).Scan(&name); err != nil {
			return err
		}
		_, err = conn.Exec(ctx, "INSERT INTO tasks (project_id, title) VALUES ($1, 'while A is down')", id)
		return expect(err == nil && n >= 20 && name == "seed 1", "%d projects, %q, %v", n, name, err)
	})
	check("uuid: A fails with an error instead of falling back to the cloud", func() error {
		_, err := countOf(ctx, conn, "SELECT count(*) FROM projects WHERE account_id = $1", accountA)
		if err == nil {
			return errors.New("query unexpectedly succeeded")
		}
		fmt.Println("     error:", err)
		return nil
	})
}
