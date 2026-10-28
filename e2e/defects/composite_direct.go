package main

// Composite keys on the direct path (tasks/composite-direct.md): an organization in fdw+direct
// whose tables have a key of several columns. Reads by the whole key of a kind B table
// (parcels (tracking_no, piece)) and writes of both kinds go straight to its database, and
// __refs (id_1, …, id_n) follows the writes as it does for a key of one column.

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "DK1", name: "composite keys on the direct path: reads by the whole key of a kind B table go direct and answer as fdw does", run: checkCompositeDirectReads},
		check{id: "DK2", name: "composite keys on the direct path: INSERT and DELETE of both kinds go direct, __refs (id_1, id_2) follows, the cloud's FK and another organization's key refuse", run: checkCompositeDirectWrites},
		check{id: "DK3", name: "composite keys on the direct path, stage 2: upsert, INSERT … SELECT, unnest without the id and COPY of both kinds go direct, __refs (id_1, id_2) follows", run: checkCompositeDirectBatch},
	)
}

// compositeDirectSetup: K8's tables, orders' id from a sequence of the cloud, organization 1
// moved and in fdw+direct, the proxy under reject.
func (e *env) compositeDirectSetup() (func(), error) {
	return e.compositeDirectSetupCfg(nil)
}

// compositeDirectSetupCfg: compositeDirectSetup with more of the proxy's configuration (DK3's
// COPY: tenant.local_only off, as in DU4).
func (e *env) compositeDirectSetupCfg(cfg map[string]any) (func(), error) {
	if err := compositeSchema(e); err != nil {
		return nil, err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE SEQUENCE orders_id_seq START 100", "ALTER SEQUENCE orders_id_seq OWNER TO app", "ALTER SEQUENCE orders_id_seq OWNED BY orders.id",
		"ALTER TABLE orders ALTER COLUMN id SET DEFAULT nextval('orders_id_seq')"); err != nil {
		return nil, err
	}
	var stop func()
	var err error
	if cfg == nil {
		stop, err = e.directSetup(nil)
	} else {
		if err = e.enable(); err != nil {
			return nil, err
		}
		if _, err = e.move(1, "-customer-app", e.appCustomer()); err != nil {
			return nil, err
		}
		cfg["fanout"] = map[string]any{e.db: map[string]string{"mode": "reject"}}
		cfg["customers_app"] = map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}}
		stop, err = e.proxyCfg(map[string]string{"1": e.appCustomer()}, cfg)
	}
	if err != nil {
		return nil, err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		stop()
		return nil, fmt.Errorf("org-profile fdw+direct refused: %s", lastErr(out))
	}
	return stop, nil
}

func checkCompositeDirectReads(e *env) (bool, string, error) {
	stop, err := e.compositeDirectSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("dk1")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	for _, q := range []struct {
		what, sql string
		args      []any
		want      string
		direct    int
	}{
		{"a parcel by the whole key", `SELECT weight FROM parcels WHERE tracking_no = $1 AND piece = $2`, []any{1002, 2}, "20", 1},
		{"parcels by rows of the key", `SELECT count(*) FROM parcels WHERE (tracking_no, piece) IN ((1001, 1), (1001, 2))`, nil, "2", 1},
		{"an order by its whole key (kind A)", `SELECT title FROM orders WHERE account_id = $1 AND id = $2`, []any{1, 3}, "o1-3", 1},
		{"parcels by part of the key", `SELECT count(*) FROM parcels WHERE tracking_no = $1`, []any{1001}, "refused", 0},
	} {
		n := directCount(e.proxyRoutes(), "1")
		got := rowsText(e, c, q.sql, q.args...)
		g := strings.Join(got[:max(0, len(got)-1)], ",")
		if len(got) == 1 && strings.HasPrefix(got[0], "error:") {
			g = got[0]
			if strings.Contains(g, "lokadb:") {
				g = "refused"
			}
		}
		d := directCount(e.proxyRoutes(), "1") - n
		s := fmt.Sprintf("%s: %s, on the direct path %d", q.what, g, d)
		if g != q.want || d != q.direct {
			ok, s = false, "WRONG "+s
		}
		notes = append(notes, s)
	}
	return ok, strings.Join(notes, "; "), nil
}

func checkCompositeDirectWrites(e *env) (bool, string, error) {
	stop, err := e.compositeDirectSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("dk2")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	cloud := func(sql string) string {
		v, err := one(e.ctx, e.cloud(), sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	cust := func(sql string) string {
		v, err := one(e.ctx, e.customer(), sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	run := func(sql string, args ...any) (string, int) {
		n := directCount(e.proxyRoutes(), "1")
		got := rowsText(e, c, sql, args...)
		g := strings.Join(got[:max(0, len(got)-1)], ",")
		if len(got) == 1 && strings.HasPrefix(got[0], "error:") {
			g = got[0]
		}
		return g, directCount(e.proxyRoutes(), "1") - n
	}

	// kind A, the id from the cloud's sequence, the organization column given
	id, d := run(`INSERT INTO orders (account_id, title) VALUES ($1, 'dk2') RETURNING id`, 1)
	note(d == 1 && cust("SELECT count(*) FROM orders WHERE title = 'dk2' AND id = "+id) == "1" &&
		cloud("SELECT tenant::text FROM orders__refs WHERE id_1 = 1 AND id_2 = "+id) == "1",
		fmt.Sprintf("a new order (kind A, id %s from the cloud): direct %d, at the organization %s, in __refs %s", id,
			d, cust("SELECT count(*) FROM orders WHERE title = 'dk2'"), cloud("SELECT count(*) FROM orders__refs WHERE id_1 = 1 AND id_2 = "+id)))
	// kind B, the key given
	got, d := run(`INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES ($1, $2, 1, 5), ($1, $3, 1, 6)`, 1777, 1, 2)
	note(d == 1 && cloud("SELECT count(*) FROM parcels__refs WHERE id_1 = 1777 AND tenant = 1") == "2" && cust("SELECT count(*) FROM parcels WHERE tracking_no = 1777") == "2",
		fmt.Sprintf("two new parcels (kind B): %s, direct %d, in __refs %s", got, d, cloud("SELECT count(*) FROM parcels__refs WHERE id_1 = 1777")))
	// a key of organization 2: __refs refuses it, nothing stays
	got, _ = run(`INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES (2001, 1, 1, 7)`)
	note(strings.Contains(got, "23505") && cust("SELECT count(*) FROM parcels WHERE tracking_no = 2001") == "0",
		"a parcel with organization 2's key: "+got)
	// DELETE: the keys leave __refs
	got, d = run(`DELETE FROM parcels WHERE tracking_no = $1 AND piece = $2`, 1777, 1)
	note(d == 1 && cloud("SELECT count(*) FROM parcels__refs WHERE id_1 = 1777") == "1" && cust("SELECT count(*) FROM parcels WHERE tracking_no = 1777") == "1",
		fmt.Sprintf("deleting a parcel: %s, direct %d, left in __refs %s", got, d, cloud("SELECT count(*) FROM parcels__refs WHERE id_1 = 1777")))
	// an order a cloud invoice references: the cloud's FK onto __refs refuses, nothing goes
	got, _ = run(`DELETE FROM orders WHERE account_id = $1 AND id = $2`, 1, 3)
	note(strings.Contains(got, "invoices") && cust("SELECT count(*) FROM orders WHERE id = 3") == "1" && cloud("SELECT count(*) FROM orders__refs WHERE id_1 = 1 AND id_2 = 3") == "1",
		"deleting an order a cloud invoice references: "+got)
	// inside a transaction: the third commit of the same form goes direct (learn-first)
	var txDirect int
	for i := 0; i < 3; i++ {
		n := directCount(e.proxyRoutes(), "1")
		err := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
			if _, err := tx.Exec(e.ctx, `INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES ($1, 1, 1, 1)`, 1800+i); err != nil {
				return err
			}
			_, err := tx.Exec(e.ctx, `DELETE FROM parcels WHERE tracking_no = $1 AND piece = $2`, 1800+i, 1)
			return err
		})
		if err != nil {
			note(false, fmt.Sprintf("transaction %d: %s", i+1, firstErrLine(err)))
		}
		txDirect = directCount(e.proxyRoutes(), "1") - n
	}
	note(txDirect > 0 && cloud("SELECT count(*) FROM parcels__refs WHERE id_1 BETWEEN 1800 AND 1802") == "0",
		fmt.Sprintf("the third transaction inserting and deleting a parcel: direct statements %d, left in __refs %s", txDirect, cloud("SELECT count(*) FROM parcels__refs WHERE id_1 BETWEEN 1800 AND 1802")))
	diff, code, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	note(code == 0, fmt.Sprintf("refs-diff exit %d: %s", code, oneLine(diff)))
	return ok, strings.Join(notes, "; ") + e.proxyLines("direct write"), nil
}

// DK3: the batch forms of stage 2 (tasks/composite-direct.md, 2.3).
func checkCompositeDirectBatch(e *env) (bool, string, error) {
	stop, err := e.compositeDirectSetupCfg(map[string]any{"tenant": map[string]any{"local_only": false}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("dk3")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(db, sql string) string {
		v, err := one(e.ctx, db, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	run := func(sql string, args ...any) (string, int) {
		n := directCount(e.proxyRoutes(), "1")
		got := rowsText(e, c, sql, args...)
		g := strings.Join(got[:max(0, len(got)-1)], ",")
		if len(got) == 1 && strings.HasPrefix(got[0], "error:") {
			g = got[0]
		}
		return g, directCount(e.proxyRoutes(), "1") - n
	}
	refs := func(table, where string) string {
		return val(e.cloud(), "SELECT count(*) FROM "+table+"__refs WHERE "+where)
	}

	// upsert of kind B: a new parcel and one there (updated): one refs row more, none left over
	got, d := run(`INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES (1901, 1, 1, 1), (1001, 1, 1, 99)
		ON CONFLICT (tracking_no, piece) DO UPDATE SET weight = excluded.weight`)
	note(d == 1 && refs("parcels", "id_1 = 1901 AND tenant = 1") == "1" && refs("parcels", "id_1 = 1001") == "2" &&
		val(e.customer(), "SELECT weight FROM parcels WHERE tracking_no = 1001 AND piece = 1") == "99",
		fmt.Sprintf("an upsert of parcels: %s, direct %d, 1901 in __refs %s, 1001 in __refs %s", got, d, refs("parcels", "id_1 = 1901"), refs("parcels", "id_1 = 1001")))
	// upsert of kind A without its id: the cloud's sequence, DO NOTHING
	id, d := run(`INSERT INTO orders (account_id, title) VALUES ($1, 'dk3-u') ON CONFLICT DO NOTHING RETURNING id`, 1)
	note(d == 1 && !strings.HasPrefix(id, "error") && refs("orders", "id_1 = 1 AND id_2 = "+id) == "1" && val(e.customer(), "SELECT count(*) FROM orders WHERE title = 'dk3-u'") == "1",
		fmt.Sprintf("an upsert of an order without its id: %s, direct %d", id, d))
	// INSERT … SELECT of kind B naming the key
	got, d = run(`INSERT INTO parcels (tracking_no, piece, account_id, weight) SELECT tracking_no + 5000, piece, account_id, weight FROM parcels WHERE account_id = $1 AND tracking_no < 1900`, 1)
	note(d == 1 && refs("parcels", "id_1 BETWEEN 6001 AND 6004 AND tenant = 1") == "8" && val(e.customer(), "SELECT count(*) FROM parcels WHERE tracking_no BETWEEN 6001 AND 6004") == "8",
		fmt.Sprintf("INSERT … SELECT of parcels: %s, direct %d, in __refs %s", got, d, refs("parcels", "id_1 BETWEEN 6001 AND 6004")))
	// Django's unnest without the id (kind A)
	got, d = run(`INSERT INTO orders (account_id, title) SELECT * FROM unnest($1::bigint[], $2::text[]) RETURNING id`, []int64{1, 1}, []string{"dk3-n1", "dk3-n2"})
	note(d == 1 && val(e.customer(), "SELECT count(*) FROM orders WHERE title LIKE 'dk3-n%'") == "2" && !strings.HasPrefix(got, "error"),
		fmt.Sprintf("unnest of orders without the id: %s, direct %d", got, d))
	if ids, err := e.customerRows("SELECT id::text FROM orders WHERE title LIKE 'dk3-n%'"); err == nil && len(ids) == 2 {
		note(refs("orders", "id_1 = 1 AND id_2 IN ("+strings.Join(ids, ", ")+")") == "2", "unnest's orders in __refs: "+refs("orders", "id_1 = 1 AND id_2 IN ("+strings.Join(ids, ", ")+")"))
	}
	// COPY with the organization named: parcels with their key, orders without the id
	if _, err := c.Exec(e.ctx, "SET lokadb.tenant = '1'"); err != nil {
		return false, "", fmt.Errorf("SET lokadb.tenant: %w", err)
	}
	n := directCount(e.proxyRoutes(), "1")
	_, errP := c.PgConn().CopyFrom(e.ctx, bytes.NewReader([]byte("7001\t1\t1\t1\n7001\t2\t1\t2\n")), "COPY parcels (tracking_no, piece, account_id, weight) FROM STDIN")
	_, errO := c.PgConn().CopyFrom(e.ctx, bytes.NewReader([]byte("1\tdk3-c1\n1\tdk3-c2\n")), "COPY orders (account_id, title) FROM STDIN")
	d = directCount(e.proxyRoutes(), "1") - n
	cIDs, _ := e.customerRows("SELECT id::text FROM orders WHERE title LIKE 'dk3-c%'")
	inRefs := "none"
	if len(cIDs) > 0 {
		inRefs = refs("orders", "id_1 = 1 AND id_2 IN ("+strings.Join(cIDs, ", ")+")")
	}
	note(errP == nil && errO == nil && d == 2 && refs("parcels", "id_1 = 7001 AND tenant = 1") == "2" && len(cIDs) == 2 && inRefs == "2",
		fmt.Sprintf("COPY of parcels (%s) and of orders without the id (%s): direct %d, parcels in __refs %s, orders %v in __refs %s",
			short(errP), short(errO), d, refs("parcels", "id_1 = 7001"), cIDs, inRefs))
	// a pipeline (pgx.Batch, block 9's pipeWrite): two orders without their id and a parcel
	n = directCount(e.proxyRoutes(), "1")
	b := &pgx.Batch{}
	b.Queue(`INSERT INTO orders (account_id, title) VALUES ($1, 'dk3-b')`, 1)
	b.Queue(`INSERT INTO orders (account_id, title) VALUES ($1, 'dk3-b')`, 1)
	b.Queue(`INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES ($1, 1, 1, 1)`, 9001)
	errB := c.SendBatch(e.ctx, b).Close()
	d = directCount(e.proxyRoutes(), "1") - n
	bIDs, _ := e.customerRows("SELECT id::text FROM orders WHERE title = 'dk3-b'")
	bRefs := "none"
	if len(bIDs) > 0 {
		bRefs = refs("orders", "id_1 = 1 AND id_2 IN ("+strings.Join(bIDs, ", ")+")")
	}
	note(errB == nil && d > 0 && len(bIDs) == 2 && bRefs == "2" && refs("parcels", "id_1 = 9001 AND tenant = 1") == "1",
		fmt.Sprintf("a pipeline of two orders without their id and a parcel (%s): direct %d, orders %v in __refs %s, the parcel in __refs %s",
			short(errB), d, bIDs, bRefs, refs("parcels", "id_1 = 9001")))
	// inside a transaction (the organization named for the session): a COPY of parcels, an
	// upsert of an order without its id and an INSERT … SELECT; the third commit of the form goes
	// direct (learn first)
	var txDirect int
	for i := 0; i < 3; i++ {
		n := directCount(e.proxyRoutes(), "1")
		err := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
			if _, err := tx.CopyFrom(e.ctx, pgx.Identifier{"parcels"}, []string{"tracking_no", "piece", "account_id", "weight"},
				pgx.CopyFromRows([][]any{{int64(8000 + i), int32(1), int64(1), int32(1)}})); err != nil {
				return err
			}
			if _, err := tx.Exec(e.ctx, `INSERT INTO orders (account_id, title) VALUES ($1, 'dk3-t') ON CONFLICT DO NOTHING`, 1); err != nil {
				return err
			}
			_, err := tx.Exec(e.ctx, `INSERT INTO parcels (tracking_no, piece, account_id, weight) SELECT tracking_no, 2, account_id, weight FROM parcels WHERE account_id = $1 AND tracking_no = $2`, 1, 8000+i)
			return err
		})
		if err != nil {
			note(false, fmt.Sprintf("transaction %d: %s", i+1, firstErrLine(err)))
		}
		txDirect = directCount(e.proxyRoutes(), "1") - n
	}
	tIDs, _ := e.customerRows("SELECT id::text FROM orders WHERE title = 'dk3-t'")
	tRefs := "none"
	if len(tIDs) > 0 {
		tRefs = refs("orders", "id_1 = 1 AND id_2 IN ("+strings.Join(tIDs, ", ")+")")
	}
	note(txDirect > 0 && refs("parcels", "id_1 BETWEEN 8000 AND 8002 AND tenant = 1") == "6" && len(tIDs) == 3 && tRefs == "3",
		fmt.Sprintf("three transactions of a COPY, an upsert and an INSERT … SELECT: the third's direct statements %d, parcels in __refs %s, orders %v in __refs %s",
			txDirect, refs("parcels", "id_1 BETWEEN 8000 AND 8002"), tIDs, tRefs))
	// a COPY with another organization's key: 23505, nothing stays
	_, errX := c.PgConn().CopyFrom(e.ctx, bytes.NewReader([]byte("2001\t1\t1\t1\n")), "COPY parcels (tracking_no, piece, account_id, weight) FROM STDIN")
	note(errX != nil && strings.Contains(errX.Error(), "23505") && val(e.customer(), "SELECT count(*) FROM parcels WHERE tracking_no = 2001") == "0",
		"a COPY of a parcel with organization 2's key: "+short(errX))
	diff, code, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	note(code == 0, fmt.Sprintf("refs-diff exit %d: %s", code, oneLine(diff)))
	return ok, strings.Join(notes, "; ") + e.proxyLines("direct"), nil
}
