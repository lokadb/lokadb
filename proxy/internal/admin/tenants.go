package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Organizations with a uuid number (block 18, tasks/uuid-tenant.md). An organization has two
// names: its value V, what the application's organization column holds, which is the
// organization for rows (partitions LIST by value, the policy, lokadb.org, <t>__refs.tenant,
// the proxy); and its number N, an integer lokadb names things by (<t>__o<N>, <t>__org<N>,
// lokadb_org<N>, the tunnel, lokadb.orgs). For an integer column N = V, as before. For a uuid
// column lokadb.tenants maps them, filled by lokadb org-add. V is kept as text, so that text
// numbers can follow the same way.
const (
	TenantInt  = "int"
	TenantUUID = "uuid"
)

// NilUUID is the organization key of rows of no organization in a table numbered by uuid:
// a row whose organization column holds it cannot be told from one without an organization
// (check: tenant_value_reserved).
const NilUUID = "00000000-0000-0000-0000-000000000000"

// TenantKind: the kind of an organization column of base type base (regtype text), "" for a
// type lokadb does not take.
func TenantKind(base string) string {
	switch base {
	case "smallint", "integer", "bigint":
		return TenantInt
	case "uuid":
		return TenantUUID
	}
	return ""
}

// TenantTypes names the types lokadb takes as an organization column, for messages.
const TenantTypes = "smallint, integer, bigint or uuid"

// tenantKey is the partition key of a LokaDB table whose organization column tc is of kind:
// the column, the rows of no organization (NULL) under a value of their own (-2, the nil
// uuid), so that the policy keeps a range (pruning) and still lets them through.
func tenantKey(tc, kind string) string {
	if kind == TenantUUID {
		return fmt.Sprintf("(coalesce(%s, '%s'::uuid))", q(tc), NilUUID)
	}
	return partKey(tc)
}

// tenantType: the type of an organization column enable adds to a child table of kind.
func tenantType(kind string) string {
	if kind == TenantUUID {
		return "uuid"
	}
	return "bigint"
}

// tenantDefault: the default of that column (lokadb.org, the proxy's, of the right type).
func tenantDefault(kind string) string {
	if kind == TenantUUID {
		return "lokadb.org_default_uuid()"
	}
	return "lokadb.org_default()"
}

// tenantKindOf: the kind of column tc of table t, "" for a type lokadb does not take or no
// such column; base: the column's type.
func (r *runner) tenantKindOf(t, tc string) (kind, base string, err error) {
	rows, err := r.rows(`SELECT a.atttypid::regtype::text FROM pg_attribute a
		WHERE a.attrelid = $1::regclass AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped`, qr(t), tc)
	if err != nil || len(rows) == 0 {
		return "", "", err
	}
	return TenantKind(rows[0][0]), rows[0][0], nil
}

// OrgAddOptions of OrgAdd.
type OrgAddOptions struct {
	DSN    string
	Tenant string // V: the organization's value in the organization column
	Org    int64  // N to give it; 0: the next of lokadb.tenants' sequence
	Log    *slog.Logger
}

// OrgAdd gives organization V (a uuid) its number N in lokadb.tenants, which every command
// with -org takes. A value registered already keeps its number. For a database numbered by
// integers N = V: nothing to register.
func OrgAdd(ctx context.Context, o OrgAddOptions) (int64, error) {
	if o.Org < 0 { // 0: the next free number
		return 0, ValidOrg(o.Org)
	}
	conn, err := connectPlain(ctx, o.DSN)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	r := &runner{ctx: ctx, tx: tx, log: o.Log}
	if err := r.exec(baseSQL); err != nil {
		return 0, err
	}
	kind, err := r.dbTenantKind()
	if err != nil {
		return 0, err
	}
	switch kind {
	case "":
		return 0, fmt.Errorf("lokadb enable has not run in this database: register organizations after it")
	case TenantInt:
		return 0, fmt.Errorf("the organizations of this database are numbered by their own value (an integer organization column): give it as -org N to every command, nothing to register")
	}
	v, err := r.rows(`SELECT $1::uuid::text`, o.Tenant)
	if err != nil {
		return 0, fmt.Errorf("%q is no uuid: %w", o.Tenant, err)
	}
	val := v[0][0]
	if val == NilUUID {
		return 0, fmt.Errorf("%s stands for rows of no organization in lokadb: it cannot be an organization", NilUUID)
	}
	had, err := r.rows(`SELECT org::text FROM lokadb.tenants WHERE tenant = $1`, val)
	if err != nil {
		return 0, err
	}
	if len(had) > 0 {
		var n int64
		fmt.Sscan(had[0][0], &n)
		if o.Org != 0 && o.Org != n {
			return 0, fmt.Errorf("organization %s has number %d already, not %d", val, n, o.Org)
		}
		return n, nil
	}
	var n int64
	if o.Org != 0 {
		taken, err := r.rows(`SELECT tenant FROM lokadb.tenants WHERE org = $1`, o.Org)
		if err != nil {
			return 0, err
		}
		if len(taken) > 0 {
			return 0, fmt.Errorf("number %d is organization %s's", o.Org, taken[0][0])
		}
		n = o.Org
		if err := r.exec(`SELECT setval('lokadb.tenants_org_seq', greatest($1, (SELECT last_value FROM lokadb.tenants_org_seq)))`, n); err != nil {
			return 0, err
		}
	} else {
		next, err := r.rows(`SELECT nextval('lokadb.tenants_org_seq')::text`)
		if err != nil {
			return 0, err
		}
		fmt.Sscan(next[0][0], &n)
	}
	if err := r.exec(`INSERT INTO lokadb.tenants (org, tenant) VALUES ($1, $2)`, n, val); err != nil {
		return 0, err
	}
	// proxies read the numbers with the metadata (the organization's database by its number)
	if err := r.exec("NOTIFY lokadb_meta"); err != nil {
		return 0, err
	}
	return n, tx.Commit(ctx)
}

// dbTenantKind: the kind of the organization columns of the database's LokaDB tables ("" when
// none is enabled).
func (r *runner) dbTenantKind() (string, error) {
	rows, err := r.rows(`SELECT DISTINCT a.atttypid::regtype::text FROM lokadb.tables t
		JOIN pg_attribute a ON a.attrelid = to_regclass(lokadb.rel(t.table_name)) AND a.attname = t.tenant_column AND NOT a.attisdropped`)
	if err != nil {
		return "", err
	}
	kind := ""
	for _, row := range rows {
		k := TenantKind(row[0])
		if kind != "" && k != kind {
			return "", fmt.Errorf("the LokaDB tables have organization columns of different kinds (%s and %s)", kind, k)
		}
		kind = k
	}
	return kind, nil
}

// Tenant is an organization by both its names: its number Org and its value Value (canonical
// text, N itself in a database numbered by integers) of the database's Kind.
type Tenant struct {
	Org   int64
	Kind  string
	Value string
}

// Lit: the organization's value as an SQL literal of its column's type: N, '<uuid>'::uuid.
func (t Tenant) Lit() string {
	if t.Kind == TenantUUID {
		return lit(t.Value) + "::uuid"
	}
	return fmt.Sprint(t.Org)
}

// Key: the partition key of organization column tc (tenantKey).
func (t Tenant) Key(tc string) string { return tenantKey(tc, t.Kind) }

// LockCall: the organization's pause lock key in SQL, the one the proxy takes:
// lokadb.org_lock(N), lokadb.org_lock_v('<uuid>') for one numbered by uuid.
func (t Tenant) LockCall() string {
	if t.Kind == TenantUUID {
		return "lokadb.org_lock_v(" + lit(t.Value) + ")"
	}
	return fmt.Sprintf("lokadb.org_lock(%d)", t.Org)
}

// ResolveTenant: an organization given by -org N or -tenant V (a lookup in lokadb.tenants
// only: a command never numbers an organization by itself, org-add does). For a database
// numbered by integers V is N.
func ResolveTenant(ctx context.Context, q querier, org int64, tenant string) (Tenant, error) {
	kind, err := tenantKindOfDB(ctx, q)
	if err != nil {
		return Tenant{}, err
	}
	if kind != TenantUUID {
		if tenant != "" {
			return Tenant{}, fmt.Errorf("-tenant: the organizations of this database are numbered by their own value: give -org %s", tenant)
		}
		return Tenant{Org: org, Kind: TenantInt, Value: fmt.Sprint(org)}, nil
	}
	var rows [][]string
	if tenant != "" {
		rows, err = rowsOf(ctx, q, `SELECT org::text, tenant FROM lokadb.tenants WHERE tenant = $1::uuid::text`, tenant)
	} else {
		rows, err = rowsOf(ctx, q, `SELECT org::text, tenant FROM lokadb.tenants WHERE org = $1`, org)
	}
	if err != nil {
		return Tenant{}, err
	}
	if len(rows) == 0 {
		if tenant != "" {
			return Tenant{}, fmt.Errorf("organization %s has no number yet: first lokadb org-add -tenant %s", tenant, tenant)
		}
		return Tenant{}, fmt.Errorf("no organization has number %d: first lokadb org-add -tenant <its uuid>", org)
	}
	var n int64
	fmt.Sscan(rows[0][0], &n)
	if org != 0 && org != n {
		return Tenant{}, fmt.Errorf("-org %d and -tenant %s are different organizations (%s has number %d)", org, tenant, rows[0][1], n)
	}
	return Tenant{Org: n, Kind: TenantUUID, Value: rows[0][1]}, nil
}

// LookupOrg: the number of organization tenant, a command's -tenant V (org, its -org, when
// it has none): a lookup in lokadb.tenants only, org-add gives the numbers.
func LookupOrg(ctx context.Context, dsn string, org int64, tenant string) (int64, error) {
	if tenant == "" {
		return org, nil
	}
	conn, err := connectPlain(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)
	t, err := ResolveTenant(ctx, conn, org, tenant)
	return t.Org, err
}

// RequireOrg is LookupOrg for a command that acts on one organization (move-org, move-back,
// repoint, org-profile, org-credentials): no organization is an error, not organization 0.
func RequireOrg(ctx context.Context, dsn string, org int64, tenant string) (int64, error) {
	n, err := LookupOrg(ctx, dsn, org, tenant)
	if err != nil {
		return 0, err
	}
	return n, ValidOrg(n)
}

// ValidOrg: the number of an organization a command acts on is positive. 0 is what a flag
// left out reads as (move-org of organization 0 sent the rows whose tenant is 0 to the
// customer database of whoever the other flags named); -2 is the key of the rows of no
// organization (<table>__none) and -1 "not found" (lokadb.org_default).
func ValidOrg(org int64) error {
	if org > 0 {
		return nil
	}
	if org == 0 {
		return errors.New("no organization: give -org N (a positive number) or -tenant UUID")
	}
	return fmt.Errorf("organization %d: a number below 1 is not an organization (-1 and -2 are reserved)", org)
}

// org: the organization moved by both its names (resolve; N = V before it).
func (o MoveOptions) org() Tenant {
	if o.tenant.Kind == "" {
		return Tenant{Org: o.Org, Kind: TenantInt, Value: fmt.Sprint(o.Org)}
	}
	return o.tenant
}

// resolve finds the organization's value (lokadb.tenants for one numbered by uuid), once.
func (o *MoveOptions) resolve(ctx context.Context, q querier) error {
	if o.tenant.Kind != "" {
		return nil
	}
	t, err := ResolveTenant(ctx, q, o.Org, "")
	if err != nil {
		return err
	}
	o.tenant = t
	return nil
}

// tenantKindOfDB: dbTenantKind outside a runner ("" for a database before enable).
func tenantKindOfDB(ctx context.Context, q querier) (string, error) {
	ok, err := rowsOf(ctx, q, `SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil || ok[0][0] != "true" {
		return "", err
	}
	rows, err := rowsOf(ctx, q, `SELECT DISTINCT a.atttypid::regtype::text FROM lokadb.tables t
		JOIN pg_attribute a ON a.attrelid = to_regclass(`+relSQL("t.table_name")+`) AND a.attname = t.tenant_column AND NOT a.attisdropped`)
	if err != nil || len(rows) == 0 {
		return "", err
	}
	return TenantKind(rows[0][0]), nil
}
