# Routing

The proxy parses every statement with the Postgres parser (pg_query) and decides which organization it belongs to. Before the statement it sets `lokadb.org` in the cloud session, and Postgres prunes every partition but that organization's. A statement that touches only one organization therefore never reaches another customer's database.

## How the organization is found

- **Conditions:** `=`, `IN (…)` and `= ANY(array)` on the tenant column, the row key, or a foreign-key column (looked up through `<table>__refs`, which maps every key to its organization). Values may be literals or bound parameters, text or binary, scalars or arrays (`'{1,2}'`, `ARRAY[…]`, `$1::bigint[]`).
- **Inserts:** the organization comes from `VALUES` or from `UNNEST` arrays (Django `bulk_create`); for a child table, from its parent.
- **Joins:** a value bound to one column binds every column equal to it: `d.project_id = p.id AND p.id = $1` gives the organization through `d.project_id`. `ON` conditions of inner joins count like `WHERE`; those of outer joins do not.
- **Subqueries:** `conversation_id IN (SELECT id FROM conversations WHERE account_id = $1)` (Rails `where(conversation_id: account.conversations)`) goes to that organization's database when both columns resolve through the same lookup. A correlated subquery (`EXISTS (SELECT 1 FROM versions v WHERE v.document_id = d.id)`) takes the organization of the outer query.
- **`INSERT … SELECT`** gets the organization when the lookup column receives a constant, a parameter or a source column with the same lookup, and the `SELECT` names the organization.
- If the tenant column is named (`organization_id = 1 AND id = 5`), the id is not looked up: the policy already shows only that organization's rows, and writing another organization's key is refused by `__refs` and the parent trigger.

Values of the condition that belong to several organizations (`organization_id IN (1, 2)`) make a statement for all organizations, under the same policy as a statement without one.

## Statements without an organization

Postgres would run such a statement in every partition, which means in the database of every moved organization: slowly, and with an error if any of those databases is down. The `fanout` policy in the proxy config decides, per database (that is, per service):

```json
"fanout": {
  "*":       {"mode": "reject"},
  "reports": {"mode": "allow"},
  "app":     {"mode": "reject", "allow": ["SELECT * FROM documents WHERE slug = $1 LIMIT $2"]}
}
```

| `mode` | What it does |
|---|---|
| `reject` | **default** (no entry, or no `fanout` at all): refused |
| `reject-writes` | `INSERT`, `UPDATE`, `DELETE` refused; reads run in every database |
| `log` | runs everywhere, with a warning in the proxy log |
| `allow` | runs everywhere |

`allow` lists statements that were reviewed and allowed under any `mode` — an admin report across organizations, a background job. They are compared by pg_query fingerprint: constants, parameters, list lengths and quoting do not matter.

A refused statement gets an ordinary Postgres error (`0A000`) raised by the server itself, so the transaction ends up exactly as after any error:

```
ERROR:  lokadb: the statement does not name the organization of documents rows, so it would run in every organization's database (fanout policy "reject")
DETAIL:  why: unknown_column documents.slug (=)
HINT:  Restrict it by the organization column, id or a parent id (= or IN), or add it to "allow" of this database's fanout policy in the proxy config.
```

`lokadb check -proxy-config` applies the same policy in advance. The policy applies only after `enable`; before it, the proxy passes everything through.

### Statements in which the proxy sees no LokaDB table

A statement that reads LokaDB tables in a way the analyzer did not recognize does **not** run under "all organizations". It runs:

- under the organization the application named (`lokadb.tenant`, [cooperative mode](profiles-and-cooperative-mode.md#cooperative-mode));
- under all organizations only where that is explicitly allowed (`lokadb.tenant = '*'` for a role in `all_roles`, policy `allow` or `log`, a statement in `allow`);
- otherwise under "no organization": it sees no rows of LokaDB tables, and writes to them are refused.

A gap in the analyzer therefore gives an empty answer, never another organization's rows. Known forms are recognized one by one: `EXECUTE` of an SQL `PREPARE`, functions over LokaDB tables in any schema, views through `search_path`, `REFRESH MATERIALIZED VIEW`, functions and views created through the proxy. Dynamic SQL inside functions and `DO` blocks is not recognized: it sees no rows unless the organization is named, and then only that organization's rows.

### Why the organization was not found

For every part of a statement (`SELECT`, subquery, CTE, `UPDATE`, `DELETE`, `INSERT`) that reads or writes a LokaDB table without an organization, the analyzer names the reason — in the client error (`DETAIL`), in the proxy log, and in `lokadb check`:

| `kind` | Meaning | Example |
|---|---|---|
| `no_condition` | the table has no conditions at all | `SELECT * FROM documents` |
| `unknown_column` | a condition on a column with no organization lookup | `WHERE slug = $1`; `WHERE conversation_id = $1` without a foreign key |
| `unsupported_form` | a form the analyzer does not read | `id <> ANY($1)`, `id = 1 OR id = 2`, `INSERT … SELECT` without the organization, a view or function over LokaDB tables, a `DO` block |
| `unreadable_value` | the lookup column is compared with an expression, not a value | `organization_id = lower($1)::bigint` |

A statement the proxy's parser (PostgreSQL 17 grammar) cannot parse, and whose text names a LokaDB table, view or function, is a statement without an organization: the policy decides.

## Declarations

The proxy looks up the organization only where a lookup exists: the tenant column, the key, a foreign key to a LokaDB table. Declarations add lookups the schema does not have. After `enable`, apply them with `lokadb routing apply -db DSN -spec spec.json [-patch patch.json]`, without a new `enable`; proxies pick them up when they reload metadata.

### Relations without a foreign key: `routing.relations`

Rails, Laravel without `foreign()` and many other applications do not declare foreign keys. Then the application's main query — a conversation feed `WHERE "messages"."conversation_id" = $1` — has no organization. Declare the relation:

```json
"routing": {"relations": [{"from": "messages.conversation_id", "to": "conversations.id"}]}
```

- Before registering it, LokaDB checks it on the data: how many rows with a value found a parent, and how many parents belong to another organization. Unless it is 100 % and 0, the relation is refused until the owner confirms it (`"confirmed": true`).
- The target is the key of a LokaDB table.
- A polymorphic reference (`<x>_id` next to `<x>_type`, like Rails `taggable_type`/`taggable_id`) points at one of several tables: its ids would find the organization in the wrong place. Such a relation is refused.
- New rows are not checked: if the application writes a reference to a parent of another organization, queries by it will find the parent's organization.

### Cloud tables that know the organization: `routing.cloud_tables`

A table can stay in the cloud and still know the organization of its rows (`projects.account_id`). A query that names the organization only through it (`… JOIN projects p ON p.id = t.project_id WHERE p.id = $1`) would otherwise go to every database.

```json
"routing": {"cloud_tables": [{"table": "projects", "tenant_column": "account_id"}]}
```

A condition on `projects.account_id` then gives the organization directly; a condition on `projects.id` gives it through a lookup in the cloud. The declaration asserts that LokaDB rows linked to a cloud row belong to its organization: the owner must verify that on the data.

### Unique keys: `routing.keys`

For queries by a column unique across the table (`WHERE slug = $1`, `WHERE public_token = $1`):

```json
"routing": {"keys": [{"table": "documents", "column": "slug"}]}
```

A unique column that the application looks up without the organization (a webhook token, an order number, an external id) is the usual case for this declaration: `check` offers it first, with the fragment to paste, for every single-column unique index (`unique_not_tenant_scoped`), and `-suggest` proposes it for unique columns named like a token, a number or an external id. A composite unique key (`provider, provider_payment_id`) cannot be a key yet; if one of its columns is unique on the data (provider ids usually are), `check` says so: add a unique index on that column and declare it. LokaDB checks that values do not repeat, creates a mapping table `lokadb.<table>__key__<column>` (value → organization) kept by a trigger, and the proxy looks up the organization by value like by key. The mapping's primary key keeps the value unique across all organizations — which an index on a table with foreign partitions cannot do; a duplicate is refused by `check`, by `enable` and on insert. `enable -prepare` fills the mapping in advance, while the application writes.

### Foreign keys to cloud tables after the move

Postgres keeps a foreign key from a LokaDB table to a cloud table (`documents.project_id → projects`) only on cloud partitions. For a moved organization LokaDB replaces it with two triggers: one on the organization's foreign partition (a row written through the cloud must reference an existing cloud row, locked `FOR KEY SHARE`; error `23503`), and one on the cloud table (deleting a row or changing its key performs the foreign key's action — `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, `NO ACTION` — in the moved organizations' databases too).

Where the referencing rows are searched:

- the cloud table is declared in `routing.cloud_tables` — only in that row's organization;
- the organizations table itself (`tenants`, `organizations`: the table that the organization columns of the spec's tables reference by foreign key onto its `id`) is a cloud table whose organization column is its `id`, with no declaration: deleting a tenant, or changing its key, concerns that one organization, so the action runs in its database only, under any fanout policy. `{"table": "tenants", "tenant_column": "id"}` in `routing.cloud_tables` says the same;
- otherwise in every moved organization, as a statement without an organization: under `log` or `allow` the action runs in every organization database (if one is down, the delete is refused); under `reject-writes` only `RESTRICT` and `NO ACTION` pass; under `reject` (the default) the delete or key change is refused once any organization has moved.

A table shared by organizations (`users`, linked to them through `tenant_users`) has no column that holds the organization of its rows, so it cannot be declared: deleting a user, or `SET NULL` on its key, touches every moved organization that references it. Either allow that statement to run in every organization (fanout policy `log` or `allow`, or an allow-list entry for the reviewed `DELETE` in the proxy config), or delete softly in the application.

`check` names such tables in advance (`fk_to_cloud_delete`). The check is not deferred to `COMMIT`, even for `DEFERRABLE` keys, and writes made directly in the customer database are not checked.

## Rows without an organization

A row without an organization — `NULL` in the tenant column, or a child whose parent is missing or itself without an organization — is accepted by `enable` and stays in the cloud in `<table>__none`. No move takes it. Statements without an organization see these rows as before `enable`; statements of an organization do not (they did not before either: `organization_id = 1` does not match `NULL`). A child row's organization is taken from its parent on insert and never recomputed, so a new parent from another organization is refused.

## Pausing one organization

Every statement on LokaDB tables takes a shared advisory lock of its organization in the same transaction; a statement not reduced to one organization takes the shared "all organizations" lock. `move-org` (and `repoint`, `org-profile`) takes both exclusively while it changes that organization: its open transactions finish, new ones wait, other organizations do not wait. Connections that bypass the proxy do not take the lock; a `CHECK` constraint on the cloud partition protects against their inserts during a move.
