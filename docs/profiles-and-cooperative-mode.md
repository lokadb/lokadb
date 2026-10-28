# Routing profiles and cooperative mode

A moved organization's rows always live in its database. Its **profile** decides how statements reach them.

| Profile | Path |
|---|---|
| `fdw` | every statement goes to the cloud Postgres, which reaches the organization's database through `postgres_fdw` |
| `fdw+direct` | statements that touch only this organization's LokaDB tables go straight to its database through the tunnel; the rest go through `postgres_fdw` |
| `direct` | no `postgres_fdw` path; statements the direct path cannot take are refused with the reason |

```bash
lokadb org-profile -db CLOUD -org 1 fdw+direct
lokadb check -db CLOUD -workload workload.json -profile fdw+direct -org 1   # estimate first
```

`org-profile` changes the profile under the organization's pause: its open transactions finish, new ones wait, proxies learn about the change at once. **`fdw+direct` is the default** when `move-org` is given the proxy config (`-proxy-config`) and it has the organization's direct-path credentials; without them the organization stays on `fdw`, and `move-org` and `check` say what that costs (about five round trips to the organization per statement on `fdw`, about one and a half on `fdw+direct`).

## The direct path (`fdw+direct`)

The direct path is stricter than `fdw`: the organization must be known for **every** LokaDB table of the statement (by its own condition, or by equality with a column resolved through the same `__refs`), or named by the application. A mistake on the direct path would be a read from the wrong database, and it would be silent. The same function decides in the proxy and in `check`, so the estimate matches the behaviour.

What goes direct:

- reads; `UPDATE` that changes neither the key nor the tenant column; `INSERT … VALUES`; `DELETE`; upsert (`ON CONFLICT` on the key, the key with the tenant column, or a unique index the organization has); `INSERT … SELECT` that names the key; `INSERT … SELECT * FROM unnest(…)` (Django `bulk_create`); `COPY FROM STDIN` when the application names the organization;
- as a simple query, as `Bind`/`Execute` of a prepared statement, as an unnamed `Parse`/`Bind`/`Execute` batch, and as a pipeline of such statements of one organization (`pgx.Batch`, JDBC `executeBatch`) — one round trip for the pipeline;
- **transactions**, once their shape is learnt: the proxy holds `BEGIN` and decides on the first statement. A new shape goes through the cloud; after two committed transactions of the same shape whose every statement would qualify, it goes direct. Shapes can be learnt in advance from a recorded workload: `lokadb check … -profile fdw+direct -direct-forms forms.sql`, then `psql -f forms.sql` as the cloud database owner. A direct transaction keeps a cloud transaction open for the pause, the permission check and `__refs` rows; it commits the organization first, then the cloud (as `postgres_fdw` does). A transaction of reads and an `UPDATE` waits 5 round trips instead of 8; two reads, 4 instead of 8.

How writes stay consistent: the proxy does what cloud triggers do on the `fdw` path. It opens a cloud transaction under the organization's pause, checks permissions, writes the `__refs` rows of new keys (a key the statement does not name is computed in the cloud from its default: a sequence, `gen_random_uuid()`, `uuidv7()`), then runs the statement in the organization's database with the key and tenant column added, and commits the organization first. A failure between the two commits leaves the same window as `postgres_fdw` — a row without its `__refs` entry or the other way round; the client gets `40003`, and `lokadb refs-diff` / `refs-sync` repair it. If the connection to the organization breaks on its `COMMIT`, the proxy asks Postgres for the transaction's status and follows it; if it cannot know, the client gets `08007`.

What stays on the cloud path: tables with a row trigger of the cloud (the application's own trigger, a foreign key to a cloud table), identity or generated columns, inserts that rely on a cloud default, a key that belongs to another organization, statements that join cloud tables, functions outside a short safe list, operators from extensions, other `INSERT … SELECT` and `MERGE`.

The direct-path role (`customers_app` in the proxy config, or `org-credentials -kind direct`) is a separate role in the organization's database with only `SELECT, INSERT, UPDATE, DELETE` on its tables — not an owner, no `CREATE`, not a superuser. The proxy verifies it on every new direct connection; with extra rights or missing DML, statements go through the cloud and the log says why. Permissions of the client role are checked in the cloud on every statement, so a `REVOKE` takes effect at once.

Before using a direct connection or replaying DDL, the proxy reads the comments `move-org` put on the organization's tables (`lokadb: organization N database <id>`): a database labelled for another organization, or with no label of its own, is not used.

Session settings the application sets (`TimeZone`, `DateStyle`, `statement_timeout`, `application_name`, `client_min_messages` and a few more) are repeated on the direct connection. Any other setting, `SET ROLE`, or a `search_path` change when the organization's tables are not in `public`, turns the direct path off for that session.

### Estimating a profile before switching

`check -profile fdw+direct` reports the share of patterns and calls on moved tables that would go direct, the reason for the rest (join with a cloud table, organization not named on every table, statement without an organization, `MERGE`, a function, a sequence), transactions that would need two connections and two commits, and transactions that would start direct and then need `postgres_fdw` (which would not see their uncommitted rows — those are refused once and then routed through the cloud). `check -profile direct` adds the blockers of that profile: joins with cloud tables, reads without a named organization, statements across organizations.

## The `direct` profile

```bash
lokadb org-profile -db CLOUD -org 1 -control-plane CP -key /keys/lokadb.key [-drop-fdw-credentials] direct
```

The organization has no `postgres_fdw` path. Its statements on LokaDB tables go only to its database; what the direct path does not take gets `0A000` with the reason instead of going through the cloud — including reads that do not name the organization (a guessed read from the wrong database would silently return nothing). Transactions are direct from the first one. The profile needs both credentials of the organization in the control plane. `-drop-fdw-credentials` removes the user mapping of its server: the cloud database and its roles can no longer reach the organization's database; only the proxy can, and only for the application's statements. Going back to `fdw` needs `-key` to recreate the mapping. Reports across organizations and copies of cloud tables in the organization's database are not available on this profile yet.

## Cooperative mode

The proxy guesses the organization from the statement. The application can name it instead, in one place where it already knows it (request middleware, job start):

```sql
SELECT set_config('lokadb.tenant', '42', true);   -- or: SET LOCAL lokadb.tenant = 42
```

Enable it with a `"tenant"` section in the proxy config. Then, for statements on LokaDB tables:

- **no `__refs` lookups** at all, including child inserts and statements inside a pipeline;
- **relations without foreign keys need no declaration**: `WHERE conversation_id = $1` runs in the named organization;
- **cross-check**: a statement that itself names another organization (`organization_id = 7` while `lokadb.tenant = 42`) is refused with `42501` (or, with `"mismatch": "log"`, run under the named organization and logged — for the migration period);
- **another organization's row is not found by id**: the cloud policy shows only the named organization's rows;
- on `fdw+direct`, a statement with no organization condition at all (`SELECT … FROM documents`) can go direct: the organization's database holds only its rows.

The value follows Postgres rules: `SET LOCAL` and `set_config(…, true)` last until the end of the transaction, `ROLLBACK TO` restores the savepoint's value, `RESET` clears it. **Per-session values are refused by default** (`"local_only": true`): behind a pooler in transaction mode (pgbouncer, Odyssey, RDS Proxy) a session value would leak to the next client of the same server connection and send its statements to another organization. Allow them with `"local_only": false` only when there is no pooler in front of the proxy or it runs in session mode (for example a pool split by organization; then the value can also come in the connection string, `options=-c lokadb.tenant=42`).

`lokadb.tenant = '*'` declares a statement across all organizations explicitly. It does not lift the `reject` policy: only a statement in `allow`, or a role listed in `"all_roles"`, passes. Otherwise middleware that sets `'*'` by mistake would silently open the whole application.

If the application already sets its own RLS variable with the organization (`app.current_tenant`), name it: `"tenant": {"setting": "app.current_tenant"}`.

A child insert under a named organization runs without a parent lookup; a parent of another organization is refused by the database (`23514`, the parent trigger).
