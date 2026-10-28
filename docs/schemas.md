# Schemas and keys

## Tables outside `public`

A table is named in the spec and in LokaDB's metadata by a key: just the name for `public` (`documents`), `schema.name` otherwise (`app.documents`; a schema with a dot or a quote is quoted: `"a.b".t`). A `public` table with a dot in its name is written with the schema: `public.acct.docs` for `public."acct.docs"`. In `relations`, the column is the last part: `app.documents.owner_id`.

Helper tables (`__cloud`, `__refs`, `__o<N>`, `__org<N>`, …) are created in the table's own schema. In the organization's database, a table of schema S ≠ `public` goes into the same schema S (created if missing); `public` tables go into `public` or `lokadb`. If the customer already has a table of the same name in S, `check` reports it and `move-org` refuses before any change.

The proxy resolves table names as the cloud does: a qualified name by its key, a bare name by the session's `search_path`. It tracks the path itself — the startup parameter and `options`, role and database defaults including `"$user"`, `SET`, `SET LOCAL`, `set_config` with a constant, `ROLLBACK` — and re-reads it from the cloud after `RESET`, `DISCARD ALL`, `SET ROLE`, a computed `set_config`, or a call to an application function or a `DO` block that mentions `search_path`. If the path is unknown and a bare name exists in several schemas, the proxy refuses and asks for a qualified name. DDL with a bare name is replayed in the organization with the same path, `public` replaced by the organization's schema.

## Row keys: integers, UUIDs, text

The row key is what `__refs` uses to find a row's organization: a single-column primary key, or `id`.

- **Types:** `smallint`, `integer`, `bigint`, `uuid`, and `text` / `varchar` with a deterministic collation. Other types (`numeric`, `char(n)`, `citext`, `bytea`, dates, enums, domains, non-deterministic collations) are reported by `check` (`id_type`) and refused by `enable`.
- **Keys from a default** (`gen_random_uuid()`, `uuidv7()`, an extension function) are computed in the cloud on insert; `RETURNING id` returns them.
- **The proxy** reads key values in any form Postgres accepts (`'A0EEBC99-…'`, `'{a0eebc99…}'`, without dashes), as a literal or a parameter in text or binary (pgx sends `uuid` as 16 bytes), as `uuid[]` or `IN (…)`. A value that does not read as a key names no organization.
- Moving, moving back, `disable`, `refs-diff` / `refs-sync` and the direct path work with every supported key type.

### Composite keys

A key can have several columns when the table has no `id` or `id` repeats (a unique `id` stays the key). Two common shapes:

- **the tenant column is part of the key** — `orders PRIMARY KEY (account_id, id)` (Citus, Rails 7.1 `query_constraints`): the tenant column itself names the organization; `WHERE id = 3` without it is a statement without an organization;
- **a key without the tenant column** — `parcels PRIMARY KEY (tracking_no, piece)` with its own `account_id`: a query by the whole key (`WHERE tracking_no = $1 AND piece = $2`, `(tracking_no, piece) IN ((…), (…))`, values through join equalities) finds the organization in `parcels__refs`; by part of the key — no organization.

`__refs` then has columns `id_1 … id_n`; foreign keys to a composite key point at it; moving, `-prepare`, `disable`, `move-back` and reconciliation work on every column; the direct path takes such tables like single-column ones.

Not supported yet: a child table without its own organization column that gets it from its parent through a composite foreign key; `routing.relations` to a composite key.

## Organizations identified by a UUID

Rails, Django and Laravel applications with UUID keys often have a `uuid` organization too (`account_id uuid`). LokaDB distinguishes two names of an organization:

- the **value** — what is in the tenant column. It is the organization for everything that sees rows: partitions are split by value (`FOR VALUES IN ('…')`), the proxy puts it in `lokadb.org`, the application names the organization by it (`WHERE account_id = $1`, `SET LOCAL lokadb.tenant = '<uuid>'`);
- the **number** `N` — an integer LokaDB uses for names and settings: partitions `<t>__o<N>` and `<t>__org<N>`, the FDW server, the tunnel and tokens (`-org N`), the proxy config (`customers`, `customers_app`).

For an integer column they coincide. For `uuid`, give each organization its number once, after `enable` and before anything that names the organization (`lokadb token`, the relay's `orgs` entry, `org-credentials`, `move-org`):

```bash
lokadb org-add -db "$CLOUD_DSN" -tenant 0f8fad5b-d9cb-469f-a165-70867728950e [-org N]      # prints: organization 7: 0f8fad5b-…
```

Without `-org N` it takes the next free number. After that, `check`, `enable`/`move-org`, `move-back`, `repoint`, `org-profile` and `org-credentials` take either `-org N` or `-tenant <uuid>`; every other command (`token`, `revoke`, `org-endpoint`, `schema-diff`, `refs-diff`, …) takes the number only. Rows without an organization live under the nil UUID, so an organization with the nil UUID is impossible (`check` reports such rows as a blocker). All LokaDB tables of one database must use the same kind of tenant column. Everything works for UUID organizations: `check`, `enable`, routing, cooperative mode, both move modes, `repoint`, `move-back`, `disable`, reconciliation and the direct path. Text organization identifiers are not supported yet.
