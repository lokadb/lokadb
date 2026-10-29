# Migrations and reconciliation

The application's migrations go through the proxy as usual. DDL on LokaDB tables is replayed in the database of every moved organization.

## How DDL is replayed

- The replay happens **before the transaction commits** (at `COMMIT`, or at the end of an implicit transaction; DDL in autocommit is wrapped in a transaction by the proxy). The DDL runs in open transactions in every organization's database, then the cloud commits, then they do. If one organization's database refuses the DDL while it runs, nothing is committed anywhere. If a commit then fails in an organization's database after the cloud has committed (a dropped connection, say), the cloud has the change and that organization does not: the proxy reports "ddl committed in the cloud but not in the organization's database", and [`schema-sync`](#schema-reconciliation-schema-diff-schema-sync) `-org N` repairs it.
- `SAVEPOINT` / `ROLLBACK TO` and failed transactions are honoured: what was rolled back is not replayed.
- Other sessions reload metadata right after such a migration, and every session at least every 30 s.
- **Where to:** the proxy reads the list of targets in the same cloud transaction — each moved organization's schema, the address of its FDW server and the credentials of its `-customer-app` role from the user mapping. An organization moved a minute ago gets the next migration without a proxy restart; a returned one gets none; `customers` in the proxy config is only needed when the proxy sees a database at a different address than the cloud. An organization with no credentials makes the DDL fail with a clear error instead of silently falling behind.

### What is replayed

- `ADD` / `DROP COLUMN`, `ALTER COLUMN TYPE`, `SET` / `DROP DEFAULT`, `SET` / `DROP NOT NULL`;
- `CREATE INDEX`, `CREATE UNIQUE INDEX` (on each local partition);
- `RENAME COLUMN` (Rails `rename_column`), except the tenant column, the key and lookup columns; `ALTER INDEX … RENAME`;
- `CREATE INDEX CONCURRENTLY` — as a statement of its own outside a transaction, as Postgres requires, and with a name. It builds concurrently in every organization's database, then on every local cloud partition, then creates the parent index, which only attaches the ready ones. Writes of every organization continue. If anything fails to build, what was built is dropped. Permissions are checked in the cloud before the first build.

A plain (non-unique) index that an organization's database cannot build (a missing operator class or extension) is skipped there with a warning in the proxy log; `schema-diff` shows it as missing. The table answers the same, only slower.

If an organization's tables live in the `lokadb` schema of its database (because the customer has their own table of the same name in `public`), `public.<table>` in a migration is rewritten to `lokadb.<table>` for that organization.

### What is refused

With a clear error: renaming a table, `DROP TABLE`, constraints (including `ADD COLUMN … REFERENCES / UNIQUE / CHECK`), `DROP INDEX` of a LokaDB table (indexes are named differently in organizations), `TRUNCATE` (it would empty the table of every organization). DDL on LokaDB tables is also refused while `move-org` runs: run the migration again after the move.

Constraints, renames and `DROP TABLE` on moved tables are **not supported yet** ([limits](limits.md#known-limitations)). Keep such changes off the moved tables. If a migration needs one, there is a way round that costs a move: [`move-back`](rollback.md) every organization, [`disable`](rollback.md#lokadb-disable-undoing-enable), run the migration, `enable` again and move the organizations out again.

A migration that writes into a table after `ADD COLUMN` or `RENAME COLUMN` in the same transaction (Django `AddField` and `RunPython` in one migration) fails: the organization's schema changes only before `COMMIT`. Nothing is corrupted; split the schema change and the write into two migrations. `lokadb check` finds such transactions in the recorded workload (`ddl_then_write_transaction`). Send the `COMMIT` of a transaction that contains DDL as a statement of its own, not in the same query string as the DDL (`ALTER TABLE …; COMMIT`): the proxy refuses that with "DDL on LokaDB tables and transaction control in one query string".

## Schema reconciliation: `schema-diff`, `schema-sync`

```bash
lokadb schema-diff -db env:CLOUD_DSN [-org N[,N]] [-json]
lokadb schema-sync -db env:CLOUD_DSN (-org N[,N] | -all) [-drop-extra]
```

`schema-diff` compares each moved organization's copies of LokaDB tables with the cloud: columns (name, type, `NOT NULL`, generated, collation), indexes (by definition, not name), `CHECK`, foreign keys between LokaDB tables. Exit code 0 — identical, 2 — differences or an organization unreachable, 1 — failure. `schema-sync` brings the chosen organizations to the cloud's schema, one transaction per organization, and compares again; what exists only in the organization is dropped only with `-drop-extra`. While it runs, the schema of LokaDB tables in the cloud does not change (the migration waits, writes continue).

Run them after the proxy error "ddl committed in the cloud but not in the organization's database", after manual changes in an organization, after DDL that bypassed the proxy, and in CI after migrations (`schema-diff`, by exit code).

## Data reconciliation: `refs-diff`, `refs-sync`

`<table>__refs` holds the organization of every key; the proxy finds organizations by it and foreign keys of other tables point at it. The cloud keeps it by trigger for every write that goes through the cloud. Rows written directly in an organization's database are not in it; rows deleted there leave their key behind.

```bash
lokadb refs-diff -db env:CLOUD_DSN [-org N[,N]] [-cloud]
lokadb refs-sync -db env:CLOUD_DSN (-org N[,N] | -all [-cloud]) [-delete-stale] [-max-sequence-jump N]
```

`refs-diff` reports rows written around the cloud, keys `__refs` gives to another organization (manual fix only), keys whose rows are gone, and an organization's largest id when it is ahead of the cloud sequence (the cloud would issue that id again). `refs-sync` adds missing keys and advances the sequence — by at most `-max-sequence-jump` (100 000) by default, because the sequence is shared by all organizations and one huge id would exhaust it for everyone; a larger jump is named and needs the flag explicitly. `-delete-stale` removes keys of vanished rows (foreign keys of cloud tables then follow their `ON DELETE`). The maps of unique columns are reconciled too. The organization is paused while this runs. `-cloud` adds the organizations that live in the cloud (moved back, or never moved) to the comparison; with `refs-sync` it goes with `-all`. Both commands exit with 2 when differences are left, 1 when they failed.
