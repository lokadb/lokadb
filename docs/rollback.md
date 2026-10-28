# The way back: `move-back`, `repoint`, `disable`

## `lokadb move-back`: returning an organization to the cloud

*Run it where you ran `move-org`: on a machine that reaches your database and, through the relay, the organization's.*

```bash
lokadb move-back -db "$CLOUD_DSN" -spec spec.json -org 1 -customer "$CUST_ADMIN_DSN" [-dry-run]
```

`-spec` is the same spec file as for `enable`; `-customer` is the organization's administrator through the relay's port for it (the names are those of [Install on your database](install.md#the-names-used-on-this-page)). **Success:** the command logs `move-back done`; the organization's rows are in your database again, in a partition of their own (`<table>__o<N>`), and the proxy reads them there.

Use it at the end of a contract, to stop using LokaDB for this organization, or to move to another database. It is the trigger-based move run in reverse:

- capture triggers are installed on the organization's tables **in its database** and log every change there — including foreign-key cascades and writes that bypass the cloud, which the cloud never sees;
- the tables are copied into unattached cloud partitions and the log is applied to them. Logical replication is not used in this direction (a subscription writes by table name, and the cloud table is the parent: rows would flow back to the customer through the foreign partition);
- **the switch-over:** first only this organization is paused while the log drains; then `ACCESS EXCLUSIVE` on the cloud parents and `EXCLUSIVE` on the customer's tables (so direct writers there wait too), cloud first; the tail of the log; snapshots of both sides; `DETACH` of the foreign partition and `ATTACH` of the cloud copy (nothing is scanned); row counts and row hashes are compared before any change — a difference cancels the switch and the organization stays in its database; `COMMIT`;
- after the switch: a full comparison of the two snapshots (a mismatch names up to five tables and keys), foreign keys of the new partitions validated, the FDW server dropped;
- `__refs` catch up with rows written in the customer database bypassing the cloud; if such a row's id belongs to another organization, nothing switches and the error names the id;
- for 60 s (`-bridge`) a view stands in for the old foreign table, so a proxy with stale metadata writes into the new partition, not into a database the cloud no longer reads;
- **the customer's tables stay** with a comment "returned to the cloud by LokaDB at …"; LokaDB does not drop them (the command to do so is logged), and a later `move-org` of the same organization into the same database sets them aside in a schema `lokadb_returned_<N>_<date>` (or drops them with `-drop-returned`; see [Moving organizations](moving.md#moving-an-organization-again)), while the customer's own tables of the same names are never touched;
- without an administrative DSN (the role does not own the tables or cannot create the log schema), the copy runs **under the organization's pause** without triggers: slower, but the data comes back. `-dry-run` says which way it will take and how many rows.

**The customer database is unavailable.** `move-back` refuses and names the way out: LokaDB keeps no copy of a moved organization's rows. The customer restores the database from their backup, then `lokadb repoint`, then `move-back` (the loss is their backup's RPO). If the data is gone for good, `lokadb move-back -db … -spec spec.json -org N -abandon -confirm-org N` returns the organization **empty**: its `__refs` rows are deleted with the foreign-key actions of cloud tables, with a loud warning.

## `lokadb repoint`: a new database with the same rows

```bash
lokadb repoint -db "$CLOUD_DSN" -org 1 -customer-app "$NEW_CUST_APP_DSN" -proxy-config lokadb.json
```

For a restore on another host, a failover to a replica, a new password.

- The new database is checked first: every foreign table has its table there with the same columns and types, made for this organization.
- Every other address the proxy uses for the organization (direct path, DDL replay — from the control plane, or from `customers_app` / `customers` in the config) must lead to the same database: `repoint` holds a random advisory lock on its connection to the new database and expects every other address to see it in `pg_locks`. A copy, a replica or another database of the same server does not. Credentials in the control plane are moved to the new host, port and database; config files are not edited — if an entry leads elsewhere, `repoint` changes nothing and names it.
- The new database gets a new id in its table labels; the proxy refuses a database with a different id, so the old database is no longer used after `repoint`.
- TLS options of the FDW server come from the new DSN; options it does not have are removed.
- `ALTER SERVER` and `ALTER USER MAPPING` happen under the organization's pause; proxies reload credentials before their next statement. An ambiguous `COMMIT` is resolved by checking the cloud over a new connection.
- No rows are copied. To move with the data from database A to database B: `move-back`, then `move-org`.

## `lokadb disable`: undoing `enable`

```bash
lokadb disable -db "$CLOUD_DSN" -spec spec.json [-dry-run]
```

- **One transaction under `ACCESS EXCLUSIVE`.** The table `enable` started from, `<table>__cloud`, stays with its indexes, constraints and statistics; rows of other partitions (returned organizations, rows without an organization) are poured into it with triggers and foreign-key checks off. The pause grows with these rows; `-dry-run` counts them.
- **What comes back:** views, rules and functions that pointed at the parent; policies of other tables and functions over the row type; the application's triggers with their firing mode; privileges including column privileges; the application's own RLS (`FORCE` too) and policies; foreign keys to `__refs` point at the table again; the added organization column of child tables is dropped; identity and the original owner; index names.
- **Waiting for locks is bounded:** a long reader does not hold `disable` — and every statement behind it — forever. Each lock waits at most `-lock-timeout` (5 s), repeated `-attempts` (5) times; if the tables never free up, `disable` changes nothing.
- While any organization is in its own database, `disable` refuses: `move-back` each first.
- `__refs` and the `lokadb` schema are dropped after `-grace` (60 s), while proxies with old metadata may still read them. If the application built something on `lokadb.*` after `enable` (a view using `lokadb.org()`, a default, a policy), `disable` names it and leaves the schema rather than dropping it with `CASCADE`.
- Open proxy sessions learn about `disable` by notification (or within 30 s) and pass statements through as before `enable`, including statements prepared while the tables were enabled.
- After `enable -prepare` without `enable`, `disable` removes what the preparation did.

We test `move-back` and `disable` under continuous writes, and check that the rows equal those before the return and the schema equals the one before `enable`.
