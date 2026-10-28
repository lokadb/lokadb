# Moving organizations

## `lokadb enable`: converting the schema

`enable` is a one-time conversion of the tables in the spec. All organizations stay in the cloud.

```bash
lokadb enable -db CLOUD_ADMIN -spec spec.json -owner app             # one step
lokadb enable -db CLOUD_ADMIN -spec spec.json -owner app -prepare    # large tables: online preparation first
lokadb enable -db CLOUD_ADMIN -spec spec.json -owner app             # then the switch under a short lock
```

- **No row copying.** Each table is renamed to `<table>__cloud` and attached as the DEFAULT partition of a new parent with the old name. Indexes, constraints and statistics stay with it; no double disk space. An organization gets its own partition `<table>__o<N>` on the first step of `move-org`. After commit, `enable` builds an index on the partition key of `<table>__cloud` concurrently.
- **Two steps for large tables.** `enable -prepare` runs online and can be interrupted and repeated: it fills `__refs`, the organization column of child tables, the key mappings, adds foreign keys and indexes, and checks declared relations on the data. `enable` then switches under a short lock (the downtime of the switch does not grow with the table; in one step it does). If the application moves a parent to another organization between the two, `enable` notices and asks to run `-prepare` again. Without `-prepare`, `enable` warns about the expected downtime.
- **What refers to the table by OID moves to the parent:** triggers, views, rules and SQL functions with `BEGIN ATOMIC` (recreated from their own definitions), RLS policies of other tables, functions over the row type, publications (with `publish_via_partition_root`), materialized views (recreated with their indexes, owner, grants; a populated one is refreshed under `enable`'s locks), privileges of other roles (and removed from the partition, otherwise it could be read around the policy), identity (becomes a sequence on the parent that continues the identity). The application's own RLS stays in force; LokaDB's organization policy becomes `RESTRICTIVE` and narrows it.
- **`__refs`.** For incoming foreign keys, `<table>__refs` maps each key to its organization, kept in sync by a trigger on insert, delete and changes of the key or organization. Foreign keys to the table point at it (to the key alone or the key with the tenant column, as in Citus and Rails). A foreign key to another single-column unique column points at a per-column map.
- **Child tables without an organization column** get one with a default that takes the organization from `lokadb.org`; an insert whose parent was not found is refused instead of being written under an invented organization. A parent trigger checks on insert that the child's organization equals the parent's.
- **Refused:** constraint triggers and triggers with transition tables (Postgres does not allow them with foreign partitions), and a materialized view that other objects depend on beyond views, rules and functions.
- **Writes to a moved organization go through the cloud** (the proxy). A write made directly in its database is invisible to `__refs` and to cross-organization key uniqueness until `refs-sync`.
- `enable` is safe to repeat: enabled tables are skipped, new tables are added (while no organization is moved).

## `lokadb move-org`: moving an organization online

```bash
lokadb move-org -db CLOUD_ADMIN -org 1 \
  -customer CUSTOMER_ADMIN_DSN -customer-app CUSTOMER_APP_DSN \
  -proxy-config lokadb.json -cp CONTROL_PLANE
```

Two modes (`-mode`):

- **`trigger`** — no logical replication. Triggers on the organization's partitions log every change to `lokadb.move_log`; `move-org` copies the partitions with `COPY`, applies the log in the customer database whole cloud transactions at a time, in commit order, idempotently, and drains the log under the switch-over lock. It works on managed Postgres without logical replication and with a customer role that cannot subscribe. Cost: one log write per change of the organization while the move runs.
- **`logical`** — the customer database subscribes to a cloud publication of the organization's partition. It needs `wal_level = logical` in the cloud, a subscription right in the customer database, and the explicit flag `-allow-customer-replication`, because a role with the `REPLICATION` attribute can decode the change stream of the **whole** cloud database, not only one organization's tables. Use it only when the cloud and the customer database have the same owner.

  Logical moves need the path from the customer database to the cloud, which also goes through the tunnel:
  - `-publisher DSN` — the cloud database **as the customer database sees it**: the agent's `-cloud-listen` address (for example `host=agent port=5433 …`), not the cloud's own. Without it `move-org` uses `-db`, which is usually not reachable from the customer's network. Use `sslmode=disable` or `prefer` in this DSN, not `require`: the relay reads the start message and answers a TLS request with a refusal, since the agent-to-relay leg is already mutual TLS.
  - the agent's `-cloud-allow` — which addresses may connect to `-cloud-listen`. By default: the host of the agent's `-db`, loopback and the addresses of the agent host's own interfaces, so a subscription that comes in through the host's external address works without configuration. If it comes from elsewhere, list it. A refused connection gets the agent's own error — `lokadb agent: connection from X to the cloud window refused; add it to -cloud-allow` — and when `CREATE SUBSCRIPTION` fails, `move-org` prints a hint about `-cloud-allow` (also for an older agent that only closes the connection, which shows up as `server closed the connection unexpectedly`).
  - the cloud's `pg_hba` must admit the move's temporary role from the relay's address (see [limits](limits.md#installation-direct-access-to-the-cloud-database-is-closed)).
- **`auto`** (default) always uses triggers.

### The steps

| Step | What happens | Writes |
|---|---|---|
| 1 | The organization's rows are split out of the DEFAULT partition into its own partition, **while the organization keeps working**: capture triggers on DEFAULT for this organization's rows only (a `WHEN` condition on its number) log every change; its rows are copied in batches into a partition that is not attached yet; the changes made meanwhile are applied in rounds. Only then is the organization paused: `CHECK (organization <> N)` on DEFAULT, the rest of the log, and for each table one transaction — copies of rows that a foreign-key cascade deleted after the snapshot are removed, row counts are compared, the rows are deleted from DEFAULT in one statement, `VALIDATE` and `ATTACH` (which scans nothing). | one pause for all tables, bounded by `-max-split-pause` (10 min); others: milliseconds |
| 2 | Changes of the partition start being captured (log triggers, or a publication of the whole partition). | continue |
| 3 | Initial copy into the customer database (a consistent snapshot, in batches, resumed after a dropped WAN connection). | continue |
| 4 | Catch-up until the lag is small or stops changing; a constant lag is delivery time, not a backlog. | continue |
| 5 | `LOCK … IN ACCESS EXCLUSIVE` on parents and partitions; drain the remaining changes; snapshot of the customer database; `DETACH` the organization's partition and `ATTACH` the foreign partition; compare row counts and row hashes before committing (a difference cancels the switch, the organization stays in the cloud); `COMMIT`. | **pause** (seconds at a distance of tens of milliseconds) |
| 6 | Compare the detached partition with the customer snapshot (row count and hash of every row), then drop it (`-retain-source` keeps it). | continue |

While waiting for its locks, the switch-over uses short attempts (a quarter of `deadlock_timeout`) and releases what it took between them, so an application transaction that holds the organization's partition and then cascades into a parent commits instead of getting `40P01`.

### Limits and progress

| Flag | Default | Limits |
|---|---|---|
| `-copy-timeout` | 24 h | the initial copy |
| `-catchup-timeout` | 2 h | catch-up |
| `-catchup-stall` | 10 min | a trigger-mode catch-up that does not converge: the rest of the log has not become shorter for that long (`0` turns it off) |
| `-max-pause` | 10 s | the switch-over pause (otherwise the attempt is repeated) |
| `-max-split-pause` | 10 min | the organization's pause at the end of step 1 |
| `-batch-rows` | 10 000 | rows per transaction when copying |
| `-lock-timeout` | 5 s | waiting for a lock before retrying |

Progress is logged once per step and table, at most every two seconds:

```
msg=progress org=N step=split|copy|catch-up table=T rows=N rows_total=M bytes=B [bytes_total=S] elapsed=D [eta=D] [state=started|waiting|done]
```

What remains of the pause in step 1 is the `DELETE` of the organization's rows from DEFAULT, without which `ATTACH` does not pass. It grows with the organization's volume: about 2 s per million rows of a table (6.1 and 7.2 s for two tables of 3 million in the measurement). It is a limit of LokaDB, not of Postgres; the ways around it — keeping the organization in a partition of its own from the start (splitting it at `enable` or by a background job while it is small) — are planned.

### Catch-up speed in trigger mode

The log is applied in rounds of up to 5,000 records or 8 MB of row images. A round goes to the organization's database as one exchange: `BEGIN`, all the changes and `COMMIT` in one pipeline, and the drain under the switch-over pause uses the same rounds. Earlier a round was 500 records in 6 exchanges, which capped the rate at about 1,900 changes per second for an organization 43 ms away, however fast its server was. Now the ceiling at that distance is 5,000 ÷ (the round-trip time + the server's work on 5,000 records) per second — about 7,800 for 200-byte rows and 0.12 ms of work per record.

Every 5 s the catch-up logs a `progress` line with `in_per_s` (changes arriving), `out_per_s` (applied), `left` (the rest of the log), `round` (duration of a round) and `eta` (or `never at these rates`). If the rest does not get shorter for `-catchup-stall` (10 min), `move-org` stops with nothing switched and the organization stays in the cloud; the error names the rates and the way out: move when the organization writes less, or use `-mode logical -allow-customer-replication`, where Postgres applies changes as a stream, without rounds.

`move-org -dry-run` changes nothing and estimates this in advance: it measures the round trip from here to both databases, counts the changes of the organization's rows over `-sample` (10 s) from `pg_stat` (its partitions, or, while it is in DEFAULT, the whole DEFAULT — an overestimate), and compares them with the ceiling.

### Moving an organization again

An organization that was returned with `move-back` and is moved to the same database again finds its old copy there. By default `move-org` **sets it aside**: the tables go to a schema `lokadb_returned_<N>_<date>` (`ALTER TABLE … SET SCHEMA`: names, indexes and constraints go with the table, nothing is deleted), the move continues as usual, and a warning says where the copy is, which tables, and how to delete it (`DROP SCHEMA … CASCADE`). With `-drop-returned` the copy is dropped (`DROP TABLE` without `CASCADE`). If the customer built a view or a foreign key on the copy, the move stops in both cases — a copy set aside would leave them reading stale data — and the error names the dependency and the exact `DROP TABLE … CASCADE`. Another organization's copy and the customer's own tables of the same names are never touched; the move stops on them. The schema is created by the `-customer` role (it needs `CREATE` on the database).

### Failures

Any failure before `COMMIT` leaves the organization entirely in the cloud; running `move-org` again starts over. Ctrl-C and `SIGTERM` clean up (triggers, log, subscription, slot, publication, temporary role). If `move-org` is killed hard, capture triggers stop writing on their own after 10 minutes without its heartbeat, the move refuses to complete if anything could have gone past the log, and `lokadb move-cleanup -db CLOUD` removes the leftovers (`check` reports them). Two `move-org` runs on one cloud database do not overlap: the second fails at once; organizations move one after another.

During a move, write to the organization's data only through the proxy. A writer that bypasses it gets a `CHECK` error on DEFAULT (not a lost write). Application DDL on the moved tables is refused during a move: run the migration again afterwards.

## Where the customer database comes from

The agent is only a tunnel: it does not and cannot create a Postgres. Today the customer prepares the database (their server, cluster or managed service); automatic provisioning next to the agent is not built. The customer provides:

- a database and an administrative role for the move (`-customer`);
- a second, non-privileged role (`-customer-app`) that becomes the owner of the moved tables; the cloud reads and writes through it (`postgres_fdw`) and the proxy replays DDL with it. It needs no rights beyond `CREATE` on the schema of its tables. Without it the cloud would keep the customer administrator's credentials, and `move-org` warns;
- for `fdw+direct`, a third role with DML only (the direct path);
- network access from the agent to the database, and nothing else.

Where the tables go: into `public` if the roles can create and own them there and the customer has no table of the same name; otherwise into a `lokadb` schema that `move-org` creates. Tables are full copies of the cloud tables (columns with collations, generated columns, `CHECK`, indexes; `UNIQUE` and `EXCLUDE` as constraints). Missing extensions, collations, enum types (and their labels), domains, composite types and functions are created first; existing objects are never overwritten. `move-org` labels each table it creates (`lokadb: organization N database <id>`) and only ever drops tables with its own label; a customer table with the same name is left alone. `lokadb check -customer -customer-app -org N` verifies all of this in advance.

## Measured

The pause of writes at the switch-over, the duration of the move and the lag of the trigger mode, measured with an organization 43 ms from the cloud under continuous writes, are in [performance.md](performance.md#moving-an-organization-under-writes). The pause at the switch does not depend on the volume the way the copy does — it covers the marker, `DETACH` and `ATTACH`, not a copy — but at a distance of tens of milliseconds it is seconds, not milliseconds. The end-to-end stand moves an organization under a load generator for each of three applications and verifies every acknowledged write afterwards.
