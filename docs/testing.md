# Testing

## The end-to-end stand

```bash
cd e2e && docker compose down -v && docker compose --profile tools --profile apps build && cd ..
bash e2e/run.sh
```

Requires Docker. Together with the defect checks below it runs several hundred checks. The stand (`e2e/compose.yml`) has a cloud Postgres (with `pg_stat_statements`), a customer Postgres, the relay, the agent and the proxy, on three Docker networks: the cloud network and the customer network share nothing; only the relay and the agent are on a third one. Before anything else the script verifies that no cloud container can even resolve the customer database, and that it is reachable through the tunnel.

For each of the test applications — unmodified **Laravel**, **Django** and **Go** applications, which stand for any other (LokaDB's scope is the Postgres protocol, not these three) — it:

1. runs migrations and seeding;
2. runs `lokadb check` with the recorded workload and the customer database (the verdict must not be `BLOCKED`);
3. runs `enable`, then `move-org` of organization 1 while a load generator writes for both organizations (20 000 rows per organization by default; `BULK=100000 bash e2e/run.sh goapp` for more), then verifies every acknowledged write;
4. runs the application's scenario, a second release with a new column, and the scenario again: lists with eager loading, lookup by id without the organization, create with relations, update, commit and rollback, upsert, delete with cascades (in the customer database and in the cloud), join with group by, a search without the tenant key;
5. checks physical placement: organization 1's rows exist **only** in its database, zero in the cloud;
6. stops the agent, then the customer database: organization 2 keeps reading and writing, organization 1 gets errors, and the agent reconnects on its own;
7. runs `move-back` and `disable` under load and compares rows and schema with those before;
8. runs `lokadb check` again over the whole run's workload.

Around the applications it also checks: enrolment, renewal, revocation and re-enrolment of agents; `org_allow`, the cloud window, file permissions of keys; the `fanout` policy and refusals of internal objects; `check` on a database with planted problems; `move-org` refusing to touch an existing customer table; a schema the demo apps do not have (extensions, enums, domains, functional indexes, ICU collations, generated columns, a table without a primary key updated during the move); a new organization joining a running relay under load; TLS between the application and the proxy and end to end to the customer database; `pg_hba` closing direct access to the cloud database.

An open transaction when the tunnel drops has a part of its own, `E2E_ONLY=tunnel bash e2e/run.sh`: the agent stopped, the relay killed and a silent network, on both profiles ([results](tunnel.md#when-the-tunnel-drops)).

Two stands can run on one machine from two copies of the repository: `E2E_NET=232 COMPOSE_PROJECT_NAME=lokadb-e2e2 bash e2e/run.sh`.

## Regression checks for defects

```bash
bash e2e/defects/defects.sh                    # all
DEFECTS_ONLY=<name>,<name> bash e2e/defects/defects.sh
DEFECTS_MODE=trigger bash e2e/defects/defects.sh
```

Each check runs on fresh databases and reproduces one defect found earlier — data loss, security, protocol, moving, migrations, the direct path, poolers — and passes only on code that fixes it. A change that touches routing, moving or the tunnel should come with such a check: one that fails before the change and passes after.

## The cross-tenant suite

```bash
bash e2e/defects/crosstenant.sh
```

A session of organization A tries to reach organization B's rows, on real Postgres through the real proxy (`e2e/defects/crosstenant.go`, checks XT1 to XT15). It runs in both directions, on `fdw` and `fdw+direct`, with B in the cloud and with B moved to a database of its own. Every row carries a mark, and a check fails if A's session gets B's mark anywhere: a result, `COPY` data or an error message. So that refusing everything could not pass, each read shape also runs under the organization that owns the row and must return its mark there.

| What it tries | What comes back |
|---|---|
| Organization named (`SET LOCAL lokadb.tenant`): B's rows by organization, by id, through a child table | `42501` when the statement names B, nothing by id |
| A join through a table without an organization column, `LEFT JOIN`, `LATERAL`, a self-join | A's rows alone |
| A CTE or subquery that hides or renames the tenant column; recursive and data-modifying CTEs; `UNION` | A's rows alone, or nothing |
| A view and a function over the table; `COPY`; a partition or helper table by its name | A's rows alone, or a refusal |
| The value in an error message (`title::int`); a yes or no by `EXISTS` or a count | nothing of B's |
| Changing the organization from inside: `lokadb.org`, `row_security`, `lokadb.tenant = '*'` | no effect, or a refusal |
| Writes at B's rows: `UPDATE`, `DELETE`, `INSERT` with B's organization or under B's document, moving a row to A | B's rows unchanged |
| Organization only in the statement: a statement naming A that also tries to read everything | A's rows alone |
| A statement that names nobody, whatever it hides behind | refused (`0A000`, the `reject` policy) |
| One prepared statement reused across organizations on one connection, past the point where Postgres switches to a generic plan, and with `force_generic_plan` | each organization's own rows |
| A pooled connection passing from a client of A to the next client: settings, temp tables, prepared statements, `lokadb.tenant`; a client gone inside a transaction | nothing left; that connection is closed, not reused |
| Clients of both organizations at once for 20 s, connecting and disconnecting on the pools | tens of thousands of reads, none of the other's |
| Postgres's own views: `pg_stats`, `pg_stats_ext`, `pg_stat_activity` and its text by `*`, a whole row, a string for `query_to_xml`, a `DO` block | refused; a count of connections still answers |
| `ANALYZE` of the parent through the proxy, of the whole database, of a foreign partition by name, after a column was added | no value of a moved organization in the cloud's statistics; the row count stays, and on Postgres 18 the numbers |
| An older install with values in its statistics, then `lokadb stats-sync` | the values are gone and stay gone after `ANALYZE` |

**What it does not promise:**

- **LokaDB is not an authorization layer, and it has no per-tenant credentials.** All sessions use the application's role; the organization comes from the application or from the statement. With no organization named, a statement that asks for B's row by its id gets it, and XT5 and XT6 check exactly that. So an attacker who controls the application's SQL can name B. Who may ask for what stays the application's job.
- **Tables you did not choose are shared.** A table without an organization, such as `users` in these checks, is the same for everyone, as it was before LokaDB.
- **The refusals of Postgres's own views read the statement's text.** A function made past the proxy, or a name put together inside dynamic SQL, is not seen: [statistics and running statements](limits.md#what-lokadb-does-not-isolate).
- **A connection to the cloud database past the proxy** sees every organization's rows, as the [security model](../README.md#security-model) says.
- The tunnel's own isolation (one organization's agent cannot serve another's port, `org_allow`) is checked in `e2e/run.sh`, not here.

We checked on throwaway copies that the suite notices a broken protection. With row security turned off in the cloud, XT1, XT3 and XT5 fail. With the policy replaced by one that lets everything through, XT1 fails. With the pool's reset removed, XT9 fails. XT7 and XT11 still pass with row security off, because the proxy's routing alone keeps those statements apart; we have not broken the routing itself to see them fail.

## Unit tests

```bash
docker run --rm -v "$PWD/proxy:/src" -w /src golang:1.25 go test ./...
```

Among them: the proxy protocol against a fake server (errors on commit of an implicit transaction, errors in a batch without `Sync`, `COPY` in the extended protocol, a client disconnecting mid-response); the whole tunnel life cycle in one process (enrolment, token reuse, restart, renewal, revocation, several relays, several agents, draining, the issuing service); and the `check` report format, which fails if a field or a finding code disappears.

## Measurements

`bash e2e/bench.sh` runs the same statements on the same data along seven paths (plain table, LokaDB in the cloud with and without the proxy, a moved organization with and without the proxy, the tunnel alone, the customer database locally), optionally with an artificial delay on the tunnel. Measurements on real networks use `bench/`; their methodology and results are in `research/bench/`.
