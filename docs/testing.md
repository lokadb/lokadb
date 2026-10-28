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

Two stands can run on one machine from two copies of the repository: `E2E_NET=232 COMPOSE_PROJECT_NAME=lokadb-e2e2 bash e2e/run.sh`.

## Regression checks for defects

```bash
bash e2e/defects/defects.sh                    # all
DEFECTS_ONLY=<name>,<name> bash e2e/defects/defects.sh
DEFECTS_MODE=trigger bash e2e/defects/defects.sh
```

Each check runs on fresh databases and reproduces one defect found earlier — data loss, security, protocol, moving, migrations, the direct path, poolers — and passes only on code that fixes it. A change that touches routing, moving or the tunnel should come with such a check: one that fails before the change and passes after.

## Unit tests

```bash
docker run --rm -v "$PWD/proxy:/src" -w /src golang:1.25 go test ./...
```

Among them: the proxy protocol against a fake server (errors on commit of an implicit transaction, errors in a batch without `Sync`, `COPY` in the extended protocol, a client disconnecting mid-response); the whole tunnel life cycle in one process (enrolment, token reuse, restart, renewal, revocation, several relays, several agents, draining, the issuing service); and the `check` report format, which fails if a field or a finding code disappears.

## Measurements

`bash e2e/bench.sh` runs the same statements on the same data along seven paths (plain table, LokaDB in the cloud with and without the proxy, a moved organization with and without the proxy, the tunnel alone, the customer database locally), optionally with an artificial delay on the tunnel. Measurements on real networks use `bench/`; their methodology and results are in `research/bench/`.
