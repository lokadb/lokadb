# Quickstart

This is a **demo stand**: it runs on its own throwaway databases and installs nothing on yours. To put LokaDB in front of your own database, see [Install on your database](install.md).

Get the repository first, then one command starts a complete small setup and moves one organization out while an application keeps writing:

```bash
git clone https://github.com/lokadb/lokadb && cd lokadb
docker compose -f quickstart/compose.yml up --build
```

Needs Docker with Compose v2.17 or newer and about 2 GB of free disk. The first run builds LokaDB from source (a few minutes). `psql` on the host is optional: every `psql` command below can be run inside the stand instead (shown after the first one). Run every command on this page from the root of the cloned repository. `up` stays attached and prints the logs of all services: run it in one terminal (or add `-d`) and the commands below in another.

## What runs

| Service | Network | Role |
|---|---|---|
| `cloud` | cloud | the SaaS database `demo`: `organizations`, `documents`, `document_versions`, 2000 seed documents of two organizations; also the relay's registry database `controlplane` |
| `customer` | customer | organization 1's own database `cust_demo`, prepared with two roles: `lokadb_app` (will own the moved tables) and `lokadb_dml` (the proxy's direct path, DML only) |
| `certs` | none | builds the LokaDB image and creates a demo CA and the relay certificate |
| `relay` | cloud + internet | accepts the agent's tunnel; exposes organization 1's database inside the cloud as `relay:15001` |
| `agent` | customer + internet | waits for its one-time token, enrols, opens the **outbound** tunnel; forwards only to `customer:5432` |
| `proxy` | cloud | the application's database endpoint, `localhost:6432` |
| `demo` | cloud | the application: writes a document and a version for each organization every half second through the proxy, and reports counts and write errors |
| `init` | cloud | one shot: token → wait for the tunnel → `check` → `enable` → `move-org` → where the rows are |

The cloud network and the customer network share nothing: the cloud containers cannot even resolve `customer`, and reach it only through the relay and the agent.

## What `init` does

```text
1. lokadb token -org 1                     one-time enrolment token, handed to the agent
2. wait until cust_demo answers through relay:15001
3. lokadb check  -db … -spec /quickstart/spec.json -customer … -customer-app … -org 1
4. lokadb enable -db … -spec /quickstart/spec.json -owner app        documents and document_versions become partitioned (no row copy)
5. lokadb move-org -org 1 -customer … -customer-app … -proxy-config lokadb.json -direct-role lokadb_dml
6. counts: organization 1's rows in the cloud (0) and in its database; organization 2's in the cloud
```

`/quickstart/spec.json` is the stand's spec (the file [`quickstart/spec.json`](../quickstart/spec.json) of this repository): which tables move with an organization and how each finds it. For your own database you do not write it by hand: `lokadb check -org-table TABLE -write-spec spec.json` makes it, see [Install on your database](install.md#3-write-the-spec) and [the spec file](configuration.md#the-spec-file).

It worked when the `init` log ends with `Done.` and shows `rows of organization 1 stored in the cloud = 0` (the organization's rows are in its own database now), while `demo` keeps writing for both organizations. What you will see on the way, none of it a failure:

- `check` ends with the verdict `NEEDS REVIEW` and a few warnings: the demo application does not declare every relation, and the report says so. It is not `BLOCKED`, which is what would stop the move.
- Every `lokadb` command prints `warning: -db holds a password on the command line` (and the same for `-customer` and `-customer-app`). The demo does it for brevity; [Configuration](configuration.md#passwords-on-the-command-line) shows how to keep passwords out of arguments.
- The cloud and customer database logs show `WARNING: subscription was created, but is not connected` (the permission probe of `check` creates and drops a throwaway subscription), and the cloud log also shows `ERROR: relation "lokadb.orgs" does not exist` (LokaDB's metadata is looked up before `enable` creates it).
- Counts are not round: the demo application is already writing, so organization 1 has about 1000 rows when it moves, not exactly its 1000 seed documents.

`move-org` runs in trigger mode (no logical replication) and sets organization 1 to the `fdw+direct` profile, because the proxy config has its direct-path credentials. `demo` keeps writing for both organizations the whole time. If a write that was waiting for `enable` gets the transient error "the row reached the table without its organization … retry the statement", `demo` retries it once, as an application would; write errors otherwise appear only while the agent is stopped. See [Limits and isolation](limits.md#known-limitations).

## Things to try

The passwords on this page (`app`, `pw`) belong to the demo stand: they are fixed in its compose file and protect nothing. For a real database, never write a password on a command line: pass the DSN as `env:NAME` ([install](install.md#the-names-used-on-this-page)).

```bash
psql "host=localhost port=6432 dbname=demo user=app password=app"
# no psql on the host? the same, from inside the stand:
docker compose -f quickstart/compose.yml exec cloud psql "host=proxy port=6432 dbname=demo user=app password=app"
```

```sql
SELECT count(*) FROM documents WHERE organization_id = 1;   -- read from organization 1's own database
SELECT count(*) FROM documents WHERE organization_id = 2;   -- read from the cloud
SELECT count(*) FROM documents;                              -- names no organization: refused by the default policy
```

```bash
docker compose -f quickstart/compose.yml stop agent     # takes about 15 s, the agent drains first; then organization 1 fails with a readable error and organization 2 keeps working
docker compose -f quickstart/compose.yml start agent    # the agent reconnects with its certificate, no new token
docker compose -f quickstart/compose.yml exec customer psql -U postgres cust_demo -c 'SELECT count(*) FROM documents'
```

To move the organization back and undo everything:

`init` is the stand's one-shot service that has the `lokadb` binary and `psql`; the commands below borrow it. `--no-deps` matters: without it Compose rebuilds and recreates the certificate, relay and proxy services first, the agent loses its tunnel for a few seconds, and the command fails with "the agent of organization 1 is not connected". Each of the two commands also waits about a minute on purpose (the `-bridge` and `-grace` flags, 60 s each by default, see [The way back](rollback.md): proxies with old metadata drain before anything is dropped), so a quiet minute is not a hang.

```bash
docker compose -f quickstart/compose.yml run --rm --no-deps --entrypoint lokadb init move-back \
  -db "host=cloud dbname=demo user=postgres password=pw sslmode=disable" -spec /quickstart/spec.json -org 1 \
  -customer "host=relay port=15001 dbname=cust_demo user=postgres password=pw sslmode=disable"
docker compose -f quickstart/compose.yml run --rm --no-deps --entrypoint lokadb init disable \
  -db "host=cloud dbname=demo user=postgres password=pw sslmode=disable" -spec /quickstart/spec.json
```

After `move-back` the customer database `cust_demo` still has the tables, marked "returned to the cloud by LokaDB"; LokaDB never drops a customer's tables, and the log prints the `DROP TABLE` command for the customer to run when ready ([The way back](rollback.md)).

Clean up with `docker compose -f quickstart/compose.yml down -v`. It removes the containers, networks and data volumes. The image built from source, `lokadb-quickstart` (about 1 GB), stays so that the next run starts fast; remove it with `docker rmi lokadb-quickstart`. If you set `COMPOSE_PROJECT_NAME`, set it for every command of this page (the stand names its own project `lokadb-quickstart`).

## Words in the commands

| Word | Meaning |
|---|---|
| `-customer` and `-customer-app` | two connections to the customer database: an administrator that prepares it, and a plain role that will **own** the moved tables and is what the cloud uses afterwards. See [Moving organizations](moving.md) |
| `-owner app` | the role that owns the application's tables in the cloud database |
| `-proxy-config lokadb.json`, `-direct-role lokadb_dml` | the proxy's config and the role it uses for the direct path (profile `fdw+direct`), see [Profiles and cooperative mode](profiles-and-cooperative-mode.md) |
| trigger mode | the move copies rows and captures changes with triggers instead of logical replication, see [Moving organizations](moving.md) |
| `fanout` policy `reject` | a statement that names no organization is refused instead of being sent to every database, see [Routing](routing.md) |
| `org_allow` | which addresses may use an organization's endpoint on the relay, see [The tunnel](tunnel.md) |
| `controlplane` | the relay's registry database (enrolled agents, certificates); in the stand it lives in the same Postgres as the cloud database |
| `certs` | one-shot service that builds the image and makes a demo certificate authority |
| `sslmode=disable` | the demo has no TLS between its containers; a real setup does, see [The tunnel](tunnel.md) |

## Files

| File | What it is |
|---|---|
| `quickstart/compose.yml` | the services above; `LOKADB_SRC` points at the LokaDB Go module (default `../proxy`) |
| `quickstart/initdb/cloud.sql`, `customer.sql` | the two databases as they are before LokaDB |
| `quickstart/spec.json` | which tables move with an organization |
| `quickstart/lokadb.json` | the proxy: upstream, the direct-path credentials of organization 1, `fanout` policy `reject` |
| `quickstart/relay.json` | the relay: certificates, registry, organization 1's endpoint, `org_allow` (the cloud network) |
| `quickstart/init.sh`, `demo.sh` | the steps above and the writing application |

Passwords and addresses in these files are for the demo only. For a real setup see [Install on your database](install.md), [Configuration](configuration.md), [The tunnel](tunnel.md) and [Limits and isolation](limits.md#installation-direct-access-to-the-cloud-database-is-closed).
