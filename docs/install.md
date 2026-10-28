# Install on your database

This page is the path from nothing to a LokaDB proxy running in front of **your own** Postgres, and then to the first organization moved out. For a demo stand that needs nothing of yours, see the [quickstart](quickstart.md).

Each step says where it runs, gives the command, and says what success looks like. The flags are those of `lokadb <command> -h` and the keys are those of [configuration.md](configuration.md). Words in capitals are yours to replace.

| Step | Where | What you get |
|---|---|---|
| [1. Get LokaDB](#1-get-lokadb) | every machine below | the `lokadb` binary or image |
| [2. Check](#2-check-your-database-and-workload) | a machine that reaches your database | a verdict and the list of what to review |
| [3. The spec](#3-write-the-spec) | the same | `spec.json`: which tables move with an organization |
| [4. Control plane and key](#4-the-control-plane-and-the-credentials-key) | the database server and the proxy host | a registry database and a key file |
| [5. The proxy](#5-the-proxy) | a host next to your database | a proxy that passes everything through |
| [6. The application](#6-point-the-application-at-the-proxy) | the application's config | the application works through the proxy |
| [7. Enable](#7-enable) | a machine that reaches your database | the tables are partitioned by organization |
| [8. The tunnel](#8-the-relay-and-the-agent-for-an-organizations-database) | the relay in your network, the agent in the organization's | an organization's database reachable from yours |
| [9. The first organization](#9-move-the-first-organization) | a machine that reaches both | an organization's rows in its own database |

## What you need

- A **Postgres** database that holds many organizations' rows in shared tables, and a role that owns the application's tables (`app` below). The role that runs `check` and `enable` must be able to create objects and `CREATE EXTENSION postgres_fdw` ([check](check.md) tries exactly that, in a transaction it rolls back, and tells you what is missing).
- One host that runs the proxy (and, later, the relay). It reaches your database's port. The application reaches the proxy instead of the database.
- For the move: the organization's own Postgres and a host in its network for the agent. You can leave this to step 8.

LokaDB changes nothing in your database until step 7.

## 1. Get LokaDB

LokaDB is one binary, `lokadb`, with subcommands for the proxy, the relay, the agent and the administrative commands ([configuration.md](configuration.md)). Get it in one of three ways.

**A release.** Each version tag publishes a Linux x86-64 binary with its checksum, and an image:

```bash
VERSION=v0.1.0   # the version you choose; the releases page of the repository lists them
curl -fLO https://github.com/lokadb/lokadb/releases/download/$VERSION/lokadb-linux-amd64.tar.gz
curl -fLO https://github.com/lokadb/lokadb/releases/download/$VERSION/lokadb-linux-amd64.tar.gz.sha256
sha256sum -c lokadb-linux-amd64.tar.gz.sha256 && tar -xzf lokadb-linux-amd64.tar.gz
sudo install lokadb /usr/local/bin/lokadb
```

```bash
docker pull ghcr.io/lokadb/lokadb:$VERSION     # the same binary; its entrypoint is `lokadb`
```

**Until the first release**, build it from the source. You need Docker only:

```bash
git clone https://github.com/lokadb/lokadb && cd lokadb
docker build -t lokadb proxy                   # the image `lokadb`; a few minutes the first time
```

With Go 1.25 and a C compiler, `cd proxy && CGO_ENABLED=1 go build -o lokadb ./cmd/lokadb` makes the binary instead.

Everywhere below, `lokadb …` means the binary, or `docker run --rm -i lokadb …` (with the image you have: `lokadb` or `ghcr.io/lokadb/lokadb:$VERSION`), with `--network host` or the right network so the container reaches your database, and `-v` for the files it reads.

**Success:** `lokadb -h` prints the list of commands.

## 2. Check your database and workload

*Runs on any machine that reaches your database; it changes nothing.* `check` answers whether your organizations can be moved: it reads the schema and `pg_stat_statements` and tries what `enable` and `move-org` will do inside a transaction that is rolled back.

First let the check see the application's real statements. Start a recording proxy that passes everything through (the config is the smallest one; step 5 extends it):

```bash
mkdir -p /etc/lokadb
cat > /etc/lokadb/lokadb.json <<'EOF'
{
  "listen": ":6432",
  "upstream": "host=DB_HOST port=5432 sslmode=require",
  "record": "/var/lib/lokadb/workload.json"
}
EOF
lokadb proxy -config /etc/lokadb/lokadb.json
```

Point the application, or its tests, at `PROXY_HOST:6432` (step 6 says how) and use it as it is used in production. The recording is rewritten every two seconds and appended across restarts; it holds normalized statements, never values. Then run the check. If you do not have a spec yet, let it propose one from your organizations table (`-org-table`, here `organizations`):

```bash
lokadb check -db "host=DB_HOST dbname=APPDB user=ADMIN sslmode=require" \
  -org-table organizations -write-spec spec.json \
  -workload /var/lib/lokadb/workload.json -proxy-config /etc/lokadb/lokadb.json
```

Keep the passwords out of the arguments: every DSN flag also takes `env:NAME` or `-` for stdin ([configuration.md](configuration.md#passwords-on-the-command-line)).

**Success:** the report ends with a verdict and the exit code says which:

| Verdict | Exit code | What to do |
|---|---|---|
| `READY` | 0 | go on |
| `NEEDS REVIEW` | 2 | read the warnings; most are changes in behaviour you accept |
| `BLOCKED` | 3 | stop: the report names what blocks, and how to remove it |
| `NOT SUPPORTED YET`, `INVALID SPEC` | 4, 5 | some tables cannot be moved yet, or the spec has errors |

Every finding, the report's fields and `-org-table` are described in [check.md](check.md) and [the check report format](check.md#json-and-ci).

## 3. Write the spec

*Review the file `check` wrote.* The spec names the tables whose rows move with an organization, and how each finds its organization ([configuration.md](configuration.md#the-spec-file)). The check also says why each other table stays in the cloud database. Which data may leave your database is a human decision: read it, edit it, and run the check again with `-spec spec.json` instead of `-org-table`.

```bash
lokadb check -db "host=DB_HOST dbname=APPDB user=ADMIN sslmode=require" \
  -spec spec.json -workload /var/lib/lokadb/workload.json -proxy-config /etc/lokadb/lokadb.json
```

If the report suggests routing declarations (relations without a foreign key, columns that are unique across the table), `-suggest patch.json` saves them for `enable` ([check.md](check.md#from-findings-to-declarations), [routing.md](routing.md#declarations)).

**Success:** the same verdict as before with `-spec`, and no `INVALID SPEC`.

## 4. The control plane and the credentials key

*The control plane is a small database of LokaDB's own: enrolment tokens and certificates of the tunnel, and (sealed) the credentials of organizations' databases. The key seals those credentials.* Both can wait until you move an organization, but make them now so the proxy's config is final.

Create the database on the server of your Postgres (or any other Postgres; it is not the application's database):

```bash
psql "host=DB_HOST user=ADMIN dbname=postgres sslmode=require" -c 'CREATE DATABASE controlplane'
```

LokaDB makes its tables in it: the relay at its first start (step 8), and `lokadb org-credentials` when it stores a credential (step 9). The role `CP_USER` that the relay and the proxy use needs to create objects in it.

Make the key on the host that runs the proxy. It is 32 random bytes as 64 hexadecimal digits, mode `0600`, and **you must keep it**: without it the stored credentials cannot be opened.

```bash
umask 077
openssl rand -hex 32 > /etc/lokadb/credentials.key
```

**Success:** `ls -l /etc/lokadb/credentials.key` shows `-rw-------`, and `psql … -d controlplane -c 'SELECT 1'` works for `CP_USER`. Rotation of the key: [configuration.md](configuration.md#credentials-in-the-control-plane).

## 5. The proxy

*Runs on a host next to your database; your database accepts connections from it.* Extend the config of step 2 with the final keys ([configuration.md](configuration.md#proxy-lokadbjson)):

```json
{
  "listen": ":6432",
  "upstream": "host=DB_HOST port=5432 sslmode=require",
  "control_plane": "host=DB_HOST dbname=controlplane user=CP_USER password=… sslmode=require",
  "credentials_key": "/etc/lokadb/credentials.key",
  "fanout": {"*": {"mode": "reject"}},
  "tls": {"cert": "/etc/lokadb/proxy.crt", "key": "/etc/lokadb/proxy.key", "require": true},
  "metrics_listen": "127.0.0.1:9187"
}
```

- `upstream` has no user, password or database: they come from the application's connection.
- `fanout` decides what happens to a statement that names no organization. `reject` is the default and the safe one; the check of step 2 told you which statements of your application it would refuse.
- `tls` is what the application talks to the proxy over; leave it out for a first try on a private network, not for production: the proxy receives the application's password in clear text to pass it on.
- `metrics_listen` serves `/healthz` and `/metrics` to your monitoring ([status and monitoring](https://docs.lokadb.com/operations/status-and-monitoring/)).

Stop the recording proxy of step 2 and start the real one. As a service, from the binary:

```bash
lokadb proxy -config /etc/lokadb/lokadb.json
```

or from the image:

```bash
docker run -d --name lokadb-proxy --restart unless-stopped --network host \
  -v /etc/lokadb:/etc/lokadb:ro ghcr.io/lokadb/lokadb:$VERSION proxy -config /etc/lokadb/lokadb.json
```

A restart is safe: the application's connections are re-made. `SIGHUP` re-reads credentials and drops pooled connections ([configuration.md](configuration.md#proxy-lokadbjson)).

Close the other way in: the application's role must reach the database **only** through the proxy, or LokaDB protects nothing ([limits.md](limits.md#installation-direct-access-to-the-cloud-database-is-closed)). `lokadb check -proxy-hosts PROXY_ADDR …` reports any `pg_hba.conf` rule that admits the application's role from elsewhere.

**Success:** the process stays up, `curl -s http://127.0.0.1:9187/healthz` prints `ok`, and

```bash
psql "host=PROXY_HOST port=6432 dbname=APPDB user=APP_USER sslmode=require" -c 'SELECT 1'
```

answers like the database would (use `sslmode=disable` if you left `tls` out).

## 6. Point the application at the proxy

*In the application's configuration.* Change the **host and port** of its database connection to the proxy's (`PROXY_HOST`, `6432`). User, password and database name stay as they are: the proxy passes them to Postgres. With `tls` on, the application connects with `sslmode=require` or stricter.

**Success:** the application works as before. Before `enable` the proxy passes every statement through unchanged. In `curl -s http://127.0.0.1:9187/metrics` the counters of statements grow. Run the application's tests or a normal day against it; every statement that the check of step 2 listed as refused is a place to fix first.

Do not go on while any part of the application still connects to the database directly: after step 7, a write that bypasses the proxy reaches the table with no organization, and is refused.

## 7. Enable

*Runs on any machine that reaches your database, as the administrator.* `enable` converts the tables of the spec: each becomes a partitioned table, and all organizations stay where they are, in the default partition. No row is copied ([moving.md](moving.md#lokadb-enable-converting-the-schema)).

```bash
lokadb enable -db "host=DB_HOST dbname=APPDB user=ADMIN sslmode=require" -spec spec.json -owner app
# add -patch patch.json if step 3 saved routing declarations
```

For large tables do it in two steps, so that the switch itself takes a short lock: run `lokadb enable … -prepare` first (online, can be stopped and repeated), then the same command without `-prepare`.

**Success:** the command ends without an error, and in the database `\d+ documents` (a table of your spec) says "Partitioned table" with a `documents__cloud` default partition. The application, still through the proxy, works as before. If it gets "the row reached the table without its organization … retry the statement" once, that is a write that was waiting for `enable`; it succeeds on retry.

To undo it: `lokadb disable` ([rollback.md](rollback.md#lokadb-disable-undoing-enable)).

## 8. The relay and the agent for an organization's database

*Needed only to move an organization into its own database.* The cloud never connects into the organization's network. An **agent** in that network opens one outbound TLS connection to a **relay** in yours, and every access to the organization's database goes through it ([tunnel.md](tunnel.md)).

**Certificates.** The relay needs a CA that signs the agents' certificates, and its own certificate. For a first try, `lokadb certs` makes a demo CA; for production use your own and see [CA key in a separate issuing service](tunnel.md#ca-key-in-a-separate-issuing-service):

```bash
lokadb certs -dir /etc/lokadb/certs -relay-host RELAY_NAME    # RELAY_NAME: the name the agents connect to
```

**The relay**, on a host in your network. Its config ([configuration.md](configuration.md#relay-relayjson)):

```json
{
  "listen": ":7443",
  "enroll_listen": ":7444",
  "health_listen": "127.0.0.1:7446",
  "cert": "/etc/lokadb/certs/relay.crt", "key": "/etc/lokadb/certs/relay.key",
  "ca": "/etc/lokadb/certs/ca.crt", "ca_key": "/etc/lokadb/certs/ca.key",
  "cert_ttl": "24h",
  "control_plane": "host=DB_HOST dbname=controlplane user=CP_USER password=… sslmode=require",
  "orgs": {"1": ":15001"},
  "org_allow": ["PROXY_ADDR", "DB_ADDR", "ADMIN_ADDR"]
}
```

`orgs` gives each organization a port inside your network; `org_allow` lists the only addresses that may use it: the proxy, your database (`postgres_fdw`), and the machines that run `check` and `move-org`. It is required. The agents reach `listen` and `enroll_listen` from outside.

```bash
lokadb relay -config /etc/lokadb/relay.json
```

**Success:** `curl -s http://127.0.0.1:7446/healthz` answers 200; the relay's first start made the tables of the control plane.

**The token and the agent.** For each organization, one-time token (valid 15 minutes, shown once), carried to the organization's administrator:

```bash
lokadb token -cp "host=DB_HOST dbname=controlplane user=CP_USER sslmode=require" -org 1 > token
```

The administrator puts the token in a file, the relay's `ca.crt` next to it, and starts the agent **in the organization's network**, next to its Postgres (`ORG_DB_HOST:5432`):

```bash
lokadb agent -relay RELAY_NAME:7443 -ca ca.crt -state /var/lib/lokadb-agent \
  -token-file token -db ORG_DB_HOST:5432
```

The agent enrols (the token is consumed), then keeps an outbound tunnel; it opens no port, and forwards only to `-db`. It keeps its key and certificate in `-state`, so a restart needs no new token.

**Success:** the agent logs `enrolled` and then `tunnel up`; the relay logs `agent connected`; `lokadb agents -cp … ` lists the certificate; and from a machine in `org_allow`:

```bash
psql "host=RELAY_HOST port=15001 dbname=ORG_DB user=ORG_ADMIN sslmode=require"
```

reaches the organization's database through the tunnel. If the agent is not connected, the relay answers with a Postgres error that says so.

The organization's database needs two roles, prepared by its administrator before the move: one that **owns** the moved tables (`lokadb_app`) and one that does DML only (`lokadb_dml`, used by the proxy's direct path). The quickstart's [`customer.sql`](https://github.com/lokadb/lokadb/blob/main/quickstart/initdb/customer.sql) is the smallest example; the Postgres of the organization must also let them in from the agent's address (`pg_hba.conf`).

## 9. Move the first organization

*Runs on a machine that reaches your database and, through the relay, the organization's.* First check this particular move: the same `check` as in step 3, with the organization's connections added.

```bash
ADMIN="host=DB_HOST dbname=APPDB user=ADMIN password=… sslmode=require"
CP="host=DB_HOST dbname=controlplane user=CP_USER password=… sslmode=require"
CUST_ADMIN="host=RELAY_HOST port=15001 dbname=ORG_DB user=ORG_ADMIN password=… sslmode=require"
CUST_APP="host=RELAY_HOST port=15001 dbname=ORG_DB user=lokadb_app password=… sslmode=require"
lokadb check -db "$ADMIN" -spec spec.json -proxy-config /etc/lokadb/lokadb.json   -customer "$CUST_ADMIN" -customer-app "$CUST_APP" -org 1
```

Then store the credentials the proxy will use for this organization's database, sealed with the key of step 4, so they live in the control plane and not in a config file (the DSN comes from stdin). The direct path's role must have DML alone; the owner is the role that will own the moved tables, not an administrator:

```bash
echo "host=RELAY_HOST port=15001 dbname=ORG_DB user=lokadb_dml password=… sslmode=require" |
  lokadb org-credentials -db "$ADMIN" -control-plane "$CP" -key /etc/lokadb/credentials.key -org 1 -kind direct -dsn -
echo "$CUST_APP" |
  lokadb org-credentials -db "$ADMIN" -control-plane "$CP" -key /etc/lokadb/credentials.key -org 1 -kind owner -dsn -
```

And move it. The application keeps working through the proxy all along:

```bash
lokadb move-org -db "$ADMIN" -spec spec.json -owner app -org 1   -customer "$CUST_ADMIN" -customer-app "$CUST_APP"   -proxy-config /etc/lokadb/lokadb.json -direct-role lokadb_dml
```

**Success:** the command prints its progress per step and table and ends without an error; the organization's rows are in its own database and not in yours:

```sql
SELECT count(*) FROM documents WHERE organization_id = 1;   -- through the proxy: read from the organization's database
SELECT count(*) FROM ONLY documents__cloud WHERE organization_id = 1;   -- in your database: 0
```

Everything about this step is in [moving.md](moving.md) (modes, the pause, limits, progress) and [profiles and cooperative mode](profiles-and-cooperative-mode.md) (the `fdw+direct` path). To return the organization: [rollback.md](rollback.md).

## Using LokaDB Cloud

[LokaDB Cloud](https://docs.lokadb.com/cloud/) puts a panel and an API on top of the same programs: it does not replace this page. Its **runner** (`lokadb runner`) does the checks, moves and move-backs for you, and it needs exactly what steps 1 to 8 give: the proxy with `control_plane` and `credentials_key`, and the relay with the control plane. Once they run, install the runner as [the runner page](https://docs.lokadb.com/cloud/runner/) says.
