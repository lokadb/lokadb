# Install on your database

This page is the path from nothing to a LokaDB proxy running in front of **your own** Postgres, and then to the first organization moved into its own database. For a demo that needs nothing of yours, see the [quickstart](quickstart.md).

Each step says which machine it runs on, gives the exact commands, and says what success looks like. The flags are those of `lokadb <command> -h` and the keys are those of the [configuration reference](configuration.md).

| Step | What you get |
|---|---|
| [1. Get LokaDB](#1-get-lokadb) | the `lokadb` binary or image on the proxy host |
| [2. Check](#2-check-your-database-and-workload) | a verdict, and the list of what to review |
| [3. The spec](#3-write-the-spec) | `spec.json`: which tables move with an organization |
| [4. `lokadb init`](#4-lokadb-init-the-control-plane-the-keys-and-the-configs) | the control plane, the credentials key, the certificates and the configs of the proxy and the relay, in one command |
| [5. The proxy](#5-the-proxy) | a proxy in front of your database |
| [6. The application](#6-point-the-application-at-the-proxy) | the application working through the proxy |
| [7. Enable](#7-enable) | the tables partitioned by organization |
| [8. The tunnel](#8-the-tunnel-the-relay-and-the-agent) | an organization's own database reachable from yours |
| [9. The first organization](#9-move-the-first-organization) | its rows in its own database |

LokaDB changes nothing in your database until step 7.

**Two setups are written out, and the commands that differ are given for both:** a Postgres that is a service of a `docker-compose.yml` (its port not published to the host, no SSL), which is the common small SaaS, and a Postgres on a separate host. Where they differ, the compose command comes first. [Postgres in Docker](#postgres-in-docker-docker-compose) collects what is special about the first.

## What you need

- **Postgres.** A database in which many organizations (tenants, accounts, companies) share tables, each table with a column that says whose row it is. **Versions:** the stand, the end-to-end tests and the measurements run on Postgres 18, and the SQL LokaDB generates is written for 17 and 18. The check and the runner have been reported working on Postgres 16 (one real install); we have not tested 16 or anything older ourselves, and the check names what a version lacks. The role that runs `check` and `enable` must be able to create objects and `CREATE EXTENSION postgres_fdw`. You do not have to work this out: the [check](check.md) tries it, in a transaction it rolls back, and names what is missing. On a managed service, use the platform's administrator role (`rds_superuser`, `cloudsqlsuperuser`, `azure_pg_admin`).
- **`pg_stat_statements`**, if you can: with it in `shared_preload_libraries` and created in the database (`CREATE EXTENSION pg_stat_statements`), the check also reads the statements your application has already run.
- **The proxy host.** One machine that reaches your database's port, and that the application can reach. **Run steps 2 to 9 on it**: they read and write files there (`spec.json`, the recording, the key) and several commands must come from an address that the relay allows (`org_allow`, step 8), which the proxy host is. The relay can run on the same machine.
- **For step 8 and 9**, the organization's own Postgres and a machine in its network for the agent.

### The names used on this page

Words in capitals are yours to replace, the same everywhere. Lowercase values (`app`, `organizations`, `documents`, `1`) are examples from the demo: use your own.

| Name | What it is | Example |
|---|---|---|
| `DB_HOST` | the host name of your Postgres, as the proxy host and the containers reach it. With docker-compose: **the service name** | `db.internal`; `pgsql` |
| `DB_SSLMODE` | the `sslmode` of the connections to it | `require`; with docker-compose, inside the network, `disable` |
| `NET` | how the containers join your network | `--network host`; with docker-compose `--network myapp_default` ([how to find it](#postgres-in-docker-docker-compose)) |
| `DB_ADDR` | the IP address of the machine your Postgres runs on, as the relay sees it (used in allow lists) | the machine's address |
| `DB_NAME` | the application's database | `shop` |
| `APP_ROLE` | the role the application connects as; usually also the role that owns its tables (`-owner`) | `app` |
| `ADMIN_ROLE` | a role of your Postgres that may create objects (a superuser, or the managed service's administrator) | `postgres` |
| `PROXY_HOST`, `PROXY_ADDR` | the proxy host: the name the application uses, and its IP address. With docker-compose and an application in the same project: the proxy's container name, `lokadb-proxy` | |
| `RELAY_NAME` | the name the agents connect to; it goes into the relay's certificate | `relay.example.com` |
| `RELAY_HOST` | the relay as machines in **your** network reach the organizations' ports (15001…): the proxy host and your database server must reach it. It is the same machine as `RELAY_NAME` seen from inside. With docker-compose: `lokadb-relay`, the container's name | |
| `ORG_DB_HOST`, `ORG_DB` | the organization's Postgres, as its agent reaches it, and the database in it | |
| `ORG_ADMIN` | an administrator role of the organization's database | |
| `1` (in `-org 1`) | the organization's number: the value of its key in your organizations table (an integer). For a uuid key, see [step 8](#8-the-tunnel-the-relay-and-the-agent) | |
| `VERSION`, `IMAGE` | the release you install, and the image that runs it (step 1) | `0.1.0`, `ghcr.io/lokadb/lokadb:0.1.0` |

The commands take their database addresses as connection strings. Where a long one is used several times, it is a shell variable:

| Variable | Is the DSN of |
|---|---|
| `CLOUD_DSN` | your application's database, as `ADMIN_ROLE` |
| `CUST_ADMIN_DSN`, `CUST_APP_DSN`, `CUST_DML_DSN` | the organization's database through the relay, as `ORG_ADMIN`, as `lokadb_app` and as `lokadb_dml` |

The control plane's DSN is not one of them: `lokadb init` writes it, with the password it made up, to `/etc/lokadb/control-plane.dsn`, and the commands read it from there (`-cp file:/etc/lokadb/control-plane.dsn`).

Every connection string on this page is in one format, `key=value` (`host=… dbname=… user=… password=… sslmode=…`; a password with a space or a quote is written `password='my pass'`). A `postgres://` URL works too, with the password URL-encoded; use one form throughout.

**No password goes on a command line**, where `ps` and the shell history would keep it. The commands take each DSN from an environment variable (`-db env:CLOUD_DSN`; `-` reads stdin, `file:PATH` a file: [passwords on the command line](configuration.md#passwords-on-the-command-line)), and `psql` takes its password from `PGPASSWORD`. Type each password once per shell; `read -s` shows nothing and leaves nothing in the history:

```bash
read -rsp 'ADMIN_ROLE password: ' ADMIN_PW; echo
export CLOUD_DSN="host=DB_HOST dbname=DB_NAME user=ADMIN_ROLE password=$ADMIN_PW sslmode=DB_SSLMODE"
```

The steps below say when to set the others. Config files may name a DSN as `env:NAME` or `file:PATH` instead of writing it.

## 1. Get LokaDB

*On the proxy host, and on the relay and agent hosts later.* LokaDB is one program, `lokadb`, with subcommands for the proxy, the relay, the agent and the administrative commands.

**The image** is the simplest: Docker is all you need. Version 0.1.0 is the current release:

```bash
VERSION=0.1.0
IMAGE=ghcr.io/lokadb/lokadb:$VERSION
docker pull "$IMAGE"
```

**Or the binary**, for Linux on x86-64 (`amd64`) or ARM64 (`arm64`), released with its checksum:

```bash
VERSION=0.1.0; ARCH=amd64        # or arm64
curl -fLO https://github.com/lokadb/lokadb/releases/download/v$VERSION/lokadb-linux-$ARCH.tar.gz
curl -fLO https://github.com/lokadb/lokadb/releases/download/v$VERSION/lokadb-linux-$ARCH.tar.gz.sha256
sha256sum -c lokadb-linux-$ARCH.tar.gz.sha256 && tar -xzf lokadb-linux-$ARCH.tar.gz
sudo install lokadb /usr/local/bin/lokadb
```

**If you run the image**, the commands below say `lokadb …`. Define it once per shell. It mounts the directories the commands read and write, runs in the current directory so that files like `spec.json` land on the host, not in a container that is deleted, and passes on the DSN variables:

```bash
NET="--network host"      # a Postgres in docker-compose: NET="--network myapp_default", see the end of this page
lokadb() { docker run --rm -i $NET \
    -e PGPASSWORD -e CLOUD_DSN -e CUST_ADMIN_DSN -e CUST_APP_DSN -e CUST_DML_DSN \
    -v /etc/lokadb:/etc/lokadb -v /var/lib/lokadb:/var/lib/lokadb -v "$PWD":/work -w /work \
    "$IMAGE" "$@"; }
```

(`-i` lets a DSN come through stdin; `--network host` lets the container reach your database. The long-running programs get their own `docker run -d` command in the steps below.)

To build from the source instead (a development version, or a platform without a release): `git clone https://github.com/lokadb/lokadb && cd lokadb && docker build -t lokadb proxy` makes the image `lokadb` (set `IMAGE=lokadb`), and with Go 1.25 and a C compiler `cd proxy && CGO_ENABLED=1 go build -o lokadb ./cmd/lokadb` makes the binary.

Create the two directories, which the commands below write to, and check that `lokadb` works:

```bash
sudo mkdir -p /etc/lokadb /var/lib/lokadb
```

**Success:** `lokadb -h` prints what LokaDB is, where to start, and the list of commands.

## 2. Check your database and workload

*On the proxy host. It changes nothing in your database.* `check` answers whether your organizations can be moved: it reads the schema, tries what `enable` and `move-org` will do inside a transaction that is rolled back, and judges the statements your application sends.

To judge the statements, the check wants a **recording** of the application's real workload. It is optional but worth it: from `pg_stat_statements` alone the check does not know which statements run together in a transaction, and the verdict is never `READY`. Record by running a pass-through proxy and using the application, or just its test suite, through it. A staging copy is fine; you need not put production behind a new proxy yet.

```bash
cat > /etc/lokadb/recording.json <<'EOF'
{
  "listen": ":6432",
  "upstream": "host=DB_HOST port=5432 sslmode=DB_SSLMODE",
  "record": "/var/lib/lokadb/workload.json"
}
EOF
lokadb proxy -config /etc/lokadb/recording.json     # leave it running in its own terminal
```

Point the application (or its tests) at `PROXY_HOST:6432` ([step 6](#6-point-the-application-at-the-proxy) says how) and use it. The recording is rewritten every two seconds, never holds values (statements are normalized), and is appended to across restarts.

Which table is your organizations table? Ask the check, with only `-db`:

```bash
lokadb check -db env:CLOUD_DSN -org-candidates
```

It prints the tables that look like it, best first. Then run the check on it (here `organizations`); it also writes a first spec:

```bash
lokadb check -db env:CLOUD_DSN \
  -org-table organizations -write-spec spec.json \
  -workload /var/lib/lokadb/workload.json -proxy-config /etc/lokadb/recording.json
```

Leave out `-workload` if you have no recording.

**Success:** the report ends with a verdict, and the exit code says which:

| Verdict | Exit code | What to do |
|---|---|---|
| `READY` | 0 | go on |
| `NEEDS REVIEW` | 2 | read the warnings; most are changes in behaviour you accept |
| `BLOCKED` | 3 | stop: the report names what blocks and how to remove it |
| `NOT SUPPORTED YET`, `INVALID SPEC` | 4, 5 | some tables cannot be moved yet, or the spec has errors |

Every finding, and what the report can and cannot promise, is in the [compatibility check](check.md).

## 3. Write the spec

*On the proxy host.* The spec names the tables whose rows move with an organization, and how each finds its organization ([the spec file](configuration.md#the-spec-file)). The check says why every other table stays. Which data may leave your database is a human decision: read `spec.json`, edit it, and run the check again with `-spec` instead of `-org-table`.

```bash
lokadb check -db env:CLOUD_DSN \
  -spec spec.json -workload /var/lib/lokadb/workload.json -proxy-config /etc/lokadb/recording.json
```

If the report suggests routing declarations (relations without a foreign key, columns that are unique across the table), add `-suggest patch.json` to save them, and pass `-patch patch.json` to `check`, `enable` and `move-org` alike ([from findings to declarations](check.md#from-findings-to-declarations), [routing declarations](routing.md#declarations)).

**Success:** the same verdict, and no `INVALID SPEC`.

## 4. `lokadb init`: the control plane, the keys and the configs

*On the proxy host.* LokaDB keeps a few things of its own: a small **control plane** database (the tunnel's enrolment tokens and certificates, and, sealed, the credentials of organizations' databases), a **credentials key** that seals those, a **CA** that signs the agents' certificates, and the configs of the proxy and the relay. `lokadb init` makes all of it, and it types no password or path for you:

- the control plane's role (`lokadb_cp`) and database (`lokadb_cp`) on your Postgres, with a random password that goes only into `/etc/lokadb/control-plane.dsn` (mode `0600`) and is never printed;
- `credentials.key`: 32 random bytes, mode `0600`. **Keep it safe**: without it the stored credentials cannot be opened;
- the CA and the relay's certificate in `/etc/lokadb/relay/`, which only the relay mounts, and the public `ca.crt` in `/etc/lokadb/`, which is the file the agents, the runner and the proxy need;
- `relay.json` (ports 7443, 7444 and 7446) and `proxy.json` (the skeleton of step 5), and the `docker run` lines for the relay and the proxy.

It is idempotent: what exists is kept and never overwritten (a key and a CA least of all), so running it again is safe.

**A Postgres in docker-compose** (`NET="--network myapp_default"` is set; the commands run on that network, so `pgsql` is found and no SSL is asked):

```bash
export PGPASSWORD=$ADMIN_PW
lokadb init -relay-host RELAY_NAME -image "$IMAGE" -network myapp_default \
  -admin "host=pgsql port=5432 dbname=postgres user=ADMIN_ROLE sslmode=disable"
```

**A Postgres on a separate host:**

```bash
export PGPASSWORD=$ADMIN_PW
lokadb init -relay-host RELAY_NAME -image "$IMAGE" \
  -admin "host=DB_HOST port=5432 dbname=postgres user=ADMIN_ROLE sslmode=require"
```

(`-admin` carries no password: `PGPASSWORD` does. The role must be allowed to create roles and databases. Without `-admin` the command makes everything else and tells you to create the role and the database, and write their DSN to `control-plane.dsn`, yourself.)

`RELAY_NAME` is the name the agents will dial; it goes into the relay's certificate. Several names, comma separated, are allowed. Other flags, all in `lokadb init -h`: `-dir` (where the files go on this machine, default `/etc/lokadb`), `-container-dir` (where the containers see it), `-cp-host`, `-cp-port` and `-cp-sslmode` (the control plane's address *as the containers reach it*, when it differs from `-admin`'s), `-cp-role`, `-cp-db`, `-org-allow`.

Every line it prints says created or kept. It ends with warnings and the commands to run next:

- **`org_allow`** in `relay.json` is the three private ranges. It says who may reach an organization's port on the relay. Container addresses change, so a range is the right shape; narrow it to your own network in step 8.
- The CA key is in `/etc/lokadb/relay/ca.key`: mount that directory into the relay and nowhere else.

**Success:** `ls -l /etc/lokadb` shows `credentials.key` and `control-plane.dsn` as `-rw-------`, `ca.crt`, `relay.json` and `proxy.json`, and a `relay/` directory. The command ended without an error.

The long way, without `init`, is at [the end of this page](#appendix-the-same-without-lokadb-init).

## 5. The proxy

*On the proxy host.* `init` wrote `/etc/lokadb/proxy.json`:

```json
{
  "listen": ":6432",
  "upstream": "env:LOKADB_UPSTREAM",
  "control_plane": "file:/etc/lokadb/control-plane.dsn",
  "credentials_key": "/etc/lokadb/credentials.key",
  "fanout": {"*": {"mode": "reject"}}
}
```

Add what is yours ([the proxy's keys](configuration.md#proxy-lokadbjson)):

```json
  "tls": {"cert": "/etc/lokadb/proxy.crt", "key": "/etc/lokadb/proxy.key", "require": true},
  "metrics_listen": "127.0.0.1:9187"
```

- `upstream` is your database, as the **environment variable** `LOKADB_UPSTREAM` the proxy is started with: no user, password or database in it, they come from the application's connection. `export LOKADB_UPSTREAM="host=DB_HOST port=5432 sslmode=DB_SSLMODE"`.
- `fanout` decides what happens to a statement that names no organization. `reject` is the safe default; the check of step 2 told you which of your application's statements it would refuse.
- `tls` is the proxy's certificate for the application's connections: a certificate for `PROXY_HOST` from your CA, or, for a first try with `sslmode=require` (which does not verify the certificate), a self-signed one: `openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj /CN=PROXY_HOST -keyout /etc/lokadb/proxy.key -out /etc/lokadb/proxy.crt`. Leave `tls` out only on a private network for a first try: the proxy receives the application's password in clear text to pass it on. With docker-compose, inside the project's network and with only your own containers connecting, leaving it out is reasonable.
- `metrics_listen` serves `/healthz`, `/status` and `/metrics` to your monitoring ([status and monitoring](https://docs.lokadb.com/operations/status-and-monitoring/)). In a container write `":9187"` (`127.0.0.1` is reachable from nowhere there) and do not publish the port.

Stop the recording proxy of step 2 (Ctrl-C) and start this one. From the binary (`lokadb proxy -config /etc/lokadb/proxy.json`, as a service of your init system), or from the image, with the line `init` printed:

```bash
docker run -d --name lokadb-proxy --restart unless-stopped $NET -p 6432:6432 -e LOKADB_UPSTREAM \
  -v /etc/lokadb/proxy.json:/etc/lokadb/proxy.json:ro -v /etc/lokadb/credentials.key:/etc/lokadb/credentials.key:ro \
  -v /etc/lokadb/control-plane.dsn:/etc/lokadb/control-plane.dsn:ro "$IMAGE" proxy -config /etc/lokadb/proxy.json
```

(Add `-v /etc/lokadb/proxy.crt:… -v /etc/lokadb/proxy.key:…` for `tls`. With `--network host` leave out `-p`; with docker-compose and an application in the same project leave it out too: the application reaches `lokadb-proxy:6432` on the network.)

The proxy does not drain: when it stops, the connections of the application are cut and the application reconnects. Restart it when that is acceptable ([restarts and upgrades](https://docs.lokadb.com/operations/restarts-and-upgrades/)). `SIGHUP` re-reads credentials and drops pooled connections ([the proxy's keys](configuration.md#proxy-lokadbjson)).

**Close the other way in.** The application's role must reach the database **only** through the proxy, or LokaDB protects nothing ([limits](limits.md#installation-direct-access-to-the-cloud-database-is-closed)). The check reads the `pg_hba.conf` rules and reports any that admits the application's role from another address:

```bash
lokadb check -db env:CLOUD_DSN -spec spec.json -proxy-hosts PROXY_ADDR
```

(`-db env:CLOUD_DSN` is set under [the names](#the-names-used-on-this-page); it must reach the database itself, not the proxy.)

**Success:** the process stays up, `/healthz` of `metrics_listen` prints `ok` (`curl -s http://127.0.0.1:9187/healthz`; in a container, `docker logs lokadb-proxy` shows it listening), and through the proxy the database answers like it did:

```bash
read -rsp 'APP_ROLE password: ' PGPASSWORD; echo; export PGPASSWORD      # the application's own password
psql "host=PROXY_HOST port=6432 dbname=DB_NAME user=APP_ROLE sslmode=require" -c 'SELECT 1'      # sslmode=disable if you left tls out
```

## 6. Point the application at the proxy

*In the application's configuration.* Change the **host and port** of its database connection to the proxy's (`PROXY_HOST`, `6432`). User, password and database name stay: the proxy passes them to Postgres. With `tls` on, the application connects with `sslmode=require` or stricter.

**Success:** the application works as before. Before `enable` the proxy passes every statement through. The counters in `curl -s http://127.0.0.1:9187/metrics` grow. Run the application's tests, or a normal day, against it.

Do not go on while any part of the application (a job, a console, a migration tool) still connects to the database directly: after step 7, a write that bypasses the proxy reaches the table without an organization and is refused.

## 7. Enable

*On the proxy host.* `enable` converts the tables of the spec: each becomes a partitioned table, and every organization stays where it is, in the table's default partition. No row is copied ([`enable`](moving.md#lokadb-enable-converting-the-schema)).

```bash
lokadb enable -db env:CLOUD_DSN -spec spec.json -owner APP_ROLE         # add -patch patch.json if step 3 saved one
```

For large tables do it in two steps, so that the switch itself takes a short lock: first the same command with `-prepare` (online, can be stopped and repeated), then once more without it.

**Success:** the command ends without an error, `\d+` of a table of your spec in psql says "Partitioned table" with a default partition named `<table>__cloud`, and the application, still through the proxy, works as before. If one of its writes got "the row reached the table without its organization … retry the statement" while `enable` was running, that write succeeds on retry.

To undo it: [`lokadb disable`](rollback.md#lokadb-disable-undoing-enable).

## 8. The tunnel: the relay and the agent

*Needed only to move an organization into its own database.* Your side never connects into the organization's network. An **agent** in that network opens one outbound TLS connection to a **relay** in yours, and every access to the organization's database goes through it ([the tunnel](tunnel.md)).

**If the organization's key is a uuid**, give it a number first; every command below that says `-org 1` takes this number instead. Commands that take `-tenant UUID` instead of `-org` are named in `lokadb -h`.

```bash
lokadb org-add -db env:CLOUD_DSN -tenant 0f8fad5b-d9cb-469f-a165-70867728950e      # prints: organization 7: 0f8fad5b-…
```

### 8a. Prepare the organization's database

*Done by the organization's administrator, in the organization's own Postgres, before the agent is started.* LokaDB needs two roles there: one that will **own** the moved tables (`lokadb_app`; your database reads and writes through it), and one for DML only (`lokadb_dml`; the proxy's direct path). Their passwords go into the DSNs below.

```bash
read -rsp 'A new password for lokadb_app: ' APP2_PW; echo
read -rsp 'A new password for lokadb_dml: ' DML_PW; echo
PGPASSWORD=$ORG_ADMIN_PW psql "host=ORG_DB_HOST dbname=ORG_DB user=ORG_ADMIN" <<EOF
CREATE ROLE lokadb_app LOGIN PASSWORD '$APP2_PW';
CREATE ROLE lokadb_dml LOGIN PASSWORD '$DML_PW';
GRANT CREATE ON SCHEMA public TO lokadb_app;
EOF
```

(`ORG_ADMIN_PW` is `ORG_ADMIN`'s password, set like the others with `read -s`. The organization's administrator can run the same SQL in any client.)

`ORG_ADMIN` itself is an administrator of that database (it creates the extensions and types the tables need, and sets the move up). The organization's `pg_hba.conf` must let `lokadb_app`, `lokadb_dml` and `ORG_ADMIN` in from the agent's address.

### 8b. The relay

*On the relay's machine (it can be the proxy host).* `lokadb init` (step 4) already made the relay's certificate, the CA that signs the agents' certificates, and `/etc/lokadb/relay.json`:

```json
{
  "listen": ":7443",
  "enroll_listen": ":7444",
  "health_listen": ":7446",
  "cert": "/etc/lokadb/relay/relay.crt", "key": "/etc/lokadb/relay/relay.key",
  "ca": "/etc/lokadb/relay/ca.crt", "ca_key": "/etc/lokadb/relay/ca.key",
  "control_plane": "file:/etc/lokadb/control-plane.dsn",
  "orgs": {},
  "org_allow": ["the private ranges"]
}
```

([the relay's keys](configuration.md#relay-relayjson).) Organizations get their ports later, at their move; `orgs` stays empty for now. `org_allow` is required, and `init` filled it with the private ranges: edit it to the addresses that may reach an organization's port, which are the proxy, the machine your Postgres runs on (`postgres_fdw`) and any other machine from which you run `check` or `move-org`. An entry is an IP address, a CIDR range or a host name. [The tunnel page](tunnel.md#ports) lists every port in one table: the agents reach `7443` (the tunnel) and `7444` (enrolment) from outside, so those two must be open to them, and the organizations' ports and `7446` stay inside your network.

Start the relay, with the line `init` printed:

```bash
docker run -d --name lokadb-relay --restart unless-stopped $NET -p 7443:7443 -p 7444:7444 \
  -v /etc/lokadb/relay.json:/etc/lokadb/relay.json:ro -v /etc/lokadb/relay:/etc/lokadb/relay:ro \
  -v /etc/lokadb/control-plane.dsn:/etc/lokadb/control-plane.dsn:ro "$IMAGE" relay -config /etc/lokadb/relay.json
# or from the binary:  lokadb relay -config /etc/lokadb/relay.json
```

(`init` also publishes `-p 7446:7446` in its line; leave it out unless a load balancer outside the host needs it. With `--network host` leave out every `-p`.)

**Success:** `docker logs lokadb-relay` shows it listening, or, from the host, `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:7446/healthz` prints `200` when 7446 is published; and the relay has created its tables in the control plane.

**The certificates live one year, and nothing renews them.** The CA and the relay's certificate that `lokadb init` and `lokadb certs` make are valid for a year; the relay logs, at start and daily, when one of them expires in less than 30 days. The agents' own certificates are renewed by the agents (`cert_ttl`), but they are signed by this CA, so a CA that has expired takes every tunnel down. There is no command that renews the CA: make a new one in an empty directory (`lokadb certs -dir NEW_DIR -relay-host RELAY_NAME`) and follow [rotating the CA without restarts](tunnel.md#rotating-the-ca-without-restarts), which moves the relays and the agents over without a new token. Put the date a month before the expiry in your calendar. For production, use your own CA with the lifetime you choose, and read [the CA key in a separate issuing service](tunnel.md#ca-key-in-a-separate-issuing-service).

### 8c. The token and the agent

*Token: on the proxy host. Agent: on a machine in the organization's network, next to its Postgres.*

A one-time token (valid for 15 minutes, printed once), to be carried to the organization's administrator:

```bash
lokadb token -cp file:/etc/lokadb/control-plane.dsn -org 1 > token
```

(The commands spell the control plane's DSN `-cp` (`token`, `revoke`, `agents`, `org-endpoint`, `move-org`) or `-control-plane` (`org-credentials`, `org-profile`); it is the same database.)

The administrator puts the token in a file and the relay's `ca.crt` (public; `/etc/lokadb/ca.crt` on the relay) next to it, and starts the agent:

```bash
lokadb agent -relay RELAY_NAME:7443 -ca ca.crt -state /var/lib/lokadb-agent \
  -token-file token -db ORG_DB_HOST:5432
# or the image:  docker run -d --name lokadb-agent --restart unless-stopped --network host \
#   -v "$PWD":/work -v lokadb-agent:/var/lib/lokadb-agent "$IMAGE" agent -relay RELAY_NAME:7443 -ca /work/ca.crt -state /var/lib/lokadb-agent -token-file /work/token -db ORG_DB_HOST:5432
```

The agent enrols (the token is consumed), then keeps an outbound tunnel; it opens no port, and forwards only to `-db`. It keeps its key and certificate in `-state`, so a restart needs no new token.

**Success:** the agent logs `enrolled`, then `tunnel up`; the relay logs `agent connected`; `lokadb agents -cp file:/etc/lokadb/control-plane.dsn` lists the certificate; and from the proxy host

```bash
PGPASSWORD=$APP2_PW psql "host=RELAY_HOST port=15001 dbname=ORG_DB user=lokadb_app sslmode=require" -c 'SELECT 1'
```

reaches the organization's database through the tunnel. If the agent is not connected, the relay answers with a Postgres error that says so.

## 9. Move the first organization

*On the proxy host* (an address in the relay's `org_allow`, which reaches both your database and the organization's, through the relay).

```bash
export CUST_ADMIN_DSN="host=RELAY_HOST port=15001 dbname=ORG_DB user=ORG_ADMIN password=$ORG_ADMIN_PW sslmode=require"
export CUST_APP_DSN="host=RELAY_HOST port=15001 dbname=ORG_DB user=lokadb_app password=$APP2_PW sslmode=require"
export CUST_DML_DSN="host=RELAY_HOST port=15001 dbname=ORG_DB user=lokadb_dml password=$DML_PW sslmode=require"
```

First check this particular move: the check of step 3 with the organization's connections added.

```bash
lokadb check -db env:CLOUD_DSN -spec spec.json -proxy-config /etc/lokadb/proxy.json \
  -customer env:CUST_ADMIN_DSN -customer-app env:CUST_APP_DSN -org 1
```

Then store the credentials the proxy will use for this organization's database, sealed with the key of step 4, so that they live in the control plane and not in a config file. The direct path's role must have DML alone; the owner is the role that will own the moved tables, not an administrator:

```bash
lokadb org-credentials -db env:CLOUD_DSN -control-plane file:/etc/lokadb/control-plane.dsn -key /etc/lokadb/credentials.key -org 1 -kind direct -dsn env:CUST_DML_DSN
lokadb org-credentials -db env:CLOUD_DSN -control-plane file:/etc/lokadb/control-plane.dsn -key /etc/lokadb/credentials.key -org 1 -kind owner -dsn env:CUST_APP_DSN
```

And move it. The application keeps working through the proxy all along:

```bash
lokadb move-org -db env:CLOUD_DSN -spec spec.json -owner APP_ROLE -org 1 \
  -customer env:CUST_ADMIN_DSN -customer-app env:CUST_APP_DSN \
  -proxy-config /etc/lokadb/proxy.json -direct-role lokadb_dml
```

`-proxy-config` lets `move-org` see that the proxy has the direct path's credentials, and so set the organization's profile to `fdw+direct` instead of `fdw` ([profiles](profiles-and-cooperative-mode.md)). Add `-patch patch.json` if you use one.

**Success:** the command prints its progress per step and table and ends without an error; the organization's rows are in its own database and not in yours:

```sql
SELECT count(*) FROM documents WHERE organization_id = 1;                -- through the proxy: read from the organization's database
SELECT count(*) FROM ONLY documents__cloud WHERE organization_id = 1;   -- in your own database: 0
```

How the move works (modes, the pause, limits, progress) is in [moving organizations](moving.md). To return the organization: [the way back](rollback.md).

## Postgres in Docker (docker-compose)

The common small setup: the application and its Postgres are services of one `docker-compose.yml`, the database's port is not published, and the database has no SSL. The steps above already give the commands for it; this is what is behind them. Everything that talks to your database joins the compose project's network.

**The network.** Compose makes one network per project, named `<project>_default` (the project is the directory's name unless you set `-p` or `COMPOSE_PROJECT_NAME`). Ask Docker, with the database service's name (here `pgsql`):

```bash
docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$(docker compose ps -q pgsql)"
# myapp_default
```

**The names.** On that network a container reaches another by its service or container name; `localhost` and the host's addresses do not work. So: `DB_HOST` is the service name (`pgsql`); `sslmode=disable` in every DSN to that database (the traffic stays inside the Docker network, and a stock Postgres container has no SSL); the proxy is `lokadb-proxy` (the application's `DB_HOST` becomes that, with port `6432`), and the relay is `lokadb-relay`: the proxy and the database reach the organizations' ports (15001…) by that name, and those ports are never published. Only `7443` and `7444` are published, and `RELAY_NAME` is the Docker host's public DNS name, which goes into the relay's certificate.

**The commands** are those of the steps, with `NET="--network myapp_default"` and `DB_HOST=pgsql`, `DB_SSLMODE=disable`. `lokadb check`, `enable` and the other administrative commands run in a one-off container on the network (the `lokadb()` function of step 1 does that).

**`org_allow`** must name the containers that reach the organizations' ports: the proxy, the database (`postgres_fdw`) and the one-off containers of `check` and `move-org`. A container's address changes on every start, so write a network range or names, not addresses. The default of `init` (the private ranges) already covers a Docker network; the narrow form is the network's own range, which also covers the one-off containers:

```bash
docker network inspect myapp_default -f '{{(index .IPAM.Config 0).Subnet}}'      # prints the network's range, address/prefix
```

```json
"org_allow": ["NETWORK_RANGE"]
```

`NETWORK_RANGE` is what the command printed. It admits every container of the project to the organizations' ports, and the organization's Postgres still checks the password. To admit fewer, list names (`"lokadb-proxy", "pgsql"`; a name is looked up again every 30 seconds); a one-off container then needs a name of its own (`docker run --name …`) in the list.

**The organization's side** (steps 8a and 8c) does not change: the agent runs in the organization's network and connects out to `RELAY_NAME:7443`. The DSNs of step 9 use `host=lokadb-relay port=15001`; their `sslmode` belongs to the *organization's* Postgres, not to your Docker network.

**The runner**, if you use [LokaDB Cloud](#using-lokadb-cloud), must run on this network too, or it does not find `pgsql`. Add `--network myapp_default` to its command. In its `runner.json`: `"db"` is a DSN with `host=pgsql`, `relay.cloud_host` is `lokadb-relay`, and `relay.ca` is `/etc/lokadb/ca.crt`. [The runner page](https://docs.lokadb.com/cloud/runner/) has the rest.

## Appendix: the same without `lokadb init`

What `init` does, by hand. Steps 4, 5 and 8b of the page are replaced by this; the rest is the same.

**The control plane** (as `ADMIN_ROLE`; the password is yours to make up):

```bash
read -rsp 'A new password for cp_role: ' CP_PW; echo
PGPASSWORD=$ADMIN_PW psql "host=DB_HOST dbname=postgres user=ADMIN_ROLE sslmode=DB_SSLMODE" <<EOF
CREATE ROLE cp_role LOGIN PASSWORD '$CP_PW';
CREATE DATABASE controlplane OWNER cp_role;
EOF
( umask 077; echo "host=DB_HOST dbname=controlplane user=cp_role password=$CP_PW sslmode=DB_SSLMODE" | sudo tee /etc/lokadb/control-plane.dsn >/dev/null )
```

**The credentials key**: 32 random bytes as 64 hexadecimal digits, mode `0600`.

```bash
( umask 077; openssl rand -hex 32 | sudo tee /etc/lokadb/credentials.key >/dev/null )
```

**The certificates:** `lokadb certs` makes a demo CA and the relay's certificate. It writes `ca.crt` (public), `ca.key` (**secret**: whoever has it can sign an agent's certificate), `relay.crt` and `relay.key`. Keep the directory to the relay, and copy the public file out for the others:

```bash
lokadb certs -dir /etc/lokadb/relay -relay-host RELAY_NAME
sudo cp /etc/lokadb/relay/ca.crt /etc/lokadb/ca.crt
```

**The configs:** `proxy.json` as in step 5 (with `"control_plane": "file:/etc/lokadb/control-plane.dsn"`), and `relay.json` as in step 8b, with `"org_allow"` filled in. Both read the control plane from the file, so no password is written in them.

## Using LokaDB Cloud

[LokaDB Cloud](https://docs.lokadb.com/cloud/) puts a web panel and an API on top of the same programs; it does not replace this page. Its **runner** (`lokadb runner`) does the checks, moves and move-backs for you, and it needs exactly what steps 1 to 8 give: the proxy with `control_plane` and `credentials_key`, and the relay with the control plane. Once they run, install the runner as [the runner page](https://docs.lokadb.com/cloud/runner/) says. With a Postgres in docker-compose, the runner joins the compose network: [see above](#postgres-in-docker-docker-compose).
