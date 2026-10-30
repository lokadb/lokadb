# Configuration

LokaDB is one binary, `lokadb`, with subcommands: `proxy`, `relay`, `agent`, `issuer`, and the administrative commands (`check`, `enable`, `move-org`, `move-back`, `repoint`, `disable`, `org-profile`, `org-credentials`, `org-add`, `routing`, `schema-diff`, `schema-sync`, `refs-diff`, `refs-sync`, `move-cleanup`, `token`, `revoke`, `agents`, `org-endpoint`, `certs`). `lokadb <command> -h` lists the flags of each.

## The spec file

The spec names the tables whose rows move with an organization, and how each table finds its organization.

```json
{
  "tables": [
    {"name": "documents", "tenant_column": "organization_id"},
    {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"},
    {"name": "app.invoices", "tenant_column": "account_id"}
  ],
  "routing": {
    "relations":    [{"from": "messages.conversation_id", "to": "conversations.id"}],
    "cloud_tables": [{"table": "projects", "tenant_column": "account_id"}],
    "keys":         [{"table": "documents", "column": "slug"}]
  }
}
```

- `tenant_column` — the table has its own organization column (integer or `uuid`).
- `parent_table` / `parent_column` — the table gets its organization from its parent through a foreign key; `enable` adds the organization column to it (filled from the parent).
- A table outside `public` is written `schema.name`. The order of tables does not matter.
- `routing` declares what the schema does not say: relations without a foreign key, cloud tables that know the organization, columns unique across the table. See [Routing](routing.md#declarations).

`lokadb check -org-table organizations -write-spec spec.json` proposes a spec from the schema: it follows the foreign keys to the organizations table, whatever the organization column of each table is called (`tenant_id`, `owner_company_id`), and says why every other table stays in the cloud ([Compatibility check](check.md#without-a-spec)). `-tenant-column organization_id` does the same for a schema where all tables share one column name. Which data leaves the cloud is a human decision: review it.

<a id="proxy-lokadbjson"></a>

## Proxy: `proxy.json`

`lokadb proxy` reads `/etc/lokadb/proxy.json` unless `-config` says otherwise, and `lokadb relay` reads `/etc/lokadb/relay.json`: LokaDB's containers mount one directory at `/etc/lokadb`, with these fixed file names ([install, step 4](install.md#4-lokadb-init-the-control-plane-the-keys-and-the-configs)).

```json
{
  "listen": ":6432",
  "upstream": "host=cloud port=5432 sslmode=require",
  "fanout": {
    "*":       {"mode": "reject"},
    "reports": {"mode": "allow"},
    "app":     {"mode": "reject", "allow": ["SELECT * FROM documents WHERE slug = $1 LIMIT $2"]}
  },
  "customers_app": {"app": {"1": "host=relay port=15001 dbname=cust user=lokadb_dml sslmode=verify-full sslrootcert=/certs/cust1.crt"}},
  "tls": {"cert": "/certs/proxy.crt", "key": "/certs/proxy.key", "require": true},
  "tenant": {"local_only": true}
}
```

| Key | Default | Meaning |
|---|---|---|
| `listen` | — | address the application connects to |
| `upstream` | — | libpq connection string of the cloud Postgres (host, port, TLS); user, password and database come from the client |
| `fanout` | `reject` for every database | policy for statements that name no organization, per database (`"*"` for the rest): `reject`, `reject-writes`, `log`, `allow`, plus an `allow` list of reviewed statements. See [Routing](routing.md#statements-without-an-organization) |
| `customers` | — | per database and organization, the address of the organization's database for DDL replay, when the proxy reaches it at a different address than the cloud does. Normally not needed: the proxy reads it from the cloud |
| `customers_app` | — | per database and organization, the DSN of the **direct-path role** (DML only) for `fdw+direct` / `direct` |
| `control_plane`, `credentials_key`, `credentials_key_old` | — | read organization credentials from the control-plane database, encrypted, instead of `customers` / `customers_app` (see [Credentials](#credentials-in-the-control-plane)) |
| `tls` | off | TLS between the application and the proxy: `cert`, `key`, `require` (refuse clients without TLS). Certificate files are re-read without a restart |
| `tenant` | absent | [cooperative mode](profiles-and-cooperative-mode.md#cooperative-mode): `setting` (an existing application setting such as `app.current_tenant`), `local_only` (default `true`: only per-transaction values), `mismatch` (`error`, the default, or `log`), `all_roles` (roles allowed to use `'*'`) |
| `upstream_pool` | on, for all databases | reuse cloud connections between clients (`DISCARD ALL` between them). `false` turns it off, `{}` turns it on everywhere; `max_idle` (10 per key), `max_idle_total` (200), `idle_ttl` (60s), `max_age` (5m) |
| `direct_pool` | off | reuse direct connections to organization databases (`{}` to turn on; same settings) |
| `upstream_pooling` | — | `"session"` when a pooler in session mode sits between the proxy and Postgres. Transaction mode there is not supported |
| `meta_cache` | on | shared cache of LokaDB metadata per database and role; `{"off": true}` disables it, `{"verify": "1s"}` lets new connections skip the freshness check for up to that long (default `0`: every new connection checks) |
| `keyed_statements` | 1024 | statements with the partition key's condition that one cloud connection keeps prepared (about 25 KB of the cloud backend's memory each) |
| `cache_bytes` | 8 MiB | per-session cache of statement analyses |
| `cache_bytes_total` | 512 MiB | all sessions together |
| `org_cache_bytes` | 64 MiB | organizations of row ids, shared by all sessions |
| `route_log` | off | one `route` log line per statement on a LokaDB table (costs throughput: leave off when measuring) |
| `no_route_log` | off | suppress `route` lines even at debug level |
| `record` | — | path of a workload recording (same as `-record`) |
| `metrics_listen` | off | address that serves `GET /metrics` (Prometheus text), `GET /status` (JSON) and `GET /healthz`. Bind it to an address only the monitoring reaches: the counters carry organization numbers |
| `max_connections` | 1000 | client connections |
| `max_message_bytes` | 256 MB | largest protocol message |

`SIGHUP` drops every pooled connection (cloud and organization databases) and the shared metadata cache: send it after `ALTER ROLE … PASSWORD`, `NOLOGIN`, `REVOKE CONNECT` or a `pg_hba.conf` change, otherwise pooled connections stay usable with old credentials for up to `max_age`.

## Relay: `relay.json`

```json
{
  "listen": ":7443",
  "enroll_listen": ":7444",
  "health_listen": ":7446",
  "cert": "/certs/relay.crt", "key": "/certs/relay.key",
  "ca": "/certs/ca.crt", "ca_key": "/certs/ca.key",
  "cert_ttl": "24h",
  "control_plane": "host=cloud dbname=controlplane user=relay sslmode=require",
  "orgs": {"1": ":15001"},
  "org_allow": ["proxy.internal", "cloud-db.internal"],
  "cloud": "cloud:5432",
  "cloud_access": "window",
  "org_tls": "require"
}
```

| Key | Default | Meaning |
|---|---|---|
| `listen` | — | port agents connect to (mutual TLS) |
| `enroll_listen` | — | enrolment and renewal (token → certificate) |
| `health_listen` | — | `GET /healthz` (200 / 503 while draining), `GET /sessions`. Internal only |
| `cert`, `key`, `ca`, `ca_key` | — | relay certificate, CA bundle, CA key (not needed with `issuer_url`). Re-read when the files change |
| `cert_names`, `ca_public` | — | with `cert_names` (a name or several, comma separated) the relay makes the CA and its own certificate at start when they are missing, and never replaces ones that exist; `cert`, `key`, `ca`, `ca_key` must then be `relay.crt`, `relay.key`, `ca.crt`, `ca.key` of one directory that only the relay mounts. `ca_public` is where the relay writes the public `ca.crt` for the proxy, the runner and the agents (once, never replaced). This is what `lokadb init` and the runner write |
| `issuer_url` | — | sign agent certificates through a separate issuing service instead of holding the CA key |
| `cert_ttl` | `1h` | lifetime of agent certificates; agents renew at 2/3 of it |
| `renew_expired` | `24h` | still renew a certificate that expired at most this long ago (`"0"` disables) |
| `control_plane` | — | the registry database: tokens, certificates, revocations, endpoints, cloud windows |
| `orgs` | — | organization → listen address inside the cloud. More can be added at run time with `lokadb org-endpoint` |
| `org_allow` | **required** | addresses allowed to connect to organization endpoints: the proxy, the cloud Postgres (`postgres_fdw`), the machines that run `check` and `move-org`. Each entry is an IP address, a CIDR range or a host name (looked up again every 30 s). In Docker the containers' addresses change when they are recreated: give the network's range, not a container's address. `lokadb init` writes the three private ranges |
| `org_tls` | off | `require`: refuse organization connections that do not start TLS |
| `org_pin` | on | `off`: let `org_allow` reach the organizations' ports without the SaaS database's proven addresses. On (the default): once `lokadb pin` (move-org `-cp`, the runner) proved them, an organization's connections come from those and from LokaDB's own components only (a restored copy of the SaaS database is refused: [tunnel](tunnel.md#copies-of-the-saas-database)) |
| `cloud`, `cloud_access` | —, `window` | where the agent's connection to the cloud goes (the subscription during a logical move), and when it is allowed (`window`: only while `move-org` opens it; `always`: any connected agent, at any time) |
| `registry_grace` | off | admit an agent whose certificate the registry confirmed within this period while the registry is down (a certificate revoked in that period also gets in) |
| `drain_delay`, `drain_timeout` | `0`, `5s` | graceful stop on `SIGTERM`: the wait before the ports close, and how long open streams may finish. Behind several relays raise them (for example `5s`, `1m`) |
| `max_agents_per_org` | 4 | concurrent agents per organization |
| `max_org_conns` | 1000 | concurrent connections per organization endpoint |
| `max_streams` | 2048 | connections through the relay in total (memory: up to about `max_streams × 4 MiB`) |
| `max_cloud_streams` | 64 | connections to the cloud per agent session |
| `max_handshakes`, `max_enroll_conns` | 256, 256 | agents in TLS handshake; connections to the enrolment port |
| `enroll_fail_per_min` | 10 | failed enrolment/renewal attempts per client address (`-1`: unlimited) |

Agent flags: `-relay host:port[,host:port]` (or one name resolving to every relay), `-ca`, `-state` (key and certificate, mode 0600), `-token-file`, `-db host:port` (the only address it forwards to), `-cloud-listen` and `-cloud-allow` (the subscription path during a logical move), `-sessions N` (1–8 TCP connections per relay), `-max-conns` (200).

## Issuing service: `issuer.json`

```json
{"listen": ":7445", "cert": "/certs/issuer.crt", "key": "/certs/issuer.key",
 "ca": "/certs/ca.crt", "ca_key": "/certs/ca.key", "cert_ttl": "1h", "relays": ["relay"]}
```

The only process that holds the CA key; it signs agent client certificates for the relays listed in `relays`.

| Key | Default | Meaning |
|---|---|---|
| `listen` | — | the address the relays ask for certificates on |
| `cert`, `key` | — | the issuing service's own certificate |
| `ca`, `ca_key` | — | the CA certificate and its key |
| `cert_ttl` | `1h` | lifetime of the certificates it signs |
| `relays` | — | the common names of the relays' client certificates that may ask it for signatures |

 See [The tunnel](tunnel.md#ca-key-in-a-separate-issuing-service).

## Passwords on the command line

A DSN with a password in an argument is visible in `ps` and in shell history. Every DSN flag (`-db`, `-customer`, `-customer-app`, `-publisher`, `-cp`, `-dsn`, …) accepts instead:

- `-flag -` — read the DSN from stdin (one flag per command);
- `-flag env:NAME` — read it from the environment variable `NAME`;
- a DSN without a password, with the password in `PGPASSFILE` (`~/.pgpass`) or `PGPASSWORD`, as libpq does.

A password written in the argument still works, with a warning. Commands never print passwords.

## Credentials in the control plane

The proxy needs credentials of every organization database it reaches directly (the direct-path role, and the owner role for DDL replay). Instead of keeping them in `proxy.json`, store them encrypted in the control-plane database:

```bash
lokadb org-credentials -db env:CLOUD_DSN -control-plane file:/etc/lokadb/control-plane.dsn -key /keys/lokadb.key -org 1 -kind direct -dsn -   # DSN from stdin
lokadb org-credentials -db env:CLOUD_DSN -control-plane file:/etc/lokadb/control-plane.dsn -key /keys/lokadb.key -org 1 -kind owner  -dsn -
```

- The command connects with the credentials first (the direct role must have DML only), then encrypts them with AES-256-GCM. The organization, role, database and the cloud cluster's identifier are bound into the ciphertext: a row cannot be replayed for another organization or another cloud.
- The proxy reads them at start and every 30 s with `control_plane` and `credentials_key` (a 32-byte file or 64 hex digits, mode 0600). They take precedence over `customers` / `customers_app`.
- Key rotation: put the new key in `credentials_key` and the old one in `credentials_key_old`, run `org-credentials -reseal -key NEW -key-old OLD`, then drop the old key. `-list` shows what is stored without decrypting.
- The owner role may not be a superuser or have `BYPASSRLS` or `REPLICATION`.
