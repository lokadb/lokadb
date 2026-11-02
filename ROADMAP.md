# Roadmap

What LokaDB does not do yet, and what we plan. Items are grouped by area, not by date; priorities follow what pilots need. The current limitations are listed in detail in [Limits and isolation](docs/limits.md).

## Released

One line per version, newest first. Every version is the `lokadb` binary (Linux amd64 and arm64) and the container image `ghcr.io/lokadb/lokadb:<version>` ([Install on your database](docs/install.md)).

- **0.1.18**: a statement refused under `log` after a move names `allow`, which runs it in every database; `check`, `schema-diff`/`schema-sync`, `move-org` and `repoint` given `-proxy-config` prove themselves to the relay with its `control_plane`; `move-org` right after a new organization was added no longer falls back to `fdw` because a process had read the relay's ports before.
- **0.1.17**: `move-org` and `routing apply` tell every proxy at once (a proxy learned of a move only after up to 30 s, so `log` became `reject` that much later); a move under two proxies is tested.
- **0.1.16**: fixes from the third code review: a statement without an organization after the first move (`log` now works like `reject`), a table with an RLS policy, rule or statement trigger of the application stays correct after a move, `move-back` after a failed `move-org` leaves the customer's own tables alone, the split copies from one snapshot, a copy left by an interrupted move is found, lost jobs end instead of blocking, long connections survive a tunnel renewal, `check` reports no row values. In production at KeepPlain, our own SaaS.
- **0.1.15**: statements without a customer for a runner whose proxy runs outside it; a second job on the same database is refused while one runs.
- **0.1.14**: `ctid` on a LokaDB table: Laravel's limit-delete (`MassPrunable`) is rewritten to the key and other uses are refused, because they could delete other organizations' rows (`check` reports `query_ctid`); an `INSERT` that names its organization through a parent column is routed.
- **0.1.13**: the panel's step 5 offers "read" only where it changes something; the account's and each database's id are shown for the API.
- **0.1.12**: `move-org` without a superuser pauses an organization for minutes, not tens of minutes; the pause is forecast and guarded (`-max-split-pause`); functions of databases enabled by older versions are brought up to date.
- **0.1.11**: a failed split keeps the tables already split (a data-loss fix); a customer database that holds another organization is refused before any change; `move-org`, `enable` and `disable` work without a superuser; the relay admits the cloud database by identity and rotates customer roles' passwords hourly; the cloud database needs PostgreSQL 15 or newer.
- **0.1.10** (installer image only): a moved organization's partition gets the parent's `CHECK` constraints exactly (Laravel enum columns no longer fail the switch).
- **0.1.9**: a customer chosen by a value (ULID, uuid, text) is numbered by the runner.
- **0.1.8**: statements without a customer decided by table (`fanout.tables`); the proxy logs each shape once and a refusal once a minute.
- **0.1.7**: the runner announces what it can do in every poll, so an updated runner gets the new job kinds.
- **0.1.6**: statements without a customer are gathered by shape, without values, and decided before the first move: the column names the customer, a cloud table, or allow.
- **0.1.5**: the runner applies the panel's `fanout` policy (`log`, `allow`, `reject-writes`, `reject`); an application role that bypasses row level security is found at once (`app_role_bypasses_rls`).
- **0.1.4**: row values are stripped from database error texts before anything leaves the runner; a setup run again keeps its files.
- **0.1.3**: every way to install (compose, `docker run`, binary under systemd, Kubernetes), managed Postgres with TLS, a public proxy for an application on a PaaS.
- **0.1.2**: install from the panel in one container (runner, relay and proxy), a six-step wizard; `enable` prepares online and then switches under a short lock; turning LokaDB off with a dry run.
- **0.1.1**: `character(n)` keys and text organizations; `lokadb init`; the runner reports every setup problem; organizations-table discovery.
- **0.1.0**: the first release.

## Schemas

- **Polymorphic references** (`<x>_type` + `<x>_id`, Rails `taggable`): relations with a condition on the type column, one per target table. Today such a relation is refused, and the table needs its own organization column.
- **Child tables reached through a composite foreign key** without their own organization column, and `routing.relations` to a composite key.
- **Constraint triggers and triggers with transition tables** on moved tables are refused by Postgres when a table has foreign partitions; we will look for an equivalent where one exists.

## Query paths

- **Fast `COPY FROM` on the `fdw` profile.** Today it costs about 10× a plain `COPY` for organizations in the cloud and about 85× for moved ones, because of row triggers; batched handling of `__refs` and foreign-key checks would bring the cloud case near a plain `COPY` and let `postgres_fdw` batch inserts to moved organizations. Until then, import into a moved organization on `fdw+direct` with the organization named.
- **Joins of two LokaDB tables of one moved organization** pushed down as a whole to its database (with `ORDER BY` and `LIMIT`), as joins with cloud tables already are.
- **Statements of one cloud organization targeted at its local partition**, and real pruning of the DEFAULT partition (today it is probed through an index on every statement).
- **More forms recognized** without an `allow` entry: `<> ANY`, `OR` between keys, subqueries in the select list.

## Profiles

- **`direct` without `postgres_fdw` for everything:** copies of the cloud tables an organization's statements join with, kept by the proxy in the organization's database, and reports across organizations on this profile.
- Fewer round trips per transaction on `fdw+direct` for the remaining shapes.

## Moving

- **Logical move without a replication role at the customer.** Today the logical mode would give the customer database a cloud role that can decode the whole cloud database, so `auto` uses triggers. Reading the slot in LokaDB itself, under the cloud's credentials, would make logical replication the default again for large organizations.
- **A second organization into a customer database that already holds one.** Today `move-org` refuses it (one database takes one organization), because moving it in would drop the first one's tables. Support is planned after the launch.
- **Several organizations moving at the same time** on one cloud database (today one after another).
- **Large organizations over a WAN** (10+ GB): measured initial copy, catch-up, pause, cloud WAL and agent memory.
- **Provisioning the customer database** next to the agent (a Postgres image with a data volume, compose or Helm), where the customer allows it. Today the customer provides the database.

## Known gaps, to be fixed

Small things we know about and have not fixed yet:

- **Identity columns lose their step, `MAXVALUE` and `CYCLE`** while LokaDB is on (an identity with `INCREMENT 10` counts 1, 11, 21, 22, 23).
- **`check` can say `READY` where the switch refuses:** on a table above `-count-limit`, rows with a `NULL` organization are only sampled.
- **An `UPDATE` that names the key of one organization and the organization column of another** finds no row and says nothing.
- **`PREPARE` / `EXECUTE` through SQL:** the text of a refused `PREPARE` can be used by a later `EXECUTE` of the same name; with `reject`, a `PREPARE` with parameters is refused.
- **Row images in the trigger-mode move log** are written in the application's session settings (`extra_float_digits`, `IntervalStyle`), so a float or an interval can be rounded.
- **Rotations of a role's password** from the runner and from `org-credentials -rotate` at the same time can leave the wrong one in the control plane.
- **The tunnel:** right after a relay starts, address pinning does not hold until it has read the registry; delayed connections of a database copy take relay slots.

## Tunnel and operations

- A ready integration of the issuing service with a KMS or HSM (signing already goes through `crypto.Signer`).
- A stand with two relays behind a load balancer in the end-to-end suite (unit-tested today).
- A version counter of LokaDB metadata instead of a catalog fingerprint, so new connections check freshness with one number.

## Not planned

- **Atomic commit across the cloud and a customer database.** `postgres_fdw` has no two-phase commit; LokaDB commits the organization first and provides `refs-diff` / `refs-sync` for the window between the two commits.
- **A connection pooler in transaction mode between the proxy and Postgres.** The proxy's own statements and the client's must share a server session. Put the pooler in front of the proxy instead (any mode works there).
- **Authorization.** LokaDB routes by row ownership; deciding who may read what stays in the application.

## Hosted service

The open-source `lokadb check` is complete: it already includes the latency forecast for remote organizations (`-rtt`) and N+1 detection.

LokaDB Cloud works today at [app.lokadb.com](https://app.lokadb.com): the panel and the API (accounts, setup wizard, organizations and moves, agents, statistics and alerts, API keys and the team), the open runner that does the work in your network, the installer and agent for the customer, the customer portal, webhooks, and plans with billing. See [LokaDB Cloud](https://docs.lokadb.com/cloud/).

Ahead, without a date: an AI advisor over the findings of `check` is being researched (nothing is built yet). The Cloud's own gaps are in the list above.
