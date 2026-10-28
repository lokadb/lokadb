# Roadmap

What LokaDB does not do yet, and what we plan. Items are grouped by area, not by date; priorities follow what pilots need. The current limitations are listed in detail in [docs/limits.md](docs/limits.md).

## Before the first release

- Publish the measurements on real networks: the cost of the proxy for organizations that stay in the cloud, `fdw` against `fdw+direct` for remote organizations, switch-over pauses under load, ceilings for each workload, with methodology and raw results.
- A one-command local quickstart ([docs/quickstart.md](docs/quickstart.md)).
- CI on GitHub Actions (`go vet`, `go test`, `-race`; the end-to-end stand on demand).
- `SECURITY.md`, `CONTRIBUTING.md`, issue templates.

## Schemas

- **Polymorphic references** (`<x>_type` + `<x>_id`, Rails `taggable`): relations with a condition on the type column, one per target table. Today such a relation is refused, and the table needs its own organization column.
- **Child tables reached through a composite foreign key** without their own organization column, and `routing.relations` to a composite key.
- **Text organization identifiers.** Integer and `uuid` are supported.
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
- **Several organizations moving at the same time** on one cloud database (today one after another).
- **Large organizations over a WAN** (10+ GB): measured initial copy, catch-up, pause, cloud WAL and agent memory.
- **Provisioning the customer database** next to the agent (a Postgres image with a data volume, compose or Helm), where the customer allows it. Today the customer provides the database.

## Tunnel and operations

- A ready integration of the issuing service with a KMS or HSM (signing already goes through `crypto.Signer`).
- A stand with two relays behind a load balancer in the end-to-end suite (unit-tested today).
- A version counter of LokaDB metadata instead of a catalog fingerprint, so new connections check freshness with one number.

## Not planned

- **Atomic commit across the cloud and a customer database.** `postgres_fdw` has no two-phase commit; LokaDB commits the organization first and provides `refs-diff` / `refs-sync` for the window between the two commits.
- **A connection pooler in transaction mode between the proxy and Postgres.** The proxy's own statements and the client's must share a server session. Put the pooler in front of the proxy instead (any mode works there).
- **Authorization.** LokaDB routes by row ownership; deciding who may read what stays in the application.

## Hosted service

[lokadb.com](https://lokadb.com) will offer, on top of the open-source check: reports on an uploaded schema and workload, the latency forecast for remote organizations and N+1 detection, an interface with fixes and patches, an AI advisor over the findings, and report comparison.
