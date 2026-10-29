# Compatibility check: `lokadb check`

`lokadb check` answers "can the organizations of this database and this application be moved?" before anything is changed. It reads the catalog, `pg_stat_statements` and the proxy's workload recording. It does not infer permissions from the catalog: it **tries** what `enable` and `move-org` will do (`CREATE TABLE`, `CREATE EXTENSION`, `CREATE SERVER`, `CREATE SUBSCRIPTION … WITH (connect = false)`, `ALTER TABLE … OWNER TO`) in a transaction that is always rolled back. The answer is therefore right for any role, for managed-service roles (`rds_superuser`, `cloudsqlsuperuser`, `azure_pg_admin`) and settings (`default_transaction_read_only`, a replica).

```bash
lokadb check -db env:CLOUD_DSN -spec spec.json -workload workload.json -proxy-config lokadb.json \
  -customer env:CUST_ADMIN_DSN -customer-app env:CUST_APP_DSN -org 1
```

**Before you run it.** None of the inputs is needed to start:

- `-db` is the administrator's connection string to your database. With only `-db` and `-org-candidates` the check names the table that looks like your organizations table; with `-org-table TABLE` it discovers the tables to move and writes a first spec (`-write-spec spec.json`); [without a spec](#without-a-spec) explains how.
- `-workload` is a recording of the application's statements, made by the proxy (`lokadb proxy -record workload.json`, [recording the workload](#recording-the-workload)). It is optional, but without it the verdict is never `READY`.
- `-customer`, `-customer-app` and `-org` are for checking one particular move: the organization's database (through the relay) and its number. Leave them out for a first check of your own database.

The names (`$CLOUD_DSN`, `$CUST_ADMIN_DSN`, …) are explained in [Install on your database](install.md#the-names-used-on-this-page), which also shows the whole path.

## Findings

Findings come in groups, and only blockers are a problem of the customer's environment:

- **Spec errors** — the spec does not match the database: a table or column is missing, a table has no way to find its organization, a parent is not in the spec, parents form a cycle. The report suggests a similar table name or a foreign key that shows the organization.
- **Blockers** — what stops the move whatever LokaDB does: an environment that is not ready (your database or the organization's own), or a move that would overwrite the organization's data.
- **Not supported by LokaDB yet** — an ordinary schema that LokaDB cannot move yet (key types, composite foreign keys to a child without its own organization column, polymorphic references, constraint triggers…). These are limitations of LokaDB, not defects of the schema; where there is a workaround, the report names it.
- **Changes after the move** — the move works, but behaviour changes and the application owner decides: a global unique constraint becomes unique per organization; a transaction that writes cloud and moved tables becomes two commits.

Example — a database in which problems are planted on purpose (shortened). In the report "migration" means moving organizations, not schema migrations ([migrations](migrations.md)):

```
Database compatibility report: example
Verdict: BLOCKED: the migration is blocked until the blockers below are fixed

  0 of 4 tables ready for tenant partitioning
  3 tables not supported by lokadb yet
  1 global unique constraint becomes per organization after the move

Blockers (2)
  [orphan_rows] 2 rows of document_versions have no row in documents ...
  [customer_table_exists] the organization's database already has a table documents that lokadb did not create ...

Not supported by lokadb yet (limitations of lokadb, not problems of the schema) (4)
  [id_type] attachments.id is numeric(26,0): lokadb takes keys of type smallint, integer, bigint, uuid, text, varchar or character(n) ...
  ...

Changes after the migration (the application works, but these behave differently) (1)
  [unique_not_tenant_scoped] users_email_key on users is global today; after the move it holds within each organization only ...
```

Real applications usually get `NEEDS REVIEW`, and the findings are real: a transaction that writes moved tables and a table that stays in the cloud becomes two commits after the move; an ORM cascades a delete into a table that stays in the cloud; migration tools add constraints on moved tables (the proxy refuses such DDL after `enable`).

### What is checked

| Area | What | Outcome if not |
|---|---|---|
| Spec | tables and columns exist; each table has exactly one way to find its organization; parents are in the spec | spec error |
| Rows without an organization | `NULL` in the tenant column, children without a parent; tables over 1M rows are sampled (`-count-limit`, marked as estimated) | change (the rows stay in the cloud) |
| Keys | key type; duplicates; no unique index | not supported yet / change |
| Large objects | `oid` / `lo` columns: rows move, objects stay in the cloud | change |
| Names | LokaDB's helper names (`__cloud`, `__refs`, `__o<N>`, …) already taken | not supported yet |
| Publications and slots | CDC / replicas will not see moved organizations' changes | change, warning |
| Types, functions, extensions, collations | exist in the customer database or can be created there; an extension needed only by a plain index is a warning (the index is skipped there) | blocker / not supported yet / warning |
| Functions | created in the customer database owned by the application role, not the admin; untrusted languages are refused | change / blocker |
| Unique and exclusion constraints without the tenant column | become per organization | change |
| Foreign keys | to a non-key column, multi-column; from cloud tables to moved rows; from moved tables to cloud tables (kept by triggers after the move) | not supported yet / warning / change |
| Triggers, views, rules, materialized views, RLS | carried over by `enable` (constraint triggers and transition tables are refused) | info / change / not supported yet |
| Query patterns without an organization | from `pg_stat_statements` and the recording, through the same analyzer and policy as the proxy | blocker if the policy refuses them, warning if it lets them through |
| Mixed transactions | write both cloud and moved tables | change |
| Statements on LokaDB's internal objects | refused by the proxy under any policy | blocker |
| DDL the proxy will refuse; DDL followed by a write in one transaction | — | warning |
| Cloud readiness | `postgres_fdw`, server creation | blocker |
| Application role | not superuser, no `BYPASSRLS`, no `REPLICATION`, cannot `SET ROLE` into such a role | blocker |
| Move mode | `wal_level`, slots, subscription rights; which mode `auto` will choose | info |
| Customer database | reachable; where tables will go (`public` or a `lokadb` schema); the `-customer-app` role can own them and is not a superuser | blocker / info |
| Leftovers | of an interrupted move | warning (`lokadb move-cleanup`) |
| Direct access to the cloud database | with `-proxy-hosts`, `pg_hba` rules that let the application role in from other addresses | warning |

## Verdicts and exit codes

| Verdict | Exit code |
|---|---|
| `READY` | 0 |
| `NEEDS REVIEW` — changes in behaviour or warnings | 2 |
| `BLOCKED` | 3 |
| `NOT SUPPORTED YET` — no blockers, but some tables cannot be moved yet | 4 |
| `INVALID SPEC` — tables with spec errors were not checked | 5 |
| the check itself failed (including bad flags) | 1 |

## Without a spec

`-org-table TABLE` (recommended) proposes the spec from the organizations table, whatever the columns are called. The organization is a row of that table (its one-column primary key, an integer or a uuid); `-write-spec FILE` saves the proposed spec.

- **Roots.** Every table with a one-column foreign key to the organizations table gets that column as its `tenant_column`: `tenant_id` in one table, `owner_company_id` in another. A table with two or more foreign keys to it (`from_company_id`, `to_company_id`) is not guessed: it is listed as ambiguous with both columns, and you name one in the spec.
- **Children.** Breadth first, every table with a one-column foreign key to the key of a table already chosen gets that column as `parent_column`, so a chain of any depth is followed. A table that reaches the organization by two parents takes the shorter path if the data shows the other one gives the same organization on every row; otherwise it is ambiguous.
- **Stays in the cloud, with the reason.** The organizations table itself. Tables that do not reach it (`users`, reference data), including polymorphic references (`tokenable_id` with `tokenable_type`), which are never followed. Membership tables (`tenant_users`: a foreign key to the organizations table and one to a table that stays in the cloud, and no data of their own): a query like "the organizations of user X" names no organization and would be refused, so they are proposed as `routing.cloud_tables` instead of being moved.
- **No foreign keys** (Laravel without `foreign()`, Rails): a column `<singular of the table>_id` is the organization column, and `<x>_id` points at a chosen table named `<x>` or its plural. Only after the data check of `routing.relations`: every value of the column is found in the table. A table with no rows yet cannot be checked: its organization column is taken by name and the report says it is not verified. Such tables are marked "by name, verified on the data" and get a `routing.relations` entry where the proxy needs it.

The JSON report has the `discovery` object, one entry per table: its role (`organization`, `root`, `child`, `cloud`, `ambiguous`), the column, the path up to the organizations table, the basis (`fk` or `name`) and, for tables left out, a reason code and text. The text report prints the same. When none of `-spec`, `-org-table` and `-tenant-column` is given, `check` names the tables that look like the organizations: the ones most tables reach by foreign key (a users table on the many side of a membership table is left out).

`-tenant-column organization_id` finds every table with a column of that name and, through foreign keys to their `id`, the child tables. It needs one column name for the whole schema; use it when the tables do share one.

## Recording the workload

Run the proxy with `-record workload.json` (or `"record"` in the config). It records normalized statements (constants replaced by `$n`; parameter values are never seen) and transaction shapes: which statements ran in one transaction, how often, and how the client sent them. The proxy can run as a transparent pass-through before `enable` just to record. The file is rewritten every two seconds and appended to across restarts.

`pg_stat_statements` needs no LokaDB proxy, but it must be in `shared_preload_libraries` and created in the database, and it does not know transaction boundaries: without a recording, mixed transactions are not checked and the verdict is never `READY`. Statements of the role running the check, and statements the application never sent (the proxy's own, and Postgres's), are excluded.

**What the report cannot promise.** It sees the schema and the statements already run. Rare code paths and dynamic SQL are not covered: run the application's tests through the recording proxy before `enable`, and re-run the check before every move. What the report misses, the proxy catches: a statement without an organization is refused by default.

## From findings to declarations

For every query pattern without an organization the report names the table, the column and the form of the condition (the [gap](routing.md#why-the-organization-was-not-found)): `unknown_column messages.conversation_id (=)` means the proxy sees the condition but has no way to find the organization through that column. The usual fixes:

- a column that refers to a moved table without a foreign key — declare it in the spec as a [relation](routing.md#relations-without-a-foreign-key-routingrelations); `enable` (or `lokadb routing apply` after it) checks it on the data before registering it;
- a cloud table that knows the organization — declare it in [`cloud_tables`](routing.md#cloud-tables-that-know-the-organization-routingcloud_tables);
- a column unique across the table (a slug, a public token) — declare it as a [key](routing.md#unique-keys-routingkeys);
- a query that really must run across organizations (a report, a job) — add it to `allow` in the proxy's `fanout` policy after review;
- otherwise — name the organization in the query.

Re-run `check` with the updated spec: the patterns the declarations cover disappear from the report.

**Suggestions.** `check` proposes the declarations itself where the names make it likely: a column `<x>_id` compared by value next to a moved table `<x>s` becomes a suggested relation, a cloud table with the organization column a suggested `cloud_tables` entry, a column with a unique index of its own a suggested key. Each suggestion (`routing_suggestion`) comes with its data check and the patterns it would route; it never changes the verdict.

```bash
lokadb check -db DSN -spec spec.json -workload workload.json -suggest routing.patch.json   # write the suggestions as one patch
lokadb check -db DSN -spec spec.json -workload workload.json -patch routing.patch.json     # the report as if it were declared
lokadb enable -db DSN -spec spec.json -patch routing.patch.json -owner app
```

## Latency forecast and feed indexes

`-rtt <ms>` estimates how long the recorded transactions of a moved organization would take at that round trip time to its database, on the `fdw` profile and with the direct path (`fdw+direct`), and names the most expensive transactions and the chains of one query repeated within a transaction (N+1). The exchange counts come from measurements on a real network; the report says it is an estimate. It needs the proxy's recording (`-workload`): `pg_stat_statements` has no transaction boundaries.

A feed — a query that reads one organization's rows in order with a `LIMIT` — gets `feed_index` when its table has no index starting with (organization column, ordering column), with the `CREATE INDEX` to add.

## JSON and CI

`check -json` is the machine format: for the application's CI, for interfaces, for tools. It carries a `schema_version`, and finding codes are never renamed. Besides the text, each finding has:

- `key` — a stable key (code and object), the same across runs;
- `subject` — the object in parsed form (table, column, constraint, role, organization database);
- `patterns` — every query pattern of the finding, with its pg_query fingerprint (the same the proxy uses for `allow`), calls, tables, roles and source;
- `transactions` — transaction shapes;
- `measure` — how many rows were found and how (exact or sampled).

The report echoes its input (spec, policy with `allow` fingerprints, LokaDB version) so two reports can be compared, and contains no values from data rows — only names, numbers and SQL without literals.

The format — a JSON Schema of version 1, the list of finding codes and sample reports — is published in **[lokadb/check](https://github.com/lokadb/check)**.

**In CI**, run the application's tests through the proxy with `-record`, then (or use the GitHub Action of [lokadb/check](https://github.com/lokadb/check), which runs the same and posts the summary on the pull request):

```bash
lokadb check -db DSN -spec spec.json -workload workload.json -ci -baseline accepted.json
```

Exit code 0 — nothing new; 6 — something `accepted.json` has not accepted, above all a new query pattern without an organization, each with a hint (name the organization, add it to `allow`, or accept it). `-write-baseline FILE` writes the current items as accepted, keeping existing reasons. The file lives in the application's repository next to its tests, not in the proxy config: it also accepts what cannot be in `allow` (global uniqueness, mixed transactions), each with its own reason for review. Patterns are matched by fingerprint, so constants and list lengths do not matter. `info` items do not take part.

## Profiles

`check -profile fdw+direct|direct [-org N]` estimates, from the recorded workload, what share of statements on moved tables would take the direct path and why the rest would not. See [Profiles and cooperative mode](profiles-and-cooperative-mode.md#estimating-a-profile-before-switching).
