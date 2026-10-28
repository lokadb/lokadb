# Measurement method

How the numbers of the README's "Performance" section are taken. Everything is in `bench/`; anyone can repeat a run on their own servers and get tables of the same shape.

## What and with what

| What | With what |
|---|---|
| All scenarios in a row | `bench/run-all.sh INVENTORY`: in order, resumable after a failure, a report after every scenario |
| Deploying the stand | `bench/deploy.sh up|down|status|ping INVENTORY`: Docker, Postgres (the cloud one and one per organization), relay, agents, proxy; the hosts are described in `bench/inventory.example` |
| Binaries | `bench/build.sh`: `lokadb` of this commit and `benchload` are built in Docker `golang:1.25` and copied to the hosts (`bench/dist/COMMIT` is written into the run's metadata) |
| Load | `bench/load` (`benchload`): a closed loop of N clients, or an open model at a set rate (`-rate`); SQL (`sql`), HTTP (`http`), connect (`connect`), COPY, cursor, NOTIFY; the output is one JSON line per operation: operations, errors, req/s, p50/p95/p99/max, the first error, the kinds of errors; `-timeline` adds per second and per class of organizations |
| HTTP applications | `bench/apps/{laravel,django}`: the e2e applications with five routes (a document page, a feed, insert, update, a report); whole requests go through the ORM, as an application makes them |
| Scenarios | `bench/scenarios/*.sh INVENTORY` |
| Report | `python3 bench/report.py`: tables and charts (SVG) from the raw data, no external libraries; `python3 bench/charts/build.py`: the charts of the README (two themes) with their tables in `research/bench/charts-data.md` |
| Data check | after the scenarios that change data (`misc`, `outage`, `agents`, `move`, `volume`, `extra`) `bench/verify-data.py` compares the rows by organization in the cloud and in the organizations' databases with what `benchload` counted; the line goes into `notes.txt`, `data-check.txt` and `report.md` |

## Topology

Each role is a server of its own if the inventory says so (a trial run puts all roles on one machine):

- **cloud**: the cloud Postgres 18 (`wal_level=logical`, `fsync` and `synchronous_commit` on, so the write numbers come with a real disk; `shared_buffers` a quarter of the RAM, `max_connections` 1500);
- **proxy** and **relay**: on a server apart from the database;
- **load**: the load generator and the containers of the Laravel and Django applications;
- **org1 … orgN**: the organizations' databases and their agents on servers of their own at different distances from the cloud, RTT from 0 to 50 ms (real, no `tc netem`). In the results the hosts are named only by role, cores, memory and disk, and the organizations' databases by their RTT from the cloud ("an organization at about 10 ms"): no address, provider or place (`bench/check-clean.py` checks it before a commit).

The RTT between the hosts is measured with `ping` (10 packets) at the start and at the end of a scenario: `rtt-start.json`, `rtt-end.json`. If the RTT changed noticeably, the run is not fit for the README.

## What is measured in each run

One result line is one kind of operation under one configuration and number of clients:

- **req/s**: operations per second that completed without an error (errors are counted apart and are not in req/s);
- **p50, p95, p99, max**, ms: over the operations that completed without an error;
- **errors**: the count and the first one; any non-zero count is read as a result;
- **resources** (`raw/resources.jsonl`, a snapshot in the middle of the run): the CPU and memory of the proxy and relay (`top`, 2 s), `docker stats` of the Postgres containers, the CPU of each host, the connections of the cloud and of the organizations' databases.

## Path configurations

| Configuration | What |
|---|---|
| `plain` | plain Postgres without LokaDB: the same tables, the application connects straight to the cloud database |
| `fdw` | LokaDB, the organization moved, profile `fdw`: queries go through the proxy, the cloud database, `postgres_fdw`, the relay, the agent |
| `fdw+direct` | the same with profile `fdw+direct`: the proxy sends the organization's reads and updates straight to its database |
| `direct` | profile `direct`: the cloud has no user mapping for the organization; the application names the organization in every transaction (`lokadb.tenant`, the cooperative mode); for applications without that support, "not measured" |
| "class" `moved` / `cloud` | the organizations moved to databases of their own, and the organizations that stayed in the cloud (a partition in the cloud database, the same proxy) |

## Rules of a run

1. **Warm-up.** `WARMUP` (15 s by default) works like the measured part and is not in the numbers: it warms the Postgres caches, the plans, the connections, the proxy's metadata. The applications (HTTP) are warmed 15 s more before the first measurement.
2. **Measured time.** `DURATION` (60 s by default); in the closed loop a client sends the next request when it has the answer to the previous one.
3. **Repeats.** `REPEATS` (3; the runs of 27 October used 2), the report shows the median of the repeats; the number of repeats and the spread (min/max over the raw lines) are in `raw/` and in the reports.
4. **Data.** Each scenario makes its database anew (`mkdb`), fills it and runs `VACUUM ANALYZE` before measuring; the ids are chosen uniformly over the organization. The setup creates an index `(organization_id, id)`, as an application with a feed has.
5. **One scenario at a time.** While a scenario runs, nothing else works on the stand's servers.
6. **Changes of a run are not kept:** the database is dropped at the end; `deploy.sh down` removes everything (and first saves the logs of the run into `bench/state-archive/`).
7. **Ports.** `deploy.sh up` sets `net.ipv4.tcp_tw_reuse=1` and `net.ipv4.ip_local_port_range="1024 65000"` on every server. Runs with a new connection for every request (`-reconnect`) and a proxy without a pool leave sockets in TIME_WAIT. Without these settings the generator runs out of local ports ("cannot assign requested address") and the run measures the stand, not the path. The runs of 26 October that were hit by it before the setting are kept in `excluded/` and were measured again.
8. **A trial run** (`BENCH_TRIAL=1`) is marked in `meta.json` and in the report; its numbers do not go to the README.
9. **Defaults of the code.** The runs name what they ran on: `move-org` is told its profile (`MOVE_PROFILE`, `fdw` by default), the cache of cloud connections is switched off (`NO_POOL=1`) where a run is compared with an earlier one, the route log is off unless the run measures it.

## Scenarios

| Scenario | Question | Main parameters (defaults) | Time estimate* |
|---|---|---|---|
| `paths` | what each path costs: a read by id, a feed, insert, update; a new connection | 20 organizations × 20 000 documents, 3 moved; clients 1 and 32; 4 configurations | 3 h |
| `http` | the same by whole HTTP requests of Laravel and Django | clients 1 and 16; plain, fdw, fdw+direct | 4.5 h (3 h with `REPEATS=2`) |
| `overhead` | the proxy's own cost on a LAN without routing: plain, LokaDB straight, the proxy by default and with `route_log`, `meta_cache`, pools; prepared statements and literals; new and "hot" keys; a connection per request | 20 organizations × 20 000; 1 and 32 clients; 2 repeats of 45 s (`OVERHEAD_*`) | 2.8 h |
| `clients` | many clients at once (1 … 1000) | read+update; the cloud straight, through the proxy: the cloud's organizations and the moved ones | 2 h |
| `sessions` | thousands of organizations with many sessions: p99, the cloud's CPU, metadata; a run of 330 s | N = 1000, 3000; 10 and 100 sessions; without and with `meta_cache` | 2.5 h |
| `rtt` | a request by distance: each moved organization on its own, profiles fdw, fdw+direct, direct | the inventory's organizations at different distances; 2 repeats of 30 s (`RTT_*`) | 2.5 h |
| `agents` | many agents on one relay (10, 50, 200) | the latency of an organization with the agents connected; the return after a relay restart | 1 h |
| `orgs` | thousands of organizations: planning with a partition per organization | N = 1, 100, 1000, 3000; extended and simple protocol | 4 h |
| `volume` | `enable` of a big table under writes; the cost of the DEFAULT partition for a moved organization | 1, 10, 100 million rows | 5 h (mostly loading 100 million rows and `enable`) |
| `move` | moving a big organization under writes: modes, batches, a real network | 60 MB, 600 MB, 10 GB; logical and trigger | 2.5 h (+3 h with 50 GB) |
| `misc` | COPY, cursors, LISTEN/NOTIFY on 300 thousand rows | three paths | 0.7 h |
| `outage` | the organization's database is unreachable: errors, time to the error, the effect on the others | `db_down`, `agent_down`, (`blackhole`) | 0.3 h |
| `after` | a short set after fixes: the cloud class through the proxy, the feed against the same table without LokaDB, a connection per request, the direct profile's first query | 20 organizations × 20 000; clients 1 and 32; 16 for a connection per request | 1.5 h |
| `extra` | controls: a connection per request without LokaDB through pgbouncer (transaction and session mode); the open model at several rates (`RATES`) | 16 clients; 256 workers; timeout 1 s | 0.5 h |

**About 31 hours of the stand's work** with the defaults (including about 38 s for deploying, building, repeating failed runs and a reserve); the scenarios go one at a time, so faster only with a second set of servers. In case time is short: `paths`, `http`, `overhead`, `clients`, `sessions`, `rtt` (about 17 h) give the numbers for the README; the others answer narrower questions. Run in order with resume: `bench/run-all.sh`.

\* For a cloud server of 8 cores / 32 GB, NVMe disk, `REPEATS=3`; without deploying the stand (about 10 minutes) and building the application images (about 10 minutes on the load host).

## What is recorded

`research/bench/<date>-<scenario>/`:

- `meta.json`: the version and commit of `lokadb`, the Postgres version and the key settings of the cloud database, the cores/RAM/disk of each host and its role, the warm-up, the time, the repeats, the trial flag;
- `rtt-start.json`, `rtt-end.json`: the RTT between the hosts that talk to each other;
- `raw/*.jsonl`: `benchload` lines (each with tags: scenario, configuration, class, operation, clients, repeat); `raw/*.csv`: per-second series (the writer during `enable`, a move, an outage); `raw/move-*.log`: the `move-org` log; `raw/wal-*.csv`: the replication slot and WAL of the cloud; `raw/explain-*.txt`: plans;
- `notes.txt`: the course of the run: what ran, how long, what was noticed; `data-check.txt`: the data check;
- `report.md` and `*.svg`: made by `bench/report.py`;
- `excluded/`: runs that hit a failure of the stand, with the reason (not results).

The summary `research/bench/REPORT.md` is built by the same script from all the directories.

## What else affects the numbers (taken into account in the scenarios after the second performance review)

- **The route log** of the proxy (`route`, a line for every statement) is off in all scenarios (`"no_route_log": true`, `proxy_config` in `bench/scenarios/common.sh`) except the `routelog` variant of `overhead`, which measures it; without that, journald on the proxy's host becomes part of the measurement. The proxy runs without `-record`.
- **The metadata cache** is on by default in the proxy. Off is `"meta_cache": {"off": true}` (`sessions`, the `meta_off` variant of `overhead`); "with the cache" is a configuration without the section or with `verify`.
- **The shape of a transaction.** Profile `direct` reads only in a transaction with `lokadb.tenant` (`-coop`: BEGIN, `set_config`, the statement, COMMIT). In `paths` and `rtt` the other profiles are also measured that way (`shape=tx`) and in autocommit (`shape=auto`): rows of different shapes are not compared with each other.
- **Keys.** Rows with `keys=cold` take a random key from the whole organization (new to the session: the proxy looks up the organization of the row), `keys=hot` from 50 keys (`-hot 50`): the hot ones measure the steady state. In the report such rows are labelled "(hot keys)".
- **Pools.** PHP without persistent connections (Laravel) is measured twice in `http`: without pools (each connection to the organization's database is about eight exchanges: connect, SCRAM, five `SET`) and with `upstream_pool`, `direct_pool`, `meta_cache.verify` (the `+pools` configuration). Since the cache of cloud connections is on by default for databases with moved organizations, a run that is compared with an earlier one sets `"upstream_pool": false` (`NO_POOL=1`).
- **Access to the cloud database** is closed by `pg_hba` to everything except the stand's machines: the superuser from the proxy, the relay (control plane, subscriptions of organizations) and the database itself, the application role from the proxy and the database itself; the load host is open to the application role on purpose (the baselines plain and lokadb-direct and the applications' migrations go round the proxy). A move's replication role (`lokadb_repl_*`) is let in from the relay's address. `lokadb check -proxy-hosts` is not run in the measurements. A `pg_hba.conf` mounted into the container as a single file must be changed in place or the container restarted: replacing the file leaves the container with the old one.
- **Agent sessions.** By default an agent keeps one TCP session to the relay. If on a far channel p99 comes up against it, `AGENT_SESSIONS=4 bash bench/deploy.sh push INVENTORY` restarts the agents with `-sessions 4`; repeat `rtt` and compare the rows.
- **Ceilings of the stand.** On the organizations' databases and agents `max_connections` and `-max-conns` are raised to `ORG_MAX` (1500) (default 100/200): otherwise `clients` shows the limit of the stand, not LokaDB's. LokaDB's own ceilings (the relay's `max_cloud_streams` 64 per agent session, `max_org_conns` 1000, `max_handshakes` 256, `max_streams` 2048) stay at their defaults; the values are in `meta.json` (`limits`). At 1000 clients through `fdw` it was the relay's `max_streams` that was reached.
- **Each result line is one operation.** For a mix (`mix`) `read` and `update` are separate in the report, as are the three kinds of COPY, `cursor_total`/`cursor_fetch`, `connect`/`first_query`.
- **Thousands of organizations.** Before `cf56419` for every statement the cloud planned all the partitions and computed `lokadb.local_parts()` (O(number of organizations)); since then the proxy adds a redundant condition on the partition key (the planner prunes the partitions) and the policy reads `lokadb.foreign_parts` (a point read on 3000 organizations: 295 → 0.6 ms on the simple protocol). The scenarios `orgs` and `sessions` are taken on code not older than `cf56419`; the "before" point is `research/bench/baseline-before-pf/`.
- **The feed.** `setup` creates an index `(organization_id, id)`, as an application with a feed has (`-feed-index`, on by default): `ORDER BY id DESC LIMIT 20` of one organization reads the tail of its range. Without it the backward walk over the primary key goes through almost the whole table, because the organizations were inserted one after another, and the feed measures not the path. The baseline of the feed of a cloud organization is the same table without LokaDB (`plain`, `bench_plain`), not the feed of a moved organization in its own database, where all the rows are its: "straight" for a feed in `plain` is the feed of a moved organization, and it must not be put into the comparison table of the cloud class. The runs of 26 October were taken without the index.
- **Start of the clients** is spread over half of the warm-up (at most 10 s, `-stagger`): no storm of connections at the start, and the metadata refresh cycles of the sessions do not coincide in phase, as with a real application.
- **`lokadb-direct` in `overhead`** (the application sets `lokadb.org` itself) is an exchange with the cloud of its own for every operation: the baseline is slower than a real application, while the proxy sends `set_config` together with the statement. In the tables it is labelled "(set_config apart)"; compare it as "the LokaDB schema without the proxy, with an extra exchange", not as a limit.
- **The open model.** `benchload -rate N` sends operations at a set rate whether or not the earlier ones were answered; `-c` workers take them in order; the latency counts from when an operation was due. An operation not started within the timeout (`-timeout`, 1 s) of being due is a timeout ("not started"); one that was started gets the timeout of its own to finish. The numbers past saturation are the share of timeouts and the latency of the ones that were served.

## Limits

- **The closed loop:** latencies do not include the queue an open load forms; at saturation p99 is understated against the open model. The comparison of the paths is not changed by this (all are measured the same way). `extra` measures the open model for the cloud class.
- **One load generator:** its CPU is visible in the resource snapshot; if it is busier than 80 %, the number of clients on one machine comes up against it, not against the stand. Then the clients are divided over several load hosts.
- **Synthetic data:** organizations of equal size, uniform choice of rows. A Zipf distribution and peaks are a separate scenario over a helpdesk (`research/realistic`).
- **ORM applications:** Laravel without persistent connections (per request), Django with persistent ones (`CONN_MAX_AGE`); the difference explains part of the differences between them and does not belong to LokaDB.
- **The provider's disk and network** are shared with other customers' virtual machines; that is why the repeats and the RTT before and after.
- **The SQL scenarios' schema** is `users`, `documents`, `document_versions` without a foreign key to a cloud table: profile `direct` refuses writes to a table with an FK to a cloud table, and with such a key there would be nothing to compare it with. The HTTP applications (e2e) have such links, but they do not lead to the moved tables.
- **`direct`** is measured only on a SQL load with the cooperative mode (`lokadb.tenant` in a transaction); the HTTP applications without it are not measured in this configuration.
- **The Postgres version** is 18 in Docker; managed services (RDS, Cloud SQL) did not take part in these runs.
- The results belong to the commit recorded in `meta.json`; after the code changes the numbers must be taken again.

## Replay of a pass on the stand page

`python3 bench/replay.py plan` turns the raw results of the scenarios `paths`, `clients`, `rtt` and a logical move into about 34 frames of writes (the documents `bench/live`, `scenarios/<id>`, `log/<id>` of the stand page's database) for a sped-up replay of the pass, about three minutes at 5 s a frame; `--frames` and `--scenarios` choose the length and the scenarios, `show DIR` lists the frames. The numbers on screen are the measured ones; only the clock is made up. The frames are applied by a Claude Code session through the artifact tool (nothing else writes a page's database). The hosts are by role: no address, no place.

## How to repeat

```bash
cp bench/inventory.example my.inv        # put in the hosts: role, name, ssh, address
export BENCH_KEY=~/.ssh/id_ed25519
bash bench/build.sh
bash bench/deploy.sh up my.inv
bash bench/scenarios/paths.sh my.inv     # any scenario; or all in order: bash bench/run-all.sh my.inv
python3 bench/report.py                  # research/bench/REPORT.md and the charts
python3 bench/charts/build.py            # the README's charts and research/bench/charts-data.md
bash bench/deploy.sh down my.inv
```
