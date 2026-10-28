
## 2026-10-27-agents

- run: 2026-10-27 (agents), LokaDB `f7983a30fde2e6654f2f75faecece71650bd6ff7 dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.179/0.240/0.605; cloud>relay 0.167/0.197/0.224; cloud>load 0.179/0.240/0.365; cloud>org1 0.185/0.273/0.554; cloud>org2 9.468/9.599/10.247; cloud>org3 38.770/38.853/39.079; cloud>org4 43.481/43.592/43.857
- RTT ms min/avg/max, end: cloud>proxy 0.141/0.180/0.254; cloud>relay 0.161/0.360/1.994; cloud>load 0.168/0.205/0.279; cloud>org1 0.187/0.224/0.317; cloud>org2 9.396/9.446/9.511; cloud>org3 38.821/39.285/40.070; cloud>org4 43.288/43.392/43.791

| agents | class | clients | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|
| 10 | moved | 16 | mix | - | - | - | - | - |  |  |
| 10 | moved | 16 | read | 19 (19–19) | 440.9 | 446.11 | 477.69 | 530.18 |  |  |
| 10 | moved | 16 | update | 21 (21–21) | 353.16 | 356.85 | 386.84 | 442.21 |  |  |
| 100 | moved | 16 | mix | - | - | - | - | - |  |  |
| 100 | moved | 16 | read | 19 (19–19) | 451.51 | 456.35 | 461.68 | 468.46 |  |  |
| 100 | moved | 16 | update | 21 (21–21) | 361.59 | 365.37 | 369.48 | 380.61 |  |  |
| 50 | moved | 16 | mix | - | - | - | - | - |  |  |
| 50 | moved | 16 | read | 19 (19–19) | 446.14 | 451.95 | 457.5 | 468.05 |  |  |
| 50 | moved | 16 | update | 21 (21–21) | 357.34 | 361.99 | 365.76 | 375.03 |  |  |

### data check

- data check agents: ok: 100000 rows in 5 organizations (100000 before, +0: 0 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
