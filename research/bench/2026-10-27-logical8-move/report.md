
## 2026-10-27-logical8-move

- run: 2026-10-27-logical8 (move), LokaDB `f7983a30fde2e6654f2f75faecece71650bd6ff7 dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.115/0.180/0.378; cloud>relay 0.153/0.181/0.236; cloud>load 0.159/0.238/0.476; cloud>org1 0.171/0.206/0.328; cloud>org2 9.353/9.507/10.342; cloud>org3 38.688/38.766/38.901; cloud>org4 44.208/44.301/44.698
- RTT ms min/avg/max, end: cloud>proxy 0.143/0.165/0.270; cloud>relay 0.141/0.161/0.179; cloud>load 0.150/0.190/0.302; cloud>org1 0.150/0.237/0.448; cloud>org2 9.394/9.770/12.567; cloud>org3 38.754/38.900/39.377; cloud>org4 44.162/44.267/44.494

| run | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|
| rows300000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |
| rows3000000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |

### data check

- data check move rows300000-body200-logical-batch10000: ok: 926779 rows in 5 organizations (424000 before, +502779: 502777 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
- data check move rows3000000-body200-logical-batch10000: ok: 4578864 rows in 5 organizations (4204000 before, +374864: 374862 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
