
## 2026-10-27-logical8d-move

- run: 2026-10-27-logical8d (move), LokaDB `4d38caa1cb4c54fc6444a58ca14d59a363305eeb dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.150/0.219/0.584; cloud>relay 0.137/0.176/0.253; cloud>load 0.184/0.266/0.486; cloud>org1 0.170/0.195/0.283; cloud>org2 9.401/9.549/10.540; cloud>org3 38.777/38.843/38.891; cloud>org4 43.426/43.520/44.014
- RTT ms min/avg/max, end: cloud>proxy 0.155/0.229/0.519; cloud>relay 0.154/0.182/0.203; cloud>load 0.167/0.222/0.364; cloud>org1 0.182/0.228/0.396; cloud>org2 9.439/9.496/9.585; cloud>org3 38.780/38.946/39.758; cloud>org4 43.454/43.515/43.769

| run | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|
| rows300000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |
| rows3000000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |

### data check

- data check move rows300000-body200-logical-batch10000: ok: 531550 rows in 5 organizations (424000 before, +107550: 107550 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
- data check move rows3000000-body200-logical-batch10000: ok: 4771908 rows in 5 organizations (4204000 before, +567908: 567907 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
