
## 2026-10-27-logical8c-move

- run: 2026-10-27-logical8c (move), LokaDB `4d38caa1cb4c54fc6444a58ca14d59a363305eeb dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.148/0.196/0.426; cloud>relay 0.142/0.162/0.187; cloud>load 0.173/0.248/0.609; cloud>org1 0.181/0.195/0.240; cloud>org2 9.379/9.413/9.453; cloud>org3 38.703/38.795/38.982; cloud>org4 43.396/43.507/43.954
- RTT ms min/avg/max, end: cloud>proxy 0.149/0.191/0.392; cloud>relay 0.144/0.172/0.203; cloud>load 0.146/0.218/0.525; cloud>org1 0.190/0.226/0.402; cloud>org2 9.379/9.489/9.633; cloud>org3 38.749/38.944/39.876; cloud>org4 43.425/43.496/43.632

| run | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|
| rows300000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |
| rows3000000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |

### data check

- data check move rows300000-body200-logical-batch10000: ok: 999425 rows in 5 organizations (424000 before, +575425: 575425 inserts ok, 1 failed), no rows of another organization in any database, none of the moved left in the cloud
- data check move rows3000000-body200-logical-batch10000: ok: 4618897 rows in 5 organizations (4204000 before, +414897: 414895 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
