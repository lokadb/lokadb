
## 2026-10-27-logical8again-move

- run: 2026-10-27-logical8again (move), LokaDB `4d38caa1cb4c54fc6444a58ca14d59a363305eeb dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.126/0.233/0.737; cloud>relay 0.131/0.408/2.683; cloud>load 0.155/0.198/0.306; cloud>org1 0.161/0.198/0.303; cloud>org2 9.369/9.984/15.097; cloud>org3 38.703/38.905/39.711; cloud>org4 44.100/44.256/44.718
- RTT ms min/avg/max, end: cloud>proxy 0.154/0.379/1.916; cloud>relay 0.153/0.174/0.254; cloud>load 0.173/0.382/1.756; cloud>org1 0.152/0.215/0.334; cloud>org2 9.386/9.446/9.564; cloud>org3 38.728/38.954/39.989; cloud>org4 44.112/44.241/44.821

| run | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|
| rows300000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |
| rows3000000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |

### data check

- data check move rows300000-body200-logical-batch10000: ok: 1052673 rows in 5 organizations (424000 before, +628673: 628671 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
- data check move rows3000000-body200-logical-batch10000: ok: 4433701 rows in 5 organizations (4204000 before, +229701: 229700 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
