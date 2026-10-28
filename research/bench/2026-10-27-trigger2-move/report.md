
## 2026-10-27-trigger2-move

- run: 2026-10-27-trigger2 (move), LokaDB `f7983a30fde2e6654f2f75faecece71650bd6ff7 dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.159/0.190/0.327; cloud>relay 0.140/0.477/2.862; cloud>load 0.151/0.250/0.492; cloud>org1 0.177/0.230/0.454; cloud>org2 9.354/9.778/12.370; cloud>org3 38.706/38.809/39.108; cloud>org4 44.147/44.235/44.573
- RTT ms min/avg/max, end: cloud>proxy 0.201/0.237/0.351; cloud>relay 0.183/0.271/0.668; cloud>load 0.199/0.250/0.302; cloud>org1 0.238/0.307/0.591; cloud>org2 9.442/9.495/9.564; cloud>org3 38.770/38.984/39.581; cloud>org4 43.470/43.592/43.786

| run | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|
| rows300000-body200-trigger-batch10000 |  | - | - | - | - | - |  |  |
| rows3000000-body200-trigger-batch10000 |  | - | - | - | - | - |  |  |

### data check

- data check move rows300000-body200-trigger-batch10000: ok: 504984 rows in 5 organizations (424000 before, +80984: 80984 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
- data check move rows3000000-body200-trigger-batch10000: ok: 4541103 rows in 5 organizations (4204000 before, +337103: 337103 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
