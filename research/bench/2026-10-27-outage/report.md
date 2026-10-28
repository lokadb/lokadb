
## 2026-10-27-outage

- run: 2026-10-27 (outage), LokaDB `f7983a30fde2e6654f2f75faecece71650bd6ff7 dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.162/0.209/0.438; cloud>relay 0.165/0.189/0.212; cloud>load 0.186/0.246/0.507; cloud>org1 0.183/0.226/0.295; cloud>org2 9.354/9.535/10.362; cloud>org3 38.710/39.136/41.023; cloud>org4 43.430/43.867/47.553
- RTT ms min/avg/max, end: cloud>proxy 0.164/0.202/0.305; cloud>relay 0.139/0.164/0.203; cloud>load 0.140/0.207/0.312; cloud>org1 0.206/0.265/0.493; cloud>org2 9.392/9.459/9.620; cloud>org3 38.804/39.097/40.004; cloud>org4 43.549/43.709/44.191

| outage | class | seen as down | errors before | during: ok / errors | errors after | mean ms before | mean ms during | time to the error ms |
|---|---|---|---|---|---|---|---|---|
| agent_down | cloud |  | 0 | 8746 / 0 | 0 | 0.66 | 0.7 |  |
| agent_down | moved1 | 59-75 s | 26 | 0 / 605 | 0 | 4 | - | 1.02 |
| agent_down | moved2 |  | 0 | 582 / 0 | 0 | 91.11 | 89.85 |  |
| agent_down | moved3 |  | 0 | 549 / 0 | 0 | 401.78 | 396.49 |  |
| agent_down | moved4 |  | 1 | 596 / 0 | 0 | 398.11 | 393.42 |  |
| db_down | cloud |  | 0 | 19761 / 0 | 0 | 0.64 | 0.58 |  |
| db_down | moved1 | 22-56 s | 17 | 0 / 1174 | 0 | 3.92 | - | 1.19 |
| db_down | moved2 |  | 0 | 1249 / 0 | 0 | 90.39 | 89.78 |  |
| db_down | moved3 |  | 0 | 1168 / 0 | 0 | 396.34 | 400.14 |  |
| db_down | moved4 |  | 1 | 1256 / 0 | 0 | 393.45 | 398.83 |  |

### data check

- data check outage: ok: 400000 rows in 20 organizations (400000 before, +0: 0 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
