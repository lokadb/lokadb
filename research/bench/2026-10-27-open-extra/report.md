
## 2026-10-27-open-extra

- run: 2026-10-27-open (extra), LokaDB `4d38caa1cb4c54fc6444a58ca14d59a363305eeb dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.160/0.199/0.357; cloud>relay 0.156/0.287/1.273; cloud>load 0.165/0.222/0.421; cloud>org1 0.167/0.248/0.505; cloud>org2 9.410/9.474/9.585; cloud>org3 38.783/40.226/50.084; cloud>org4 43.428/43.547/43.704
- RTT ms min/avg/max, end: cloud>proxy 0.154/0.215/0.590; cloud>relay 0.156/0.233/0.714; cloud>load 0.172/0.241/0.349; cloud>org1 0.191/0.253/0.609; cloud>org2 9.417/9.537/9.626; cloud>org3 38.860/39.168/40.038; cloud>org4 43.455/43.540/43.674

| class | clients | config | keys | protocol | rate | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud | 256 | fdw | cold | extended | 17500 | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 17500 | read | 8 704 (8 669–8 740) | 1.1 | 389.42 | 517.06 | 557.08 |  |  |
| cloud | 256 | fdw | cold | extended | 17500 | timeout | 0 | 0 | 0 | 0 | 0 | 8650 | timeout: not started within the timeout (all workers busy) |
| cloud | 256 | fdw | cold | extended | 17500 | update | 8 724 (8 688–8 760) | 1.16 | 389.81 | 520.13 | 589.95 |  |  |
| cloud | 256 | fdw | cold | extended | 20000 | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 20000 | read | 9 906 (9 815–9 997) | 1.58 | 415.56 | 536.42 | 608.04 |  |  |
| cloud | 256 | fdw | cold | extended | 20000 | timeout | 0 | 0 | 0 | 0 | 0 | 21735 | timeout: not started within the timeout (all workers busy) |
| cloud | 256 | fdw | cold | extended | 20000 | update | 9 913 (9 822–10 003) | 1.59 | 418.18 | 539.03 | 624.08 |  |  |
| cloud | 256 | fdw | cold | extended | 22500 | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 22500 | read | 11 193 (11 139–11 247) | 6.47 | 675.15 | 718.48 | 787.22 |  |  |
| cloud | 256 | fdw | cold | extended | 22500 | timeout | 0 | 0 | 0 | 0 | 0 | 12956 | timeout: not started within the timeout (all workers busy) |
| cloud | 256 | fdw | cold | extended | 22500 | update | 11 199 (11 146–11 253) | 10.1 | 681.74 | 726.01 | 917.32 |  |  |
| cloud | 256 | fdw | cold | extended | 25000 | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 25000 | read | 12 052 (11 715–12 389) | 339.74 | 1 009 | 1 019 | 1 092 |  |  |
| cloud | 256 | fdw | cold | extended | 25000 | timeout | 0 | 0 | 0 | 0 | 0 | 105390 | timeout: not started within the timeout (all workers busy) |
| cloud | 256 | fdw | cold | extended | 25000 | update | 12 070 (11 730–12 409) | 344.98 | 1 015 | 1 026 | 1 214 |  |  |
| cloud | 256 | plain | cold | extended | 25000 | mix | - | - | - | - | - |  |  |
| cloud | 256 | plain | cold | extended | 25000 | read | 12 493 (12 492–12 493) | 0.42 | 260.1 | 428.93 | 504.07 |  |  |
| cloud | 256 | plain | cold | extended | 25000 | update | 12 507 (12 507–12 508) | 0.59 | 259.97 | 431.73 | 468.24 |  |  |

### data check

- data check extra (open model): ok: 400000 rows in 20 organizations (400000 before, +0: 0 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
