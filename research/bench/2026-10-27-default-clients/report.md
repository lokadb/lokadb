
## 2026-10-27-default-clients

- run: 2026-10-27-default (clients), LokaDB `f7983a30fde2e6654f2f75faecece71650bd6ff7 dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.145/0.175/0.254; cloud>relay 0.152/0.173/0.230; cloud>load 0.144/0.204/0.457; cloud>org1 0.174/0.211/0.285; cloud>org2 9.339/9.416/9.504; cloud>org3 38.734/38.807/39.111; cloud>org4 43.406/43.532/44.236
- RTT ms min/avg/max, end: cloud>proxy 0.141/0.173/0.364; cloud>relay 0.129/0.142/0.169; cloud>load 0.177/0.208/0.322; cloud>org1 0.145/0.218/0.501; cloud>org2 9.342/9.406/9.493; cloud>org3 38.811/40.012/50.027; cloud>org4 44.086/44.153/44.353

| class | clients | config | keys | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud | 1000 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 1000 | fdw | cold | read | 12 026 (11 918–12 134) | 17.61 | 59.15 | 98.6 | 812.49 |  |  |
| cloud | 1000 | fdw | cold | update | 12 049 (11 941–12 157) | 47.3 | 125.12 | 302.51 | 2 210 |  |  |
| cloud | 1000 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 1000 | plain | cold | read | 34 814 (34 729–34 900) | 4.3 | 19.52 | 33.1 | 643.44 |  |  |
| cloud | 1000 | plain | cold | update | 34 827 (34 744–34 910) | 18.71 | 52.43 | 85.73 | 878.45 |  |  |
| cloud | 500 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 500 | fdw | cold | read | 13 453 (13 441–13 465) | 9.77 | 31.03 | 49.26 | 383.08 |  |  |
| cloud | 500 | fdw | cold | update | 13 472 (13 466–13 477) | 21.6 | 48.03 | 80.71 | 674.87 |  |  |
| cloud | 500 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 500 | plain | cold | read | 42 784 (42 521–43 046) | 3.78 | 9.26 | 15.93 | 386.03 |  |  |
| cloud | 500 | plain | cold | update | 42 720 (42 460–42 981) | 6.13 | 16.2 | 25.56 | 468.38 |  |  |
| moved | 1000 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 1000 | fdw | cold | read | 1 025 (1 009–1 040) | 443.38 | 624.44 | 744.93 | 2 579 | 562657 | ERROR: could not connect to server "lokadb_org1" (SQLSTATE 08001) |
| moved | 1000 | fdw | cold | update | 1 032 (1 014–1 049) | 393.76 | 520.32 | 660.68 | 2 523 | 564628 | ERROR: could not connect to server "lokadb_org1" (SQLSTATE 08001) |
| moved | 1000 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 1000 | plain | cold | read | 35 259 (34 574–35 944) | 4.25 | 18.36 | 35.5 | 352.17 |  |  |
| moved | 1000 | plain | cold | update | 35 270 (34 582–35 958) | 18.32 | 51.73 | 84.59 | 586.09 |  |  |
| moved | 500 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 500 | fdw | cold | read | 1 035 (1 025–1 045) | 358.62 | 499.39 | 557.14 | 1 415 |  |  |
| moved | 500 | fdw | cold | update | 1 048 (1 038–1 059) | 353.67 | 414.43 | 478.51 | 1 344 | 267 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 500 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 500 | plain | cold | read | 37 687 (33 160–42 214) | 3.13 | 9.74 | 16.44 | 238.85 |  |  |
| moved | 500 | plain | cold | update | 37 653 (33 150–42 157) | 8.29 | 20.46 | 31.34 | 289.62 |  |  |
