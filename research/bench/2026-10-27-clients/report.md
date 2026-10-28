
## 2026-10-27-clients

- run: 2026-10-27 (clients), LokaDB `9f405c2110073afc3851972b14336cc57f819141`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.155/0.198/0.464; cloud>relay 0.152/0.168/0.191; cloud>load 0.198/0.301/0.646; cloud>org1 0.144/0.333/1.429; cloud>org2 9.359/9.385/9.436; cloud>org3 38.712/38.772/38.912; cloud>org4 43.396/43.482/43.594
- RTT ms min/avg/max, end: cloud>proxy 0.147/0.190/0.382; cloud>relay 0.136/0.167/0.198; cloud>load 0.165/0.230/0.339; cloud>org1 0.170/0.208/0.335; cloud>org2 9.321/9.592/11.715; cloud>org3 38.732/38.871/39.511; cloud>org4 43.190/43.263/43.436

| class | clients | config | keys | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud | 1000 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 1000 | fdw | cold | read | 12 795 (12 615–12 974) | 16.99 | 60.45 | 96.57 | 782.86 |  |  |
| cloud | 1000 | fdw | cold | update | 12 813 (12 630–12 997) | 43.94 | 111.4 | 263.01 | 2 705 |  |  |
| cloud | 1000 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 1000 | plain | cold | read | 37 194 (36 637–37 751) | 4.32 | 15.74 | 31.16 | 639.1 |  |  |
| cloud | 1000 | plain | cold | update | 37 203 (36 653–37 753) | 17.34 | 49.54 | 83.32 | 668.35 |  |  |
| cloud | 200 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 200 | fdw | cold | read | 15 323 (15 100–15 546) | 3.84 | 13.17 | 21.41 | 120.2 |  |  |
| cloud | 200 | fdw | cold | update | 15 332 (15 111–15 552) | 7.17 | 15.67 | 25.88 | 150.09 |  |  |
| cloud | 200 | fdw | hot | mix | - | - | - | - | - |  |  |
| cloud | 200 | fdw | hot | read | 14 343 (12 715–15 970) | 3.92 | 11.91 | 19.65 | 162.27 |  |  |
| cloud | 200 | fdw | hot | update | 14 350 (12 725–15 975) | 8.06 | 18.65 | 28.08 | 174.76 |  |  |
| cloud | 200 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 200 | plain | cold | read | 41 570 (39 737–43 403) | 2.26 | 3.64 | 6.48 | 103.07 |  |  |
| cloud | 200 | plain | cold | update | 41 592 (39 761–43 423) | 1.98 | 4.59 | 9.64 | 118.11 |  |  |
| cloud | 200 | plain | hot | mix | - | - | - | - | - |  |  |
| cloud | 200 | plain | hot | read | 42 338 (41 659–43 017) | 2.16 | 3.24 | 4.73 | 38.69 |  |  |
| cloud | 200 | plain | hot | update | 42 358 (41 675–43 040) | 2.12 | 4.72 | 8.59 | 109.66 |  |  |
| cloud | 500 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 500 | fdw | cold | read | 13 635 (13 540–13 730) | 9.48 | 30.54 | 50.86 | 358.23 |  |  |
| cloud | 500 | fdw | cold | update | 13 651 (13 560–13 741) | 21.2 | 47.81 | 83.7 | 735.71 |  |  |
| cloud | 500 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 500 | plain | cold | read | 41 575 (40 781–42 369) | 2.93 | 9.74 | 19.66 | 214.25 |  |  |
| cloud | 500 | plain | cold | update | 41 520 (40 726–42 313) | 6.99 | 18.13 | 28.33 | 320.7 |  |  |
| moved | 1000 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 1000 | fdw | cold | read | 1 019 (1 017–1 022) | 387.34 | 583.2 | 661.98 | 2 487 | 575475 | ERROR: could not connect to server "lokadb_org1" (SQLSTATE 08001) |
| moved | 1000 | fdw | cold | update | 1 025 (1 020–1 029) | 313.19 | 465.1 | 544.42 | 2 403 | 577070 | ERROR: could not connect to server "lokadb_org2" (SQLSTATE 08001) |
| moved | 1000 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 1000 | plain | cold | read | 33 024 (30 933–35 116) | 4.42 | 19.01 | 36.53 | 578.21 |  |  |
| moved | 1000 | plain | cold | update | 33 045 (30 955–35 135) | 19.97 | 56.56 | 90.76 | 602.58 |  |  |
| moved | 200 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 200 | fdw | cold | read | 447 (447–448) | 109.41 | 446.01 | 456.87 | 572.17 |  |  |
| moved | 200 | fdw | cold | update | 453 (452–454) | 348.38 | 357.05 | 365.55 | 469.96 | 32 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 200 | fdw | hot | mix | - | - | - | - | - |  |  |
| moved | 200 | fdw | hot | read | 455 (454–456) | 106.02 | 444.66 | 449.8 | 793.67 |  |  |
| moved | 200 | fdw | hot | update | 359 (357–360) | 77.94 | 355.9 | 359.45 | 709.37 | 12321 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 200 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 200 | plain | cold | read | 40 598 (40 234–40 962) | 1.86 | 3.42 | 6.45 | 67.75 |  |  |
| moved | 200 | plain | cold | update | 40 620 (40 254–40 985) | 2.46 | 5.52 | 10.81 | 82.17 |  |  |
| moved | 200 | plain | hot | mix | - | - | - | - | - |  |  |
| moved | 200 | plain | hot | read | 40 651 (38 704–42 597) | 1.79 | 3.18 | 4.63 | 68.88 |  |  |
| moved | 200 | plain | hot | update | 40 665 (38 712–42 617) | 2.46 | 6.27 | 11.69 | 85.07 |  |  |
| moved | 500 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 500 | fdw | cold | read | 1 071 (1 050–1 093) | 396.34 | 478.79 | 562.23 | 1 127 |  |  |
| moved | 500 | fdw | cold | update | 1 085 (1 062–1 107) | 349.9 | 386.78 | 474.64 | 1 067 | 284 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 500 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 500 | plain | cold | read | 34 939 (33 062–36 815) | 2.91 | 9.46 | 17.87 | 204 |  |  |
| moved | 500 | plain | cold | update | 34 921 (33 055–36 788) | 9.59 | 21.56 | 32.03 | 247.49 |  |  |
