
## 2026-10-27-misc

- run: 2026-10-27 (misc), LokaDB `f7983a30fde2e6654f2f75faecece71650bd6ff7 dirty`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.155/3.238/30.670; cloud>relay 0.140/0.182/0.242; cloud>load 0.175/0.250/0.537; cloud>org1 0.163/0.206/0.297; cloud>org2 9.353/9.479/10.153; cloud>org3 38.727/38.809/38.955; cloud>org4 43.143/43.281/43.817
- RTT ms min/avg/max, end: cloud>proxy 0.147/0.179/0.240; cloud>relay 0.150/0.171/0.188; cloud>load 0.175/0.216/0.380; cloud>org1 0.161/0.217/0.362; cloud>org2 9.334/9.536/10.302; cloud>org3 38.738/39.130/40.165; cloud>org4 43.424/43.912/46.213

| clients | config | fetch | rows | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  | fdw-cloud-org |  |  | copy_in | 2 (2–3) | 429.25 | 429.25 | 429.25 | 429.25 |  |  |
|  | fdw-cloud-org |  |  | copy_out_query | 2 (2–3) | 412.98 | 412.98 | 412.98 | 412.98 |  |  |
|  | fdw-cloud-org |  |  | copy_out_table | 0 | 0 | 0 | 0 | 0 | 2 | ERROR: lokadb: the statement does not name the organization of document |
|  | fdw-cloud-org |  |  | notify | 200 | 0.57 | 1.75 | 2.43 | 4.39 |  |  |
|  | fdw-cloud-org | 100 |  | cursor_fetch | 9 603 | 0.33 | 0.4 | 0.51 | 4.55 |  |  |
|  | fdw-cloud-org | 100 |  | cursor_total | 3 | 1 069 | 1 069 | 1 069 | 1 137 |  |  |
|  | fdw-cloud-org | 1000 |  | cursor_fetch | 963 | 2.67 | 2.92 | 3.45 | 6.55 |  |  |
|  | fdw-cloud-org | 1000 |  | cursor_total | 3 | 871.59 | 871.59 | 871.59 | 873.82 |  |  |
|  | fdw-cloud-org | 10000 |  | cursor_fetch | 99 | 20.38 | 23.86 | 27.69 | 29.37 |  |  |
|  | fdw-cloud-org | 10000 |  | cursor_total | 3 | 633.27 | 633.27 | 633.27 | 693.56 |  |  |
|  | fdw-moved-org |  |  | copy_in | 0 (0–0) | 3 769 | 3 769 | 3 769 | 3 769 |  |  |
|  | fdw-moved-org |  |  | copy_out_query | 1 (1–1) | 928.72 | 928.72 | 928.72 | 928.72 |  |  |
|  | fdw-moved-org |  |  | copy_out_table | 0 | 0 | 0 | 0 | 0 | 2 | ERROR: lokadb: the statement does not name the organization of document |
|  | fdw-moved-org |  |  | notify | 200 | 0.39 | 0.56 | 0.73 | 1.49 |  |  |
|  | fdw-moved-org | 100 |  | cursor_fetch | 9 603 | 0.34 | 2.48 | 2.72 | 10.04 |  |  |
|  | fdw-moved-org | 100 |  | cursor_total | 3 | 1 817 | 1 817 | 1 817 | 1 831 |  |  |
|  | fdw-moved-org | 1000 |  | cursor_fetch | 963 | 5 | 5.59 | 6.49 | 13.34 |  |  |
|  | fdw-moved-org | 1000 |  | cursor_total | 3 | 1 611 | 1 611 | 1 611 | 1 625 |  |  |
|  | fdw-moved-org | 10000 |  | cursor_fetch | 99 | 43.42 | 46.81 | 51.04 | 54.93 |  |  |
|  | fdw-moved-org | 10000 |  | cursor_total | 3 | 1 376 | 1 376 | 1 376 | 1 436 |  |  |
|  | plain |  |  | copy_in | 24 (22–26) | 42.03 | 42.03 | 42.03 | 42.03 |  |  |
|  | plain |  |  | copy_out_query | 4 (4–4) | 271.25 | 271.25 | 271.25 | 271.25 |  |  |
|  | plain |  |  | copy_out_table | 0 (0–0) | 5 454 | 5 454 | 5 454 | 5 454 |  |  |
|  | plain |  |  | notify | 200 | 0.3 | 0.45 | 0.67 | 2.41 |  |  |
|  | plain | 100 |  | cursor_fetch | 9 603 | 0.18 | 0.22 | 0.27 | 3.06 |  |  |
|  | plain | 100 |  | cursor_total | 3 | 597.9 | 597.9 | 597.9 | 606.8 |  |  |
|  | plain | 1000 |  | cursor_fetch | 963 | 1.57 | 1.68 | 2.04 | 3.46 |  |  |
|  | plain | 1000 |  | cursor_total | 3 | 487.72 | 487.72 | 487.72 | 491.33 |  |  |
|  | plain | 10000 |  | cursor_fetch | 99 | 15.25 | 16.08 | 17.5 | 22.22 |  |  |
|  | plain | 10000 |  | cursor_total | 3 | 492.19 | 492.19 | 492.19 | 495.53 |  |  |
| 1 | fdw+direct-moved-org |  | 300000 | feed | 1 606 (1 550–1 662) | 0.6 | 0.75 | 1.42 | 19.19 |  |  |
| 1 | fdw+direct-moved-org |  | 300000 | read | 728 (710–746) | 1.32 | 1.64 | 2.82 | 38.37 |  |  |
| 1 | fdw+direct-moved-org |  | 300000 | update | 1 339 (1 321–1 358) | 0.72 | 0.86 | 1.23 | 17.69 |  |  |
| 1 | fdw-cloud-org |  | 300000 | feed | 3 602 (3 473–3 731) | 0.26 | 0.34 | 0.65 | 16.36 |  |  |
| 1 | fdw-cloud-org |  | 300000 | read | 1 801 (1 687–1 915) | 0.54 | 0.68 | 0.88 | 17.23 |  |  |
| 1 | fdw-cloud-org |  | 300000 | update | 2 057 (2 041–2 074) | 0.47 | 0.59 | 0.91 | 17.76 |  |  |
| 1 | fdw-moved-org |  | 300000 | feed | 519 (517–521) | 1.85 | 2.26 | 3.66 | 40.67 |  |  |
| 1 | fdw-moved-org |  | 300000 | read | 257 (256–258) | 3.8 | 4.38 | 6.93 | 21.08 |  |  |
| 1 | fdw-moved-org |  | 300000 | update | 323 (306–340) | 2.94 | 3.52 | 7.83 | 21.76 |  |  |
| 1 | plain |  | 300000 | feed | 7 031 (6 941–7 120) | 0.13 | 0.19 | 0.27 | 6.85 |  |  |
| 1 | plain |  | 300000 | read | 3 545 (3 374–3 716) | 0.27 | 0.35 | 0.72 | 7.61 |  |  |
| 1 | plain |  | 300000 | update | 3 503 (3 418–3 588) | 0.26 | 0.37 | 0.81 | 8.24 |  |  |
| 8 | fdw+direct-moved-org |  | 300000 | feed | 11 859 (11 670–12 048) | 0.64 | 0.84 | 1.33 | 18.56 |  |  |
| 8 | fdw+direct-moved-org |  | 300000 | read | 5 452 (5 444–5 460) | 1.42 | 1.81 | 2.77 | 18.3 |  |  |
| 8 | fdw+direct-moved-org |  | 300000 | update | 8 269 (8 047–8 492) | 0.9 | 1.25 | 2.92 | 22.09 |  |  |
| 8 | fdw-cloud-org |  | 300000 | feed | 25 434 (24 628–26 240) | 0.29 | 0.41 | 0.83 | 21.64 |  |  |
| 8 | fdw-cloud-org |  | 300000 | read | 13 996 (13 504–14 487) | 0.53 | 0.72 | 1.7 | 21.73 |  |  |
| 8 | fdw-cloud-org |  | 300000 | update | 13 495 (13 420–13 569) | 0.56 | 0.79 | 1.24 | 21.75 |  |  |
| 8 | fdw-moved-org |  | 300000 | feed | 3 257 (3 230–3 285) | 2.32 | 3.01 | 6.46 | 68.09 |  |  |
| 8 | fdw-moved-org |  | 300000 | read | 1 714 (1 707–1 722) | 4.53 | 5.4 | 8.4 | 25.92 |  |  |
| 8 | fdw-moved-org |  | 300000 | update | 1 945 (1 894–1 996) | 3.89 | 4.9 | 11.12 | 27.76 | 6 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| 8 | plain |  | 300000 | feed | 35 104 (35 098–35 109) | 0.22 | 0.26 | 0.33 | 8.07 |  |  |
| 8 | plain |  | 300000 | read | 27 615 (27 391–27 839) | 0.27 | 0.38 | 0.5 | 8.51 |  |  |
| 8 | plain |  | 300000 | update | 16 994 (16 628–17 360) | 0.44 | 0.62 | 1.57 | 10.66 |  |  |

### data check

- data check misc: ok: 640000 rows in 2 organizations (600000 before, +40000: 40000 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
