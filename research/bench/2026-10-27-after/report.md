
## 2026-10-27-after

- run: 2026-10-27 (after), LokaDB `9f405c2110073afc3851972b14336cc57f819141`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- org5: 4 CPU, 7939 MB, disk  80G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.145/0.196/0.369; cloud>relay 0.164/0.182/0.241; cloud>load 0.159/0.226/0.424; cloud>org1 0.186/0.235/0.475; cloud>org2 9.394/9.455/9.506; cloud>org3 38.724/38.791/38.875; cloud>org4 43.406/43.477/43.671; cloud>org5 48.309/48.625/50.224
- RTT ms min/avg/max, end: cloud>proxy 0.192/0.317/0.933; cloud>relay 0.245/0.536/1.373; cloud>load 0.237/0.340/0.756; cloud>org1 0.210/0.266/0.462; cloud>org2 9.445/9.707/11.254; cloud>org3 38.782/38.835/38.916; cloud>org4 43.465/43.560/43.831; cloud>org5 49.233/49.782/54.217

| class | clients | config | keys | protocol | reconnect | shape | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud | 1 | fdw | cold | extended |  |  | feed | 3 386 (3 348–3 424) | 0.29 | 0.35 | 0.45 | 17.61 |  |  |
| cloud | 1 | fdw | cold | extended |  |  | insert | 556 (530–581) | 1.77 | 2.19 | 3.12 | 17.61 |  |  |
| cloud | 1 | fdw | cold | extended |  |  | read | 1 589 (1 480–1 698) | 0.61 | 0.77 | 1.46 | 14.12 |  |  |
| cloud | 1 | fdw | cold | extended |  |  | update | 1 975 (1 774–2 175) | 0.46 | 0.72 | 1.41 | 21.1 |  |  |
| cloud | 1 | plain | cold | extended |  |  | feed | 7 477 (7 390–7 564) | 0.12 | 0.19 | 0.28 | 5.62 |  |  |
| cloud | 16 | lokadb-direct | cold | extended | 1 |  | read | 1 015 (968–1 061) | 15.39 | 21.4 | 26.42 | 83.15 |  |  |
| cloud | 16 | fdw | cold | extended | 1 |  | read | 821 (819–823) | 19.21 | 24.68 | 27.68 | 57.13 |  |  |
| cloud | 16 | fdw+upstream_pool | cold | extended | 1 |  | read | 2 795 (2 717–2 872) | 5.47 | 7.18 | 13.8 | 37.49 |  |  |
| cloud | 16 | plain | cold | extended | 1 |  | read | 1 321 (1 295–1 347) | 11.79 | 17 | 20.04 | 37.96 |  |  |
| cloud | 16 | proxy | cold | extended | 1 |  | read | 874 (870–879) | 18.03 | 23.33 | 26.28 | 52.2 |  |  |
| cloud | 32 | fdw | cold | extended |  |  | feed | 28 375 (27 772–28 977) | 1.08 | 1.25 | 2.53 | 46.75 |  |  |
| cloud | 32 | fdw | cold | extended |  |  | insert | 8 612 (8 597–8 628) | 3.43 | 5.18 | 11.21 | 48.92 |  |  |
| cloud | 32 | fdw | cold | extended |  |  | read | 29 799 (28 771–30 827) | 0.94 | 1.73 | 3.75 | 39.66 |  |  |
| cloud | 32 | fdw | cold | extended |  |  | update | 33 880 (33 688–34 072) | 0.88 | 1.35 | 2.11 | 39.63 |  |  |
| cloud | 32 | plain | cold | extended |  |  | feed | 35 526 (35 520–35 533) | 0.89 | 0.93 | 1.05 | 7.84 |  |  |
| moved |  | direct |  |  |  | tx | connect | 300 | 0.86 | 1.06 | 2.96 | 25.75 |  |  |
| moved |  | direct |  |  |  | tx | first_query | 300 | 7.38 | 8.75 | 10.12 | 58.99 |  |  |
| moved | 16 | fdw | cold | extended | 1 |  | read | 28 (28–28) | 827.19 | 900.7 | 915.25 | 975.43 |  |  |
| moved | 16 | fdw+upstream_pool | cold | extended | 1 |  | read | 53 (53–53) | 451.59 | 492.2 | 499.24 | 733 |  |  |
