
## 2026-10-27-sessions

- run: 2026-10-27 (sessions), LokaDB `9f405c2110073afc3851972b14336cc57f819141`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 1 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.145/0.224/0.515; cloud>relay 0.147/0.183/0.236; cloud>load 0.238/0.293/0.525; cloud>org1 0.169/0.221/0.376; cloud>org2 9.360/9.478/10.122; cloud>org3 38.709/38.770/38.830; cloud>org4 43.207/43.375/43.838
- RTT ms min/avg/max, end: cloud>proxy 0.132/0.164/0.281; cloud>relay 0.137/0.156/0.174; cloud>load 0.137/0.195/0.298; cloud>org1 0.154/0.194/0.226; cloud>org2 9.325/9.387/9.426; cloud>org3 38.761/38.915/39.313; cloud>org4 43.213/43.751/45.429

| clients | meta_cache | orgs | protocol | sessions | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | off | 1000 | extended | 10 | mix | - | - | - | - | - |  |  |
| 10 | off | 1000 | extended | 10 | read | 4 754 | 1.01 | 1.65 | 3.99 | 247.68 |  |  |
| 10 | off | 1000 | extended | 10 | update | 4 740 | 0.83 | 1.54 | 4.1 | 314.68 |  |  |
| 10 | off | 1000 | simple | 10 | mix | - | - | - | - | - |  |  |
| 10 | off | 1000 | simple | 10 | read | 3 804 | 1.44 | 1.98 | 2.72 | 215.41 |  |  |
| 10 | off | 1000 | simple | 10 | update | 3 793 | 1.03 | 1.59 | 2.64 | 257.47 |  |  |
| 10 | off | 3000 | extended | 10 | mix | - | - | - | - | - |  |  |
| 10 | off | 3000 | extended | 10 | read | 4 220 | 1.27 | 1.8 | 2.32 | 960.69 |  |  |
| 10 | off | 3000 | extended | 10 | update | 4 204 | 0.93 | 1.42 | 2.31 | 810.73 |  |  |
| 10 | off | 3000 | simple | 10 | mix | - | - | - | - | - |  |  |
| 10 | off | 3000 | simple | 10 | read | 3 292 | 1.66 | 2.35 | 3.01 | 1 127 |  |  |
| 10 | off | 3000 | simple | 10 | update | 3 279 | 1.16 | 1.79 | 2.84 | 998.16 |  |  |
| 10 | on | 1000 | extended | 10 | mix | - | - | - | - | - |  |  |
| 10 | on | 1000 | extended | 10 | read | 5 111 | 0.98 | 1.52 | 1.99 | 184.78 |  |  |
| 10 | on | 1000 | extended | 10 | update | 5 102 | 0.84 | 1.43 | 2.15 | 207.53 |  |  |
| 10 | on | 1000 | simple | 10 | mix | - | - | - | - | - |  |  |
| 10 | on | 1000 | simple | 10 | read | 3 662 | 1.44 | 2.14 | 4.82 | 189.69 |  |  |
| 10 | on | 1000 | simple | 10 | update | 3 651 | 1.02 | 1.66 | 5.1 | 23.06 |  |  |
| 10 | on | 3000 | extended | 10 | mix | - | - | - | - | - |  |  |
| 10 | on | 3000 | extended | 10 | read | 4 148 | 1.32 | 1.92 | 2.44 | 895.62 |  |  |
| 10 | on | 3000 | extended | 10 | update | 4 134 | 0.96 | 1.51 | 2.45 | 22.3 |  |  |
| 10 | on | 3000 | simple | 10 | mix | - | - | - | - | - |  |  |
| 10 | on | 3000 | simple | 10 | read | 3 283 | 1.6 | 2.63 | 5.91 | 14.86 |  |  |
| 10 | on | 3000 | simple | 10 | update | 3 271 | 1.09 | 1.97 | 5.75 | 692.55 |  |  |
| 100 | off | 1000 | extended | 100 | mix | - | - | - | - | - |  |  |
| 100 | off | 1000 | extended | 100 | read | 7 062 | 4.88 | 12.8 | 19.02 | 1 802 |  |  |
| 100 | off | 1000 | extended | 100 | update | 7 061 | 7.17 | 14.36 | 20.55 | 1 756 |  |  |
| 100 | off | 1000 | simple | 100 | mix | - | - | - | - | - |  |  |
| 100 | off | 1000 | simple | 100 | read | 5 902 | 7.15 | 17.1 | 23.83 | 2 389 |  |  |
| 100 | off | 1000 | simple | 100 | update | 5 896 | 7.25 | 15.13 | 21.77 | 2 340 |  |  |
| 100 | off | 3000 | extended | 100 | mix | - | - | - | - | - |  |  |
| 100 | off | 3000 | extended | 100 | read | 3 180 | 8.69 | 25.97 | 36.7 | 12 066 |  |  |
| 100 | off | 3000 | extended | 100 | update | 3 195 | 12.65 | 27.75 | 39.96 | 11 641 |  |  |
| 100 | off | 3000 | simple | 100 | mix | - | - | - | - | - |  |  |
| 100 | off | 3000 | simple | 100 | read | 2 728 | 10.22 | 28.69 | 39.31 | 12 569 |  |  |
| 100 | off | 3000 | simple | 100 | update | 2 742 | 14.43 | 29.53 | 40.05 | 12 603 |  |  |
| 100 | on | 1000 | extended | 100 | mix | - | - | - | - | - |  |  |
| 100 | on | 1000 | extended | 100 | read | 8 270 | 4.56 | 11.37 | 15.88 | 116.5 |  |  |
| 100 | on | 1000 | extended | 100 | update | 8 271 | 6.42 | 11.63 | 16.38 | 859.79 |  |  |
| 100 | on | 1000 | simple | 100 | mix | - | - | - | - | - |  |  |
| 100 | on | 1000 | simple | 100 | read | 6 373 | 7.71 | 16.21 | 22.26 | 589.04 |  |  |
| 100 | on | 1000 | simple | 100 | update | 6 373 | 6.56 | 12.82 | 18.2 | 54.91 |  |  |
| 100 | on | 3000 | extended | 100 | mix | - | - | - | - | - |  |  |
| 100 | on | 3000 | extended | 100 | read | 6 595 | 5.77 | 13.88 | 20.25 | 2 583 |  |  |
| 100 | on | 3000 | extended | 100 | update | 6 596 | 8.01 | 14.67 | 20.52 | 2 397 |  |  |
| 100 | on | 3000 | simple | 100 | mix | - | - | - | - | - |  |  |
| 100 | on | 3000 | simple | 100 | read | 5 857 | 7.4 | 17.52 | 24.1 | 2 823 |  |  |
| 100 | on | 3000 | simple | 100 | update | 5 857 | 7.9 | 15.19 | 21.27 | 56.58 |  |  |
