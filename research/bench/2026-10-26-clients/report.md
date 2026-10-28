
## 2026-10-26-clients

- run: 2026-10-26 (clients), LokaDB `c9e50325dff6ef2b8cd9cd30c9771203728d34b1`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 60s, 3 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- org5: 4 CPU, 7939 MB, disk  80G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.174/0.242/0.586; cloud>relay 0.151/0.205/0.341; cloud>load 0.182/0.307/0.871; cloud>org1 0.183/0.224/0.379; cloud>org2 9.425/9.531/9.657; cloud>org3 38.756/38.920/39.646; cloud>org4 43.238/43.363/43.610; cloud>org5 49.215/49.365/50.106
- RTT ms min/avg/max, end: cloud>proxy 0.149/0.190/0.309; cloud>relay 0.156/0.204/0.233; cloud>load 0.212/0.255/0.475; cloud>org1 0.180/0.241/0.553; cloud>org2 9.402/9.535/9.753; cloud>org3 38.772/39.019/39.903; cloud>org4 44.108/44.409/45.125; cloud>org5 49.186/49.429/50.396

| class | clients | config | keys | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud | 1 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 1 | fdw | cold | read | 630 (603–657) | 0.94 | 1.09 | 1.43 | 12.17 |  |  |
| cloud | 1 | fdw | cold | update | 628 (601–654) | 0.6 | 0.75 | 1.34 | 12.52 |  |  |
| cloud | 1 | fdw | hot | mix | - | - | - | - | - |  |  |
| cloud | 1 | fdw | hot | read | 749 (730–769) | 0.71 | 0.87 | 1.1 | 14.29 |  |  |
| cloud | 1 | fdw | hot | update | 753 (732–774) | 0.57 | 0.77 | 1.36 | 9.81 |  |  |
| cloud | 1 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 1 | plain | cold | read | 1 496 (1 482–1 511) | 0.3 | 0.37 | 0.48 | 4.92 |  |  |
| cloud | 1 | plain | cold | update | 1 505 (1 490–1 521) | 0.33 | 0.44 | 0.82 | 8.16 |  |  |
| cloud | 1 | plain | hot | mix | - | - | - | - | - |  |  |
| cloud | 1 | plain | hot | read | 1 826 (1 820–1 832) | 0.25 | 0.31 | 0.39 | 6.08 |  |  |
| cloud | 1 | plain | hot | update | 1 834 (1 829–1 839) | 0.26 | 0.37 | 0.54 | 7.72 |  |  |
| cloud | 10 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 10 | fdw | cold | read | 5 629 (5 536–5 721) | 0.99 | 1.34 | 2.38 | 22.37 |  |  |
| cloud | 10 | fdw | cold | update | 5 620 (5 527–5 713) | 0.7 | 0.97 | 1.98 | 13.73 |  |  |
| cloud | 10 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 10 | plain | cold | read | 12 745 (12 717–12 772) | 0.3 | 0.42 | 0.53 | 7.54 |  |  |
| cloud | 10 | plain | cold | update | 12 722 (12 695–12 748) | 0.44 | 0.62 | 0.97 | 10.8 |  |  |
| cloud | 100 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 100 | fdw | cold | read | 9 875 (9 713–10 038) | 6.13 | 11.98 | 16.12 | 50.78 | 1 | ERROR: lokadb: the organization of this statement is found in the refs  |
| cloud | 100 | fdw | cold | update | 9 869 (9 705–10 033) | 3.06 | 7.12 | 11.71 | 56.37 |  |  |
| cloud | 100 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 100 | plain | cold | read | 41 667 (41 246–42 088) | 1.04 | 1.63 | 2.27 | 29.83 |  |  |
| cloud | 100 | plain | cold | update | 41 729 (41 312–42 147) | 1.15 | 2.07 | 3.17 | 49.54 |  |  |
| cloud | 50 | fdw | cold | mix | - | - | - | - | - |  |  |
| cloud | 50 | fdw | cold | read | 9 993 (9 945–10 042) | 2.84 | 5.2 | 7.81 | 37.93 |  |  |
| cloud | 50 | fdw | cold | update | 9 971 (9 922–10 021) | 1.74 | 3.31 | 5.92 | 43.1 |  |  |
| cloud | 50 | fdw | hot | mix | - | - | - | - | - |  |  |
| cloud | 50 | fdw | hot | read | 10 215 (10 137–10 294) | 2.73 | 5.45 | 7.25 | 37.59 |  |  |
| cloud | 50 | fdw | hot | update | 10 188 (10 113–10 263) | 1.69 | 3.73 | 5.64 | 31.07 |  |  |
| cloud | 50 | plain | cold | mix | - | - | - | - | - |  |  |
| cloud | 50 | plain | cold | read | 33 496 (32 938–34 054) | 0.57 | 0.95 | 1.35 | 14.63 |  |  |
| cloud | 50 | plain | cold | update | 33 527 (32 965–34 088) | 0.78 | 1.31 | 2.2 | 31.46 |  |  |
| cloud | 50 | plain | hot | mix | - | - | - | - | - |  |  |
| cloud | 50 | plain | hot | read | 33 891 (33 607–34 176) | 0.56 | 0.94 | 1.34 | 12.27 |  |  |
| cloud | 50 | plain | hot | update | 33 925 (33 643–34 207) | 0.77 | 1.3 | 2.11 | 49.99 |  |  |
| moved | 1 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 1 | fdw | cold | read | 2 | 440.48 | 503.25 | 504.65 | 507.76 |  |  |
| moved | 1 | fdw | cold | update | 2 (2–2) | 352.95 | 402.94 | 404.14 | 406.87 |  |  |
| moved | 1 | fdw | hot | mix | - | - | - | - | - |  |  |
| moved | 1 | fdw | hot | read | 2 (2–2) | 439.87 | 503.94 | 505.04 | 505.74 |  |  |
| moved | 1 | fdw | hot | update | 2 (2–2) | 352.65 | 403.73 | 408.53 | 616.46 |  |  |
| moved | 1 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 1 | plain | cold | read | 1 665 (1 658–1 671) | 0.27 | 0.35 | 0.48 | 5.1 |  |  |
| moved | 1 | plain | cold | update | 1 675 (1 668–1 682) | 0.28 | 0.46 | 0.82 | 7.61 |  |  |
| moved | 1 | plain | hot | mix | - | - | - | - | - |  |  |
| moved | 1 | plain | hot | read | 1 647 (1 591–1 703) | 0.28 | 0.36 | 0.51 | 5.99 |  |  |
| moved | 1 | plain | hot | update | 1 657 (1 599–1 715) | 0.29 | 0.43 | 0.7 | 7.94 |  |  |
| moved | 10 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 10 | fdw | cold | read | 18 (18–18) | 437.28 | 504.94 | 552.64 | 619.52 |  |  |
| moved | 10 | fdw | cold | update | 20 (20–20) | 350.43 | 402.38 | 436.28 | 496.35 |  |  |
| moved | 10 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 10 | plain | cold | read | 11 481 (11 083–11 879) | 0.32 | 0.48 | 0.93 | 9.39 |  |  |
| moved | 10 | plain | cold | update | 11 470 (11 073–11 866) | 0.47 | 0.79 | 1.9 | 15.53 |  |  |
| moved | 100 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 100 | fdw | cold | read | 160 (158–161) | 482.15 | 638.08 | 699.68 | 830.54 |  |  |
| moved | 100 | fdw | cold | update | 160 (159–162) | 384.76 | 518.41 | 576.67 | 759.91 | 9 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 100 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 100 | plain | cold | read | 41 043 (39 558–42 529) | 1.06 | 1.74 | 3.38 | 23.68 |  |  |
| moved | 100 | plain | cold | update | 41 099 (39 611–42 587) | 1.13 | 2.11 | 4.22 | 36.37 |  |  |
| moved | 200 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 50 | fdw | cold | mix | - | - | - | - | - |  |  |
| moved | 50 | fdw | cold | read | 90 (89–91) | 439.64 | 543.21 | 596.11 | 681.55 |  |  |
| moved | 50 | fdw | cold | update | 90 (89–91) | 351.62 | 436.84 | 484.33 | 573.28 | 1 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 50 | fdw | hot | mix | - | - | - | - | - |  |  |
| moved | 50 | fdw | hot | read | 89 (89–90) | 438.3 | 555.65 | 635.04 | 791.22 |  |  |
| moved | 50 | fdw | hot | update | 85 (84–85) | 350.68 | 445.06 | 513.18 | 626.21 | 670 | ERROR: could not serialize access due to concurrent update (SQLSTATE 4 |
| moved | 50 | plain | cold | mix | - | - | - | - | - |  |  |
| moved | 50 | plain | cold | read | 34 557 (34 097–35 016) | 0.55 | 0.96 | 1.38 | 12.21 |  |  |
| moved | 50 | plain | cold | update | 34 593 (34 136–35 049) | 0.77 | 1.29 | 1.98 | 24.42 |  |  |
| moved | 50 | plain | hot | mix | - | - | - | - | - |  |  |
| moved | 50 | plain | hot | read | 34 119 (33 897–34 340) | 0.55 | 0.94 | 1.32 | 11.73 |  |  |
| moved | 50 | plain | hot | update | 34 155 (33 933–34 376) | 0.77 | 1.35 | 2.2 | 38.9 |  |  |
