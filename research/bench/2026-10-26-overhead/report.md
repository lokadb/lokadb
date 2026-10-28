
## 2026-10-26-overhead

- run: 2026-10-26 (overhead), LokaDB `c9e50325dff6ef2b8cd9cd30c9771203728d34b1`, Postgres 18.6 (Debian 18.6-1.pgdg13+2)
- load: warm-up 15s, measured 45s, 2 repeats (median)
- cloud: 8 CPU, 32095 MB, disk  100G 1 disk
- proxy+relay: 4 CPU, 15991 MB, disk  60G 1 disk
- load: 4 CPU, 15991 MB, disk  60G 1 disk
- org1: 2 CPU, 7939 MB, disk  40G 1 disk
- org2: 2 CPU, 3913 MB, disk  50G 1 disk
- org3: 2 CPU, 3913 MB, disk  50G 1 disk
- org4: 2 CPU, 3913 MB, disk  50G 1 disk
- org5: 4 CPU, 7939 MB, disk  80G 1 disk
- RTT ms min/avg/max, start: cloud>proxy 0.186/0.379/1.463; cloud>relay 0.166/0.208/0.265; cloud>load 0.210/0.357/0.852; cloud>org1 0.219/0.304/0.678; cloud>org2 9.458/9.620/10.407; cloud>org3 38.787/39.035/40.112; cloud>org4 44.066/44.158/44.323; cloud>org5 49.199/49.874/55.081
- RTT ms min/avg/max, end: cloud>proxy 0.163/0.194/0.263; cloud>relay 0.153/0.199/0.366; cloud>load 0.182/0.231/0.409; cloud>org1 0.180/0.209/0.226; cloud>org2 9.403/9.477/9.600; cloud>org3 38.725/38.931/39.821; cloud>org4 43.266/43.391/43.877; cloud>org5 49.231/49.387/50.271

| clients | config | keys | protocol | reconnect | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | lokadb-direct | cold | extended |  | read | 2 489 (2 440–2 537) | 0.39 | 0.47 | 0.6 | 7.19 |  |  |
| 1 | lokadb-direct | cold | extended |  | update | 2 216 (2 142–2 289) | 0.41 | 0.53 | 1.2 | 7.54 |  |  |
| 1 | lokadb-direct | cold | simple |  | read | 1 295 (1 220–1 370) | 0.77 | 0.86 | 1.05 | 8.13 |  |  |
| 1 | lokadb-direct | cold | simple |  | update | 1 676 (1 647–1 705) | 0.57 | 0.69 | 0.89 | 8.61 |  |  |
| 1 | lokadb-direct | hot | extended |  | read | 2 379 (2 263–2 495) | 0.4 | 0.5 | 1.09 | 8.53 |  |  |
| 1 | lokadb-direct | hot | simple |  | read | 1 366 (1 289–1 442) | 0.69 | 0.91 | 1.53 | 7.25 |  |  |
| 1 | plain | cold | extended |  | read | 3 570 (3 486–3 655) | 0.26 | 0.35 | 0.7 | 8.57 |  |  |
| 1 | plain | cold | extended |  | update | 3 566 (3 551–3 581) | 0.26 | 0.35 | 0.48 | 8.94 |  |  |
| 1 | plain | cold | simple |  | read | 3 334 (3 236–3 431) | 0.28 | 0.36 | 0.61 | 32.91 |  |  |
| 1 | plain | cold | simple |  | update | 3 049 (2 956–3 143) | 0.29 | 0.43 | 0.99 | 10.08 |  |  |
| 1 | plain | hot | extended |  | read | 4 331 (4 326–4 337) | 0.22 | 0.27 | 0.34 | 6.63 |  |  |
| 1 | plain | hot | simple |  | read | 3 244 (3 097–3 391) | 0.29 | 0.38 | 0.53 | 6.02 |  |  |
| 1 | proxy-base | cold | extended |  | read | 1 159 (1 128–1 190) | 0.85 | 1.02 | 1.31 | 20.05 |  |  |
| 1 | proxy-base | cold | extended |  | update | 1 937 (1 906–1 967) | 0.49 | 0.62 | 0.91 | 16.12 |  |  |
| 1 | proxy-base | cold | simple |  | read | 774 (727–822) | 1.27 | 1.5 | 1.89 | 16.42 |  |  |
| 1 | proxy-base | cold | simple |  | update | 1 385 (1 345–1 424) | 0.68 | 0.87 | 1.75 | 14.04 |  |  |
| 1 | proxy-base | hot | extended |  | read | 1 527 (1 468–1 587) | 0.64 | 0.76 | 0.97 | 14.26 |  |  |
| 1 | proxy-base | hot | simple |  | read | 1 504 (1 484–1 524) | 0.64 | 0.76 | 0.97 | 14.27 |  |  |
| 1 | proxy-meta_off | cold | extended |  | read | 1 243 (1 237–1 249) | 0.79 | 0.92 | 1.26 | 13.6 |  |  |
| 1 | proxy-meta_off | cold | extended |  | update | 2 020 (2 013–2 028) | 0.48 | 0.54 | 0.7 | 13.18 |  |  |
| 1 | proxy-meta_off | cold | simple |  | read | 747 (728–766) | 1.32 | 1.51 | 1.92 | 16.63 |  |  |
| 1 | proxy-meta_off | cold | simple |  | update | 1 192 (1 172–1 212) | 0.82 | 0.98 | 1.37 | 17.17 |  |  |
| 1 | proxy-meta_off | hot | extended |  | read | 1 643 (1 642–1 643) | 0.59 | 0.68 | 0.87 | 13.07 |  |  |
| 1 | proxy-meta_off | hot | simple |  | read | 1 136 (1 080–1 191) | 0.85 | 1.06 | 2.19 | 17.72 |  |  |
| 1 | proxy-pools | cold | extended |  | read | 1 198 (1 162–1 234) | 0.8 | 1.01 | 1.94 | 24.68 |  |  |
| 1 | proxy-pools | cold | extended |  | update | 2 010 (2 006–2 015) | 0.48 | 0.55 | 0.76 | 13.22 |  |  |
| 1 | proxy-pools | cold | simple |  | read | 825 (818–833) | 1.18 | 1.38 | 1.96 | 13.17 |  |  |
| 1 | proxy-pools | cold | simple |  | update | 1 440 (1 436–1 444) | 0.67 | 0.78 | 1.04 | 12.8 |  |  |
| 1 | proxy-pools | hot | extended |  | read | 1 575 (1 509–1 640) | 0.61 | 0.73 | 0.95 | 14.23 |  |  |
| 1 | proxy-pools | hot | simple |  | read | 1 501 (1 494–1 508) | 0.65 | 0.74 | 0.98 | 13.87 |  |  |
| 1 | proxy-routelog | cold | extended |  | read | 1 195 (1 153–1 238) | 0.82 | 0.97 | 1.3 | 13.49 |  |  |
| 1 | proxy-routelog | cold | extended |  | update | 1 952 (1 940–1 964) | 0.49 | 0.59 | 0.85 | 15.35 |  |  |
| 1 | proxy-routelog | cold | simple |  | read | 820 (813–826) | 1.2 | 1.34 | 1.88 | 31.61 |  |  |
| 1 | proxy-routelog | cold | simple |  | update | 1 378 (1 358–1 399) | 0.7 | 0.8 | 1.11 | 14.53 |  |  |
| 1 | proxy-routelog | hot | extended |  | read | 1 456 (1 400–1 512) | 0.63 | 0.93 | 1.74 | 14.16 |  |  |
| 1 | proxy-routelog | hot | simple |  | read | 1 354 (1 300–1 408) | 0.68 | 0.96 | 1.97 | 14.46 |  |  |
| 1 | proxy-verify | cold | extended |  | read | 1 186 (1 179–1 193) | 0.83 | 0.98 | 1.21 | 16.22 |  |  |
| 1 | proxy-verify | cold | extended |  | update | 1 960 (1 949–1 971) | 0.49 | 0.59 | 0.89 | 17.15 |  |  |
| 1 | proxy-verify | cold | simple |  | read | 800 (756–843) | 1.2 | 1.49 | 2.39 | 41.8 |  |  |
| 1 | proxy-verify | cold | simple |  | update | 1 377 (1 334–1 419) | 0.67 | 0.88 | 1.89 | 14.32 |  |  |
| 1 | proxy-verify | hot | extended |  | read | 1 429 (1 344–1 515) | 0.68 | 0.81 | 1.09 | 16.68 |  |  |
| 1 | proxy-verify | hot | simple |  | read | 1 387 (1 368–1 406) | 0.67 | 0.91 | 1.99 | 14.64 |  |  |
| 16 | lokadb-direct | cold | extended | 1 | read | - | - | - | - | - |  |  |
| 16 | plain | cold | extended | 1 | read | - | - | - | - | - |  |  |
| 16 | proxy-base | cold | extended | 1 | read | 887 (880–895) | 17.75 | 22.98 | 25.86 | 50.77 |  |  |
| 16 | proxy-meta_off | cold | extended | 1 | read | 318 (309–326) | 50.25 | 60.6 | 65.53 | 86.42 |  |  |
| 16 | proxy-pools | cold | extended | 1 | read | 4 097 (4 000–4 194) | 3.6 | 5.53 | 11.08 | 35.25 |  |  |
| 16 | proxy-routelog | cold | extended | 1 | read | 888 (882–893) | 17.76 | 22.93 | 25.8 | 46.56 |  |  |
| 16 | proxy-verify | cold | extended | 1 | read | 880 (856–905) | 17.56 | 25.29 | 31.27 | 66.32 |  |  |
| 32 | lokadb-direct | cold | extended |  | read | 40 179 (39 932–40 427) | 0.72 | 1.27 | 1.92 | 13.99 |  |  |
| 32 | lokadb-direct | cold | extended |  | update | 32 829 (32 046–33 613) | 0.87 | 1.52 | 3.13 | 22.7 |  |  |
| 32 | lokadb-direct | cold | simple |  | read | 18 816 (18 616–19 016) | 1.51 | 2.98 | 4.03 | 18.89 |  |  |
| 32 | lokadb-direct | cold | simple |  | update | 20 736 (20 053–21 420) | 1.42 | 2.46 | 4.21 | 16.92 |  |  |
| 32 | lokadb-direct | hot | extended |  | read | 37 457 (35 000–39 914) | 0.75 | 1.45 | 3.07 | 14.74 |  |  |
| 32 | lokadb-direct | hot | simple |  | read | 19 714 (19 592–19 837) | 1.47 | 2.77 | 3.6 | 13.72 |  |  |
| 32 | plain | cold | extended |  | read | 60 514 (58 254–62 774) | 0.48 | 0.73 | 1.78 | 13.08 |  |  |
| 32 | plain | cold | extended |  | update | 53 898 (53 608–54 189) | 0.55 | 0.8 | 1.44 | 14.28 |  |  |
| 32 | plain | cold | simple |  | read | 55 717 (54 963–56 471) | 0.5 | 0.97 | 1.61 | 34.91 |  |  |
| 32 | plain | cold | simple |  | update | 43 908 (41 474–46 341) | 0.65 | 1.13 | 2.41 | 26.76 |  |  |
| 32 | plain | hot | extended |  | read | 62 534 (62 407–62 662) | 0.48 | 0.69 | 1 | 11.51 |  |  |
| 32 | plain | hot | simple |  | read | 53 852 (52 723–54 980) | 0.52 | 0.99 | 1.66 | 12.02 |  |  |
| 32 | proxy-base | cold | extended |  | read | 14 739 (14 005–15 473) | 1.96 | 3.97 | 6.51 | 27.31 |  |  |
| 32 | proxy-base | cold | extended |  | update | 30 018 (29 916–30 120) | 0.98 | 1.69 | 2.67 | 22.41 |  |  |
| 32 | proxy-base | cold | simple |  | read | 9 646 (9 565–9 728) | 3.14 | 5.09 | 7.43 | 32.79 |  |  |
| 32 | proxy-base | cold | simple |  | update | 16 995 (16 229–17 760) | 1.69 | 3.38 | 5.67 | 29.09 |  |  |
| 32 | proxy-base | hot | extended |  | read | 15 409 (15 400–15 418) | 1.89 | 3.84 | 5.35 | 26.51 |  |  |
| 32 | proxy-base | hot | simple |  | read | 17 767 (17 529–18 005) | 1.54 | 3.55 | 5.08 | 37.49 |  |  |
| 32 | proxy-meta_off | cold | extended |  | read | 15 327 (15 249–15 405) | 1.96 | 3.32 | 4.95 | 34.95 |  |  |
| 32 | proxy-meta_off | cold | extended |  | update | 27 992 (26 592–29 393) | 1 | 1.99 | 3.88 | 35.98 |  |  |
| 32 | proxy-meta_off | cold | simple |  | read | 9 445 (9 141–9 749) | 3.13 | 5.61 | 8.96 | 61.13 |  |  |
| 32 | proxy-meta_off | cold | simple |  | update | 17 030 (17 002–17 058) | 1.75 | 2.98 | 4.33 | 33.75 |  |  |
| 32 | proxy-meta_off | hot | extended |  | read | 15 210 (15 114–15 306) | 1.9 | 3.93 | 5.6 | 32.61 |  |  |
| 32 | proxy-meta_off | hot | simple |  | read | 17 014 (16 611–17 418) | 1.55 | 3.98 | 6.24 | 71.91 |  |  |
| 32 | proxy-pools | cold | extended |  | read | 15 425 (15 402–15 449) | 1.94 | 3.3 | 5.02 | 30.03 |  |  |
| 32 | proxy-pools | cold | extended |  | update | 29 647 (29 623–29 672) | 0.99 | 1.7 | 2.7 | 21.73 |  |  |
| 32 | proxy-pools | cold | simple |  | read | 9 655 (9 623–9 686) | 3.14 | 5.12 | 7.36 | 38.52 |  |  |
| 32 | proxy-pools | cold | simple |  | update | 16 577 (16 105–17 048) | 1.78 | 3.09 | 4.83 | 46.19 |  |  |
| 32 | proxy-pools | hot | extended |  | read | 15 328 (15 257–15 399) | 1.9 | 3.84 | 5.35 | 24.15 |  |  |
| 32 | proxy-pools | hot | simple |  | read | 15 988 (15 287–16 688) | 1.62 | 4.4 | 7.73 | 98.21 |  |  |
| 32 | proxy-routelog | cold | extended |  | read | 11 748 (11 744–11 752) | 2.54 | 4.56 | 6.66 | 62.76 |  |  |
| 32 | proxy-routelog | cold | extended |  | update | 24 267 (24 091–24 442) | 1.19 | 2.22 | 3.46 | 72.75 |  |  |
| 32 | proxy-routelog | cold | simple |  | read | 7 391 (7 088–7 694) | 3.98 | 7.46 | 10.88 | 114.4 |  |  |
| 32 | proxy-routelog | cold | simple |  | update | 15 323 (15 312–15 334) | 1.92 | 3.55 | 5.06 | 31.3 |  |  |
| 32 | proxy-routelog | hot | extended |  | read | 12 293 (12 288–12 299) | 2.38 | 4.73 | 6.41 | 45.39 |  |  |
| 32 | proxy-routelog | hot | simple |  | read | 17 714 (17 700–17 728) | 1.58 | 3.38 | 4.78 | 64.53 |  |  |
| 32 | proxy-verify | cold | extended |  | read | 14 445 (13 985–14 905) | 1.98 | 4 | 7.04 | 70.32 |  |  |
| 32 | proxy-verify | cold | extended |  | update | 28 211 (28 184–28 238) | 1.03 | 1.78 | 2.88 | 28.98 |  |  |
| 32 | proxy-verify | cold | simple |  | read | 9 674 (9 660–9 689) | 3.13 | 5.1 | 7.28 | 28.91 |  |  |
| 32 | proxy-verify | cold | simple |  | update | 17 839 (17 801–17 878) | 1.67 | 2.85 | 4.15 | 24.32 |  |  |
| 32 | proxy-verify | hot | extended |  | read | 15 335 (15 255–15 416) | 1.9 | 3.85 | 5.35 | 30.02 |  |  |
| 32 | proxy-verify | hot | simple |  | read | 17 377 (17 331–17 422) | 1.54 | 3.91 | 5.88 | 37.28 |  |  |
