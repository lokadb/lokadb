
## 2026-10-27-extra

_no meta.json: the run did not finish_

| class | clients | config | keys | protocol | rate | reconnect | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud | 16 | plain | cold | extended |  | 1 | read | 1 389 (1 350–1 427) | 11.13 | 16.27 | 20.02 | 85.09 |  |  |
| cloud | 16 | plain+pgbouncer-session | cold | extended |  | 1 | read | 262 (262–262) | 61.34 | 75.89 | 85.89 | 108.81 |  |  |
| cloud | 16 | plain+pgbouncer-transaction | cold | extended |  | 1 | read | 275 (272–279) | 57.2 | 76.42 | 88 | 115.56 |  |  |
| cloud | 256 | fdw | cold | extended | 15000 |  | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 15000 |  | read | 7 492 (7 492–7 492) | 0.87 | 4.83 | 11.85 | 86.61 |  |  |
| cloud | 256 | fdw | cold | extended | 15000 |  | update | 7 508 (7 508–7 508) | 0.83 | 5.34 | 14.54 | 75.34 |  |  |
| cloud | 256 | fdw | cold | extended | 25000 |  | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 25000 |  | read | 3 (3–4) | 990.86 | 999.53 | 1 000 | 1 000 | 290589 | timeout: not answered within the timeout |
| cloud | 256 | fdw | cold | extended | 25000 |  | timeout | 0 | 0 | 0 | 0 | 0 | 2416538 | timeout: not started within the timeout (all workers busy) |
| cloud | 256 | fdw | cold | extended | 25000 |  | update | 3 (3–3) | 991.74 | 999.25 | 999.84 | 1 001 | 292101 | timeout: not answered within the timeout |
| cloud | 256 | fdw | cold | extended | 35000 |  | mix | - | - | - | - | - |  |  |
| cloud | 256 | fdw | cold | extended | 35000 |  | read | 4 (2–5) | 987.92 | 999.05 | 1 000 | 1 000 | 288199 | timeout: not answered within the timeout |
| cloud | 256 | fdw | cold | extended | 35000 |  | timeout | 0 | 0 | 0 | 0 | 0 | 3621159 | timeout: not started within the timeout (all workers busy) |
| cloud | 256 | fdw | cold | extended | 35000 |  | update | 4 (2–6) | 988.47 | 999.59 | 999.86 | 1 000 | 289808 | timeout: not answered within the timeout |
| cloud | 256 | plain | cold | extended | 35000 |  | mix | - | - | - | - | - |  |  |
| cloud | 256 | plain | cold | extended | 35000 |  | read | 17 495 (17 494–17 496) | 0.44 | 1.9 | 8.96 | 61.4 |  |  |
| cloud | 256 | plain | cold | extended | 35000 |  | update | 17 506 (17 505–17 507) | 0.6 | 2.72 | 11.77 | 92.24 |  |  |

### data check

- data check extra (open model): ok: 400000 rows in 20 organizations (400000 before, +0: 0 inserts ok, 0 failed), no rows of another organization in any database, none of the moved left in the cloud
