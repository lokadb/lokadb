
## 2026-10-27-move

_no meta.json: the run did not finish_

| run | op | ops/s (min–max of repeats) | p50 ms | p95 ms | p99 ms | max ms | errors | first error |
|---|---|---|---|---|---|---|---|---|
| rows300000-body200-logical-batch10000 |  | - | - | - | - | - |  |  |
| rows300000-body200-trigger-batch10000 |  | - | - | - | - | - |  |  |

### data check

- data check move rows300000-body200-logical-batch10000: MISMATCH: rows 424000 -> 1181010 (+757010), benchload counted 744577 inserts ok and 0 failed
- data check move rows300000-body200-trigger-batch10000: MISMATCH: rows 424000 -> 12822081 (+12398081), benchload counted 12384189 inserts ok and 0 failed (slack 0)
