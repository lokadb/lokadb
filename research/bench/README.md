# research/bench: results of the load and scale measurements

What is here, one directory per scenario and day (`<date>-<scenario>/`):

- `report.md`, `*.svg`: the tables and charts of the scenario (made by `python3 bench/report.py`); `REPORT.md`: all of them in one file;
- `notes.txt`, `meta.json`, `rtt-*.json`, `data-check.txt`: the course of the run, the versions and settings, the distances between the hosts, the check of the data after it;
- `raw/*.jsonl`: one line per run of the load generator (with tags); `raw/*.log`: logs of `move-org`; the rest of the raw lines;
- `excluded/`: runs that hit a failure of the stand, with the reason: they are not results;
- `charts-data.md`: the numbers behind the charts of the README (`docs/img/bench/`), made by `python3 bench/charts/build.py`;
- `METHOD.md`: how the numbers were taken and how to repeat it.

Two passes are kept. `2026-10-26-*` is the first pass (an earlier code); `2026-10-27-*` is the last one, and only its numbers are in the README. `2026-10-26-replay/` is the first pass's log put together again after it was lost (see its README). `baseline-before-pf/` is a trial point from one machine.

## The per-second series are not in the repository

The per-second series (`timeline-*.csv` of the `sessions` scenario, `writer-*.csv` of the moves, 32 files, 47 MB) are in a release archive, attached to the [first release of the repository](https://github.com/lokadb/lokadb/releases):

- `raw-2026-10-27.tar.gz` (9.1 MB), sha256 in `raw-2026-10-27.tar.gz.sha256`: `741da721384e78bd1b787dc7b6010a30be97bbd54b010025163c38cb44e5d8b0`.

Unpack it in `research/bench/`: the files go back to `<scenario>/raw/` where they were:

```bash
cd research/bench
tar xzf raw-2026-10-27.tar.gz        # puts the series back under <scenario>/raw/
python3 ../../bench/report.py        # the reports
python3 ../../bench/charts/build.py  # the charts and charts-data.md
```

The charts and the reports of this directory do not need the series: they are built from the `raw/*.jsonl` lines and the logs, which are in the repository. The series only show a run second by second (the lag of a move, the stalls of a writer, the rate of each class of organizations).
