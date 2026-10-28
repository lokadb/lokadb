# The log of the run of 26 October (restored)

The originals of `live.jsonl`, `progress.log` and `nodes.jsonl` (the servers' load every 30 s) were deleted by `deploy.sh down` together with the stand's state directory (a mistake: it erased the whole directory). `down` now saves the logs into `bench/state-archive/` first. What can be put together again is here:

- `progress.log`: the start and the end of the scenarios, from the records; the time as it was, in the working machine's time zone.
- `live.jsonl`: a line per run. The tags and the `benchload` lines are exact: taken from the scenarios' `raw/*.jsonl` in the order of the runs, together with the ones moved to `excluded/`. The time `t` is estimated (`t_estimated: true`): a scenario is stretched between its known start and end in proportion to the length of the runs. Good for the course of a run, not for a per-second link.
- `nodes.jsonl` is lost. The servers' load per run is in the `raw/resources.jsonl` of the scenarios `overhead` and `clients` (resources were not taken in `paths` and `http`).
