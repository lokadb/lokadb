# Excluded from the results

`paths-direct-connect.jsonl`: the `connect` variant of profile direct from the run of 26 October. The first query after the connection ran without `lokadb.tenant`, and the proxy refused it, as it should on direct ("a read must name the organization"): an error of the measurement, not of the product. The variant was measured again with `connect -coop`, the first query in a transaction that names the organization (`research/bench/2026-10-27-after`).
