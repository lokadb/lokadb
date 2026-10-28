#!/usr/bin/env python3
"""The data check of a scenario (bench/scenarios/common.sh, verify_begin / verify_end).

    python3 bench/verify-data.py BEFORE AFTER MOVED LABEL [RAW_LINES...]

BEFORE and AFTER: lines "place|organization|rows" (place: cloud or orgN, the database of organization N), the
documents of every place. MOVED: the organizations moved out at the end, comma separated. RAW_LINES: files of
benchload lines written in between: their successful inserts are what the rows may have grown by.

Checks: no place holds rows of another organization (an organization's database only its own, the cloud only the
organizations that stay there), and the total grew by the inserts benchload counted successful, at most by those
plus the failed ones (an insert that failed on the client may still have committed). Prints one line: ok or what
does not add up.
"""
import json
import os
import sys
from collections import defaultdict


def counts(path):
    by = defaultdict(int)  # (place, org) -> rows
    for line in open(path, encoding="utf-8", errors="replace"):
        parts = line.strip().split("|")
        if len(parts) == 3 and parts[2].isdigit():
            by[(parts[0], parts[1])] += int(parts[2])
    return by


def main(argv):
    before, after, moved, label = counts(argv[0]), counts(argv[1]), {m for m in argv[2].split(",") if m}, argv[3]
    ok_ins = failed_ins = 0
    for f in argv[4:]:
        try:
            lines = open(f, encoding="utf-8", errors="replace").read().splitlines()
        except OSError:
            continue
        for l in lines:
            if not l.startswith("{"):
                continue
            d = json.loads(l)
            if (d.get("tags") or {}).get("config") == "plain":  # a run on the database without lokadb
                continue
            op = d.get("op", "")
            if op == "insert":
                ok_ins += d.get("ops", 0) - d.get("errors", 0)
                failed_ins += d.get("errors", 0)
            elif op == "copy_in" and not d.get("errors"):
                ok_ins += (d.get("extra") or {}).get("rows_in", 0)
    problems = []
    for (place, org), n in sorted(after.items()):
        if place.startswith("org") and org != place[3:]:
            problems.append(f"{n} rows of organization {org} in the database of {place}")
        if place == "cloud" and org in moved:
            problems.append(f"{n} rows of the moved organization {org} left in the cloud")
    tb, ta = sum(before.values()), sum(after.values())
    grew = ta - tb
    # VERIFY_SLACK: operations a stop (SIGINT) cut off are not counted, though they may have committed: at most one a client
    slack = int(os.environ.get("VERIFY_SLACK", "0") or 0)
    if not (ok_ins <= grew <= ok_ins + failed_ins + slack):
        problems.append(f"rows {tb} -> {ta} (+{grew}), benchload counted {ok_ins} inserts ok and {failed_ins} failed (slack {slack})")
    orgs = len({o for _, o in after})
    if problems:
        print(f"data check {label}: MISMATCH: " + "; ".join(problems))
        return 1
    print(f"data check {label}: ok: {ta} rows in {orgs} organizations ({tb} before, +{grew}: {ok_ins} inserts ok, "
          f"{failed_ins} failed), no rows of another organization in any database, none of the moved left in the cloud")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
