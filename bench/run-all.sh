#!/usr/bin/env bash
# All the scenarios, one after another, in the order that gives the README its numbers first. A scenario that
# finished is not run again (marker bench/state/<inventory>/done.<name>): after a failure run the same command again.
#   bash bench/run-all.sh INVENTORY                 everything
#   ONLY="paths http" bash bench/run-all.sh INV     these only
#   REPEATS=2 bash bench/run-all.sh INV             fewer repeats (shorter, a wider spread)
# The report is written after every scenario (research/bench/REPORT.md). Progress: bench/state/<inventory>/progress.log
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
inv=${1:?inventory file}
name=$(basename "$inv" | sed 's/\.[a-z]*$//')
state=$here/state/$name
mkdir -p "$state"
ORDER=${ORDER:-"paths http overhead clients orgs sessions rtt misc outage agents volume move"}
for s in ${ONLY:-$ORDER}; do
  [ -f "$state/done.$s" ] && { echo "$s: done before"; continue; }
  echo "== $s $(date -Is)" | tee -a "$state/progress.log"
  if bash "$here/scenarios/$s.sh" "$inv" >"$state/$s.log" 2>&1; then
    touch "$state/done.$s"
    echo "   ok $(date -Is)" | tee -a "$state/progress.log"
  else
    echo "   FAILED $(date -Is): $state/$s.log" | tee -a "$state/progress.log"
  fi
  # no address and no user@host in the results (a log of a tool prints what it connected to)
  find "$here/../research/bench" -type f \( -name '*.txt' -o -name '*.log' -o -name '*.json' -o -name '*.jsonl' -o -name '*.csv' -o -name '*.md' \) -print0 2>/dev/null |
    xargs -0 -r sed -i -E 's/[A-Za-z0-9_.-]+@([0-9]{1,3}\.){3}[0-9]{1,3}/<host>/g; /127\.0\.0\.1|0\.0\.0\.0/!s/([0-9]{1,3}\.){3}[0-9]{1,3}/<ip>/g'
  python3 "$here/report.py" >/dev/null 2>&1
done
python3 "$here/check-clean.py" || echo "!! the results name a place or an address: fix before anything is committed"
