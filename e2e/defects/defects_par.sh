#!/usr/bin/env bash
# defects.sh over several stands at once: every mode of MODES (default "auto logical": auto is triggers, logical is asked for) runs in
# SHARDS parts (default 1: the two modes side by side), each part on a compose project of its own.
# A check runs alone on its stand as in defects.sh; only the order of the checks is cut.
#   COMPOSE_PROJECT_NAME=lokadb-defects-b5 bash e2e/defects/defects_par.sh
# DEFECTS_DIR: the e2e/defects of another worktree (a block's branch), default this script's own.
# SHARDS above 1 needs DEFECTS_SHARD in that tree's main.go. 24.10: eight stands at once next to the
# other runs overloaded the machine (load 39 on 12 CPU, checks timed out), so keep stands few.
# Logs: $DEFECTS_LOGS (default ./par-logs)/<mode>-<k>.log; exit 0 only when every part passed.
set -uo pipefail
# One image build at a time on a shared machine (tasks/COORDINATION.md): LOKADB_LOCKS names the lock
# directory; without it (another machine) the build just runs. A build that times out is retried.
build_locked() {
  local L=${LOKADB_LOCKS:-} try
  [ -n "${LOKADB_BUILD_LOCKED:-}" ] && L=/nonexistent # the caller holds the lock already
  if [ -d "$L" ]; then
    until mkdir "$L/build" 2>/dev/null; do sleep 15; done
    echo "${COMPOSE_PROJECT_NAME:-defects} $(date -Is) build check" >"$L/build/owner"
  fi
  for try in 1 2 3; do "$@" && break; done
  local rc=$?
  [ -d "$L" ] && rm -rf "$L/build"
  return $rc
}
cd "${DEFECTS_DIR:-$(dirname "$0")}" || exit 1
export MSYS_NO_PATHCONV=1
base=${COMPOSE_PROJECT_NAME:-lokadb-defects}
n=${SHARDS:-1}
modes=${MODES:-auto logical}
logs=${DEFECTS_LOGS:-$PWD/par-logs}
mkdir -p "$logs"
if [ "$n" -gt 1 ] && ! grep -q DEFECTS_SHARD main.go; then
  echo "SHARDS=$n: this tree's main.go has no DEFECTS_SHARD (merge main), each part would run every check"; exit 2
fi
# one build; every part gets the image under its own project's name and does not build again
COMPOSE_PROJECT_NAME=$base-build build_locked docker compose build check cloud customer customergis >"$logs/build.log" 2>&1 || { echo "build failed: $logs/build.log"; exit 1; }
pids=()
for m in $modes; do
  for ((k = 0; k < n; k++)); do
    docker tag "$base-build-check" "$base-$m-$k-check" || exit 1
    COMPOSE_PROJECT_NAME=$base-$m-$k DEFECTS_NO_BUILD=1 DEFECTS_MODE=$m DEFECTS_SHARD=$k/$n bash defects.sh >"$logs/$m-$k.log" 2>&1 &
    pids+=($!)
  done
done
rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done
for m in $modes; do
  pass=$(cat "$logs/$m"-*.log | grep -cE '^ *PASS')
  bad=$(cat "$logs/$m"-*.log | grep -E '^ *(FAIL|ERROR)' | wc -l)
  echo "$m: $pass PASS, $bad not passed"
  grep -hE -B1 '^ *(FAIL|ERROR)' "$logs/$m"-*.log
  # a part that did not finish (stand did not start, the run broke) has no summary line
  for ((k = 0; k < n; k++)); do
    grep -qE 'did not pass|all checks passed' "$logs/$m-$k.log" || { echo "$m part $k did not finish: $logs/$m-$k.log"; rc=1; }
  done
done
docker image rm "$base-build-check" >/dev/null 2>&1
exit $rc
