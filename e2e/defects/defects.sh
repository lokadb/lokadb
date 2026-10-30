#!/usr/bin/env bash
# Reproduces each defect of research/ANALYSIS.md (5.1 data loss, 5.2 security) against real Postgres and
# checks it is fixed. Own compose project: independent of the e2e stand.
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
cd "$(dirname "$0")"
export MSYS_NO_PATHCONV=1 # Git Bash: leave /usr/local/bin/... arguments alone
C="docker compose --profile pgbouncer" # the profile only lets down remove pgbouncer (B9.11) too
$C down -v >/dev/null 2>&1
[ -n "${DEFECTS_NO_BUILD:-}" ] || build_locked $C build check cloud customer customergis || exit 1 # defects_par.sh builds once and tags the image for every part
$C up -d --wait cloud cloudplain cloud16 customer customergis || { $C down -v >/dev/null 2>&1; exit 1; } # never leave a half-started stand behind
# B9.11 only, named in DEFECTS_ONLY: pgbouncer in front of the check's proxy; without it the check is not registered
case ",${DEFECTS_ONLY:-}," in *,B9.11,*)
  $C up -d pgbouncer || { $C down -v >/dev/null 2>&1; exit 1; }
  export DEFECTS_PGBOUNCER=1 ;;
esac
$C run --rm --use-aliases ${DEFECTS_ONLY:+-e DEFECTS_ONLY} ${DEFECTS_MODE:+-e DEFECTS_MODE} ${DEFECTS_TIMEOUT:+-e DEFECTS_TIMEOUT} ${DEFECTS_TRACE:+-e DEFECTS_TRACE} ${DEFECTS_SHARD:+-e DEFECTS_SHARD} ${DEFECTS_PGBOUNCER:+-e DEFECTS_PGBOUNCER} check
rc=$?
$C down -v >/dev/null 2>&1
exit $rc
