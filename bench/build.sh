#!/usr/bin/env bash
# Builds the binaries the servers get (bench/dist/): lokadb of this commit and benchload. Go runs in
# Docker only (golang:1.25), the binaries are linux/amd64 and run on any glibc server.
#   bash bench/build.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1
root=$(cd "$(dirname "$0")/.." && pwd)
out=$root/bench/dist
mkdir -p "$out"
w=$(cd "$root" && (pwd -W 2>/dev/null || pwd))
(cd "$root" && git rev-parse HEAD) >"$out/COMMIT"
(cd "$root" && git status --porcelain) | grep -q . && echo "dirty" >>"$out/COMMIT" || true
docker run --rm -v "$w:/src" -v lokadb-bench-gomod:/go/pkg/mod -v lokadb-bench-gocache:/root/.cache/go-build golang:1.25-bookworm sh -c '
  cd /src/proxy && CGO_ENABLED=1 go build -o /src/bench/dist/lokadb ./cmd/lokadb &&
  cd /src/bench/load && CGO_ENABLED=0 go build -o /src/bench/dist/benchload .'
echo "built: $(cat "$out/COMMIT" | tr '\n' ' ')"
ls -l "$out"
