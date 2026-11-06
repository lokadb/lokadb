#!/usr/bin/env bash
# The cross-tenant suite alone: organization A tries to reach organization B's rows, on real Postgres
# through the real proxy (crosstenant.go; what it covers: docs/testing.md). Needs Docker.
cd "$(dirname "$0")" || exit 1
DEFECTS_ONLY=XT1,XT2,XT3,XT4,XT5,XT6,XT7,XT8,XT9,XT10,XT11,XT12,XT13,XT14,XT15 exec bash ./defects.sh
