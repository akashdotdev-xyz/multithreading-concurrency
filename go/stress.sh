#!/usr/bin/env bash
# usage: ./stress.sh <package-dir> [runs] [--race]
set -uo pipefail

DIR="${1:?usage: ./stress.sh <package-dir> [runs] [--race]}"
RUNS="${2:-500}"
RACE="${3:-}"

RACE_ARG=""
if [ "$RACE" = "--race" ]; then
  RACE_ARG="-race"
fi

cd "$(dirname "$0")"

fail=0
for i in $(seq 1 "$RUNS"); do
  if ! go run $RACE_ARG "./$DIR" > /dev/null 2>&1; then
    echo "FAILED at $i"
    fail=$((fail + 1))
  fi
done

echo "$fail failures out of $RUNS runs"
