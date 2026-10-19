#!/usr/bin/env bash
# Build the benchmark's input from one fixture log: its other events once, then its task events
# repeated, so parsing task events dominates the way it does in a long production run.
#
#   benchmarks/parse/inflate.sh LOG.zstd COPIES OUTPUT
set -euo pipefail

if [ $# -ne 3 ]; then
  echo "usage: inflate.sh LOG.zstd COPIES OUTPUT" >&2
  exit 2
fi
log=$1
copies=$2
output=$3

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

zstd -dcq "$log" >"$work/plain.json"
grep -v -E '"Event":"SparkListenerTask(Start|End)"' "$work/plain.json" >"$work/header.json"
grep -E '"Event":"SparkListenerTask(Start|End)"' "$work/plain.json" >"$work/tasks.json"
{
  cat "$work/header.json"
  for _ in $(seq 1 "$copies"); do
    cat "$work/tasks.json"
  done
} >"$output"
