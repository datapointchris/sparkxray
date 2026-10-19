#!/usr/bin/env bash
# Run every arm of the parse benchmark three times over one inflated log, printing each arm's
# wall time and the stage and task counts it computed. The counts must agree across arms.
#
#   benchmarks/parse/run.sh BIG.json
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: run.sh BIG.json" >&2
  exit 2
fi
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
input=$1

zstd -q -f -3 "$input" -o "$input.zst"
for _ in 1 2 3; do
  uv run --quiet --script "$here/duckdb_bench.py" "$input"
  uv run --quiet --script "$here/duckdb_bench.py" "$input" --threads 1
  uv run --quiet --script "$here/duckdb_bench.py" "$input.zst" --compression zstd
  uv run --quiet --script "$here/polars_bench.py" "$input"
  uv run --quiet --script "$here/orjson_bench.py" "$input" all
  uv run --quiet --script "$here/orjson_bench.py" "$input" prefilter
done
