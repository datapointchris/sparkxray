# Parse benchmark

How fast each engine reads a Spark event log's task events. The result is why sparkxray parses
logs with DuckDB.

## DuckDB is fastest, and Polars takes a fifth longer

| Arm | Command | Wall time, three runs |
| --- | --- | --- |
| DuckDB `read_json` with explicit `columns=`, 16 threads | `duckdb_bench.py LOG` | 0.29–0.30 s |
| the same, one thread | `duckdb_bench.py LOG --threads 1` | 0.67–0.72 s |
| the same, reading the zstd'd log | `duckdb_bench.py LOG.zst --compression zstd` | 0.32–0.37 s |
| Polars `scan_ndjson` with a schema | `polars_bench.py LOG` | 0.36–0.39 s |
| Python with orjson, every line | `orjson_bench.py LOG all` | 2.09–2.14 s |
| the same, skipping lines without the TaskEnd name | `orjson_bench.py LOG prefilter` | 2.02–2.05 s |

Every arm reported the same 18 stages and 126,920 tasks.

## Each arm computes the same per-stage aggregates over one inflated log

The input is the `spark-3.5.9` fixture log inflated to production size. `inflate.sh` writes the
log's other events once, then its `SparkListenerTaskStart` and `SparkListenerTaskEnd` lines 380
times. The result is 786,480,019 bytes holding 126,920 `SparkListenerTaskEnd` events:

```bash
benchmarks/parse/inflate.sh fixtures/logs/spark-3.5.9/local-*.zstd 380 /tmp/big.json
benchmarks/parse/run.sh /tmp/big.json
```

Each arm parses the whole file and keeps only `SparkListenerTaskEnd`. Per stage, it computes the
task count, the median and max `Executor Run Time`, the summed `Memory Bytes Spilled` and
`JVM GC Time`, and the max `Total Records Read`. Each times its own query with
`time.perf_counter`, so interpreter start and imports are not counted. `run.sh` writes the zstd
copy at level 3, then runs every arm in turn, three rounds.

The measured machine was an AMD Ryzen 9 6900HX with 16 threads and 30 GiB of memory, with the input
on tmpfs. Every script runs under `uv run --script` on Python 3.13, with its dependency pinned
inline: duckdb 1.5.6, polars 2.0.0 and orjson 3.13.0.

No benchmark runs in CI.
