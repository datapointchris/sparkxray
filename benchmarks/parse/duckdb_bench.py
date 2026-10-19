# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.6"]
# ///
"""DuckDB arm of the parse benchmark: `read_json` with an explicit column map.

uv run --script duckdb_bench.py LOG [--threads N] [--compression zstd]
"""

import argparse
import time

import duckdb

COLUMNS = {
    'Event': 'VARCHAR',
    'Stage ID': 'BIGINT',
    'Task Info': 'STRUCT("Task ID" BIGINT, "Launch Time" BIGINT, "Finish Time" BIGINT, "Executor ID" VARCHAR)',
    'Task Metrics': 'STRUCT("Executor Run Time" BIGINT, "JVM GC Time" BIGINT, "Memory Bytes Spilled" BIGINT, '
    '"Disk Bytes Spilled" BIGINT, "Shuffle Read Metrics" STRUCT("Total Records Read" BIGINT, '
    '"Local Bytes Read" BIGINT, "Remote Bytes Read" BIGINT))',
}


def quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('log')
    parser.add_argument('--threads', type=int)
    parser.add_argument('--compression', default='uncompressed')
    args = parser.parse_args()

    columns = '{' + ', '.join(f'{quoted(name)}: {quoted(kind)}' for name, kind in COLUMNS.items()) + '}'
    query = f"""
        select "Stage ID" stage_id, count(*) n,
          median("Task Metrics"."Executor Run Time") p50, max("Task Metrics"."Executor Run Time") mx,
          sum("Task Metrics"."Memory Bytes Spilled") spill, sum("Task Metrics"."JVM GC Time") gc,
          max("Task Metrics"."Shuffle Read Metrics"."Total Records Read") mxr
        from read_json({quoted(args.log)}, format='newline_delimited', compression={quoted(args.compression)},
          maximum_object_size=67108864, columns={columns})
        where Event = 'SparkListenerTaskEnd'
        group by 1 order by 1
    """
    connection = duckdb.connect()
    if args.threads:
        connection.execute(f'set threads = {args.threads}')
    start = time.perf_counter()
    rows = connection.execute(query).fetchall()
    elapsed = time.perf_counter() - start
    label = f'duckdb-threads{args.threads}' if args.threads else 'duckdb'
    if args.compression != 'uncompressed':
        label += f'-{args.compression}'
    print(f'{label} wall={elapsed:.2f}s stages={len(rows)} tasks={sum(row[1] for row in rows)}')


if __name__ == '__main__':
    main()
