# /// script
# requires-python = ">=3.11"
# dependencies = ["polars==2.0.0"]
# ///
"""Polars arm of the parse benchmark: `scan_ndjson` with a schema.

uv run --script polars_bench.py LOG
"""

import sys
import time

import polars as pl


def main() -> None:
    path = sys.argv[1]
    schema = {
        'Event': pl.String,
        'Stage ID': pl.Int64,
        'Task Metrics': pl.Struct(
            {
                'Executor Run Time': pl.Int64,
                'JVM GC Time': pl.Int64,
                'Memory Bytes Spilled': pl.Int64,
                'Shuffle Read Metrics': pl.Struct({'Total Records Read': pl.Int64}),
            }
        ),
    }
    start = time.perf_counter()
    metrics = pl.col('Task Metrics').struct
    out = (
        pl.scan_ndjson(path, schema=schema, ignore_errors=True)
        .filter(pl.col('Event') == 'SparkListenerTaskEnd')
        .group_by('Stage ID')
        .agg(
            n=pl.len(),
            p50=metrics.field('Executor Run Time').median(),
            mx=metrics.field('Executor Run Time').max(),
            spill=metrics.field('Memory Bytes Spilled').sum(),
            gc=metrics.field('JVM GC Time').sum(),
            mxr=metrics.field('Shuffle Read Metrics').struct.field('Total Records Read').max(),
        )
        .sort('Stage ID')
        .collect()
    )
    elapsed = time.perf_counter() - start
    print(f'polars wall={elapsed:.2f}s stages={out.height} tasks={out["n"].sum()}')


if __name__ == '__main__':
    main()
