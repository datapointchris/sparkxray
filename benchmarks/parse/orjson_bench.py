# /// script
# requires-python = ">=3.11"
# dependencies = ["orjson==3.13.0"]
# ///
"""orjson arm of the parse benchmark: every line parsed in Python.

    uv run --script orjson_bench.py LOG all|prefilter

`prefilter` skips a line unless it contains the TaskEnd event name, before parsing it.
"""

import statistics
import sys
import time
from collections import defaultdict

import orjson

NEEDLE = b'"Event":"SparkListenerTaskEnd"'


def main() -> None:
    path, mode = sys.argv[1], sys.argv[2]
    start = time.perf_counter()
    run_ms: defaultdict[int, list[int]] = defaultdict(list)
    spill: defaultdict[int, int] = defaultdict(int)
    gc: defaultdict[int, int] = defaultdict(int)
    records: defaultdict[int, int] = defaultdict(int)
    with open(path, 'rb') as log:
        for line in log:
            if mode == 'prefilter' and NEEDLE not in line:
                continue
            event = orjson.loads(line)
            if event['Event'] != 'SparkListenerTaskEnd':
                continue
            stage = event['Stage ID']
            metrics = event.get('Task Metrics') or {}
            run_ms[stage].append(metrics.get('Executor Run Time', 0))
            spill[stage] += metrics.get('Memory Bytes Spilled', 0)
            gc[stage] += metrics.get('JVM GC Time', 0)
            read = metrics.get('Shuffle Read Metrics') or {}
            records[stage] = max(records[stage], read.get('Total Records Read', 0))
    rows = [
        (stage, len(times), statistics.median(times), max(times), spill[stage], gc[stage], records[stage])
        for stage, times in sorted(run_ms.items())
    ]
    elapsed = time.perf_counter() - start
    print(f'orjson-{mode} wall={elapsed:.2f}s stages={len(rows)} tasks={sum(row[1] for row in rows)}')


if __name__ == '__main__':
    main()
