"""Per-stage task totals of one event log, read with the standard library alone.

    zstd -dc logs/spark-3.5.9/local-*.zstd | python3 stage_totals.py

The numbers a fixture's tests pin come from here, not from DuckDB, so a fault in the analyzer's
own reading cannot agree with itself.
"""

import json
import statistics
import sys
from collections import defaultdict


def main() -> None:
    tasks: defaultdict[int, list[dict[str, int]]] = defaultdict(list)
    descriptions: dict[int, str] = {}
    for line in sys.stdin:
        event = json.loads(line)
        kind = event['Event']
        if kind == 'SparkListenerStageSubmitted':
            properties = event.get('Properties') or {}
            descriptions[event['Stage Info']['Stage ID']] = properties.get('spark.job.description', '')
        elif kind == 'SparkListenerTaskEnd' and event['Task End Reason']['Reason'] == 'Success':
            metrics = event.get('Task Metrics') or {}
            read = metrics.get('Shuffle Read Metrics') or {}
            tasks[event['Stage ID']].append(
                {
                    'run_ms': metrics.get('Executor Run Time', 0),
                    'read_bytes': read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0),
                    'read_records': read.get('Total Records Read', 0),
                    'spill_memory': metrics.get('Memory Bytes Spilled', 0),
                    'spill_disk': metrics.get('Disk Bytes Spilled', 0),
                }
            )
    columns = (
        'stage',
        'tasks',
        'median_run_ms',
        'max_run_ms',
        'max_read_bytes',
        'median_read_bytes',
        'max_read_records',
        'median_read_records',
        'spill_memory',
        'spill_disk',
    )
    print('\t'.join(columns))
    for stage, rows in sorted(tasks.items()):
        values = (
            stage,
            len(rows),
            int(statistics.median(row['run_ms'] for row in rows)),
            max(row['run_ms'] for row in rows),
            max(row['read_bytes'] for row in rows),
            int(statistics.median(row['read_bytes'] for row in rows)),
            max(row['read_records'] for row in rows),
            int(statistics.median(row['read_records'] for row in rows)),
            sum(row['spill_memory'] for row in rows),
            sum(row['spill_disk'] for row in rows),
        )
        print('\t'.join(str(value) for value in values) + '\t' + descriptions.get(stage, '')[:60])


if __name__ == '__main__':
    main()
