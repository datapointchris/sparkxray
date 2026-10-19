"""Rules over the size of the files a plan reads and writes, from the scan and write nodes' metrics."""

from __future__ import annotations

import math
from collections.abc import Iterator
from typing import Any

from sparkxray.rules.plan.node_reading import FILE_SCAN_OPERATORS
from sparkxray.rules.plan.node_reading import counted_nodes
from sparkxray.rules.plan.node_reading import metric
from sparkxray.rules.plan.node_reading import scan_location
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold

_INPUT_OUTPUT_ALERTS = 'DataFlint spark-ui/src/reducers/Alerts/MemorySQLInputOutputAlerts.ts'
_MIB = 2**20


def _small_files_read(context: RuleContext) -> Iterator[dict[str, Any]]:
    small, many = context.thresholds['small_file_bytes'], context.thresholds['many_files']
    for execution_id, _, numbered in counted_nodes(context):
        if numbered.node.operator not in FILE_SCAN_OPERATORS:
            continue
        metrics = context.metrics(execution_id, numbered.number)
        files, size = metric(metrics, 'number of files read'), metric(metrics, 'size of files read')
        if not files or size is None or files <= many or size / files >= small:
            continue
        yield {
            'sql_execution_id': execution_id,
            'plan_node_id': numbered.number,
            'severity': 'medium',
            'impact_seconds': None,
            'scan_location': scan_location(numbered.node),
            'files_read_count': files,
            'read_bytes': size,
            'average_file_bytes': size / files,
        }


_SMALL_FILES_READ_LESSON = """\
## What it measures

A file scan reading more than 100 files that average under 3 MiB each, DataFlint's thresholds. The
average is the scan's `size of files read` over its `number of files read`.

## Why it costs time

Each file costs a listing entry, an open and a footer read before any of its data, and on object
storage each of those is a request. Spark packs small files into one task up to
`spark.sql.files.maxPartitionBytes`, counting `spark.sql.files.openCostInBytes` for each file. So
many small files still mean many tasks, and far more overhead than bytes.

## Where the Spark UI shows it

SQL tab › the query › the scan node: `number of files read` and `size of files read`.

## How to fix it

- Compact the table into fewer, larger files, of 128 MiB or more.
- Fix the write that produced them, which `plan.small-files-written` reports.
- Raise `spark.sql.files.maxPartitionBytes` or `spark.sql.files.openCostInBytes`, so each task
  packs more files.
"""

SMALL_FILES_READ = Rule(
    rule_id='plan.small-files-read',
    title='A scan read {files_read_count:,} files averaging {average_file_bytes:bytes}',
    signals=frozenset({'plan', 'plan_metrics'}),
    scope='plan_node',
    thresholds=(
        Threshold('small_file_bytes', 3 * _MIB, 'bytes', _INPUT_OUTPUT_ALERTS),
        Threshold('many_files', 100, 'count', _INPUT_OUTPUT_ALERTS),
    ),
    severity_ladder={'medium': 'the scan read more than many_files files averaging under small_file_bytes'},
    remediation=(
        'ask the owner of the data to compact it into larger files',
        'fix the write that produced the small files',
        'raise spark.sql.files.maxPartitionBytes or spark.sql.files.openCostInBytes, so each task packs more files',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#tuning-partitions',),
    lesson=_SMALL_FILES_READ_LESSON,
    check=_small_files_read,
)


def _small_files_written(context: RuleContext) -> Iterator[dict[str, Any]]:
    small, many, target = (context.thresholds[name] for name in ('small_file_bytes', 'many_files', 'target_file_bytes'))
    for execution_id, _, numbered in counted_nodes(context):
        metrics = context.metrics(execution_id, numbered.number)
        files, size = metric(metrics, 'number of written files'), metric(metrics, 'written output')
        if not files or size is None or files <= many or size / files >= small:
            continue
        yield {
            'sql_execution_id': execution_id,
            'plan_node_id': numbered.number,
            'severity': 'medium',
            'impact_seconds': None,
            'written_files_count': files,
            'written_bytes': size,
            'average_file_bytes': size / files,
            'dynamic_partitions_count': metric(metrics, 'number of dynamic part'),
            'suggested_file_count': max(1, math.ceil(size / target)),
        }


_SMALL_FILES_WRITTEN_LESSON = """\
## What it measures

A write of more than 100 files that average under 3 MiB each, DataFlint's thresholds, from the
write node's `written output` over its `number of written files`. `suggested_file_count` is how
many files the same bytes make at 128 MiB each, DataFlint's ideal file size.

## Why it costs time

Each task writes at least one file for every output partition it holds. So a write straight after
a wide shuffle, or after `repartition(200)`, writes a small file per task. The write pays to
create and commit each file, and every later read pays to list and open them.

## Where the Spark UI shows it

SQL tab › the query › the write node, such as `InsertIntoHadoopFsRelationCommand`: `number of
written files`, `written output`, and `number of dynamic part` for a partitioned write.

## How to fix it

- `repartition(n)` or `coalesce(n)` before the write, with n near `suggested_file_count`.
- For a partitioned write, `repartition` by the partition columns first, so one task writes each
  partition.
- Under AQE, `spark.sql.adaptive.advisoryPartitionSizeInBytes` sets how large the last shuffle's
  partitions grow, and so the files written from them.
"""

SMALL_FILES_WRITTEN = Rule(
    rule_id='plan.small-files-written',
    title='A write made {written_files_count:,} files averaging {average_file_bytes:bytes}',
    signals=frozenset({'plan', 'plan_metrics'}),
    scope='plan_node',
    thresholds=(
        Threshold('small_file_bytes', 3 * _MIB, 'bytes', _INPUT_OUTPUT_ALERTS),
        Threshold('many_files', 100, 'count', _INPUT_OUTPUT_ALERTS),
        Threshold('target_file_bytes', 128 * _MIB, 'bytes', _INPUT_OUTPUT_ALERTS),
    ),
    severity_ladder={'medium': 'the write made more than many_files files averaging under small_file_bytes'},
    remediation=(
        'repartition before the write, to about suggested_file_count partitions',
        'repartition by the partition columns before a partitioned write',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions',),
    lesson=_SMALL_FILES_WRITTEN_LESSON,
    check=_small_files_written,
)

RULES = (SMALL_FILES_READ, SMALL_FILES_WRITTEN)
