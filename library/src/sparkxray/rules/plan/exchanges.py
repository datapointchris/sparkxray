"""Rules over how a plan shuffles: how many exchanges one query runs, and a window shuffled into one partition."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from sparkxray.plan.node_fields import partitioning
from sparkxray.rules.plan.node_reading import copies_counted_elsewhere
from sparkxray.rules.plan.node_reading import counted_nodes
from sparkxray.rules.plan.node_reading import metric
from sparkxray.rules.plan.node_reading import rows_produced
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold

WINDOW_OPERATORS = frozenset({'Window', 'WindowInPandas', 'ArrowWindowPython'})
# Between a window and the exchange feeding it: the sort it needs, AQE's wrappers, and other windows over the same rows.
_WINDOW_INPUTS = frozenset({'Sort', 'AQEShuffleRead', 'ShuffleQueryStage', 'WindowGroupLimit', *WINDOW_OPERATORS})
_MIB = 2**20


def _many_exchanges(context: RuleContext) -> Iterator[dict[str, Any]]:
    minimum = context.thresholds['min_exchanges']
    elsewhere = copies_counted_elsewhere(context)
    for execution_id, nodes in context.numbered.items():
        skipped = elsewhere.get(execution_id, frozenset())
        exchanges = tuple(numbered.number for numbered in nodes if numbered.node.operator == 'Exchange' and numbered.number not in skipped)
        if len(exchanges) < minimum:
            continue
        yield {
            'sql_execution_id': execution_id,
            'severity': 'info',
            'impact_seconds': None,
            'exchange_count': len(exchanges),
            'exchange_node_ids': exchanges,
        }


_MANY_EXCHANGES_LESSON = """\
## What it measures

How many `Exchange` nodes one query runs, reported at {min_exchanges} or more. Each is
a shuffle: a stage boundary where every row is written out by key and read back.

## Why it costs time

Each shuffle writes its input to local disk, sends it across the network, and starts a new stage.
The count explains where a query's time goes, and grades nothing. A plan with one more exchange can
run faster, when the extra shuffle spreads a skewed key, so the finding is `info`.

## Where the Spark UI shows it

SQL tab › the query: each `Exchange` node, with `shuffle bytes written` and `data size`. Each one
starts a stage on the Stages tab.

## How to fix it

- Partition once by a key several steps share, with `repartition(key)`, so the joins and
  aggregates after it on that key need no exchange of their own.
- Bucket tables joined again and again on one key with `bucketBy`, so their joins shuffle nothing
  while `spark.sql.sources.bucketing.enabled` is on.
- Broadcast a small join side, which removes both of that join's exchanges.
"""

MANY_EXCHANGES = Rule(
    rule_id='plan.many-exchanges',
    title='The query shuffles {exchange_count:plural time}',
    signals=frozenset({'plan'}),
    scope='sql_execution',
    thresholds=(Threshold('min_exchanges', 4, 'count', 'SparkDoctor docs/detections.md'),),
    severity_ladder={'info': 'the query runs at least min_exchanges exchanges'},
    remediation=(
        'partition once by a key several steps share',
        'bucket tables joined again and again on one key',
        'broadcast a small join side',
    ),
    docs=('https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations',),
    lesson=_MANY_EXCHANGES_LESSON,
    check=_many_exchanges,
)


def _window_single_partition(context: RuleContext) -> Iterator[dict[str, Any]]:
    min_bytes = context.thresholds['min_bytes']
    for execution_id, nodes, numbered in counted_nodes(context):
        if numbered.node.operator not in WINDOW_OPERATORS or len(numbered.children) != 1:
            continue
        below = nodes[numbered.children[0]]
        stage_bytes = None
        while below.node.operator in _WINDOW_INPUTS and len(below.children) == 1:
            stats = below.node.runtime_stats
            if below.node.operator == 'ShuffleQueryStage' and stats is not None and stats.size_bytes is not None:
                stage_bytes = stats.size_bytes
            below = nodes[below.children[0]]
        if below.node.operator != 'Exchange':
            continue
        if not (partitioning(below.node) or '').startswith('SinglePartition'):
            continue
        size = metric(context.metrics(execution_id, below.number), 'data size')
        size = stage_bytes if size is None else size
        yield {
            'sql_execution_id': execution_id,
            'plan_node_id': numbered.number,
            'severity': 'high' if size is not None and size >= min_bytes else 'info',
            'impact_seconds': None,
            'window_operator': numbered.node.operator,
            'exchange_node_id': below.number,
            'single_partition_bytes': size,
            'single_partition_rows': rows_produced(context, execution_id, nodes, numbered.children[0]),
        }


_WINDOW_SINGLE_PARTITION_LESSON = """\
## What it measures

A `Window` whose input is shuffled to `SinglePartition`, which is what a window with an `orderBy`
and no `partitionBy` asks for. Spark logs `No Partition Defined for Window operation! Moving all
data to a single partition, this can cause serious performance degradation.`

The finding is `high` where that partition holds at least {min_bytes}, by default the size at
which AQE's `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` calls one partition
skewed. Below it,
or where nothing measured the exchange, the finding is `info`. A window over a few thousand
aggregated rows runs in one task in moments.

## Why it costs time

Every row of the input moves to one task, which sorts and ranks them alone while every other core
waits. That task's memory bounds the input, and past it the sort spills to disk.

## Where the Spark UI shows it

SQL tab › the query › the `Exchange SinglePartition` node under the `Window`, with its `data size`.
On the Stages tab, the stage after it runs one task.

## How to fix it

- Add a `partitionBy` on a key the ranking can be done within.
- For a global row number, use `monotonically_increasing_id()` where gaps are fine. Otherwise rank
  within partitions, and offset each partition by the row counts before it.
- For a top N, rank within partitions and keep each partition's top N first, so the single
  partition holds only the candidates.
"""

WINDOW_SINGLE_PARTITION = Rule(
    rule_id='plan.window-single-partition',
    title='{window_operator} moves every row into one partition, because the window has no partitionBy',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(Threshold('min_bytes', 256 * _MIB, 'bytes', 'AQE spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'),),
    severity_ladder={
        'info': 'the window reads an Exchange into SinglePartition, under min_bytes or unmeasured',
        'high': 'and the single partition holds at least min_bytes',
    },
    remediation=(
        'add a partitionBy on a key the ranking can be done within',
        'number rows with monotonically_increasing_id() where gaps are fine',
        "for a top N, keep each partition's top N before the global ranking",
    ),
    docs=('https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html',),
    lesson=_WINDOW_SINGLE_PARTITION_LESSON,
    check=_window_single_partition,
)

RULES = (MANY_EXCHANGES, WINDOW_SINGLE_PARTITION)
