"""Rules over how a plan joins: a join that could broadcast, a broadcast too large, a Cartesian product, and a join that multiplies rows."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from sparkxray.rules.plan.node_reading import OUTPUT_ROWS
from sparkxray.rules.plan.node_reading import bytes_produced
from sparkxray.rules.plan.node_reading import metric
from sparkxray.rules.plan.node_reading import rows_produced
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold

_CROSS_JOINS = frozenset({'CartesianProduct', 'BroadcastNestedLoopJoin'})
_JOIN_TO_BROADCAST = 'DataFlint spark-ui/src/reducers/Alerts/JoinToBroadcastAlert.ts'
_MIB = 2**20
_GIB = 2**30


def _smj_small_side(context: RuleContext) -> Iterator[dict[str, Any]]:
    bands = context.thresholds
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            if numbered.node.operator != 'SortMergeJoin' or len(numbered.children) != 2:
                continue
            left, right = (bytes_produced(context, execution_id, nodes, child) for child in numbered.children)
            if left is None or right is None:
                continue
            small, large = min(left, right), max(left, right)
            broadcastable = (
                small < bands['small_bytes']
                or (small < bands['small_bytes_beside_large'] and large > bands['large_bytes'])
                or (small < bands['small_bytes_beside_very_large'] and large > bands['very_large_bytes'])
                or (small < bands['small_bytes_beside_huge'] and large > bands['huge_bytes'])
            )
            if not broadcastable:
                continue
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'high' if large > bands['large_bytes'] else 'medium',
                'impact_seconds': None,
                'small_side_bytes': small,
                'large_side_bytes': large,
                'small_side': 'left' if left <= right else 'right',
            }


_SMJ_SMALL_SIDE_LESSON = """\
## What it measures

A `SortMergeJoin` shuffles both inputs by the join key, then sorts each partition before matching
rows. When one input is small enough to copy to every executor, a broadcast hash join needs neither
the large side's shuffle nor its sort. Each side's size is the nearest one measured below the join:
a finished query stage's size, an `Exchange`'s `data size`, or a scan's `size of files read`.

The rule fires on DataFlint's bands: the small side under 10 MiB; under 100 MiB beside a side over
10 GiB; under 1000 MiB beside one over 300 GiB; or under 5 GiB beside one over 1 TiB.

## Why it costs time

The shuffle writes the large side to local disk and reads it back over the network. The sort then
spills wherever a partition outgrows execution memory. A broadcast moves only the small side, once
per executor, and joins the large side where it already sits.

## Where the Spark UI shows it

SQL tab › the query › the `SortMergeJoin` node. Under it, each side's `Exchange` shows `data size`,
and under AQE the `ShuffleQueryStage` above it holds the measured size. The Stages tab shows the
large side's shuffle write.

## How to fix it

- Hint the small side: `broadcast(small_df)`, or `/*+ BROADCAST(t) */` in SQL.
- Raise `spark.sql.autoBroadcastJoinThreshold` above the small side's size. Under AQE,
  `spark.sql.adaptive.autoBroadcastJoinThreshold` decides the join once the side has run.
- Where the side is small only after a filter, `ANALYZE TABLE ... COMPUTE STATISTICS` lets the
  optimizer see its size before the query runs.
"""

SMJ_SMALL_SIDE = Rule(
    rule_id='plan.smj-small-side',
    title='SortMergeJoin shuffles a {large_side_bytes:bytes} side to join a {small_side_bytes:bytes} side small enough to broadcast',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(
        Threshold('small_bytes', 10 * _MIB, 'bytes', _JOIN_TO_BROADCAST),
        Threshold('small_bytes_beside_large', 100 * _MIB, 'bytes', _JOIN_TO_BROADCAST),
        Threshold('large_bytes', 10 * _GIB, 'bytes', _JOIN_TO_BROADCAST),
        Threshold('small_bytes_beside_very_large', 1000 * _MIB, 'bytes', _JOIN_TO_BROADCAST),
        Threshold('very_large_bytes', 300 * _GIB, 'bytes', _JOIN_TO_BROADCAST),
        Threshold('small_bytes_beside_huge', 5 * _GIB, 'bytes', _JOIN_TO_BROADCAST),
        Threshold('huge_bytes', 1024 * _GIB, 'bytes', _JOIN_TO_BROADCAST),
    ),
    severity_ladder={
        'medium': 'the smaller side falls in a band small enough to broadcast',
        'high': 'and the larger side, which the join shuffles, is over large_bytes',
    },
    remediation=(
        'broadcast the smaller side, with broadcast(small_df) or a BROADCAST hint',
        "set spark.sql.autoBroadcastJoinThreshold above the smaller side's size",
        'compute table statistics, so the optimizer sees the size before the query runs',
    ),
    docs=(
        'https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints',
        'https://spark.apache.org/docs/latest/sql-performance-tuning.html#converting-sort-merge-join-to-broadcast-join',
    ),
    lesson=_SMJ_SMALL_SIDE_LESSON,
    check=_smj_small_side,
)


def _broadcast_too_large(context: RuleContext) -> Iterator[dict[str, Any]]:
    max_bytes = context.thresholds['max_bytes']
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            if not numbered.node.operator.endswith('BroadcastExchange'):
                continue
            metrics = context.metrics(execution_id, numbered.number)
            size = metric(metrics, 'data size')
            if size is None or size <= max_bytes:
                continue
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'high',
                'impact_seconds': None,
                'broadcast_bytes': size,
                'broadcast_rows': metric(metrics, OUTPUT_ROWS),
                'collect_time_ms': metric(metrics, 'time to collect'),
                'build_time_ms': metric(metrics, 'time to build'),
                'broadcast_time_ms': metric(metrics, 'time to broadcast'),
            }


_BROADCAST_TOO_LARGE_LESSON = """\
## What it measures

A `BroadcastExchange` collects its input on the driver, builds a hash table from it, and sends a
copy to every executor. The rule reads the exchange's `data size`, the relation broadcast, and
fires above 1 GiB, DataFlint's threshold.

## Why it costs time

The driver holds the whole relation while it builds and sends it, and each executor holds a copy
beside its own work. Past a gigabyte the collect and the build take long enough to meet
`spark.sql.broadcastTimeout`, and the copies take execution memory from every task. Spark fails a
broadcast past 8 GiB outright.

## Where the Spark UI shows it

SQL tab › the query › the `BroadcastExchange` node: `data size`, `time to collect`, `time to build`
and `time to broadcast`.

## How to fix it

- Remove a `broadcast()` or `BROADCAST` hint from the large side, and let Spark plan a sort-merge
  or shuffled hash join.
- Lower `spark.sql.autoBroadcastJoinThreshold`, and `spark.sql.adaptive.autoBroadcastJoinThreshold`
  under AQE, where a high value let the relation through.
- Filter and project the side before the join, so less of it is broadcast.
"""

BROADCAST_TOO_LARGE = Rule(
    rule_id='plan.broadcast-too-large',
    title='BroadcastExchange copies {broadcast_bytes:bytes} to every executor',
    signals=frozenset({'plan', 'plan_metrics'}),
    scope='plan_node',
    thresholds=(Threshold('max_bytes', _GIB, 'bytes', 'DataFlint spark-ui/src/reducers/Alerts/BroadcastTooLargeAlert.ts'),),
    severity_ladder={'high': 'the broadcast relation is over max_bytes'},
    remediation=(
        'remove the broadcast hint from the large side',
        'lower spark.sql.autoBroadcastJoinThreshold where a high value let the relation through',
        'filter and project the side before the join, so less of it is broadcast',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints',),
    lesson=_BROADCAST_TOO_LARGE_LESSON,
    check=_broadcast_too_large,
)


def _cartesian(context: RuleContext) -> Iterator[dict[str, Any]]:
    critical = context.thresholds['critical_scanned_rows']
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            if numbered.node.operator not in _CROSS_JOINS:
                continue
            inputs = [rows_produced(context, execution_id, nodes, child) for child in numbered.children]
            left, right = inputs if len(inputs) == 2 else (None, None)
            scanned = None if left is None or right is None else left * right
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'critical' if scanned is not None and scanned > critical else 'info',
                'impact_seconds': None,
                'join_operator': numbered.node.operator,
                'left_input_rows': left,
                'right_input_rows': right,
                'cross_join_scanned_rows': scanned,
                'output_rows': metric(context.metrics(execution_id, numbered.number), OUTPUT_ROWS),
            }


_CARTESIAN_LESSON = """\
## What it measures

`CartesianProduct` and `BroadcastNestedLoopJoin` compare every row of one input with every row of
the other. Spark plans one for a join with no equality condition: a cross join, or a condition
such as `a.x < b.y` or `OR` across keys. Their input rows multiply into the rows scanned.

## Why it costs time

A hash or sort-merge join touches each row once. A nested loop touches each pair, so two inputs of
100,000 rows scan 10,000,000,000 pairs. Where the condition keeps few of them, nearly all of that
work is discarded.

## Where the Spark UI shows it

SQL tab › the query › the `CartesianProduct` or `BroadcastNestedLoopJoin` node. Its inputs'
"number of output rows" multiply into the pairs scanned, beside the join's own output rows.

## How to fix it

- Add an equality condition to the join, so Spark plans a hash or sort-merge join.
- For a range condition, bucket both sides on a coarse key, join on the bucket, then filter.
- Where a cross join is intended, shrink both sides first, and broadcast the smaller under
  `spark.sql.autoBroadcastJoinThreshold`.
"""

CARTESIAN = Rule(
    rule_id='plan.cartesian',
    title='{join_operator} compares every row of one input with every row of the other',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(
        Threshold('critical_scanned_rows', 10_000_000_000, 'rows', 'DataFlint spark-ui/src/reducers/Alerts/LargeCrossJoinScanAlert.ts'),
    ),
    severity_ladder={
        'info': 'the plan holds a CartesianProduct or BroadcastNestedLoopJoin',
        'critical': "its inputs' rows multiply to over critical_scanned_rows",
    },
    remediation=(
        'add an equality condition to the join',
        'bucket a range condition on a coarse key, join on the bucket, then filter',
        'shrink both sides before the join, and broadcast the smaller',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html#cross-join',),
    lesson=_CARTESIAN_LESSON,
    check=_cartesian,
)


def _exploding_join(context: RuleContext) -> Iterator[dict[str, Any]]:
    ratio, high_ratio, min_output = (context.thresholds[name] for name in ('ratio', 'high_ratio', 'min_output_rows'))
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            operator = numbered.node.operator
            if operator in _CROSS_JOINS or not (operator.endswith('Join') or operator == 'Generate'):
                continue
            output = metric(context.metrics(execution_id, numbered.number), OUTPUT_ROWS)
            inputs = tuple(rows_produced(context, execution_id, nodes, child) for child in numbered.children)
            measured = [rows for rows in inputs if rows is not None]
            if output is None or output < min_output or not measured or max(measured) == 0:
                continue
            largest = max(measured)
            if output / largest < ratio:
                continue
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'high' if output / largest >= high_ratio else 'medium',
                'impact_seconds': None,
                'node_operator': operator,
                'output_rows': output,
                'input_rows': inputs,
                'largest_input_rows': largest,
                'output_to_input_ratio': output / largest,
            }


_EXPLODING_JOIN_LESSON = """\
## What it measures

A join, or a `Generate` running `explode`, whose output rows are at least 10 times its largest
input's, with at least 100,000,000 rows out. Both thresholds are proposed here, because no tool
publishes one. A join key matching thousands of rows on both sides multiplies them.

## Why it costs time

Every output row is built, carried through each operator above, and often shuffled again. A key
with few distinct values turns two inputs of thousands of rows into hundreds of millions, and such
an output is often aggregated straight back down.

## Where the Spark UI shows it

SQL tab › the query › the join node's `number of output rows`, beside each input's own `number of
output rows`, or under AQE the query stage's row count.

## How to fix it

- Check the join keys are unique on at least one side. A key column left out of the condition is
  the usual cause.
- Aggregate or deduplicate a side before the join, so each key matches one row.
- Where the multiplication is intended, aggregate as soon after it as possible, and size the
  shuffle for the output with `spark.sql.adaptive.advisoryPartitionSizeInBytes`.
"""

EXPLODING_JOIN = Rule(
    rule_id='plan.exploding-join',
    title='{node_operator} emits {output_rows:,} rows, {output_to_input_ratio:,.0f}× its largest input',
    signals=frozenset({'plan', 'plan_metrics'}),
    scope='plan_node',
    thresholds=(
        Threshold('ratio', 10, 'x', 'proposed'),
        Threshold('high_ratio', 100, 'x', 'proposed'),
        Threshold('min_output_rows', 100_000_000, 'rows', 'proposed'),
    ),
    severity_ladder={
        'medium': 'the output is at least ratio times the largest input, and at least min_output_rows',
        'high': 'the output is at least high_ratio times the largest input',
    },
    remediation=(
        'check the join keys are unique on at least one side',
        'aggregate or deduplicate a side before the join',
        'aggregate as soon after the join as possible',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html',),
    lesson=_EXPLODING_JOIN_LESSON,
    check=_exploding_join,
)

RULES = (SMJ_SMALL_SIDE, BROADCAST_TOO_LARGE, CARTESIAN, EXPLODING_JOIN)
