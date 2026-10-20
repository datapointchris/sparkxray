"""Rules over a query's physical plan, which run on a bare plan as well as on a log."""

from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any

from sparkxray.plan.tree import NumberedNode
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold

_CROSS_JOINS = frozenset({'CartesianProduct', 'BroadcastNestedLoopJoin'})
_OUTPUT_ROWS = 'number of output rows'


def _cartesian(context: RuleContext) -> Iterator[dict[str, Any]]:
    critical = context.thresholds['critical_scanned_rows']
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            if numbered.node.operator not in _CROSS_JOINS:
                continue
            inputs = [_input_rows(context, execution_id, nodes, child) for child in numbered.children]
            left, right = inputs if len(inputs) == 2 else (None, None)
            scanned = None if left is None or right is None else left * right
            output = context.metrics(execution_id, numbered.number).get(_OUTPUT_ROWS)
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'critical' if scanned is not None and scanned > critical else 'info',
                'impact_seconds': None,
                'join_operator': numbered.node.operator,
                'left_input_rows': left,
                'right_input_rows': right,
                'cross_join_scanned_rows': scanned,
                'output_rows': None if output is None else int(output),
            }


def _input_rows(context: RuleContext, execution_id: int | None, nodes: Sequence[NumberedNode], number: int) -> int | None:
    """The rows one input of a join produced: the first count measured down its chain of single-child nodes.

    A node's `number of output rows` metric counts first, and a finished query stage's measured
    row count next. An exchange or a sort counts no rows of its own, so the walk passes it.
    """
    while True:
        numbered = nodes[number]
        rows = context.metrics(execution_id, number).get(_OUTPUT_ROWS)
        if rows is not None:
            return int(rows)
        stats = numbered.node.runtime_stats
        if stats is not None and stats.row_count is not None:
            return stats.row_count
        if len(numbered.children) != 1:
            return None
        number = numbered.children[0]


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

RULES = (CARTESIAN,)
