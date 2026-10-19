"""Rules over the optimizer's size estimates, which only `explain(mode='cost')` text prints."""

from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any

from sparkxray.plan.tree import LogicalEstimate
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold


def _relation_bytes(estimates: Sequence[LogicalEstimate], node_id: int) -> int | None:
    """The summed estimates of the relations under a node, each sized from its files, or None where one is unknown."""
    total = 0
    stack = [node_id]
    while stack:
        estimate = estimates[stack.pop()]
        if estimate.children:
            stack.extend(estimate.children)
        elif estimate.size_bytes is None:
            return None
        else:
            total += estimate.size_bytes
    return total


def _estimate_unreliable(context: RuleContext) -> Iterator[dict[str, Any]]:
    ratio = context.thresholds['ratio']
    for execution_id, plan in context.plans.items():
        worst: tuple[float, LogicalEstimate, int] | None = None
        for estimate in plan.estimates:
            if not estimate.children or estimate.size_bytes is None:
                continue
            relations = _relation_bytes(plan.estimates, estimate.node_id)
            if not relations:
                continue
            if worst is None or estimate.size_bytes / relations > worst[0]:
                worst = (estimate.size_bytes / relations, estimate, relations)
        if worst is None or worst[0] <= ratio:
            continue
        found, estimate, relations = worst
        yield {
            'sql_execution_id': execution_id,
            'severity': 'info',
            'impact_seconds': None,
            'logical_operator': estimate.operator,
            'logical_label': estimate.label,
            'estimated_bytes': estimate.size_bytes,
            'relation_bytes': relations,
            'estimate_ratio': found,
        }


_ESTIMATE_UNRELIABLE_LESSON = """\
## What it measures

The optimizer's size estimate for a node of the optimized logical plan, against the summed sizes
of the relations under it, reported for the node estimated furthest over them, at over 100 times.
The threshold is proposed here, because no tool publishes one. Only `explain(mode='cost')` prints
the estimates, and no event log carries them, so this rule runs through `analyze_plan` alone.

## Why it costs time

With the cost-based optimizer off, as it is by default, Spark estimates a join's output as the
product of its inputs' sizes. It sizes each relation without statistics from its files. So two
small tables can join to an estimate of gigabytes. The
estimate chooses the join strategy before anything has run: a side estimated large is never
broadcast up front, and a cost-based join reorder works from the wrong sizes.

## Where the Spark UI shows it

Nowhere: the Spark UI shows the physical plan only. `df.explain(mode='cost')` prints each logical
node's `Statistics(sizeInBytes=...)`.

## How to fix it

- Turn on the cost-based optimizer with `spark.sql.cbo.enabled`, and compute column statistics
  with `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS`. Together they estimate a join from
  row counts and column statistics. Either alone leaves the estimate a product of sizes, because
  with the optimizer off Spark multiplies the inputs' sizes whatever statistics the tables hold.
- Keep AQE on with `spark.sql.adaptive.enabled`, which replans a join from measured sizes once its
  inputs have run.
"""

ESTIMATE_UNRELIABLE = Rule(
    rule_id='plan.estimate-unreliable',
    title='The optimizer estimates {logical_operator} at {estimated_bytes:bytes}, over {relation_bytes:bytes} of input',
    signals=frozenset({'plan', 'plan_estimates'}),
    scope='sql_execution',
    thresholds=(Threshold('ratio', 100, 'x', 'proposed'),),
    severity_ladder={'info': "a node's estimate is over ratio times the relations under it"},
    remediation=(
        'turn on spark.sql.cbo.enabled and run ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS, together',
        'keep AQE on, so joins are replanned from measured sizes',
    ),
    docs=(
        'https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html',
        'https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution',
    ),
    lesson=_ESTIMATE_UNRELIABLE_LESSON,
    check=_estimate_unreliable,
)

RULES = (ESTIMATE_UNRELIABLE,)
