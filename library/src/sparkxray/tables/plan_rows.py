"""Read every plan version a log holds into rows for `plan_nodes` and the metric aggregation.

A plan event is version 0 when it is an execution's `SQLExecutionStart`, and the next version for
each `SQLAdaptiveExecutionUpdate` after it, in log order, as `last_plans` numbers them.
"""

from __future__ import annotations

import dataclasses as dc
import json
from collections.abc import Iterator
from typing import Any

import duckdb
import pyarrow as pa

from sparkxray.plan.last_plans import plan_for_event
from sparkxray.plan.tree import Plan
from sparkxray.plan.tree import PlanNode

_NODE_SCHEMA = pa.schema(
    [
        ('execution_id', pa.int64()),
        ('plan_version', pa.int64()),
        ('is_final', pa.bool_()),
        ('node_id', pa.int64()),
        ('parent_id', pa.int64()),
        ('formatted_number', pa.int64()),
        ('operator', pa.string()),
        ('label', pa.string()),
        ('normalized_label', pa.string()),
        ('codegen_stage', pa.int64()),
        ('subtree_hash', pa.string()),
        ('details', pa.map_(pa.string(), pa.string())),
        ('runtime_size_bytes', pa.int64()),
        ('runtime_row_count', pa.int64()),
    ]
)

_METRIC_SCHEMA = pa.schema(
    [
        ('execution_id', pa.int64()),
        ('plan_version', pa.int64()),
        ('node_id', pa.int64()),
        ('metric_name', pa.string()),
        ('metric_type', pa.string()),
        ('accumulator_id', pa.int64()),
    ]
)

_VERSION_SCHEMA = pa.schema(
    [('execution_id', pa.int64()), ('plan_version', pa.int64()), ('is_final', pa.bool_()), ('truncated', pa.bool_())]
)


@dc.dataclass(frozen=True, slots=True)
class PlanRows:
    nodes: pa.Table
    metrics: pa.Table
    versions: pa.Table


def plan_rows(connection: duckdb.DuckDBPyConnection) -> PlanRows:
    """Every plan version's nodes and metric declarations, from the `events` table."""
    nodes: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    versions: list[dict[str, Any]] = []
    cursor = connection.execute(
        """
        SELECT kind, executionId, sparkPlanInfo, physicalPlanDescription, sqlPlanMetrics FROM events
        WHERE kind IN (
          'SparkListenerSQLExecutionStart', 'SparkListenerSQLAdaptiveExecutionUpdate', 'SparkListenerSQLAdaptiveSQLMetricUpdates'
        ) AND executionId IS NOT NULL
        ORDER BY rowid
        """
    )
    next_version: dict[int, int] = {}
    while rows := cursor.fetchmany(64):
        for kind, execution_id, info, text, declared in rows:
            if kind == 'SparkListenerSQLAdaptiveSQLMetricUpdates':
                # AQE re-planned a subquery, whose plan it never logs, and declares its new nodes' metrics.
                metrics.extend(
                    _metric_row(execution_id, None, None, metric['name'], metric['metricType'], metric['accumulatorId'])
                    for metric in declared or ()
                )
                continue
            version = next_version.get(execution_id, 0)
            next_version[execution_id] = version + 1
            event = {'sparkPlanInfo': json.loads(info) if info is not None else None, 'physicalPlanDescription': text}
            plan = plan_for_event(event, execution_id, version)
            if plan is None:
                continue
            versions.append({'execution_id': execution_id, 'plan_version': version, 'is_final': plan.is_final, 'truncated': plan.truncated})
            for node_id, parent_id, node in _flattened(plan):
                nodes.append(_node_row(execution_id, version, plan.is_final, node_id, parent_id, node))
                metrics.extend(
                    _metric_row(execution_id, version, node_id, metric.name, metric.metric_type, metric.accumulator_id)
                    for metric in node.metrics
                    if metric.accumulator_id is not None
                )
    return PlanRows(
        nodes=pa.Table.from_pylist(nodes, schema=_NODE_SCHEMA),
        metrics=pa.Table.from_pylist(metrics, schema=_METRIC_SCHEMA),
        versions=pa.Table.from_pylist(versions, schema=_VERSION_SCHEMA),
    )


def _flattened(plan: Plan) -> Iterator[tuple[int, int | None, PlanNode]]:
    """Each node with its id and parent in one numbering: the plan's own nodes, then each subquery's, numbered on from the last."""
    count = 0

    def walk(part: Plan, root_parent: int | None) -> Iterator[tuple[int, int | None, PlanNode]]:
        nonlocal count
        offset = count
        count += len(part.nodes)
        for node in part.nodes:
            yield node.node_id + offset, root_parent if node.parent_id is None else node.parent_id + offset, node
        for subquery in part.subqueries:
            hosting = None if subquery.hosting_node_id is None else subquery.hosting_node_id + offset
            yield from walk(subquery, hosting)

    yield from walk(plan, None)


def _node_row(execution_id: int, version: int, is_final: bool, node_id: int, parent_id: int | None, node: PlanNode) -> dict[str, Any]:
    stats = node.runtime_stats
    return {
        'execution_id': execution_id,
        'plan_version': version,
        'is_final': is_final,
        'node_id': node_id,
        'parent_id': parent_id,
        'formatted_number': node.formatted_number,
        'operator': node.operator,
        'label': node.label,
        'normalized_label': node.normalized_label,
        'codegen_stage': node.codegen_stage,
        'subtree_hash': node.subtree_hash,
        'details': list(node.details.items()),
        'runtime_size_bytes': stats.size_bytes if stats else None,
        'runtime_row_count': stats.row_count if stats else None,
    }


def _metric_row(
    execution_id: int, version: int | None, node_id: int | None, name: str, metric_type: str | None, accumulator_id: int | None
) -> dict[str, Any]:
    return {
        'execution_id': execution_id,
        'plan_version': version,
        'node_id': node_id,
        'metric_name': name,
        'metric_type': metric_type,
        'accumulator_id': accumulator_id,
    }
