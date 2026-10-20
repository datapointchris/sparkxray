"""Read every plan version a log holds into rows for `plan_nodes` and the metric aggregation.

A plan event is version 0 when it is an execution's `SQLExecutionStart`, and the next version for
each `SQLAdaptiveExecutionUpdate` after it, in log order, as `last_plans` numbers them.
"""

from __future__ import annotations

import dataclasses as dc
import json
from typing import Any

import duckdb
import pyarrow as pa

from sparkxray.ingest.events_from_rest import PLAN_METRICS_EVENT
from sparkxray.plan.last_plans import plan_for_event
from sparkxray.plan.tree import NumberedNode

_REST_PLAN_METRICS = PLAN_METRICS_EVENT.rpartition('.')[2]

_NODE_SCHEMA = pa.schema(
    [
        ('execution_id', pa.int64()),
        ('plan_version', pa.int64()),
        ('is_final', pa.bool_()),
        ('node_id', pa.int64()),
        ('parent_id', pa.int64()),
        ('operator_id', pa.int64()),
        ('operator', pa.string()),
        ('label', pa.string()),
        ('normalized_label', pa.string()),
        ('codegen_stage_id', pa.int64()),
        ('enclosing_sort_id', pa.int64()),
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
        # What a server displayed for the metric, where the log was rebuilt from its REST JSON.
        ('display_unit', pa.string()),
        ('display_total', pa.float64()),
        ('display_min', pa.float64()),
        ('display_median', pa.float64()),
        ('display_max', pa.float64()),
        ('display_max_stage_id', pa.int64()),
        ('display_max_task_id', pa.int64()),
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
        f"""
        SELECT kind, executionId, sparkPlanInfo, physicalPlanDescription, sqlPlanMetrics, restPlanMetrics FROM events
        WHERE kind IN (
          'SparkListenerSQLExecutionStart', 'SparkListenerSQLAdaptiveExecutionUpdate', 'SparkListenerSQLAdaptiveSQLMetricUpdates',
          '{_REST_PLAN_METRICS}'
        ) AND executionId IS NOT NULL
        ORDER BY rowid
        """
    )
    next_version: dict[int, int] = {}
    while rows := cursor.fetchmany(64):
        for kind, execution_id, info, text, declared, displayed in rows:
            if kind == 'SparkListenerSQLAdaptiveSQLMetricUpdates':
                # AQE re-planned a subquery, whose plan it never logs, and declares its new nodes' metrics.
                metrics.extend(
                    _metric_row(execution_id, None, None, metric['name'], metric['metricType'], metric['accumulatorId'])
                    for metric in declared or ()
                )
                continue
            if kind == _REST_PLAN_METRICS:
                # It follows the start event of the plan whose nodes it numbers.
                version = next_version.get(execution_id, 1) - 1
                metrics.extend(_displayed_row(execution_id, version, metric) for metric in displayed or ())
                continue
            version = next_version.get(execution_id, 0)
            next_version[execution_id] = version + 1
            event = {'sparkPlanInfo': json.loads(info) if info is not None else None, 'physicalPlanDescription': text}
            plan = plan_for_event(event, execution_id, version)
            if plan is None:
                continue
            versions.append({'execution_id': execution_id, 'plan_version': version, 'is_final': plan.is_final, 'truncated': plan.truncated})
            numbered_nodes = plan.numbered()
            enclosing = _enclosing_sorts(numbered_nodes)
            for numbered in numbered_nodes:
                nodes.append(_node_row(execution_id, version, plan.is_final, numbered, enclosing.get(numbered.number)))
                metrics.extend(
                    _metric_row(execution_id, version, numbered.number, metric.name, metric.metric_type, metric.accumulator_id)
                    for metric in numbered.node.metrics
                    if metric.accumulator_id is not None
                )
    return PlanRows(
        nodes=pa.Table.from_pylist(nodes, schema=_NODE_SCHEMA),
        metrics=pa.Table.from_pylist(metrics, schema=_METRIC_SCHEMA),
        versions=pa.Table.from_pylist(versions, schema=_VERSION_SCHEMA),
    )


def _enclosing_sorts(numbered: tuple[NumberedNode, ...]) -> dict[int, int]:
    """Each node's nearest `Sort` above it in the same task, by number.

    A `Sort` records as its `spill size` all the task spilled while it read its input, so that
    figure counts the spills of every node below it in the task. An exchange or query stage ends
    the task, because its input ran in another stage. So does a subquery's root.
    """
    enclosing: dict[int, int] = {}
    children = {child for each in numbered for child in each.children}
    pending: list[tuple[int, int | None]] = [(each.number, None) for each in numbered if each.number not in children]
    while pending:
        number, sort = pending.pop()
        if sort is not None:
            enclosing[number] = sort
        operator = numbered[number].node.operator
        inner = None if operator.endswith(('Exchange', 'QueryStage')) else number if operator == 'Sort' else sort
        pending += [(child, inner) for child in numbered[number].children]
    return enclosing


def _node_row(execution_id: int, version: int, is_final: bool, numbered: NumberedNode, enclosing_sort_id: int | None) -> dict[str, Any]:
    node = numbered.node
    stats = node.runtime_stats
    return {
        'execution_id': execution_id,
        'plan_version': version,
        'is_final': is_final,
        'node_id': numbered.number,
        'parent_id': numbered.parent,
        'operator_id': node.formatted_number,
        'operator': node.operator,
        'label': node.label,
        'normalized_label': node.normalized_label,
        'codegen_stage_id': node.codegen_stage,
        'enclosing_sort_id': enclosing_sort_id,
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


def _displayed_row(execution_id: int, version: int, metric: dict[str, Any]) -> dict[str, Any]:
    return {
        **_metric_row(execution_id, version, metric['node'], metric['name'], None, None),
        'display_unit': metric['unit'],
        'display_total': metric['total'],
        'display_min': metric['min'],
        'display_median': metric['median'],
        'display_max': metric['max'],
        'display_max_stage_id': metric['maxStageId'],
        'display_max_task_id': metric['maxTaskId'],
    }
