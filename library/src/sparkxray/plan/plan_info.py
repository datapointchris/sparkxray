"""Read the `sparkPlanInfo` tree an event log writes with every SQL plan version."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from sparkxray.plan import labels
from sparkxray.plan import tree

_CODEGEN_NUMBER = re.compile(r'\((\d+)\)')


def parse_plan_info(obj: Mapping[str, Any], *, execution_id: int | None = None, version: int = 0) -> tree.Plan:
    """Read one `sparkPlanInfo` object, from `SQLExecutionStart` or `SQLAdaptiveExecutionUpdate`.

    Folds the wrappers formatted text never prints. A `WholeStageCodegen (n)` node is removed and
    `n` becomes the `codegen_stage` of each node inside it, while its `duration` metric moves to
    the top operator of that stage. An `InputAdapter` is removed. A `ReusedExchange` keeps none of
    the subtree the JSON repeats under it, and a subquery child becomes a subquery plan. The
    `Subquery` or `SubqueryBroadcast` node wrapping it is removed, and its metrics move to the
    subquery plan's root.
    """
    return tree.freeze(raw_plan_info(obj), source='plan_info_json', execution_id=execution_id, version=version)


def raw_plan_info(obj: Mapping[str, Any]) -> tree.RawPlan:
    root = _convert(obj, codegen_stage=None)
    return tree.RawPlan(root=root, is_final=_is_final(root), truncated=any(_truncated(node) for node in tree.raw_nodes(root)))


def _convert(info: Mapping[str, Any], codegen_stage: int | None) -> tree.RawNode:
    name = str(info.get('nodeName', ''))
    children: list[Mapping[str, Any]] = list(info.get('children') or [])
    if name.startswith('WholeStageCodegen') and children:
        number = _CODEGEN_NUMBER.search(name)
        node = _convert(children[0], int(number.group(1)) if number else None)
        node.metrics.extend(_metrics(info))
        return node
    if name == 'InputAdapter' and children:
        return _convert(children[0], None)
    operator, scan_format = labels.canonical_operator(name)
    label = str(info.get('simpleString', name))
    details = {str(key): str(value) for key, value in (info.get('metadata') or {}).items()}
    if scan_format:
        details['Format'] = scan_format
    if operator.endswith('Join'):
        found = labels.join_type(label)
        if found:
            details['Join type'] = found
    node = tree.RawNode(label=label, operator=operator, details=details, metrics=_metrics(info), codegen_stage=codegen_stage)
    if operator == 'ReusedExchange':
        return node
    for child in children:
        child_operator, _ = labels.canonical_operator(str(child.get('nodeName', '')))
        if child_operator in labels.SUBQUERY_OPERATORS:
            node.hosted.append(_subquery(child, child_operator))
        else:
            node.children.append(_convert(child, codegen_stage))
    return node


def _subquery(info: Mapping[str, Any], operator: str) -> tree.RawSubquery:
    ids = labels.subquery_ids([str(info.get('simpleString', ''))])
    children = list(info.get('children') or [])
    if operator == 'ReusedSubquery' or not children:
        return tree.RawSubquery(subquery_id=ids[0] if ids else None, root=None, is_final=True)
    root = _convert(children[0], None)
    root.metrics.extend(_metrics(info))
    return tree.RawSubquery(subquery_id=ids[0] if ids else None, root=root, is_final=_is_final(root))


def _metrics(info: Mapping[str, Any]) -> list[tree.MetricRef]:
    metrics = []
    for metric in info.get('metrics') or []:
        accumulator = metric.get('accumulatorId')
        metric_type = metric.get('metricType')
        metrics.append(
            tree.MetricRef(
                name=str(metric.get('name', '')),
                accumulator_id=int(accumulator) if accumulator is not None else None,
                metric_type=str(metric_type) if metric_type is not None else None,
            )
        )
    return metrics


def _is_final(root: tree.RawNode) -> bool:
    """An adaptive plan is final once its first `AdaptiveSparkPlan` says so. Any other plan is final as written."""
    for node in tree.raw_nodes(root, subqueries=False):
        if node.operator == 'AdaptiveSparkPlan':
            return 'isFinalPlan=true' in node.label
    return True


def _truncated(node: tree.RawNode) -> bool:
    return labels.is_truncated(node.label) or any(labels.is_truncated(value) for value in node.details.values())
