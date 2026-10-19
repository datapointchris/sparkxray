"""Read the SQL graph a History Server serves for one execution."""

from __future__ import annotations

import collections
import re
from collections.abc import Mapping
from typing import Any

from sparkxray.plan import labels
from sparkxray.plan import tree

_CODEGEN_NUMBER = re.compile(r'\((\d+)\)')


def parse_rest_graph(obj: Mapping[str, Any]) -> tree.Plan:
    """Read one execution from `/api/v1/applications/{app}/sql/{id}?details=true`.

    The server builds this graph from the same tree the event log holds, but drops every query
    stage and `InputAdapter` node, so a REST plan has no `ShuffleQueryStage`. It draws a reused
    exchange as a second edge into the original, which becomes a `ReusedExchange` leaf here, as
    JSON and text show it. Metrics arrive by name, with display strings `parse_display_value` reads.
    Raises ValueError when the graph has more than one root.
    """
    nodes = {int(node['nodeId']): node for node in obj.get('nodes') or []}
    clusters = {node_id for node_id, node in nodes.items() if str(node['nodeName']).startswith('WholeStageCodegen')}
    children_of: dict[int, list[int]] = collections.defaultdict(list)
    has_parent: set[int] = set()
    for edge in obj.get('edges') or []:
        child, parent = int(edge['fromId']), int(edge['toId'])
        children_of[parent].append(child)
        has_parent.add(child)
    roots = sorted(node_id for node_id in nodes if node_id not in clusters and node_id not in has_parent)
    if len(roots) > 1:
        raise ValueError(f'a History Server SQL graph has {len(roots)} roots, nodes {roots}; a plan has one')
    seen: set[int] = set()

    def build(node_id: int) -> tree.RawNode:
        seen.add(node_id)
        info = nodes[node_id]
        name = str(info['nodeName']).strip()
        operator, scan_format = labels.canonical_operator(name)
        stage = info.get('wholeStageCodegenId')
        node = tree.RawNode(label=name, operator=operator, metrics=_metrics(info), codegen_stage=int(stage) if stage is not None else None)
        if scan_format:
            node.details['Format'] = scan_format
        cluster = nodes.get(node_id - 1)
        if node_id - 1 in clusters and cluster is not None and _codegen_number(cluster) == node.codegen_stage:
            node.metrics.extend(_metrics(cluster))
        for child_id in sorted(children_of.get(node_id, [])):
            child_operator, _ = labels.canonical_operator(str(nodes[child_id]['nodeName']))
            if child_operator in labels.SUBQUERY_OPERATORS:
                if child_id in seen:
                    continue
                seen.add(child_id)
                below = sorted(children_of.get(child_id, []))
                node.hosted.append(tree.RawSubquery(subquery_id=None, root=build(below[0]) if below else None, is_final=True))
            elif child_id in seen:
                node.children.append(tree.RawNode(label='ReusedExchange', operator='ReusedExchange'))
            else:
                node.children.append(build(child_id))
        return node

    root = build(roots[0]) if roots else None
    execution_id = obj.get('id')
    return tree.freeze(
        tree.RawPlan(root=root, is_final=obj.get('status') != 'RUNNING', truncated=False),
        source='rest_graph',
        execution_id=int(execution_id) if execution_id is not None else None,
        version=None,
    )


def _metrics(info: Mapping[str, Any]) -> list[tree.MetricRef]:
    return [tree.MetricRef(name=str(metric['name']), accumulator_id=None, metric_type=None) for metric in info.get('metrics') or []]


def _codegen_number(info: Mapping[str, Any]) -> int | None:
    match = _CODEGEN_NUMBER.search(str(info['nodeName']))
    return int(match.group(1)) if match else None
