"""Pick each SQL execution's last plan out of an event log's events."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from sparkxray.plan import explain_text
from sparkxray.plan import plan_info
from sparkxray.plan import tree

_PLAN_EVENTS = frozenset({'SparkListenerSQLExecutionStart', 'SparkListenerSQLAdaptiveExecutionUpdate'})


def last_plans(events: Iterable[Mapping[str, Any]]) -> dict[int, tree.Plan]:
    """Map each execution id to the last plan logged for it.

    `SQLExecutionStart` is version 0 and each `SQLAdaptiveExecutionUpdate` the next, in log
    order, because an update carries no timestamp. A finished execution's last plan is its final
    plan. One that never finished, as when the application was killed or a stage failed, keeps the
    last plan AQE reached, with `is_final` False.

    The plan is read from the event's `sparkPlanInfo`, which carries each metric's accumulator
    id. When its `physicalPlanDescription` reads into a tree of the same operators in the same
    order, each node also takes the text's formatted number, details and AQE statistics, and the
    plan takes the text's initial plan.
    """
    latest: dict[int, tuple[int, Mapping[str, Any]]] = {}
    for event in events:
        if str(event.get('Event', '')).rpartition('.')[2] not in _PLAN_EVENTS:
            continue
        execution_id = event.get('executionId')
        if execution_id is None:
            continue
        execution_id = int(execution_id)
        version = latest[execution_id][0] + 1 if execution_id in latest else 0
        latest[execution_id] = (version, event)
    plans = {}
    for execution_id, (version, event) in latest.items():
        plan = plan_for_event(event, execution_id, version)
        if plan is not None:
            plans[execution_id] = plan
    return plans


def plan_for_event(event: Mapping[str, Any], execution_id: int, version: int) -> tree.Plan | None:
    """The plan one `SQLExecutionStart` or `SQLAdaptiveExecutionUpdate` carries, or None when it carries neither form."""
    info = event.get('sparkPlanInfo')
    text = event.get('physicalPlanDescription')
    if info is None:
        if not text:
            return None
        text_plan, source = explain_text.raw_plan_text(str(text))
        return tree.freeze(text_plan, source=source, execution_id=execution_id, version=version)
    raw = plan_info.raw_plan_info(info)
    if text:
        text_plan, _ = explain_text.raw_plan_text(str(text))
        raw.truncated = raw.truncated or text_plan.truncated
        if raw.root is not None and text_plan.root is not None and _aligned(raw.root, text_plan.root):
            _merge(raw.root, text_plan.root)
            if text_plan.initial is not None:
                _share_merged_nodes(raw.root, text_plan.initial.root)
            raw.initial = text_plan.initial
    return tree.freeze(raw, source='plan_info_json', execution_id=execution_id, version=version)


def _aligned(json_node: tree.RawNode, text_node: tree.RawNode) -> bool:
    if json_node.operator != text_node.operator or len(json_node.children) != len(text_node.children):
        return False
    return all(_aligned(left, right) for left, right in zip(json_node.children, text_node.children, strict=True))


def _merge(json_node: tree.RawNode, text_node: tree.RawNode) -> None:
    json_node.formatted_number = text_node.formatted_number
    json_node.runtime_stats = text_node.runtime_stats
    json_node.details = {**json_node.details, **text_node.details}
    for left, right in zip(json_node.children, text_node.children, strict=True):
        _merge(left, right)
    json_subqueries = [hosted for hosted in json_node.hosted if hosted.root is not None]
    text_subqueries = [hosted for hosted in text_node.hosted if hosted.root is not None]
    for left_subquery, right_subquery in zip(json_subqueries, text_subqueries, strict=False):
        if left_subquery.root is not None and right_subquery.root is not None and _aligned(left_subquery.root, right_subquery.root):
            _merge(left_subquery.root, right_subquery.root)


def _share_merged_nodes(final_root: tree.RawNode, initial_root: tree.RawNode | None) -> None:
    """Give each initial-plan node AQE left alone the label, details and metrics of its final node."""
    # Formatted text numbers each plan object once, so one number in both blocks is one node.
    merged: dict[int, tree.RawNode] = {}
    for node in tree.raw_nodes(final_root):
        if node.formatted_number is not None:
            merged.setdefault(node.formatted_number, node)
    for node in tree.raw_nodes(initial_root):
        final = merged.get(node.formatted_number) if node.formatted_number is not None else None
        if final is not None and final.operator == node.operator:
            node.label = final.label
            node.details = dict(final.details)
            node.metrics = list(final.metrics)
