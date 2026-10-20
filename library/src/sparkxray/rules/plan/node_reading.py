"""What a plan rule measures across a plan's nodes: rows and bytes down a chain, and a cached plan counted once.

A plan read from an event log carries each node's metric totals, which a rule reads through its
`RuleContext`. Explain text carries none, so every reader here returns None where nothing was
measured. What a node printed is read by `sparkxray.plan.node_fields`.
"""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence

from sparkxray.plan.node_fields import FILE_SCAN_OPERATORS
from sparkxray.plan.node_fields import join_type
from sparkxray.plan.node_fields import scan_location
from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import PlanNode
from sparkxray.rules.rule import RuleContext

OUTPUT_ROWS = 'number of output rows'
# A side's size passes through these, because none of them changes how many bytes the side holds.
_SIZE_KEEPING = frozenset({'Sort', 'AQEShuffleRead', 'Coalesce', 'ColumnarToRow', 'Exchange', 'ShuffleQueryStage'})
_SHUFFLES = frozenset({'Exchange', 'ShuffleQueryStage'})
# canBuildBroadcastLeft and canBuildBroadcastRight in Spark's catalyst/optimizer/joins.scala.
_BROADCASTABLE_LEFT = frozenset({'Inner', 'Cross', 'RightOuter'})
_BROADCASTABLE_RIGHT = frozenset({'Inner', 'Cross', 'LeftOuter', 'LeftSingle', 'LeftSemi', 'LeftAnti', 'ExistenceJoin'})


@dc.dataclass(frozen=True, slots=True)
class SideSize:
    """The bytes one input of a join holds, and whether a shuffle lies between it and the join."""

    size_bytes: int
    shuffled: bool


def broadcastable_sides(node: PlanNode) -> tuple[bool, bool]:
    """Whether Spark could broadcast a join's left side, and its right, by its join type. An unknown type allows neither.

    An existence join prints with its attribute, as `ExistenceJoin(exists#12)`, so the name ends at its parenthesis.
    """
    printed = join_type(node)
    if printed is None:
        return False, False
    name = printed.split('(', 1)[0]
    return name in _BROADCASTABLE_LEFT, name in _BROADCASTABLE_RIGHT


def subtree(nodes: Sequence[NumberedNode], number: int, *, below_cache: bool = True) -> Iterator[int]:
    """The numbers of a node and everything under it, in pre-order.

    With `below_cache` False the walk stops at an `InMemoryTableScan`, whose children are the plan
    that filled the cache once rather than work this query repeats.
    """
    stack = [number]
    while stack:
        current = stack.pop()
        yield current
        if below_cache or nodes[current].node.operator != 'InMemoryTableScan':
            stack.extend(reversed(nodes[current].children))


def copies_counted_elsewhere(context: RuleContext) -> dict[int | None, frozenset[int]]:
    """By execution, the nodes of each copy of a cached plan that another copy counts.

    Spark prints a cached relation's plan under every `InMemoryTableScan` reading it, and every
    copy shares the accumulators of the one run that filled the cache. So each cached plan counts
    once: at the first copy whose metrics show it ran, or at its first copy where none does. Copies
    match by their accumulator ids, or in a plan read from text, which prints none, by their hash
    and the locations their scans read.
    """
    copies: dict[tuple[object, ...], list[tuple[int | None, frozenset[int], bool]]] = {}
    for execution_id in sorted(context.numbered, key=lambda execution: -1 if execution is None else execution):
        nodes = context.numbered[execution_id]
        for numbered in nodes:
            if numbered.node.operator != 'InMemoryTableScan':
                continue
            members = frozenset(member for child in numbered.children for member in subtree(nodes, child))
            accumulators = frozenset(ref.accumulator_id for member in members for ref in nodes[member].node.metrics)
            printed = (
                tuple(nodes[child].node.subtree_hash for child in numbered.children),
                tuple(sorted(location for member in members if (location := scan_location(nodes[member].node)) is not None)),
            )
            ran = any(context.metrics(execution_id, member) for member in members)
            copies.setdefault(('accumulators', accumulators) if accumulators else ('printed', *printed), []).append(
                (execution_id, members, ran)
            )
    elsewhere: dict[int | None, set[int]] = {}
    for found in copies.values():
        counted = next((index for index, (_, _, ran) in enumerate(found) if ran), 0)
        for index, (execution_id, members, _) in enumerate(found):
            if index != counted:
                elsewhere.setdefault(execution_id, set()).update(members)
    return {execution_id: frozenset(members) for execution_id, members in elsewhere.items()}


def counted_nodes(context: RuleContext) -> Iterator[tuple[int | None, tuple[NumberedNode, ...], NumberedNode]]:
    """Every node of every plan with its execution id and plan, leaving out the copies of a cached plan that another copy counts."""
    elsewhere = copies_counted_elsewhere(context)
    for execution_id, nodes in context.numbered.items():
        skipped = elsewhere.get(execution_id, frozenset())
        for numbered in nodes:
            if numbered.number not in skipped:
                yield execution_id, nodes, numbered


def rows_produced(context: RuleContext, execution_id: int | None, nodes: Sequence[NumberedNode], number: int) -> int | None:
    """The rows one input of a node produced: the first count measured down its chain of single-child nodes.

    A node's `number of output rows` metric counts first, and a finished query stage's measured
    row count next. An exchange or a sort counts no rows of its own, so the walk passes it.
    """
    while True:
        numbered = nodes[number]
        rows = context.metrics(execution_id, number).get(OUTPUT_ROWS)
        if rows is not None:
            return int(rows)
        stats = numbered.node.runtime_stats
        if stats is not None and stats.row_count is not None:
            return stats.row_count
        if len(numbered.children) != 1:
            return None
        number = numbered.children[0]


def bytes_produced(context: RuleContext, execution_id: int | None, nodes: Sequence[NumberedNode], number: int) -> SideSize | None:
    """The bytes one input of a join holds: the nearest size measured down its chain, through nodes that keep its size.

    A finished query stage's measured size counts first, an exchange's `data size` next, and a
    file scan's `size of files read` last. A node that can change the size, such as a filter, a
    projection or an aggregate, ends the walk with no size.
    """
    shuffled = False
    while True:
        numbered = nodes[number]
        node = numbered.node
        shuffled = shuffled or node.operator in _SHUFFLES
        if node.operator == 'ShuffleQueryStage' and node.runtime_stats is not None and node.runtime_stats.size_bytes is not None:
            return SideSize(node.runtime_stats.size_bytes, shuffled)
        metrics = context.metrics(execution_id, number)
        if node.operator == 'Exchange' and 'data size' in metrics:
            return SideSize(int(metrics['data size']), shuffled)
        if node.operator in FILE_SCAN_OPERATORS and 'size of files read' in metrics:
            return SideSize(int(metrics['size of files read']), shuffled)
        if node.operator not in _SIZE_KEEPING or len(numbered.children) != 1:
            return None
        number = numbered.children[0]


def metric(metrics: Mapping[str, float], name: str) -> int | None:
    """A metric total as a whole number, or None where the node did not report it."""
    value = metrics.get(name)
    return None if value is None else int(value)
