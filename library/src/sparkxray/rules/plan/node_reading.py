"""What a plan node printed and measured, read the same way from every form a plan arrives in.

A plan read from an event log carries each node's metric totals, and its formatted details where
the log held the text too. Simple explain text carries neither, and prints a scan's fields inside
its label. There Spark cuts each value to `spark.sql.maxMetadataStringLength`, 100 characters by
default, ending it in `...`. So a field is read from the details first and the label next, and a
rule needing a whole value checks it was not cut.
"""

from __future__ import annotations

import dataclasses as dc
import re
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence

from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import PlanNode
from sparkxray.rules.rule import RuleContext

OUTPUT_ROWS = 'number of output rows'
FILE_SCAN_OPERATORS = frozenset({'Scan', 'BatchScan'})
CUT_MARK = '...'

# Formatted text leaves out a scan's filter list when it is empty, and the label of a node read only from it carries none.
_LISTS_FORMATTED_TEXT_OMITS = frozenset({'PushedFilters', 'PartitionFilters', 'DataFilters'})
_NEXT_LABEL_FIELD = re.compile(r', (?=[A-Z][A-Za-z]*: )')
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


def field(node: PlanNode, key: str) -> str | None:
    """A field the node printed, from its formatted details, or else from the `Key: value` its label carries."""
    if key in node.details:
        return node.details[key]
    marker = f' {key}: '
    start = node.label.find(marker)
    if start >= 0:
        value_start = start + len(marker)
        end = _NEXT_LABEL_FIELD.search(node.label, value_start)
        return node.label[value_start : end.start() if end else len(node.label)]
    if key in _LISTS_FORMATTED_TEXT_OMITS and node.formatted_number is not None and 'Location' in node.details:
        return '[]'
    return None


def is_cut(value: str) -> bool:
    """Whether Spark cut this value short, as it does any scan field past 100 characters in simple text."""
    return value.endswith(CUT_MARK)


def scan_location(node: PlanNode) -> str | None:
    """Where a file scan reads, as the plan printed it, or None for any other node."""
    return field(node, 'Location') if node.operator in FILE_SCAN_OPERATORS else None


def filter_condition(node: PlanNode) -> str | None:
    """A `Filter`'s condition, from its formatted `Condition` or from its label, or None for any other node."""
    if node.operator != 'Filter':
        return None
    if 'Condition' in node.details:
        return node.details['Condition']
    return node.label.removeprefix('Filter ') if node.label.startswith('Filter ') else None


def join_type(node: PlanNode) -> str | None:
    """A join's type as Spark printed it, such as `Inner` or `LeftOuter`, from its formatted details or its label's third argument."""
    if 'Join type' in node.details:
        return node.details['Join type'].strip()
    arguments = split_top_level(node.label.partition(' ')[2], ',')
    return arguments[2] if len(arguments) > 2 else None


def broadcastable_sides(node: PlanNode) -> tuple[bool, bool]:
    """Whether Spark could broadcast a join's left side, and its right, by its join type. An unknown type allows neither.

    An existence join prints with its attribute, as `ExistenceJoin(exists#12)`, so the name ends at its parenthesis.
    """
    printed = join_type(node)
    if printed is None:
        return False, False
    name = printed.split('(', 1)[0]
    return name in _BROADCASTABLE_LEFT, name in _BROADCASTABLE_RIGHT


def list_entries(value: str) -> list[str]:
    """The entries of a bracketed list Spark printed, such as `[IsNotNull(id), GreaterThan(id,10)]`."""
    inner = value.strip()
    if inner.startswith('[') and inner.endswith(']'):
        inner = inner[1:-1]
    return [entry for entry in split_top_level(inner, ',') if entry]


def split_top_level(text: str, separator: str, *, angle_brackets: bool = False) -> list[str]:
    """Split on `separator` wherever it stands outside every bracket and parenthesis.

    `angle_brackets` nests on `<` and `>` too, as a schema's `struct<...>` does. A condition
    compares with them, so it is split without.
    """
    openers, closers = ('([<', ')]>') if angle_brackets else ('([', ')]')
    parts = []
    depth = 0
    start = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char in openers:
            depth += 1
        elif char in closers and depth > 0:
            depth -= 1
        elif depth == 0 and text.startswith(separator, index):
            parts.append(text[start:index].strip())
            index += len(separator)
            start = index
            continue
        index += 1
    parts.append(text[start:].strip())
    return parts


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
