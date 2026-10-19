"""What a plan node printed and measured, read the same way from every form a plan arrives in.

A plan read from an event log carries each node's metric totals, and its formatted details where
the log held the text too. Simple explain text carries neither, and prints a scan's fields inside
its label. There Spark cuts each value to `spark.sql.maxMetadataStringLength`, 100 characters by
default, ending it in `...`. So a field is read from the details first and the label next, and a
rule needing a whole value checks it was not cut.
"""

from __future__ import annotations

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
_SIZE_KEEPING = frozenset({'Sort', 'AQEShuffleRead', 'Coalesce', 'Filter', 'ColumnarToRow', 'Exchange', 'ShuffleQueryStage'})


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


def under_cache(nodes: Sequence[NumberedNode]) -> frozenset[int]:
    """The numbers of the nodes under each `InMemoryTableScan`: the plan that filled the cache, which ran once."""
    found: set[int] = set()
    for numbered in nodes:
        if numbered.node.operator == 'InMemoryTableScan':
            for child in numbered.children:
                found.update(subtree(nodes, child))
    return frozenset(found)


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


def bytes_produced(context: RuleContext, execution_id: int | None, nodes: Sequence[NumberedNode], number: int) -> int | None:
    """The bytes one input of a join holds: the nearest size measured down its chain, through nodes that keep its size.

    A finished query stage's measured size counts first, an exchange's `data size` next, and a
    file scan's `size of files read` last. A node that can change the size, such as a projection
    or an aggregate, ends the walk with no size.
    """
    while True:
        numbered = nodes[number]
        node = numbered.node
        if node.operator == 'ShuffleQueryStage' and node.runtime_stats is not None and node.runtime_stats.size_bytes is not None:
            return node.runtime_stats.size_bytes
        metrics = context.metrics(execution_id, number)
        if node.operator == 'Exchange' and 'data size' in metrics:
            return int(metrics['data size'])
        if node.operator in FILE_SCAN_OPERATORS and 'size of files read' in metrics:
            return int(metrics['size of files read'])
        if node.operator not in _SIZE_KEEPING or len(numbered.children) != 1:
            return None
        number = numbered.children[0]


def metric(metrics: Mapping[str, float], name: str) -> int | None:
    """A metric total as a whole number, or None where the node did not report it."""
    value = metrics.get(name)
    return None if value is None else int(value)
