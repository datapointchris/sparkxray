"""What a plan node printed, read the same way from every form a plan arrives in.

A plan read from an event log carries each node's formatted details where the log held the text
too. Simple explain text carries none, and prints a scan's fields inside its label. There Spark
cuts each value to `spark.sql.maxMetadataStringLength`, 100 characters by default, ending it in
`...`. So a field is read from the details first and the label next, and a reader needing a whole
value checks it was not cut.
"""

from __future__ import annotations

import re

from sparkxray.plan import labels
from sparkxray.plan.tree import PlanNode

FILE_SCAN_OPERATORS = frozenset({'Scan', 'BatchScan'})
CUT_MARK = '...'

# Formatted text leaves out a scan's filter list when it is empty, and the label of a node read only from it carries none.
_LISTS_FORMATTED_TEXT_OMITS = frozenset({'PushedFilters', 'PartitionFilters', 'DataFilters'})
_NEXT_LABEL_FIELD = re.compile(r', (?=[A-Z][A-Za-z]*: )')


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
    """A join's type as Spark printed it, such as `Inner` or `LeftOuter`, from its formatted details or its label's arguments."""
    if 'Join type' in node.details:
        return node.details['Join type'].strip()
    return labels.join_type(node.label)


def join_keys(node: PlanNode) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    """A join's left and right keys as Spark printed them, ids included, or None for a join on a condition alone.

    Formatted text lists them as `Left keys` and `Right keys`. A label lists them as its first two
    arguments, as in `SortMergeJoin [k#2L], [k#10L], Inner`.
    """
    left, right = node.details.get('Left keys'), node.details.get('Right keys')
    if left is None or right is None:
        arguments = split_top_level(node.label.partition(' ')[2], ',')
        if len(arguments) < 2 or not all(argument.startswith('[') for argument in arguments[:2]):
            return None
        left, right = arguments[0], arguments[1]
    return tuple(list_entries(left)), tuple(list_entries(right))


def partitioning(node: PlanNode) -> str | None:
    """How an `Exchange` partitions its output, as in `hashpartitioning(k#2L, 200), ENSURE_REQUIREMENTS`, or None for any other node.

    A `ReusedExchange` prints the exchange it reads again after its output, so its partitioning is read from there.
    """
    if node.operator == 'Exchange':
        return field(node, 'Arguments') or node.label.removeprefix('Exchange').strip()
    if node.operator == 'ReusedExchange':
        _, printed, reused = node.label.partition(', Exchange ')
        return reused if printed else None
    return None


def partition_count(node: PlanNode) -> int | None:
    """How many partitions an `Exchange` writes: the count its partitioning ends with, or 1 for `SinglePartition`."""
    printed = partitioning(node)
    if printed is None:
        return None
    scheme = split_top_level(printed, ',')[0]
    if scheme == 'SinglePartition':
        return 1
    arguments = scheme.partition('(')[2]
    if not arguments.endswith(')'):
        return None
    count = split_top_level(arguments[:-1], ',')[-1]
    return int(count) if count.isdigit() else None


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
