"""What adaptive query execution changed between the plan an execution started from and its last plan.

Spark logs the plan an execution starts from, then each plan AQE re-planned it to once a query
stage had run. The last plan reads each shuffle through an `AQEShuffleRead`, which may coalesce
small partitions, split skewed ones, or read map output locally. A join AQE re-planned keeps its
keys, because expression ids hold across one execution's plans. So a join is matched to itself by
its keys and type, and a changed operator or a skew split shows what AQE did to it.
"""

from __future__ import annotations

import collections
import dataclasses as dc
from collections.abc import Mapping
from typing import Any

from sparkxray.plan.labels import QUERY_STAGE_OPERATORS
from sparkxray.plan.node_fields import join_keys
from sparkxray.plan.node_fields import join_type
from sparkxray.plan.node_fields import partition_count
from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import Plan

# AQE adds these as it creates each stage, transitions by its `postStageCreationRules`, so none changes what a query does.
_ADDED_WITH_EACH_STAGE = QUERY_STAGE_OPERATORS | {'AQEShuffleRead', 'ColumnarToRow', 'RowToColumnar'}
_SKEW_SPLIT_MARK = '(skew=true)'


@dc.dataclass(frozen=True, slots=True)
class OperatorChange:
    """An operator the last plan holds a different number of than the plan the execution started from."""

    operator: str
    initial_count: int
    final_count: int

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class ShuffleRead:
    """One `AQEShuffleRead` of the last plan, and the partitions it read of the shuffle below it.

    `kind` is what Spark printed beside it: `coalesced`, `skewed`, `coalesced and skewed`, or
    `local`, and None where it printed nothing. `exchange_partition_count` is how many partitions
    the `Exchange` below wrote. The counts are the read's metrics, None where nothing measured them.
    """

    node_id: int
    kind: str | None
    exchange_partition_count: int | None
    partition_count: int | None
    coalesced_partition_count: int | None
    skewed_partition_count: int | None
    skewed_split_count: int | None
    empty_partition_count: int | None
    data_bytes: int | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class JoinChange:
    """A join AQE switched to another operator, or whose skewed partitions it split, with its keys as Spark printed them."""

    initial_node_id: int
    final_node_id: int
    initial_operator: str
    final_operator: str
    join_type: str | None
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    skew_split: bool

    def to_json(self) -> dict[str, Any]:
        return {**dc.asdict(self), 'left_keys': list(self.left_keys), 'right_keys': list(self.right_keys)}


@dc.dataclass(frozen=True, slots=True)
class AqeChanges:
    """What AQE changed in one execution's plan. Node ids are numbers in each plan's `Plan.numbered()`."""

    operators: tuple[OperatorChange, ...]
    shuffle_reads: tuple[ShuffleRead, ...]
    joins: tuple[JoinChange, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'operators': [change.to_json() for change in self.operators],
            'shuffle_reads': [read.to_json() for read in self.shuffle_reads],
            'joins': [join.to_json() for join in self.joins],
        }


def aqe_changes(initial: Plan, final: Plan, metrics: Mapping[int, Mapping[str, float]]) -> AqeChanges:
    """Compare the plan an execution started from with its last plan.

    `metrics` gives each node of the last plan its metric totals by name, keyed by its number in
    `final.numbered()`, and may be empty. The operator counts leave out the query stages, shuffle
    reads and row-column transitions every re-planned plan adds.
    """
    before, after = initial.operator_counts(), final.operator_counts()
    operators = tuple(
        OperatorChange(operator, before[operator], after[operator])
        for operator in sorted(set(before) | set(after))
        if operator not in _ADDED_WITH_EACH_STAGE and before[operator] != after[operator]
    )
    nodes = final.numbered()
    reads = tuple(
        _shuffle_read(nodes, numbered, metrics.get(numbered.number, {})) for numbered in nodes if numbered.node.operator == 'AQEShuffleRead'
    )
    return AqeChanges(operators, reads, _join_changes(initial.numbered(), nodes))


def _shuffle_read(nodes: tuple[NumberedNode, ...], read: NumberedNode, metrics: Mapping[str, float]) -> ShuffleRead:
    below = nodes[read.children[0]] if len(read.children) == 1 else read
    while below.node.operator in QUERY_STAGE_OPERATORS and len(below.children) == 1:
        below = nodes[below.children[0]]
    kind = read.node.details.get('Arguments') or read.node.label.removeprefix('AQEShuffleRead').strip()

    def count(name: str) -> int | None:
        value = metrics.get(name)
        return None if value is None else int(value)

    return ShuffleRead(
        node_id=read.number,
        kind=kind or None,
        exchange_partition_count=partition_count(below.node),
        partition_count=count('number of partitions'),
        coalesced_partition_count=count('number of coalesced partitions'),
        skewed_partition_count=count('number of skewed partitions'),
        skewed_split_count=count('number of skewed partition splits'),
        empty_partition_count=count('number of empty partitions'),
        data_bytes=count('partition data size'),
    )


def _join_changes(initial: tuple[NumberedNode, ...], final: tuple[NumberedNode, ...]) -> tuple[JoinChange, ...]:
    """Each join of the last plan paired with the join of the first plan on the same keys and type, in pre-order, where AQE changed it."""
    started: dict[tuple[object, ...], collections.deque[NumberedNode]] = collections.defaultdict(collections.deque)
    for numbered in initial:
        key = _join_key(numbered)
        if key is not None:
            started[key].append(numbered)
    changes = []
    for numbered in final:
        key = _join_key(numbered)
        if key is None or not started[key]:
            continue
        before = started[key].popleft()
        skew_split = _SKEW_SPLIT_MARK in numbered.node.label.partition(' ')[0]
        if before.node.operator != numbered.node.operator or skew_split:
            (left, right), printed_type = key
            changes.append(
                JoinChange(
                    before.number, numbered.number, before.node.operator, numbered.node.operator, printed_type, left, right, skew_split
                )
            )
    return tuple(changes)


def _join_key(numbered: NumberedNode) -> tuple[tuple[tuple[str, ...], tuple[str, ...]], str | None] | None:
    if not numbered.node.operator.endswith('Join'):
        return None
    keys = join_keys(numbered.node)
    return None if keys is None else (keys, join_type(numbered.node))
