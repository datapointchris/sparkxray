"""Rules over work one plan does twice: a table scanned again, a subtree computed again, and an exchange Spark did not reuse.

`subtree_hash` is blind to paths, so two subtrees reading different tables of one schema hash
alike. Each repeat is confirmed by the raw locations its scans read.
"""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any

from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import Plan
from sparkxray.rules.plan.node_reading import CUT_MARK
from sparkxray.rules.plan.node_reading import field
from sparkxray.rules.plan.node_reading import is_cut
from sparkxray.rules.plan.node_reading import scan_location
from sparkxray.rules.plan.node_reading import split_top_level
from sparkxray.rules.plan.node_reading import subtree
from sparkxray.rules.plan.node_reading import under_cache
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold

REUSABLE_EXCHANGES = frozenset({'Exchange', 'BroadcastExchange'})
_REPEATED_SUBTREE_SOURCE = 'sparkDoctor docs/detections.md'
_PLAN_ONLY = "an event log holds the physical plan without the optimizer's reasons, so two subtrees that print alike may still differ"
_EXCHANGE_REUSE_SQL = """
SELECT coalesce(
  (SELECT modified_configs['spark.sql.exchange.reuse'] FROM sql_executions WHERE execution_id = $execution_id),
  (SELECT max(value) FROM environment WHERE section = 'spark' AND key = 'spark.sql.exchange.reuse')
)
"""


@dc.dataclass(frozen=True, slots=True)
class RepeatedSubtree:
    """Subtrees of one plan that print alike and read the same locations, by the numbers of their roots in plan order."""

    roots: tuple[int, ...]
    members: tuple[tuple[int, ...], ...]
    operators: frozenset[str]
    locations: tuple[str, ...]


def repeated_subtrees(nodes: Sequence[NumberedNode], *, min_occurrences: int, min_nodes: int) -> list[RepeatedSubtree]:
    """Each set of subtrees sharing a `subtree_hash` and the raw locations their scans read.

    A subtree counts only its own work, so the walk stops at a cached relation's scan, and nothing
    under one is a root. It needs `min_nodes` nodes, an exchange, join, aggregate, sort or scan
    among them, and no `ReusedExchange`, because a repeat over a reused shuffle is reuse already
    taking place.
    """
    cached = under_cache(nodes)
    by_key: dict[tuple[str, tuple[str, ...]], list[tuple[int, tuple[int, ...]]]] = {}
    operators: dict[tuple[str, tuple[str, ...]], frozenset[str]] = {}
    for numbered in nodes:
        if numbered.number in cached:
            continue
        members = tuple(subtree(nodes, numbered.number, below_cache=False))
        names = frozenset(nodes[member].node.operator for member in members)
        if len(members) < min_nodes or 'ReusedExchange' in names or not any(_does_work(name) for name in names):
            continue
        locations = tuple(location for member in members if (location := scan_location(nodes[member].node)) is not None)
        key = (numbered.node.subtree_hash, locations)
        by_key.setdefault(key, []).append((numbered.number, members))
        operators[key] = names
    return [
        RepeatedSubtree(tuple(root for root, _ in found), tuple(members for _, members in found), operators[key], key[1])
        for key, found in by_key.items()
        if len(found) >= min_occurrences
    ]


def widest(repeats: Sequence[RepeatedSubtree]) -> list[RepeatedSubtree]:
    """The repeats not inside another: drop each one whose every occurrence lies within an occurrence of another."""
    inside: dict[int, set[int]] = {}
    for index, repeat in enumerate(repeats):
        for members in repeat.members:
            for member in members[1:]:
                inside.setdefault(member, set()).add(index)
    return [repeat for index, repeat in enumerate(repeats) if not all(inside.get(root, set()) - {index} for root in repeat.roots)]


def _does_work(operator: str) -> bool:
    return 'Exchange' in operator or operator.endswith('Join') or 'Aggregate' in operator or operator == 'Sort' or 'Scan' in operator


def _exchange_rooted(repeats: Sequence[RepeatedSubtree], nodes: Sequence[NumberedNode]) -> list[RepeatedSubtree]:
    return [repeat for repeat in repeats if nodes[repeat.roots[0]].node.operator in REUSABLE_EXCHANGES]


def _missed_exchange_reuse(context: RuleContext) -> Iterator[dict[str, Any]]:
    occurrences, size = int(context.thresholds['min_occurrences']), int(context.thresholds['min_nodes'])
    for execution_id, nodes in context.numbered.items():
        # AQE reuses an exchange as it creates the stage, so a plan it has not finished shows no reuse yet.
        if not context.plans[execution_id].is_final:
            continue
        setting = None
        if context.tables is not None and execution_id is not None:
            setting = context.tables.query(_EXCHANGE_REUSE_SQL, {'execution_id': execution_id}).fetchall()[0][0]
        for repeat in widest(_exchange_rooted(repeated_subtrees(nodes, min_occurrences=occurrences, min_nodes=size), nodes)):
            reuse_off = setting is not None and setting.strip().lower() == 'false'
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': repeat.roots[0],
                'severity': 'medium',
                'impact_seconds': None,
                'confidence': 'high' if reuse_off else 'low',
                'confidence_reason': None if reuse_off else _PLAN_ONLY,
                'exchange_operator': nodes[repeat.roots[0]].node.operator,
                'occurrence_count': len(repeat.roots),
                'repeated_node_ids': repeat.roots,
                'subtree_node_count': len(repeat.members[0]),
                'scan_locations': repeat.locations,
                'exchange_reuse_setting': setting,
            }


_MISSED_EXCHANGE_REUSE_LESSON = """\
## What it measures

Two or more `Exchange` subtrees of one plan with the same partitioning over the same input, none
of them replaced by a `ReusedExchange`. Each needs at least 3 nodes, and the set at least 2
occurrences, sparkDoctor's thresholds. While `spark.sql.exchange.reuse` is on, Spark replaces a
second identical exchange with a `ReusedExchange` reading the first. So a repeat means reuse was
off, or the two differ in something the printed plan does not show.

An event log holds the physical plan without the optimizer's reasons, so the finding is low
confidence. Where the execution turned `spark.sql.exchange.reuse` off, the reason is known, and
confidence is high.

## Why it costs time

Each copy shuffles its input in full. The map side runs again and writes its output to disk, and
the reduce side reads it over the network again.

## Where the Spark UI shows it

SQL tab › the query: two `Exchange` nodes with the same partitioning, each with its own `shuffle
bytes written`, and no `ReusedExchange` node.

## How to fix it

- Set `spark.sql.exchange.reuse` back to `true`, its default.
- Build both branches from one DataFrame, so their plans match exactly.
- Where the two differ in a detail, `cache()` the shared part.
"""

MISSED_EXCHANGE_REUSE = Rule(
    rule_id='plan.missed-exchange-reuse',
    title='{occurrence_count} identical {exchange_operator} subtrees each shuffle the same input, and Spark reused none',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(
        Threshold('min_occurrences', 2, 'count', _REPEATED_SUBTREE_SOURCE),
        Threshold('min_nodes', 3, 'count', _REPEATED_SUBTREE_SOURCE),
    ),
    severity_ladder={'medium': 'an exchange subtree repeats with no ReusedExchange'},
    remediation=(
        'set spark.sql.exchange.reuse back to true',
        'build both branches from one DataFrame, so their plans match exactly',
        'cache the shared part where the two differ in a detail',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#caching-data',),
    lesson=_MISSED_EXCHANGE_REUSE_LESSON,
    check=_missed_exchange_reuse,
)


def _repeated_subtree(context: RuleContext) -> Iterator[dict[str, Any]]:
    occurrences, size = int(context.thresholds['min_occurrences']), int(context.thresholds['min_nodes'])
    for execution_id, nodes in context.numbered.items():
        final = context.plans[execution_id].is_final
        repeats = repeated_subtrees(nodes, min_occurrences=occurrences, min_nodes=size)
        exchange_roots = {root for repeat in _exchange_rooted(repeats, nodes) for root in repeat.roots}
        for repeat in widest(repeats):
            # A repeated exchange is plan.missed-exchange-reuse's, and in a plan AQE has not finished it may still be reused.
            if any(exchange_roots.intersection(members) for members in repeat.members):
                continue
            if not final and repeat.operators & REUSABLE_EXCHANGES:
                continue
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': repeat.roots[0],
                'severity': 'medium',
                'impact_seconds': None,
                'confidence': 'low',
                'confidence_reason': _PLAN_ONLY,
                'root_operator': nodes[repeat.roots[0]].node.operator,
                'occurrence_count': len(repeat.roots),
                'repeated_node_ids': repeat.roots,
                'subtree_node_count': len(repeat.members[0]),
                'scan_locations': repeat.locations,
            }


_REPEATED_SUBTREE_LESSON = """\
## What it measures

Two or more subtrees of one plan with the same operators, the same expressions once their ids are
normalized, and the same input locations: one computation planned twice. Each has at least 3
nodes, including an exchange, join, aggregate, sort or scan, and the set at least 2 occurrences,
sparkDoctor's thresholds. A repeat rooted at an exchange is `plan.missed-exchange-reuse`'s
instead.

An event log holds the physical plan without the optimizer's reasons, so the finding is low
confidence.

## Why it costs time

Spark runs each copy in full. It reuses an exchange or a subquery on its own, but nothing else, so
a repeated scan, filter and projection does its work once per copy.

## Where the Spark UI shows it

SQL tab › the query: the same chain of nodes appears more than once in the graph, each copy with
its own metrics.

## How to fix it

- Compute the shared DataFrame once, and `cache()` it where both branches read it.
- Restructure a self-union or a self-join so the shared part runs once, as one aggregate with
  conditional sums.
- Check `spark.sql.exchange.reuse` is on, so at least the shuffles are shared.
"""

REPEATED_SUBTREE = Rule(
    rule_id='plan.repeated-subtree',
    title='{occurrence_count} identical {root_operator} subtrees compute the same thing',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(
        Threshold('min_occurrences', 2, 'count', _REPEATED_SUBTREE_SOURCE),
        Threshold('min_nodes', 3, 'count', _REPEATED_SUBTREE_SOURCE),
    ),
    severity_ladder={'medium': 'a subtree repeats with the same input'},
    remediation=(
        'compute the shared DataFrame once, and cache it where both branches read it',
        'restructure a self-union or self-join so the shared part runs once',
        'check spark.sql.exchange.reuse is on',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#caching-data',),
    lesson=_REPEATED_SUBTREE_LESSON,
    check=_repeated_subtree,
)


def _read_columns(schema: str | None) -> frozenset[str] | None:
    """The top-level columns of a scan's `ReadSchema`, such as `struct<id:bigint,g:bigint>`, or None where it is unknown or cut."""
    if schema is None or is_cut(schema) or not schema.startswith('struct<') or not schema.endswith('>'):
        return None
    inner = schema[len('struct<') : -1]
    return frozenset(entry.split(':', 1)[0].strip('`').lower() for entry in split_top_level(inner, ',', angle_brackets=True) if entry)


def _repeated_scan(context: RuleContext) -> Iterator[dict[str, Any]]:
    for execution_id, nodes in context.numbered.items():
        plan = context.plans[execution_id]
        cached = under_cache(nodes)
        reused_later = _reused_later(plan, nodes)
        by_location: dict[str, list[int]] = {}
        for numbered in nodes:
            location = scan_location(numbered.node)
            if location is not None and numbered.number not in cached and numbered.number not in reused_later:
                by_location.setdefault(location, []).append(numbered.number)
        for location, scans in by_location.items():
            columns = {scan: _read_columns(field(nodes[scan].node, 'ReadSchema')) for scan in scans}
            overlapping = tuple(scan for scan in scans if any(other != scan and _overlap(columns[scan], columns[other]) for other in scans))
            if len(overlapping) < 2:
                continue
            uncertain = CUT_MARK in location or ' entries]' in location or any(columns[scan] is None for scan in overlapping)
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': overlapping[0],
                'severity': 'medium',
                'impact_seconds': None,
                'confidence': 'low' if uncertain else 'high',
                'confidence_reason': 'the plan cut the location or the columns read, so the scans may read different files or columns'
                if uncertain
                else None,
                'scan_location': location,
                'scan_count': len(overlapping),
                'scan_node_ids': overlapping,
            }


def _overlap(left: frozenset[str] | None, right: frozenset[str] | None) -> bool:
    """Whether two scans read a column in common. A schema the plan cut could hold any column, so it overlaps."""
    return left is None or right is None or bool(left & right)


def _reused_later(plan: Plan, nodes: Sequence[NumberedNode]) -> frozenset[int]:
    """In a plan AQE has not finished, the nodes under each repeat of an exchange after its first, which AQE may yet reuse."""
    if plan.is_final:
        return frozenset()
    repeats = _exchange_rooted(repeated_subtrees(nodes, min_occurrences=2, min_nodes=2), nodes)
    return frozenset(member for repeat in repeats for members in repeat.members[1:] for member in members)


_REPEATED_SCAN_LESSON = """\
## What it measures

Two or more scans in one query reading the same location, with columns in common, where neither is
a reuse of the other or of a cached relation. Spark reuses an identical exchange or a cached
relation, but it never merges two scans that differ in filter or projection.

## Why it costs time

Each scan lists the files and reads their bytes again. On object storage that repeats the requests
as well as the bytes, and each scan's work up to the first shuffle runs again with it.

## Where the Spark UI shows it

SQL tab › the query › each scan node with the same `Location` in its details, each with its own
`number of files read` and `size of files read`.

## How to fix it

- Read the table once and derive both branches from that DataFrame, filtering after the read.
- Where both branches need the same rows, `cache()` or `persist()` the shared part, which then
  shows as `InMemoryTableScan`.
- Combine the branches: a `union` of two filters over one table is one filter with `OR`, and two
  aggregates over one table are one aggregate with conditional sums.
"""

REPEATED_SCAN = Rule(
    rule_id='plan.repeated-scan',
    title='{scan_count} scans read {scan_location}',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(),
    severity_ladder={'medium': 'scans read one location twice or more, with columns in common'},
    remediation=(
        'read the table once and derive both branches from it',
        'cache or persist the shared part where both branches need the same rows',
        'combine the branches into one filter or one aggregate',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#caching-data',),
    lesson=_REPEATED_SCAN_LESSON,
    check=_repeated_scan,
)

RULES = (REPEATED_SCAN, REPEATED_SUBTREE, MISSED_EXCHANGE_REUSE)
