"""Which SQL execution and stage of run B is which of run A. Ids are assigned in submission order, so ids never align them.

Executions pair in passes, each over what the passes before left unpaired, from the strongest key
to the weakest. Inside a key, the nth of run A pairs with the nth of run B. The first pass keys on
the description and the plan before AQE ran, which two runs of one query on different data share.
The second keys on the description and the operators of the last plan, for a setting that changed
the first plan's text. The third keys on the first plan alone, for a description that changed
while the query did not. The last keys on the description and order. A pair records the pass that
made it, so a weak pairing reads as weak.

A stage pairs inside a paired execution, by the plan nodes its tasks reported metrics to and then
its order among stages with the same nodes. A stage of no SQL execution pairs by name, which is
Spark's call site, and then by order.
"""

from __future__ import annotations

import dataclasses as dc
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Hashable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Generic
from typing import Literal
from typing import TypeVar

from sparkxray.run_diff.run_facts import RunFacts

ExecutionMatch = Literal['description_and_initial_plan', 'description_and_final_operators', 'initial_plan', 'description_and_order']
StageMatch = Literal['plan_nodes', 'name']
_Row = Mapping[str, Any]
_Key = Callable[[_Row], Hashable | None]
_Match = TypeVar('_Match', bound=str)


@dc.dataclass(frozen=True, slots=True)
class Paired(Generic[_Match]):
    a: _Row
    b: _Row
    matched_by: _Match


@dc.dataclass(frozen=True, slots=True)
class Alignment(Generic[_Match]):
    paired: tuple[Paired[_Match], ...]
    only_a: tuple[_Row, ...]
    only_b: tuple[_Row, ...]


def align_executions(a: RunFacts, b: RunFacts) -> Alignment[ExecutionMatch]:
    """Run B's SQL executions paired with run A's, in the passes the module describes."""

    def final_operators(facts: RunFacts) -> Callable[[_Row], Hashable | None]:
        def key(row: _Row) -> Hashable | None:
            operators = facts.final_operators.get(row['execution_id'])
            return None if not operators else (row['description'], frozenset(operators.items()))

        return key

    def initial_plan(row: _Row) -> Hashable | None:
        return None if row['initial_plan_hash'] is None else (row['description'], row['initial_plan_hash'])

    passes: list[tuple[ExecutionMatch, Callable[[_Row], Hashable | None], Callable[[_Row], Hashable | None]]] = [
        ('description_and_initial_plan', initial_plan, initial_plan),
        ('description_and_final_operators', final_operators(a), final_operators(b)),
        ('initial_plan', lambda row: row['initial_plan_hash'], lambda row: row['initial_plan_hash']),
        ('description_and_order', lambda row: (row['description'],), lambda row: (row['description'],)),
    ]
    return _in_passes(a.executions, b.executions, passes)


def align_stages(a: RunFacts, b: RunFacts, executions: Alignment[ExecutionMatch]) -> Alignment[StageMatch]:
    """Run B's stages paired with run A's: inside each paired execution, then those of no execution among themselves."""
    a_stages = _by_execution(a.stages)
    b_stages = _by_execution(b.stages)
    paired: list[Paired[StageMatch]] = []
    only_a: list[_Row] = []
    only_b: list[_Row] = []
    for execution in executions.paired:
        a_id, b_id = execution.a['execution_id'], execution.b['execution_id']
        within_passes: list[tuple[StageMatch, _Key, _Key]] = [
            ('plan_nodes', _labels(a, a_id), _labels(b, b_id)),
            ('name', _name, _name),
        ]
        within = _in_passes(a_stages.pop(a_id, []), b_stages.pop(b_id, []), within_passes)
        paired += within.paired
        only_a += within.only_a
        only_b += within.only_b
    outside_passes: list[tuple[StageMatch, _Key, _Key]] = [('name', _name, _name)]
    outside = _in_passes(a_stages.pop(None, []), b_stages.pop(None, []), outside_passes)
    paired += outside.paired
    only_a += outside.only_a + tuple(stage for stages in a_stages.values() for stage in stages)
    only_b += outside.only_b + tuple(stage for stages in b_stages.values() for stage in stages)
    return Alignment(
        tuple(sorted(paired, key=lambda pair: pair.b['stage_id'])),
        tuple(sorted(only_a, key=lambda row: row['stage_id'])),
        tuple(sorted(only_b, key=lambda row: row['stage_id'])),
    )


def _labels(facts: RunFacts, execution_id: int) -> _Key:
    def key(row: _Row) -> Hashable | None:
        return facts.stage_labels.get((execution_id, row['stage_id'])) or None

    return key


def _name(row: _Row) -> Hashable | None:
    return row['name']


def _by_execution(stages: Sequence[_Row]) -> dict[int | None, list[_Row]]:
    grouped: dict[int | None, list[_Row]] = defaultdict(list)
    for stage in stages:
        grouped[stage['execution_id']].append(stage)
    return grouped


def _in_passes(
    a_rows: Sequence[_Row],
    b_rows: Sequence[_Row],
    passes: Sequence[tuple[_Match, Callable[[_Row], Hashable | None], Callable[[_Row], Hashable | None]]],
) -> Alignment[_Match]:
    """Pair rows pass by pass. In each, rows with equal keys pair in their order, and a row whose key is None waits for the next."""
    a_left, b_left = list(a_rows), list(b_rows)
    paired: list[Paired[_Match]] = []
    for matched_by, a_key, b_key in passes:
        a_groups: dict[Hashable, list[_Row]] = defaultdict(list)
        for row in a_left:
            if (key := a_key(row)) is not None:
                a_groups[key].append(row)
        taken_a: set[int] = set()
        taken_b: set[int] = set()
        for row in b_left:
            key = b_key(row)
            if key is not None and a_groups.get(key):
                partner = a_groups[key].pop(0)
                paired.append(Paired(partner, row, matched_by))
                taken_a.add(id(partner))
                taken_b.add(id(row))
        a_left = [row for row in a_left if id(row) not in taken_a]
        b_left = [row for row in b_left if id(row) not in taken_b]
    return Alignment(tuple(paired), tuple(a_left), tuple(b_left))
