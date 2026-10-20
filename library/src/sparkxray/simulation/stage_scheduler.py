"""Replay a group of jobs' stages onto a fixed pool of task slots, as Sparklens's scheduler replays them.

A port of Sparklens's `scheduler/CompletionEstimator.scala` and `scheduler/PQParallelStageScheduler.scala`.
"""

from __future__ import annotations

import dataclasses as dc
import heapq
from collections.abc import Mapping
from collections.abc import Sequence

from sparkxray.errors import SimulationStalledError


@dc.dataclass(frozen=True, slots=True)
class StageWork:
    """One stage as the replay runs it: its tasks' run times in ms, in partition order, and the stages whose output it reads."""

    task_run_ms: tuple[int, ...]
    parent_ids: tuple[int, ...] = ()


@dc.dataclass(frozen=True, slots=True)
class StageGate:
    """An order the replay keeps beyond stage parents: `waiting` starts `wait_ms` after every stage of `awaited` has finished.

    It holds one step of a SQL execution's jobs behind the step before it, by the time the driver
    took between them.
    """

    awaited: frozenset[int]
    waiting: frozenset[int]
    wait_ms: int


def task_cost_ms(run_ms: int) -> int:
    """What a task costs the replay: its run time, and at least 1 ms, as Sparklens charges it."""
    return max(run_ms, 1)


def group_wall_ms(stages: Mapping[int, StageWork], slots: int, gates: Sequence[StageGate] = ()) -> int:
    """How long `slots` task slots take to run every stage of `stages` with tasks, in ms.

    A stage is runnable once each parent with tasks in `stages` has finished, so a parent it lacks
    was skipped and counts as done. It also waits for each gate naming it to open. A gate opens
    `wait_ms` after the last of its awaited stages with tasks finishes, or `wait_ms` after the start
    where it awaits none. Runnable stages start in stage-id order, each queuing every task in order.
    With every slot busy, the task finishing first leaves and the clock moves to its finish. Stages
    that read each other in a cycle raise `SimulationStalledError`.
    """
    if slots < 1:
        raise ValueError(f'slots is {slots}, and a pool needs at least 1')
    pending = {stage_id: len(stage.task_run_ms) for stage_id, stage in stages.items() if stage.task_run_ms}
    blockers: dict[int, int] = {}
    children: dict[int, list[int]] = {}
    for stage_id in pending:
        live = {parent for parent in stages[stage_id].parent_ids if parent in pending}
        blockers[stage_id] = len(live)
        for parent in live:
            children.setdefault(parent, []).append(stage_id)
    awaiting: list[int] = []
    gates_awaiting: dict[int, list[int]] = {}
    opening: list[tuple[int, int]] = []
    for index, gate in enumerate(gates):
        awaited = [stage_id for stage_id in gate.awaited if stage_id in pending]
        awaiting.append(len(awaited))
        for stage_id in awaited:
            gates_awaiting.setdefault(stage_id, []).append(index)
        for stage_id in gate.waiting:
            if stage_id in pending:
                blockers[stage_id] += 1
        if not awaited:
            opening.append((gate.wait_ms, index))
    heapq.heapify(opening)
    runnable = [stage_id for stage_id, count in blockers.items() if not count]
    heapq.heapify(runnable)

    clock = 0
    running: list[tuple[int, int]] = []
    unfinished = len(pending)

    def release(stage_id: int) -> None:
        blockers[stage_id] -= 1
        if not blockers[stage_id]:
            heapq.heappush(runnable, stage_id)

    def advance() -> None:
        """Move the clock to what happens next: a gate opening, or the task finishing first leaving."""
        nonlocal clock, unfinished
        if opening and (not running or opening[0][0] <= running[0][0]):
            clock, index = heapq.heappop(opening)
            for stage_id in gates[index].waiting:
                if stage_id in pending:
                    release(stage_id)
            return
        clock, stage_id = heapq.heappop(running)
        pending[stage_id] -= 1
        if pending[stage_id]:
            return
        unfinished -= 1
        for child in children.get(stage_id, ()):
            release(child)
        for index in gates_awaiting.get(stage_id, ()):
            awaiting[index] -= 1
            if not awaiting[index]:
                heapq.heappush(opening, (clock + gates[index].wait_ms, index))

    while unfinished:
        if runnable:
            stage_id = heapq.heappop(runnable)
            for run_ms in stages[stage_id].task_run_ms:
                while len(running) == slots:
                    advance()
                heapq.heappush(running, (clock + task_cost_ms(run_ms), stage_id))
        elif running or opening:
            advance()
        else:
            stuck = sorted(stage_id for stage_id, count in blockers.items() if count)
            raise SimulationStalledError(f'stages {", ".join(map(str, stuck))} read each other in a cycle, so none of them can start')
    return max([clock, *(opens for opens, _ in opening)])


def critical_path_ms(stages: Mapping[int, StageWork], gates: Sequence[StageGate] = ()) -> int:
    """The longest chain of `stages` through their parents and gates, each stage costing its longest task: the time unlimited slots take.

    A port of `computeCriticalTimeForJob` in Sparklens's `timespan/JobTimeSpan.scala`, with the
    gates `group_wall_ms` keeps. Spark numbers a stage after every stage it reads, and after every
    stage of a job before its own, so stages are costed in id order.
    """
    longest: dict[int, int] = {}
    opened: dict[int, int] = {}
    waiting_on: dict[int, list[int]] = {}
    for index, gate in enumerate(gates):
        for stage_id in gate.waiting:
            waiting_on.setdefault(stage_id, []).append(index)

    def opens(index: int) -> int:
        if index not in opened:
            gate = gates[index]
            opened[index] = max((longest.get(stage_id, 0) for stage_id in gate.awaited), default=0) + gate.wait_ms
        return opened[index]

    for stage_id in sorted(stages):
        stage = stages[stage_id]
        own = max((task_cost_ms(run_ms) for run_ms in stage.task_run_ms), default=0)
        ready = [longest.get(parent, 0) for parent in stage.parent_ids] + [opens(index) for index in waiting_on.get(stage_id, ())]
        longest[stage_id] = own + max(ready, default=0)
    return max([*longest.values(), *(opens(index) for index in range(len(gates)))], default=0)
