"""Replay a group of jobs' stages onto a fixed pool of task slots, as Sparklens's scheduler replays them.

A port of Sparklens's `scheduler/CompletionEstimator.scala` and `scheduler/PQParallelStageScheduler.scala`.
"""

from __future__ import annotations

import dataclasses as dc
import heapq
from collections.abc import Mapping

from sparkxray.errors import SimulationStalledError


@dc.dataclass(frozen=True, slots=True)
class StageWork:
    """One stage as the replay runs it: its tasks' run times in ms, in partition order, and the stages whose output it reads."""

    task_run_ms: tuple[int, ...]
    parent_ids: tuple[int, ...] = ()


def task_cost_ms(run_ms: int) -> int:
    """What a task costs the replay: its run time, and at least 1 ms, as Sparklens charges it."""
    return max(run_ms, 1)


def group_wall_ms(stages: Mapping[int, StageWork], slots: int) -> int:
    """How long `slots` task slots take to run every stage of `stages` with tasks, in ms.

    A stage is runnable once each parent with tasks in `stages` has finished, so a parent it lacks
    was skipped and counts as done. Runnable stages start in stage-id order, each queuing every task
    in order. With every slot busy, the task finishing first leaves and the clock moves to its
    finish. A stage left waiting on one that never runs raises `SimulationStalledError`.
    """
    if slots < 1:
        raise ValueError(f'slots is {slots}, and a pool needs at least 1')
    pending = {stage_id: len(stage.task_run_ms) for stage_id, stage in stages.items() if stage.task_run_ms}
    blockers: dict[int, int] = {}
    children: dict[int, list[int]] = {}
    runnable: list[int] = []
    for stage_id in pending:
        live = {parent for parent in stages[stage_id].parent_ids if parent in pending}
        if live:
            blockers[stage_id] = len(live)
            for parent in live:
                children.setdefault(parent, []).append(stage_id)
        else:
            runnable.append(stage_id)
    heapq.heapify(runnable)

    clock = 0
    running: list[tuple[int, int]] = []
    unfinished = len(pending)

    def finish_one() -> bool:
        """Let the task finishing first leave, and whether that finished its stage."""
        nonlocal clock, unfinished
        clock, stage_id = heapq.heappop(running)
        pending[stage_id] -= 1
        if pending[stage_id]:
            return False
        unfinished -= 1
        for child in children.get(stage_id, ()):
            blockers[child] -= 1
            if not blockers[child]:
                heapq.heappush(runnable, child)
        return True

    while unfinished:
        if runnable:
            stage_id = heapq.heappop(runnable)
            for run_ms in stages[stage_id].task_run_ms:
                if len(running) == slots:
                    finish_one()
                heapq.heappush(running, (clock + task_cost_ms(run_ms), stage_id))
        elif running:
            while running and not finish_one():
                pass
        else:
            stuck = sorted(stage_id for stage_id, count in blockers.items() if count)
            raise SimulationStalledError(
                f'stages {", ".join(map(str, stuck))} wait on stages that never finish',
                field='stage_ids',
                valid=[str(stage_id) for stage_id in stuck],
            )
    return max((finish for finish, _ in running), default=clock)


def critical_path_ms(stages: Mapping[int, StageWork]) -> int:
    """The longest chain of `stages` through their parents, each stage costing its longest task: the time unlimited slots take.

    A port of `computeCriticalTimeForJob` in Sparklens's `timespan/JobTimeSpan.scala`. Spark numbers
    a stage after every stage it reads, so stages are costed in id order.
    """
    longest: dict[int, int] = {}
    for stage_id in sorted(stages):
        stage = stages[stage_id]
        own = max((task_cost_ms(run_ms) for run_ms in stage.task_run_ms), default=0)
        longest[stage_id] = own + max((longest.get(parent, 0) for parent in stage.parent_ids), default=0)
    return max(longest.values(), default=0)
