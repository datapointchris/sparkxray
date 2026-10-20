"""Group a run's jobs as the executor simulation replays them: jobs that ran at once together, apart from those that ran after.

The grouping ports `makeJobLists` in Sparklens's `helper/JobOverlapHelper.scala`, which splits each
SQL execution's jobs into steps that ran one after another. A job joins the step before it when it
starts before every job of that step has ended, where Sparklens compares it with one job alone. Steps
whose spans overlap then replay together, where Sparklens replays them one after another. Each
execution's steps keep their order inside that replay.
"""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Iterable


@dc.dataclass(frozen=True, slots=True)
class JobSpan:
    """One job: the SQL execution it ran for, when it was submitted and ended, and the stages it ran."""

    job_id: int
    execution_id: int | None
    start_epoch_ms: int
    end_epoch_ms: int
    stage_ids: tuple[int, ...] = ()


@dc.dataclass(frozen=True, slots=True)
class JobStep:
    """Jobs of one SQL execution that ran at once, or one job of no execution."""

    jobs: tuple[JobSpan, ...]

    @property
    def start_epoch_ms(self) -> int:
        return min(job.start_epoch_ms for job in self.jobs)

    @property
    def end_epoch_ms(self) -> int:
        return max(job.end_epoch_ms for job in self.jobs)

    @property
    def span_ms(self) -> int:
        return self.end_epoch_ms - self.start_epoch_ms

    @property
    def stage_ids(self) -> frozenset[int]:
        return frozenset(stage_id for job in self.jobs for stage_id in job.stage_ids)


@dc.dataclass(frozen=True, slots=True)
class JobGroup:
    """Steps the simulation replays together, from the earliest one's start to the latest one's end.

    `sequences` holds each SQL execution's steps in the group in the order they ran. A job of no
    execution is a sequence of one step.
    """

    sequences: tuple[tuple[JobStep, ...], ...]

    @property
    def jobs(self) -> tuple[JobSpan, ...]:
        return tuple(sorted((job for sequence in self.sequences for step in sequence for job in step.jobs), key=lambda job: job.job_id))

    @property
    def start_epoch_ms(self) -> int:
        return min(job.start_epoch_ms for job in self.jobs)

    @property
    def end_epoch_ms(self) -> int:
        return max(job.end_epoch_ms for job in self.jobs)

    @property
    def span_ms(self) -> int:
        return self.end_epoch_ms - self.start_epoch_ms

    @property
    def stage_ids(self) -> frozenset[int]:
        return frozenset(stage_id for job in self.jobs for stage_id in job.stage_ids)


def group_jobs(jobs: Iterable[JobSpan]) -> tuple[tuple[JobGroup, ...], int]:
    """The groups the simulation replays, in start order, and how many SQL executions joined a group another one ran in.

    A job of no execution counts as an execution of its own. So the count is above 0 only where a
    thread pool ran jobs of two executions at once.
    """
    steps: list[tuple[tuple[str, int], JobStep]] = []
    by_execution: dict[int, list[JobSpan]] = {}
    for job in jobs:
        if job.execution_id is None:
            steps.append((('job', job.job_id), JobStep((job,))))
        else:
            by_execution.setdefault(job.execution_id, []).append(job)
    for execution_id, execution_jobs in by_execution.items():
        current: list[JobSpan] = []
        for job in sorted(execution_jobs, key=lambda job: (job.start_epoch_ms, job.job_id)):
            if current and max(earlier.end_epoch_ms for earlier in current) < job.start_epoch_ms:
                steps.append((('execution', execution_id), JobStep(tuple(current))))
                current = []
            current.append(job)
        steps.append((('execution', execution_id), JobStep(tuple(current))))

    merged: list[list[tuple[tuple[str, int], JobStep]]] = []
    for key, step in sorted(steps, key=lambda entry: (entry[1].start_epoch_ms, min(job.job_id for job in entry[1].jobs))):
        if merged and step.start_epoch_ms < max(held.end_epoch_ms for _, held in merged[-1]):
            merged[-1].append((key, step))
        else:
            merged.append([(key, step)])

    groups = []
    joined = 0
    for members in merged:
        sequences: dict[tuple[str, int], list[JobStep]] = {}
        for key, step in members:
            sequences.setdefault(key, []).append(step)
        joined += len(sequences) - 1
        groups.append(JobGroup(tuple(tuple(sequence) for sequence in sequences.values())))
    return tuple(groups), joined
