"""Group a run's jobs as the executor simulation replays them: jobs that ran at once together, apart from those that ran after.

The grouping ports `makeJobLists` in Sparklens's `helper/JobOverlapHelper.scala`. Groups whose
spans overlap then merge into one, where Sparklens replays them one after another.
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
class JobGroup:
    """Jobs the simulation replays together, from the earliest one's start to the latest one's end."""

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


def group_jobs(jobs: Iterable[JobSpan]) -> tuple[tuple[JobGroup, ...], int]:
    """The groups the simulation replays, in start order, and how many merges of overlapping groups made them.

    Jobs of one SQL execution form a group, split where a job, in start order, starts after the
    one before it ended. A job of no execution is a group alone. Then a group starting before the
    groups ahead of it have ended merges into them, so jobs a thread pool ran at once replay at
    once rather than one after another.
    """
    by_execution: dict[int, list[JobSpan]] = {}
    groups: list[list[JobSpan]] = []
    for job in jobs:
        if job.execution_id is None:
            groups.append([job])
        else:
            by_execution.setdefault(job.execution_id, []).append(job)
    for execution_jobs in by_execution.values():
        current: list[JobSpan] = []
        for job in sorted(execution_jobs, key=lambda job: (job.start_epoch_ms, job.job_id)):
            if current and current[-1].end_epoch_ms < job.start_epoch_ms:
                groups.append(current)
                current = []
            current.append(job)
        groups.append(current)

    merged: list[list[JobSpan]] = []
    merges = 0
    for group in sorted(groups, key=lambda group: min((job.start_epoch_ms, job.job_id) for job in group)):
        if merged and min(job.start_epoch_ms for job in group) < max(job.end_epoch_ms for job in merged[-1]):
            merged[-1].extend(group)
            merges += 1
        else:
            merged.append(list(group))
    return tuple(JobGroup(tuple(sorted(group, key=lambda job: job.job_id))) for group in merged), merges
