"""Estimate a run's wall time at other executor counts by replaying its stages, and split its time between the driver and the executors.

A port of Sparklens's executor-count simulation. `scheduler/CompletionEstimator.scala` gives the
estimate, `analyzer/ExecutorWallclockAnalyzer.scala` the curve and the model error, and
`analyzer/EfficiencyStatisticsAnalyzer.scala` the reference times and the core-hours.
"""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from sparkxray.cache_directory import installed_version
from sparkxray.simulation.job_groups import JobGroup
from sparkxray.simulation.job_groups import JobSpan
from sparkxray.simulation.job_groups import group_jobs
from sparkxray.simulation.stage_scheduler import StageGate
from sparkxray.simulation.stage_scheduler import StageWork
from sparkxray.simulation.stage_scheduler import critical_path_ms
from sparkxray.simulation.stage_scheduler import group_wall_ms
from sparkxray.tables.registry import CORE_TIME_CTES
from sparkxray.tables.registry import EXECUTOR_POOL_CTE
from sparkxray.tables.registry import GLUE_DPU_CORES
from sparkxray.tables.registry import RUN_SPAN_SQL
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import rows_of

if TYPE_CHECKING:
    from sparkxray.ingest.event_log import EventLog

# A field removed or a meaning changed raises this. A field added does not.
SCHEMA_VERSION = 1
# Sparklens's executor counts, as percents of the most executors the run had alive at once.
DEFAULT_EXECUTOR_PERCENTS = (10, 20, 50, 80, 100, 110, 120, 150, 200, 300, 400, 500)
# What the replay cannot see, which every curve prints under it.
MODEL_LIMITS = (
    'A task takes the same time at every executor count. Fewer executors raise spill, GC and memory pressure, '
    'and more raise the shuffle fetched over the network. Neither shows in the estimate.',
    'Stages queue in stage-id order. Fair-scheduler pools are not modeled.',
    'A task costs its run time alone, without its scheduler delay or deserialization. So a stage of many tiny tasks '
    'scales better here than on a cluster, the case stage.tiny-tasks and stage.scheduler-delay report.',
    "SQL executions whose jobs ran at once all replay from their group's start. A thread that submitted its query "
    'only after another of its own finished is not seen, so such a query may start sooner here than it could.',
)
_MS_AN_HOUR = 3_600_000

_RUN_SQL = f"""
WITH {CORE_TIME_CTES}, {EXECUTOR_POOL_CTE}
SELECT application.app_id, application.platform, run.end_epoch_ms - run.start_epoch_ms AS wall_ms,
  pool.executor_count, pool.executor_core_count, pool.resource_profile_id, pool.resource_profile_count,
  task_cpus.cpus AS task_cpu_count, core_totals.core_time_ms,
  (SELECT coalesce(sum(executor_run_time_ms), 0) FROM tasks WHERE NOT effective)::BIGINT AS retry_run_ms
FROM application, run, pool, task_cpus, core_totals
"""
# A stage counts toward the first job listing it, the job that ran it. A later job lists it only to skip it.
# A job still running when the log ended runs to the run's end.
_JOBS_SQL = f"""
WITH run AS ({RUN_SPAN_SQL}), ran AS (
  SELECT stage_id, min(job_id) AS job_id
  FROM (SELECT job_id, unnest(stage_ids) AS stage_id FROM jobs)
  WHERE stage_id IN (SELECT stage_id FROM stages)
  GROUP BY stage_id
)
SELECT jobs.job_id, jobs.sql_execution_id, jobs.submit_epoch_ms, coalesce(jobs.end_epoch_ms, run.end_epoch_ms) AS end_epoch_ms,
  coalesce(list(ran.stage_id ORDER BY ran.stage_id) FILTER (WHERE ran.stage_id IS NOT NULL), []) AS stage_ids
FROM jobs CROSS JOIN run LEFT JOIN ran USING (job_id)
WHERE jobs.submit_epoch_ms IS NOT NULL
GROUP BY ALL
ORDER BY jobs.job_id
"""
# Each stage's effective tasks, a partition's first success across the attempts, in partition order.
_STAGE_WORK_SQL = """
WITH work AS (
  SELECT stage_id,
    list(coalesce(executor_run_time_ms, 0) ORDER BY coalesce(partition_id, task_index), stage_attempt, task_index) AS task_run_ms
  FROM tasks_effective GROUP BY stage_id
)
SELECT stages.stage_id, coalesce(stages.parent_ids, []) AS parent_ids, coalesce(work.task_run_ms, []) AS task_run_ms
FROM stages LEFT JOIN work USING (stage_id)
WHERE stages.is_highest_attempt
"""


@dc.dataclass(frozen=True, slots=True)
class RunModel:
    """What the simulation replays: the run's wall time, its job groups, each stage's work, and how many group merges made them."""

    wall_ms: int
    groups: tuple[JobGroup, ...]
    stages: Mapping[int, StageWork]
    overlap_merge_count: int = 0

    @classmethod
    def read(cls, tables: Tables) -> RunModel:
        """The model of one log's run."""
        wall_ms = tables.query(f'SELECT end_epoch_ms - start_epoch_ms FROM ({RUN_SPAN_SQL})').fetchone()
        jobs = [
            JobSpan(row['job_id'], row['sql_execution_id'], row['submit_epoch_ms'], row['end_epoch_ms'], tuple(row['stage_ids']))
            for row in rows_of(tables.query(_JOBS_SQL)).to_json()
        ]
        stages = {
            row['stage_id']: StageWork(tuple(row['task_run_ms']), tuple(row['parent_ids']))
            for row in rows_of(tables.query(_STAGE_WORK_SQL)).to_json()
        }
        groups, merges = group_jobs(jobs)
        return cls(wall_ms[0] if wall_ms and wall_ms[0] is not None else 0, groups, stages, merges)

    @property
    def executor_ms(self) -> int:
        """The time some job ran: the groups' spans, which never overlap."""
        return sum(group.span_ms for group in self.groups)

    @property
    def driver_ms(self) -> int:
        """The time no job ran, which the estimate keeps at every executor count."""
        return max(0, self.wall_ms - self.executor_ms)

    @property
    def task_run_ms(self) -> int:
        """The run time of every task the groups replay."""
        replayed = frozenset().union(*(group.stage_ids for group in self.groups))
        return sum(sum(self.stages[stage_id].task_run_ms) for stage_id in replayed if stage_id in self.stages)

    def estimate_wall_ms(self, slots: int) -> int:
        """The run's wall time on `slots` task slots: the driver's time, then each group replayed, or its span where it ran no task."""
        total = self.driver_ms
        for group in self.groups:
            replay = self._replay(group)
            if replay is None:
                total += group.span_ms
            else:
                stages, gates = replay
                total += group_wall_ms(stages, slots, gates)
        return total

    def critical_path_ms(self) -> int:
        """The run's wall time on unlimited slots: the driver's time, then each group's longest chain of stages."""
        total = self.driver_ms
        for group in self.groups:
            replay = self._replay(group)
            if replay is None:
                total += group.span_ms
            else:
                stages, gates = replay
                total += critical_path_ms(stages, gates)
        return total

    def _replay(self, group: JobGroup) -> tuple[dict[int, StageWork], tuple[StageGate, ...]] | None:
        """The group's stages and the gates keeping each execution's steps in order, or None where not one stage ran a task.

        A step waits on the last step before it that ran a task, by the driver's time between them
        and the span of every step between that ran none.
        """
        work = {stage_id: self.stages[stage_id] for stage_id in group.stage_ids if stage_id in self.stages}
        if not any(stage.task_run_ms for stage in work.values()):
            return None
        gates = []
        for sequence in group.sequences:
            awaited: frozenset[int] = frozenset()
            wait_ms = 0
            previous_end: int | None = None
            for step in sequence:
                if previous_end is not None:
                    wait_ms += step.start_epoch_ms - previous_end
                ran = frozenset(stage_id for stage_id in step.stage_ids if stage_id in work and work[stage_id].task_run_ms)
                if not ran:
                    wait_ms += step.span_ms
                else:
                    if awaited or wait_ms:
                        gates.append(StageGate(awaited, ran, wait_ms))
                    awaited, wait_ms = ran, 0
                previous_end = step.end_epoch_ms
            if wait_ms:
                gates.append(StageGate(awaited, frozenset(), wait_ms))
        return work, tuple(gates)


@dc.dataclass(frozen=True, slots=True)
class CurvePoint:
    """The run's estimated wall time at one executor count, how much of that pool its tasks would hold, and what the pool would cost."""

    executor_count: int
    estimated_wall_ms: int
    utilization_share: float | None
    core_hours: float
    dpu_hours: float | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class Simulation:
    """A run's reference times, its core-hours, and its estimated wall time at each executor count, as Sparklens's model gives them.

    Every time is in ms. The reference times and the model's check use the log's executors and
    cores. The curve uses `curve_executor_core_count`, which a caller may set. An executor runs
    its cores over `task_cpu_count` tasks at once, rounded down. A field is null where the log
    names no executor's cores.
    """

    generated_by: str
    app_id: str
    platform: str
    wall_ms: int
    executor_count: int
    executor_core_count: int | None
    core_count: int | None
    task_cpu_count: int
    resource_profile_id: int | None
    resource_profile_count: int
    job_group_count: int
    overlap_merge_count: int
    task_run_ms: int
    retry_run_ms: int
    driver_ms: int
    executor_ms: int
    critical_path_ms: int
    perfect_parallel_ms: int | None
    single_core_ms: int
    model_estimate_ms: int | None
    model_error_share: float | None
    available_core_hours: float | None
    executor_lifetime_core_hours: float | None
    used_core_hours: float
    driver_wasted_core_hours: float | None
    executor_wasted_core_hours: float | None
    curve_executor_core_count: int | None
    curve: tuple[CurvePoint, ...]
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> dict[str, Any]:
        fields = {field.name: getattr(self, field.name) for field in dc.fields(self) if field.name not in ('schema_version', 'curve')}
        return {'schema_version': self.schema_version, **fields, 'curve': [point.to_json() for point in self.curve]}


def simulate(log: EventLog, *, executors: Sequence[int] | None = None, cores: int | None = None) -> Simulation:
    """A run's reference times, its core-hours, and its estimated wall time at each executor count.

    `executors` lists the counts to estimate, by default 10% to 500% of the most executors the run
    had alive at once, a count rounding to 0 dropped. `cores` is each executor's cores, by default
    the log's: those of the resource profile most executors ran under. A count under 1 raises
    `ValueError`. Where the log names no executor's cores and `cores` is not given, the curve is
    empty. So is it where an executor's cores are fewer than the `spark.task.cpus` a task holds.
    """
    if cores is not None and cores < 1:
        raise ValueError(f'cores is {cores}, and an executor needs at least 1')
    if executors is not None and any(count < 1 for count in executors):
        raise ValueError(f'executors holds {min(executors)}, and a count is at least 1')
    tables = log.tables
    run = rows_of(tables.query(_RUN_SQL)).to_json()[0]
    model = RunModel.read(tables)
    run_executors: int = run['executor_count']
    log_cores: int | None = run['executor_core_count']
    task_cpus: int = run['task_cpu_count']
    total_cores = run_executors * log_cores if run_executors and log_cores else None
    run_slots = run_executors * (log_cores // task_cpus) if run_executors and log_cores else 0
    curve_cores = cores or log_cores
    if executors is not None:
        counts = sorted(set(executors))
    else:
        counts = sorted({run_executors * percent // 100 for percent in DEFAULT_EXECUTOR_PERCENTS} - {0})
    task_run_ms = model.task_run_ms
    task_core_ms = task_run_ms * task_cpus
    driver_ms, executor_ms = model.driver_ms, model.executor_ms
    estimate = model.estimate_wall_ms(run_slots) if run_slots else None
    wall_ms = model.wall_ms
    curve: tuple[CurvePoint, ...] = ()
    if curve_cores and curve_cores >= task_cpus:
        curve = tuple(_point(model, count, curve_cores, task_cpus, run['platform']) for count in counts)
    return Simulation(
        generated_by=f'sparkxray {installed_version()}',
        app_id=run['app_id'],
        platform=run['platform'],
        wall_ms=wall_ms,
        executor_count=run_executors,
        executor_core_count=log_cores,
        core_count=total_cores,
        task_cpu_count=task_cpus,
        resource_profile_id=run['resource_profile_id'],
        resource_profile_count=run['resource_profile_count'],
        job_group_count=len(model.groups),
        overlap_merge_count=model.overlap_merge_count,
        task_run_ms=task_run_ms,
        retry_run_ms=run['retry_run_ms'],
        driver_ms=driver_ms,
        executor_ms=executor_ms,
        critical_path_ms=model.critical_path_ms(),
        perfect_parallel_ms=driver_ms + round(task_run_ms / run_slots) if run_slots else None,
        single_core_ms=driver_ms + task_run_ms,
        model_estimate_ms=estimate,
        model_error_share=round(abs(wall_ms - estimate) / wall_ms, 4) if estimate is not None and wall_ms > 0 else None,
        available_core_hours=_hours(total_cores * wall_ms) if total_cores else None,
        executor_lifetime_core_hours=_hours(run['core_time_ms']) if run['core_time_ms'] is not None else None,
        used_core_hours=_hours(task_core_ms),
        driver_wasted_core_hours=_hours(total_cores * driver_ms) if total_cores else None,
        executor_wasted_core_hours=_hours(total_cores * executor_ms - task_core_ms) if total_cores else None,
        curve_executor_core_count=curve_cores,
        curve=curve,
    )


def _point(model: RunModel, executor_count: int, cores: int, task_cpus: int, platform: str) -> CurvePoint:
    estimate = model.estimate_wall_ms(executor_count * (cores // task_cpus))
    core_ms = estimate * executor_count * cores
    return CurvePoint(
        executor_count=executor_count,
        estimated_wall_ms=estimate,
        utilization_share=round(model.task_run_ms * task_cpus / core_ms, 4) if core_ms else None,
        core_hours=_hours(core_ms),
        dpu_hours=_hours(core_ms / GLUE_DPU_CORES) if platform == 'glue' else None,
    )


def _hours(core_ms: float) -> float:
    return round(core_ms / _MS_AN_HOUR, 4)
