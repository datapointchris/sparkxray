"""Each stage's tasks, time, shuffle, spill and GC, one stage's task quantiles, and one stage's task rows."""

from __future__ import annotations

import dataclasses as dc
from typing import Any

from sparkxray.errors import NotInRunError
from sparkxray.summaries.ordering import ordered_rows
from sparkxray.tables.registry import WALL_SHARES_SQL
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import Rows
from sparkxray.tables.rows import rows_of

# A stage's status is its highest attempt's. Metric totals and task durations come from
# `tasks_effective`, so a partition a retry ran again counts once. The task counts are every
# attempt's, so a retry shows as a failed task.
STAGE_SUMMARIES_SQL = f"""
WITH walls AS (
  SELECT id AS stage_id, wall_ms, share AS wall_share FROM ({WALL_SHARES_SQL}) WHERE level = 'stage'
), attempts AS (
  SELECT stage_id, max(name) AS name, count(*) AS attempt_count, min(submit_ms) AS submit_ms,
    arg_max(CASE WHEN failure_reason IS NOT NULL THEN 'failed' WHEN complete_ms IS NOT NULL THEN 'complete' ELSE 'active' END, attempt)
      AS status
  FROM stages GROUP BY stage_id
), task_attempts AS (
  SELECT stage_id, count(*) AS task_count, count(*) FILTER (WHERE failed) AS failed_task_count FROM tasks GROUP BY stage_id
), effective AS (
  SELECT stage_id,
    list_sort(list(duration_ms) FILTER (WHERE duration_ms IS NOT NULL)) AS durations,
    sum(input_bytes)::BIGINT AS input_bytes,
    sum(output_bytes)::BIGINT AS output_bytes,
    sum(coalesce(shuffle_remote_read_bytes, 0) + coalesce(shuffle_local_read_bytes, 0))::BIGINT AS shuffle_read_bytes,
    sum(shuffle_write_bytes)::BIGINT AS shuffle_write_bytes,
    sum(memory_spilled_bytes)::BIGINT AS memory_spilled_bytes,
    sum(disk_spilled_bytes)::BIGINT AS disk_spilled_bytes,
    sum(jvm_gc_time_ms)::BIGINT AS gc_time_ms
  FROM tasks_effective GROUP BY stage_id
)
SELECT attempts.stage_id, attempts.name, attempts.status, attempts.attempt_count,
  coalesce(task_attempts.task_count, 0) AS task_count, coalesce(task_attempts.failed_task_count, 0) AS failed_task_count,
  attempts.submit_ms, walls.wall_ms, walls.wall_share,
  effective.durations[len(effective.durations) // 2 + 1] AS median_task_duration_ms,
  list_max(effective.durations) AS max_task_duration_ms,
  effective.input_bytes, effective.output_bytes, effective.shuffle_read_bytes, effective.shuffle_write_bytes,
  effective.memory_spilled_bytes, effective.disk_spilled_bytes, effective.gc_time_ms
FROM attempts
LEFT JOIN task_attempts USING (stage_id)
LEFT JOIN effective USING (stage_id)
LEFT JOIN walls USING (stage_id)
"""

_SHUFFLE_READ = (
    'CASE WHEN shuffle_remote_read_bytes IS NULL AND shuffle_local_read_bytes IS NULL THEN NULL '
    'ELSE coalesce(shuffle_remote_read_bytes, 0) + coalesce(shuffle_local_read_bytes, 0) END'
)

# Every task metric the Stages tab summarizes. Each is a column of `tasks` but shuffle_read_bytes.
_QUANTILE_METRICS = (
    'duration_ms',
    'scheduler_delay_ms',
    'executor_deserialize_time_ms',
    'executor_run_time_ms',
    'executor_cpu_time_ns',
    'jvm_gc_time_ms',
    'result_serialization_time_ms',
    'getting_result_ms',
    'peak_execution_memory_bytes',
    'input_bytes',
    'input_records',
    'output_bytes',
    'output_records',
    'shuffle_read_bytes',
    'shuffle_read_records',
    'shuffle_remote_read_bytes',
    'shuffle_fetch_wait_time_ms',
    'shuffle_write_bytes',
    'shuffle_write_records',
    'shuffle_write_time_ns',
    'memory_spilled_bytes',
    'disk_spilled_bytes',
    'peak_jvm_heap_bytes',
)
_COMPUTED = {'shuffle_read_bytes': _SHUFFLE_READ}
_SORTED_LISTS = ', '.join(
    f'list_sort(list({_COMPUTED.get(name, name)}) FILTER (WHERE {_COMPUTED.get(name, name)} IS NOT NULL)) AS "{name}"'
    for name in _QUANTILE_METRICS
)
# The value at quantile q is the one at sorted index min(floor(q * n), n - 1), as the Stages tab takes it.
_QUANTILE_VALUES = ', '.join(
    f'sorted."{name}"[least(floor(quantiles.fraction * len(sorted."{name}"))::BIGINT, len(sorted."{name}") - 1) + 1] AS "{name}"'
    for name in _QUANTILE_METRICS
)
_QUANTILES_SQL = f"""
WITH sorted AS (
  SELECT {_SORTED_LISTS} FROM tasks_effective WHERE stage_id = $stage_id
), quantiles(quantile, fraction, position) AS (
  VALUES ('min', 0.0, 1), ('p25', 0.25, 2), ('p50', 0.5, 3), ('p75', 0.75, 4), ('p95', 0.95, 5), ('max', 1.0, 6)
)
SELECT quantiles.quantile, {_QUANTILE_VALUES}
FROM quantiles, sorted
ORDER BY quantiles.position
"""

_STAGE_TASKS_SQL = f'SELECT *, {_SHUFFLE_READ} AS shuffle_read_bytes FROM tasks WHERE stage_id = $stage_id'


@dc.dataclass(frozen=True, slots=True)
class StageDetail:
    """One stage's summary, and the quantiles of its successful tasks' metrics: one row each for min, p25, p50, p75, p95 and max."""

    stage: dict[str, Any]
    task_quantiles: Rows

    def to_json(self) -> dict[str, Any]:
        return {'stage': self.stage, 'task_quantiles': self.task_quantiles.to_json()}


def stage_summaries(tables: Tables, *, sort: str | None = None, limit: int | None = None) -> Rows:
    """One row per stage, in stage id order unless sorted by a column, largest first."""
    return ordered_rows(tables, STAGE_SUMMARIES_SQL, order_by='stage_id', sort=sort, limit=limit)


def stage_detail(tables: Tables, stage_id: int) -> StageDetail:
    """One stage's summary and its task quantiles. A stage the run does not hold raises `NotInRunError`."""
    _require_stage(tables, stage_id)
    summary = rows_of(tables.query(f'SELECT * FROM ({STAGE_SUMMARIES_SQL}) WHERE stage_id = $stage_id', {'stage_id': stage_id}))
    quantiles = rows_of(tables.query(_QUANTILES_SQL, {'stage_id': stage_id}))
    return StageDetail(summary.to_json()[0], quantiles)


def stage_tasks(tables: Tables, stage_id: int, *, sort: str | None = None, limit: int | None = None) -> Rows:
    """Every task attempt of one stage, with every column of `tasks` and `shuffle_read_bytes`, local and remote together.

    The rows run by stage attempt, task index and task attempt unless sorted by a column. A stage
    the run does not hold raises `NotInRunError`.
    """
    _require_stage(tables, stage_id)
    return ordered_rows(
        tables,
        _STAGE_TASKS_SQL,
        order_by='stage_attempt, task_index, attempt',
        sort=sort,
        limit=limit,
        parameters={'stage_id': stage_id},
    )


def _require_stage(tables: Tables, stage_id: int) -> None:
    held = [row[0] for row in tables.query('SELECT DISTINCT stage_id FROM stages ORDER BY stage_id').fetchall()]
    if stage_id not in held:
        raise NotInRunError(f'the run has no stage {stage_id}', field='stage_id', valid=[str(held_id) for held_id in held])
