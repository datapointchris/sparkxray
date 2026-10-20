"""Each stage's task statistics, the job and SQL execution that ran it, and its slots, as SQL the stage rules build on.

`STAGE_CTES` defines four common table expressions, and a rule's query opens with
`WITH {STAGE_CTES}` and reads the ones it needs:

- `stage_statistics`: one row per stage over its effective tasks;
- `stage_owners`: the first job that ran each stage, and that job's SQL execution;
- `stage_slots`: the cores of the executors that ran each stage's tasks;
- `stage_walls`: each stage's wall time, from its first submission to its last completion.
"""

from __future__ import annotations


def quantile(expression: str, fraction: float) -> str:
    """SQL for a quantile as the Stages tab takes one: the value at sorted index min(n × fraction, n − 1), over non-null values."""
    values = f'list_sort(list({expression}) FILTER (WHERE {expression} IS NOT NULL))'
    return f'{values}[least(floor(count({expression}) * {fraction})::BIGINT, count({expression}) - 1) + 1]'


_READ = 'coalesce(shuffle_remote_read_bytes, 0) + coalesce(shuffle_local_read_bytes, 0)'

STAGE_CTES = f"""
stage_statistics AS (
  SELECT stage_id, max(stage_attempt) AS stage_attempt, count(*) AS task_count,
    sum(duration_ms) AS task_duration_ms, max(duration_ms) AS max_duration_ms,
    {quantile('duration_ms', 0.5)} AS median_duration_ms, avg(duration_ms) AS mean_duration_ms,
    {quantile('duration_ms', 0.95)} AS p95_duration_ms,
    sum(executor_run_time_ms) AS run_time_ms, max(executor_run_time_ms) AS max_run_time_ms,
    {quantile('executor_run_time_ms', 0.5)} AS median_run_time_ms,
    sum(jvm_gc_time_ms) AS gc_time_ms,
    sum(memory_spilled_bytes) AS memory_spilled_bytes, max(memory_spilled_bytes) AS max_memory_spilled_bytes,
    {quantile('memory_spilled_bytes', 0.5)} AS median_memory_spilled_bytes,
    sum(disk_spilled_bytes) AS disk_spilled_bytes, max(disk_spilled_bytes) AS max_disk_spilled_bytes,
    {quantile('disk_spilled_bytes', 0.5)} AS median_disk_spilled_bytes,
    count(*) FILTER (WHERE memory_spilled_bytes > 0 OR disk_spilled_bytes > 0) AS spilling_task_count,
    sum(executor_run_time_ms) FILTER (WHERE memory_spilled_bytes > 0 OR disk_spilled_bytes > 0) AS spilling_run_time_ms,
    sum({_READ}) AS shuffle_read_bytes, max({_READ}) AS max_shuffle_read_bytes,
    {quantile(_READ, 0.95)} AS p95_shuffle_read_bytes,
    sum(input_bytes) AS input_bytes, max(input_bytes) AS max_input_bytes,
    max(output_bytes) AS max_output_bytes, max(shuffle_write_bytes) AS max_shuffle_write_bytes,
    sum(scheduler_delay_ms) AS scheduler_delay_ms, {quantile('scheduler_delay_ms', 0.5)} AS median_scheduler_delay_ms,
    sum(executor_deserialize_time_ms) AS deserialize_time_ms,
    {quantile('executor_deserialize_time_ms', 0.5)} AS median_deserialize_time_ms,
    sum(shuffle_fetch_wait_time_ms) AS fetch_wait_time_ms,
    -- What a task spends beyond running: scheduling, deserializing, serializing and returning its result.
    sum(duration_ms - executor_run_time_ms) AS overhead_ms
  FROM tasks_effective GROUP BY stage_id
), stage_owners AS (
  SELECT stage_id, min(job_id) AS job_id, arg_min(sql_execution_id, job_id) AS sql_execution_id
  FROM (SELECT job_id, sql_execution_id, unnest(stage_ids) AS stage_id FROM jobs)
  GROUP BY stage_id
), stage_slots AS (
  SELECT stage_executors.stage_id, sum(executors.core_count) AS core_count
  FROM (SELECT DISTINCT stage_id, executor_id FROM tasks) AS stage_executors
  JOIN executors USING (executor_id)
  GROUP BY stage_executors.stage_id
), stage_walls AS (
  SELECT stages.stage_id,
    coalesce(max(stages.complete_epoch_ms), max(task_ends.finish_epoch_ms)) - min(stages.submit_epoch_ms) AS wall_ms
  FROM stages
  LEFT JOIN (SELECT stage_id, max(finish_epoch_ms) AS finish_epoch_ms FROM tasks GROUP BY stage_id) AS task_ends USING (stage_id)
  GROUP BY stages.stage_id
)"""

# The slots a stage's tasks ran in at once: its executors' cores, and never more than its tasks.
SLOTS = 'least(stage_statistics.task_count, stage_slots.core_count)'
