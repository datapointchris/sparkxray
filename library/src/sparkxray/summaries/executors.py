"""Each executor and the driver: its lifetime, cores and memory, the tasks it ran, and how much of its time sat idle."""

from __future__ import annotations

from sparkxray.summaries.ordering import ordered_rows
from sparkxray.tables.registry import RUN_SPAN_SQL
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import Rows

# An executor never removed lives to the run's end. Its idle share is the part of its core time,
# lifetime times cores, that no task attempt ran in, failed and speculative attempts included.
EXECUTOR_SUMMARIES_SQL = f"""
WITH run AS ({RUN_SPAN_SQL}), task_totals AS (
  SELECT executor_id, count(*) AS task_count, count(*) FILTER (WHERE failed) AS failed_task_count,
    sum(duration_ms)::BIGINT AS task_time_ms, max(peak_jvm_heap_bytes) AS peak_jvm_heap_bytes
  FROM tasks GROUP BY executor_id
), lifetimes AS (
  SELECT executors.*, coalesce(executors.remove_epoch_ms, run.end_epoch_ms) - executors.add_epoch_ms AS lifetime_ms FROM executors, run
)
SELECT lifetimes.executor_id, lifetimes.host, lifetimes.is_driver, lifetimes.core_count, lifetimes.executor_memory_bytes,
  lifetimes.add_epoch_ms, lifetimes.remove_epoch_ms, lifetimes.lifetime_ms,
  coalesce(task_totals.task_count, 0) AS task_count, coalesce(task_totals.failed_task_count, 0) AS failed_task_count,
  task_totals.task_time_ms,
  CASE WHEN lifetimes.core_count > 0 AND lifetimes.lifetime_ms > 0
    THEN round(greatest(0, 1 - coalesce(task_totals.task_time_ms, 0) / (lifetimes.lifetime_ms * lifetimes.core_count)), 4)
  END AS idle_share,
  task_totals.peak_jvm_heap_bytes, lifetimes.remove_reason
FROM lifetimes LEFT JOIN task_totals USING (executor_id)
"""


def executor_summaries(tables: Tables, *, limit: int | None = None) -> Rows:
    """One row per executor, the driver first and then by executor id."""
    return ordered_rows(
        tables, EXECUTOR_SUMMARIES_SQL, order_by='is_driver DESC, try_cast(executor_id AS BIGINT), executor_id', limit=limit
    )
