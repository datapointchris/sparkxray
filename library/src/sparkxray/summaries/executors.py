"""Each executor and the driver: its lifetime, cores and memory, the tasks it ran, and how much of its time sat idle."""

from __future__ import annotations

from sparkxray.summaries.ordering import ordered_rows
from sparkxray.tables.registry import EXECUTOR_TIME_CTES
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import Rows

# An executor's idle share is the part of its core time, lifetime times cores, that no task held.
EXECUTOR_SUMMARIES_SQL = f"""
WITH {EXECUTOR_TIME_CTES}
SELECT lifetimes.executor_id, lifetimes.host, lifetimes.is_driver, lifetimes.core_count, lifetimes.executor_memory_bytes,
  lifetimes.add_epoch_ms, lifetimes.remove_epoch_ms, lifetimes.lifetime_ms,
  coalesce(task_totals.task_count, 0) AS task_count, coalesce(task_totals.failed_task_count, 0) AS failed_task_count,
  task_totals.task_time_ms,
  CASE WHEN lifetimes.core_count > 0 AND lifetimes.lifetime_ms > 0
    THEN round(greatest(0, 1 - coalesce(task_totals.task_time_ms, 0) * task_cpus.cpus / (lifetimes.lifetime_ms * lifetimes.core_count)), 4)
  END AS idle_share,
  lifetimes.peak_jvm_heap_bytes, lifetimes.peak_memory_source, lifetimes.remove_reason
FROM lifetimes LEFT JOIN task_totals USING (executor_id), task_cpus
"""


def executor_summaries(tables: Tables, *, limit: int | None = None) -> Rows:
    """One row per executor, the driver first and then by executor id."""
    return ordered_rows(
        tables, EXECUTOR_SUMMARIES_SQL, order_by='is_driver DESC, try_cast(executor_id AS BIGINT), executor_id', limit=limit
    )
