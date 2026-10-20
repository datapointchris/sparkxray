"""What one run holds for a diff: its outcome, SQL executions, stages and their metrics, settings, and the exceptions it logged."""

from __future__ import annotations

import dataclasses as dc
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from sparkxray.rules.failure_reasons import blamed_stage
from sparkxray.rules.failure_reasons import failure_line
from sparkxray.summaries.stages import STAGE_SUMMARIES_SQL
from sparkxray.tables.registry import RUN_SPAN_SQL
from sparkxray.tables.registry import WALL_SHARES_SQL
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import rows_of

CONFIG_SECTIONS = ('spark', 'hadoop', 'system')

_OUTCOME_SQL = f"""
WITH run AS ({RUN_SPAN_SQL})
SELECT application.app_id, application.app_name, application.spark_version,
  run.end_epoch_ms - run.start_epoch_ms AS wall_ms,
  application.end_epoch_ms IS NOT NULL AS ended,
  application.end_epoch_ms IS NOT NULL AND NOT EXISTS (SELECT 1 FROM jobs WHERE result IS DISTINCT FROM 'JobSucceeded') AS succeeded
FROM application, run
"""
_EXECUTIONS_SQL = f"""
WITH walls AS (
  SELECT id AS execution_id, wall_ms FROM ({WALL_SHARES_SQL}) WHERE level = 'sql_execution'
), roots AS (
  SELECT plan_nodes.execution_id,
    max(plan_nodes.subtree_hash) FILTER (WHERE plan_nodes.plan_version = 0) AS initial_plan_hash,
    max(plan_nodes.subtree_hash) FILTER (WHERE plan_nodes.plan_version = sql_executions.plan_version_count - 1) AS final_plan_hash
  FROM plan_nodes JOIN sql_executions USING (execution_id)
  WHERE plan_nodes.node_id = 0
  GROUP BY plan_nodes.execution_id
)
SELECT sql_executions.execution_id, sql_executions.description, walls.wall_ms, roots.initial_plan_hash, roots.final_plan_hash,
  sql_executions.end_epoch_ms IS NOT NULL AND sql_executions.error IS NULL AS finished
FROM sql_executions LEFT JOIN walls USING (execution_id) LEFT JOIN roots USING (execution_id)
ORDER BY sql_executions.execution_id
"""
_FINAL_NODES_SQL = """
SELECT plan_nodes.execution_id, plan_nodes.node_id, plan_nodes.operator, plan_nodes.normalized_label
FROM plan_nodes JOIN sql_executions USING (execution_id)
WHERE plan_nodes.plan_version = sql_executions.plan_version_count - 1
ORDER BY plan_nodes.execution_id, plan_nodes.node_id
"""
# A stage counts toward the SQL execution of the first job listing it, which is the job that ran it. A later
# job lists it only to skip it. Every total but the task count is over `tasks_effective`, as `stages list` takes it.
_STAGES_SQL = f"""
WITH summaries AS ({STAGE_SUMMARIES_SQL}), owners AS (
  SELECT stage_id, arg_min(sql_execution_id, job_id) AS execution_id
  FROM (SELECT job_id, sql_execution_id, unnest(stage_ids) AS stage_id FROM jobs)
  GROUP BY stage_id
), effective AS (
  SELECT stage_id,
    sum(executor_run_time_ms)::BIGINT AS executor_run_time_ms,
    sum(executor_cpu_time_ns)::BIGINT AS executor_cpu_time_ns,
    sum(input_records)::BIGINT AS input_records,
    sum(output_records)::BIGINT AS output_records,
    sum(shuffle_read_records)::BIGINT AS shuffle_read_records,
    sum(shuffle_write_records)::BIGINT AS shuffle_write_records,
    list_sort(list(coalesce(shuffle_remote_read_bytes, 0) + coalesce(shuffle_local_read_bytes, 0))) AS shuffle_reads
  FROM tasks_effective GROUP BY stage_id
)
SELECT summaries.stage_id, summaries.name, owners.execution_id, summaries.status = 'complete' AS finished,
  summaries.wall_ms, summaries.task_count,
  effective.executor_run_time_ms, effective.executor_cpu_time_ns,
  summaries.input_bytes, effective.input_records, summaries.output_bytes, effective.output_records,
  summaries.shuffle_read_bytes, effective.shuffle_read_records, summaries.shuffle_write_bytes, effective.shuffle_write_records,
  summaries.memory_spilled_bytes, summaries.disk_spilled_bytes, summaries.gc_time_ms,
  round(summaries.max_task_duration_ms / nullif(summaries.median_task_duration_ms, 0), 2) AS task_duration_max_to_median_ratio,
  round(list_max(effective.shuffle_reads) / nullif(effective.shuffle_reads[len(effective.shuffle_reads) // 2 + 1], 0), 2)
    AS shuffle_read_max_to_median_ratio
FROM summaries LEFT JOIN owners USING (stage_id) LEFT JOIN effective USING (stage_id)
ORDER BY summaries.stage_id
"""
# The nodes of the execution's last plan whose metrics a stage's tasks reported to.
_STAGE_LABELS_SQL = """
SELECT node_stages.execution_id, node_stages.stage_id, list_sort(list(DISTINCT plan_nodes.normalized_label)) AS labels
FROM node_stages
JOIN sql_executions USING (execution_id)
JOIN plan_nodes ON plan_nodes.execution_id = node_stages.execution_id AND plan_nodes.node_id = node_stages.node_id
  AND plan_nodes.plan_version = sql_executions.plan_version_count - 1
GROUP BY ALL
"""
_CONFIG_SQL = f'SELECT section, key, value FROM environment WHERE list_contains({list(CONFIG_SECTIONS)}, section)'
_HOSTS_SQL = """
SELECT DISTINCT host FROM executors WHERE host IS NOT NULL
UNION SELECT value FROM environment WHERE section = 'spark' AND key = 'spark.driver.host'
"""
# Every exception the run logged: each task attempt that did not succeed, each stage attempt that
# failed, each job that did, and each a joined driver log or Glue insight holds, by the line naming it.
_EXCEPTIONS_SQL = f"""
SELECT 'task' AS source, end_reason AS reason, {failure_line('end_reason_detail')} AS line, task_id, stage_id,
  finish_epoch_ms AS epoch_ms
FROM tasks WHERE end_reason IS NOT NULL AND end_reason <> 'Success'
UNION ALL
SELECT 'stage', NULL, {failure_line('failure_reason')}, NULL, stage_id, complete_epoch_ms FROM stages WHERE failure_reason IS NOT NULL
UNION ALL
SELECT 'job', NULL, {failure_line('failure_reason')}, NULL, {blamed_stage('failure_reason')}, end_epoch_ms
FROM jobs WHERE result = 'JobFailed'
UNION ALL
SELECT 'driver_log', NULL, concat_ws(': ', exception.class, nullif(split_part(exception.msg, chr(10), 1), '')), NULL, NULL,
  logged_epoch_ms
FROM driver_log WHERE exception.class IS NOT NULL
UNION ALL
SELECT 'glue_insights', event, {failure_line('failure_reason')}, NULL, NULL, logged_epoch_ms
FROM glue_insights WHERE failure_reason IS NOT NULL
ORDER BY epoch_ms NULLS LAST, source, task_id NULLS LAST
"""


@dc.dataclass(frozen=True, slots=True)
class LoggedException:
    """One exception a run logged, with the one line naming it.

    A task attempt, a stage attempt or a job logged it, or the driver's structured log or Glue's job run insights held it.
    """

    source: str
    reason: str | None
    line: str | None
    task_id: int | None
    stage_id: int | None
    epoch_ms: int | None


@dc.dataclass(frozen=True)
class RunFacts:
    """What a diff reads from one run, each part keyed as the alignment and the comparison look it up."""

    outcome: Mapping[str, Any]
    executions: tuple[Mapping[str, Any], ...]
    final_operators: Mapping[int, Mapping[str, int]]
    final_node_labels: Mapping[tuple[int, int], str]
    stages: tuple[Mapping[str, Any], ...]
    stage_labels: Mapping[tuple[int, int], tuple[str, ...]]
    config: Mapping[tuple[str, str], str | None]
    hosts: tuple[str, ...]
    exceptions: tuple[LoggedException, ...]

    @classmethod
    def read(cls, tables: Tables) -> RunFacts:
        operators: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        labels = {}
        for execution_id, node_id, operator, label in tables.query(_FINAL_NODES_SQL).fetchall():
            operators[execution_id][operator] += 1
            labels[(execution_id, node_id)] = label
        return cls(
            outcome=rows_of(tables.query(_OUTCOME_SQL)).to_json()[0],
            executions=tuple(rows_of(tables.query(_EXECUTIONS_SQL)).to_json()),
            final_operators={execution_id: dict(counts) for execution_id, counts in operators.items()},
            final_node_labels=labels,
            stages=tuple(rows_of(tables.query(_STAGES_SQL)).to_json()),
            stage_labels={
                (execution_id, stage_id): tuple(stage_labels)
                for execution_id, stage_id, stage_labels in tables.query(_STAGE_LABELS_SQL).fetchall()
            },
            config={(section, key): value for section, key, value in tables.query(_CONFIG_SQL).fetchall()},
            hosts=tuple(sorted({host for (host,) in tables.query(_HOSTS_SQL).fetchall() if host}, key=len, reverse=True)),
            exceptions=tuple(LoggedException(*row) for row in tables.query(_EXCEPTIONS_SQL).fetchall()),
        )
