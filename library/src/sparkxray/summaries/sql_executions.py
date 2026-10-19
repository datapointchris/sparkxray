"""Each SQL execution's duration, jobs, stages and last-plan operators, and one execution with its jobs."""

from __future__ import annotations

import dataclasses as dc
from typing import Any

from sparkxray.errors import NotInRunError
from sparkxray.summaries.ordering import ordered_rows
from sparkxray.tables.registry import WALL_SHARES_SQL
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import Rows
from sparkxray.tables.rows import rows_of

# A stage counts once it ran, so a stage a job skipped because its shuffle output was already
# written is left out. Operators are counted in the execution's last plan: an exchange other than
# a reuse of one, a join or Cartesian product, and a scan of any source.
SQL_SUMMARIES_SQL = f"""
WITH walls AS (
  SELECT id AS execution_id, wall_ms, share AS wall_share FROM ({WALL_SHARES_SQL}) WHERE level = 'sql_execution'
), job_totals AS (
  SELECT sql_execution_id AS execution_id, count(*) AS job_count FROM jobs WHERE sql_execution_id IS NOT NULL GROUP BY 1
), stage_totals AS (
  SELECT job_stages.sql_execution_id AS execution_id, count(DISTINCT job_stages.stage_id) AS stage_count
  FROM (SELECT sql_execution_id, unnest(stage_ids) AS stage_id FROM jobs WHERE sql_execution_id IS NOT NULL) AS job_stages
  JOIN (SELECT DISTINCT stage_id FROM stages) AS ran USING (stage_id)
  GROUP BY 1
), last_plans AS (
  SELECT plan_nodes.execution_id, count(*) AS node_count,
    count(*) FILTER (WHERE operator LIKE '%Exchange' AND operator <> 'ReusedExchange') AS exchange_count,
    count(*) FILTER (WHERE operator LIKE '%Join' OR operator = 'CartesianProduct') AS join_count,
    count(*) FILTER (WHERE operator LIKE '%Scan') AS scan_count
  FROM plan_nodes JOIN sql_executions USING (execution_id)
  WHERE plan_nodes.plan_version = sql_executions.plan_version_count - 1
  GROUP BY 1
)
SELECT sql_executions.execution_id, sql_executions.description,
  CASE WHEN sql_executions.error IS NOT NULL THEN 'failed' WHEN sql_executions.end_ms IS NOT NULL THEN 'complete' ELSE 'running' END
    AS status,
  sql_executions.start_ms, walls.wall_ms AS duration_ms, walls.wall_share,
  coalesce(job_totals.job_count, 0) AS job_count, coalesce(stage_totals.stage_count, 0) AS stage_count,
  sql_executions.plan_version_count,
  coalesce(last_plans.node_count, 0) AS node_count, coalesce(last_plans.exchange_count, 0) AS exchange_count,
  coalesce(last_plans.join_count, 0) AS join_count, coalesce(last_plans.scan_count, 0) AS scan_count
FROM sql_executions
LEFT JOIN walls USING (execution_id)
LEFT JOIN job_totals USING (execution_id)
LEFT JOIN stage_totals USING (execution_id)
LEFT JOIN last_plans USING (execution_id)
"""

_EXECUTION_SQL = f"""
SELECT summary.*, sql_executions.root_execution_id, sql_executions.error, sql_executions.modified_configs,
  sql_executions.final_plan_truncated, sql_executions.details
FROM ({SQL_SUMMARIES_SQL}) AS summary JOIN sql_executions USING (execution_id)
WHERE execution_id = $execution_id
"""

_JOBS_SQL = """
SELECT job_id, result, submit_ms, end_ms, end_ms - submit_ms AS duration_ms, stage_ids, job_group, failure_reason
FROM jobs WHERE sql_execution_id = $execution_id ORDER BY job_id
"""

_OPERATORS_SQL = """
SELECT plan_nodes.operator, count(*) AS node_count
FROM plan_nodes JOIN sql_executions USING (execution_id)
WHERE plan_nodes.execution_id = $execution_id AND plan_nodes.plan_version = sql_executions.plan_version_count - 1
GROUP BY plan_nodes.operator ORDER BY node_count DESC, plan_nodes.operator
"""


@dc.dataclass(frozen=True, slots=True)
class SqlExecutionDetail:
    """One SQL execution's summary, the jobs it ran, and how many of each operator its last plan holds."""

    execution: dict[str, Any]
    jobs: Rows
    operator_counts: Rows

    def to_json(self) -> dict[str, Any]:
        return {'execution': self.execution, 'jobs': self.jobs.to_json(), 'operator_counts': self.operator_counts.to_json()}


def sql_summaries(tables: Tables, *, limit: int | None = None) -> Rows:
    """One row per SQL execution, in execution id order."""
    return ordered_rows(tables, SQL_SUMMARIES_SQL, order_by='execution_id', limit=limit)


def sql_detail(tables: Tables, execution_id: int) -> SqlExecutionDetail:
    """One SQL execution with its jobs and operators. An execution the run does not hold raises `NotInRunError`."""
    held = [row[0] for row in tables.query('SELECT execution_id FROM sql_executions ORDER BY execution_id').fetchall()]
    if execution_id not in held:
        raise NotInRunError(f'the run has no SQL execution {execution_id}', field='execution_id', valid=[str(held_id) for held_id in held])
    parameters = {'execution_id': execution_id}
    execution = rows_of(tables.query(_EXECUTION_SQL, parameters)).to_json()[0]
    return SqlExecutionDetail(execution, rows_of(tables.query(_JOBS_SQL, parameters)), rows_of(tables.query(_OPERATORS_SQL, parameters)))
