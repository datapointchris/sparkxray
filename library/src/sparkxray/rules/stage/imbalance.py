"""Rules over where a stage's tasks ran: one executor or one host doing most of the work while the others wait."""

from __future__ import annotations

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Severity
from sparkxray.rules.rule import Threshold
from sparkxray.rules.stage.statistics import STAGE_CTES


def _imbalance_sql(worker: str, named_as: str) -> str:
    """The stages where one `worker`, an executor or a host, ran most of the task time and at least half the tasks, named `named_as`."""
    # The stage's totals are grouped and joined, because DuckDB 1.5.0 cannot bind three window
    # aggregates over one partition.
    return f"""
WITH {STAGE_CTES}, workers AS (
  SELECT stage_id, {worker} AS worker, count(*) AS worker_task_count, sum(duration_ms) AS worker_duration_ms
  FROM tasks_effective WHERE {worker} IS NOT NULL GROUP BY stage_id, {worker}
), worker_totals AS (
  SELECT stage_id, count(*) AS worker_count, sum(worker_task_count) AS task_count, sum(worker_duration_ms) AS duration_ms
  FROM workers GROUP BY stage_id
), shares AS (
  SELECT workers.*, worker_totals.worker_count,
    workers.worker_task_count / worker_totals.task_count AS task_share,
    workers.worker_duration_ms / nullif(worker_totals.duration_ms, 0) AS duration_share
  FROM workers JOIN worker_totals USING (stage_id)
), heaviest AS (
  SELECT * FROM shares
  QUALIFY row_number() OVER (PARTITION BY stage_id ORDER BY duration_share DESC NULLS LAST, worker) = 1
)
SELECT heaviest.stage_id, stage_statistics.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  heaviest.worker AS {named_as}, 'medium' AS severity,
  stage_statistics.task_count, heaviest.worker_count, heaviest.worker_task_count, heaviest.worker_duration_ms,
  round(heaviest.task_share, 4) AS worker_task_share, round(heaviest.duration_share, 4) AS worker_duration_share
FROM heaviest
JOIN stage_statistics USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
WHERE stage_statistics.task_count >= $min_tasks AND heaviest.worker_count >= 2
  AND heaviest.duration_share >= $min_duration_share AND heaviest.task_share >= $min_task_share
ORDER BY heaviest.stage_id
"""


_THRESHOLDS = (
    Threshold('min_tasks', 10, 'count', 'sparkDoctor docs/detections.md'),
    Threshold('min_duration_share', 0.75, 'fraction', 'sparkDoctor docs/detections.md'),
    Threshold('min_task_share', 0.5, 'fraction', 'sparkDoctor docs/detections.md'),
)
_LADDER: dict[Severity, str] = {
    'medium': 'across at least min_tasks tasks on two workers or more, one ran at least min_duration_share of the task time '
    'and min_task_share of the tasks',
}

_EXECUTOR_IMBALANCE_LESSON = """\
## What it measures

Each executor's share of a stage's successful tasks, and of their summed duration. The rule
reports the executor with the largest share of duration, where it also ran at least half the
tasks.

## Why it costs time

The stage finishes when its busiest executor does. An executor that ran most of the tasks ran
them one after another on its own cores, while the others' cores had nothing to run. That
happens when tasks wait for data locality on the executor caching their input, or when other
executors joined only after the stage's tasks were handed out.

## Where the Spark UI shows it

Stages tab › the stage › "Aggregated Metrics by Executor": Task Time, Total Tasks and Succeeded
Tasks for each executor. The "Event Timeline" shows the tasks stacked on one executor's row.

## How to fix it

- Lower `spark.locality.wait`, so a task waiting for its cached executor runs elsewhere sooner.
- Where the stage reads a cache held by one executor, repartition before caching, so the cached
  blocks spread.
- Where executors joined late, start with more: raise `spark.dynamicAllocation.initialExecutors`
  or `spark.executor.instances`.
"""

EXECUTOR_IMBALANCE = Rule(
    rule_id='stage.executor-imbalance',
    title='Executor {executor_id} ran {worker_duration_share:.0%} of the task time of stage {stage_id}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=_THRESHOLDS,
    severity_ladder=_LADDER,
    remediation=(
        'lower spark.locality.wait',
        'repartition before caching, so the cached blocks spread',
        'start with more executors',
    ),
    docs=('https://spark.apache.org/docs/latest/configuration.html#scheduling',),
    lesson=_EXECUTOR_IMBALANCE_LESSON,
    sql=_imbalance_sql('executor_id', 'executor_id'),
)

_HOST_IMBALANCE_LESSON = """\
## What it measures

Each host's share of a stage's successful tasks, and of their summed duration, counting every
executor on the host together. The rule reports the host with the largest share of duration,
where it also ran at least half the tasks.

## Why it costs time

The stage finishes when its busiest host does. A host that ran most of the tasks was the one
holding their data, under HDFS locality or a cache, or the one with the most executors placed on
it, and the other hosts' cores waited.

## Where the Spark UI shows it

Stages tab › the stage › the task table, sorted by Host, and "Aggregated Metrics by Executor",
whose Address column names each executor's host.

## How to fix it

- Lower `spark.locality.wait.node`, so a task waiting for its host runs elsewhere sooner.
- Spread the input: more, smaller files, or a repartition before the heavy stage.
- Ask the cluster manager to spread executors across hosts.
"""

HOST_IMBALANCE = Rule(
    rule_id='stage.host-imbalance',
    title='Host {host} ran {worker_duration_share:.0%} of the task time of stage {stage_id}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=_THRESHOLDS,
    severity_ladder=_LADDER,
    remediation=(
        'lower spark.locality.wait.node',
        'spread the input across more, smaller files, or repartition before the heavy stage',
        'spread executors across hosts',
    ),
    docs=('https://spark.apache.org/docs/latest/configuration.html#scheduling',),
    lesson=_HOST_IMBALANCE_LESSON,
    sql=_imbalance_sql('host', 'host'),
)

RULES = (EXECUTOR_IMBALANCE, HOST_IMBALANCE)
