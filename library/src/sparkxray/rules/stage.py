"""Rules over a stage's tasks: how evenly its work spread across them."""

from __future__ import annotations

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Threshold

# Every successful task of a stage counts, those that read nothing included, as in the Stages tab's
# summary metrics and AQE's skew join. The median is the value at sorted index n / 2, as the Stages
# tab takes it. A partition is skewed, as AQE judges one, when it is over both the ratio times the
# median and the size floor. So where most tasks read nothing, the median is 0 and the floor decides.
_SHUFFLE_SKEW_SQL = """
WITH stage_tasks AS (
  SELECT stage_id, stage_attempt, task_id, duration_ms, coalesce(shuffle_read_records, 0) AS read_records,
    coalesce(shuffle_remote_read_bytes, 0) + coalesce(shuffle_local_read_bytes, 0) AS read_bytes
  FROM tasks_effective
), stage_reads AS (
  SELECT stage_id, count(*) AS task_count,
    max(read_bytes) AS max_shuffle_read_bytes,
    list_sort(list(read_bytes))[count(*) // 2 + 1] AS median_shuffle_read_bytes,
    max(read_records) AS max_shuffle_records,
    list_sort(list(read_records))[count(*) // 2 + 1] AS median_shuffle_records,
    max(duration_ms) AS max_task_duration_ms,
    list_sort(list(duration_ms))[count(*) // 2 + 1] AS median_task_duration_ms,
    arg_max(stage_attempt, read_bytes) AS stage_attempt
  FROM stage_tasks GROUP BY stage_id
  HAVING max(read_bytes) > 0
), skewed AS (
  SELECT * FROM stage_reads
  WHERE task_count >= $min_tasks AND max_shuffle_read_bytes > $min_bytes AND max_shuffle_read_bytes > $ratio * median_shuffle_read_bytes
), worst AS (
  SELECT stage_tasks.stage_id, list(stage_tasks.task_id ORDER BY stage_tasks.read_bytes DESC, stage_tasks.task_id)[1:5] AS worst_task_ids
  FROM stage_tasks JOIN skewed USING (stage_id)
  WHERE stage_tasks.read_bytes > $ratio * skewed.median_shuffle_read_bytes AND stage_tasks.read_bytes > $min_bytes
  GROUP BY stage_tasks.stage_id
), stage_jobs AS (
  SELECT skewed.stage_id, arg_min(jobs.job_id, jobs.job_id) AS job_id, arg_min(jobs.sql_execution_id, jobs.job_id) AS sql_execution_id
  FROM skewed JOIN jobs ON list_contains(jobs.stage_ids, skewed.stage_id)
  GROUP BY skewed.stage_id
), joins AS (
  -- The join node that ran in the stage, where exactly one did.
  SELECT node_stages.execution_id, node_stages.stage_id,
    CASE WHEN count(*) = 1 THEN min(plan_nodes.node_id) END AS plan_node_id,
    CASE WHEN count(*) = 1 THEN min(plan_nodes.label) END AS plan_node_label
  FROM node_stages
  JOIN sql_executions USING (execution_id)
  JOIN plan_nodes ON plan_nodes.execution_id = node_stages.execution_id AND plan_nodes.node_id = node_stages.node_id
    AND plan_nodes.plan_version = sql_executions.plan_version_count - 1
  WHERE plan_nodes.operator IN ('SortMergeJoin', 'ShuffledHashJoin')
  GROUP BY node_stages.execution_id, node_stages.stage_id
), spark_settings AS (
  SELECT max(value) FILTER (WHERE key = 'spark.sql.adaptive.enabled') AS adaptive,
    max(value) FILTER (WHERE key = 'spark.sql.adaptive.skewJoin.enabled') AS skew_join
  FROM environment WHERE section = 'spark'
), spark_release AS (
  SELECT coalesce(try_cast(split_part(spark_version, '.', 1) AS INTEGER), 0) * 100
    + coalesce(try_cast(split_part(spark_version, '.', 2) AS INTEGER), 0) AS number
  FROM application
)
SELECT skewed.stage_id, skewed.stage_attempt, stage_jobs.job_id, stage_jobs.sql_execution_id,
  joins.plan_node_id, joins.plan_node_label, worst.worst_task_ids,
  CASE WHEN skewed.max_shuffle_read_bytes > 1073741824 THEN 'high' ELSE 'medium' END AS severity,
  round((skewed.max_task_duration_ms - skewed.median_task_duration_ms) / 1000, 3) AS impact_seconds,
  skewed.task_count,
  skewed.max_shuffle_read_bytes, skewed.median_shuffle_read_bytes,
  round(skewed.max_shuffle_read_bytes / nullif(skewed.median_shuffle_read_bytes, 0), 2) AS shuffle_read_bytes_ratio,
  skewed.max_shuffle_records, skewed.median_shuffle_records,
  round(skewed.max_shuffle_records / nullif(skewed.median_shuffle_records, 0), 2) AS shuffle_records_ratio,
  skewed.max_task_duration_ms, skewed.median_task_duration_ms,
  -- A session's own setting wins, then the application's, then the release's default:
  -- AQE is on from Spark 3.2, and its skew join is on wherever AQE is.
  CASE WHEN stage_jobs.sql_execution_id IS NOT NULL THEN
    lower(coalesce(sql_executions.modified_configs['spark.sql.adaptive.enabled'], spark_settings.adaptive,
      CASE WHEN spark_release.number >= 302 THEN 'true' ELSE 'false' END)) = 'true'
    AND lower(coalesce(sql_executions.modified_configs['spark.sql.adaptive.skewJoin.enabled'], spark_settings.skew_join, 'true')) = 'true'
  END AS aqe_skew_join_enabled
FROM skewed
JOIN worst USING (stage_id)
LEFT JOIN stage_jobs USING (stage_id)
LEFT JOIN sql_executions ON sql_executions.execution_id = stage_jobs.sql_execution_id
LEFT JOIN joins ON joins.stage_id = skewed.stage_id AND joins.execution_id = stage_jobs.sql_execution_id
LEFT JOIN spark_settings ON true
LEFT JOIN spark_release ON true
ORDER BY skewed.stage_id
"""

_SHUFFLE_SKEW_LESSON = """\
## What it measures

Each task of a stage that reads a shuffle reads one partition of it. `Shuffle Read Metrics` gives
the bytes each task fetched, from other executors (`Remote Bytes Read`) and from its own
(`Local Bytes Read`), and the records they held. The median is taken over every successful task
of the stage, those that read an empty partition included.

## Why it costs time

A stage ends when its last task does. When one key holds most of the rows, the task reading that
key's partition runs long after the others finish, and their cores sit idle until it does. A
ratio alone flags noise on stages of tiny tasks, so the rule also needs the largest read to be
over a size worth splitting.

## Where the Spark UI shows it

Stages tab › the stage › "Summary Metrics for Completed Tasks". Compare the Max column with the
Median column for Shuffle Read Size / Records. Both are taken over every completed task, as this
rule takes them. The SQL tab's join node shows the same spread in its metrics.

## How to fix it

- Turn on AQE's skew join, `spark.sql.adaptive.skewJoin.enabled=true`, which splits a partition
  over `spark.sql.adaptive.skewJoin.skewedPartitionFactor` times the median and over
  `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`.
- Salt the hot key: add a random suffix on the large side and replicate the small side over it.
- Broadcast the smaller side, under `spark.sql.autoBroadcastJoinThreshold`, so no shuffle runs.
"""

SHUFFLE_SKEW = Rule(
    rule_id='stage.shuffle-skew',
    title=(
        'One shuffle partition of stage {stage_id} read {max_shuffle_read_bytes:bytes}, '
        'against a median of {median_shuffle_read_bytes:bytes}'
    ),
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('ratio', 5, 'x', 'sparkDoctor docs/detections.md; AQE spark.sql.adaptive.skewJoin.skewedPartitionFactor'),
        Threshold('min_bytes', 268435456, 'bytes', 'AQE spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'),
        Threshold('min_tasks', 10, 'count', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'the largest shuffle read is over min_bytes and over ratio times the median, across at least min_tasks tasks',
        'high': 'the largest shuffle read is over 1 GiB',
    },
    remediation=(
        'set spark.sql.adaptive.skewJoin.enabled=true',
        'salt the hot key',
        'broadcast the smaller side',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join',),
    lesson=_SHUFFLE_SKEW_LESSON,
    sql=_SHUFFLE_SKEW_SQL,
)

RULES = (SHUFFLE_SKEW,)
