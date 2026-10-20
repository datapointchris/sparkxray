"""Rules over how evenly a stage's work spread across its tasks: by run time, and by the shuffle bytes each read."""

from __future__ import annotations

from typing import Any

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold
from sparkxray.rules.spark_settings import ADAPTIVE
from sparkxray.rules.spark_settings import SKEW_JOIN
from sparkxray.rules.spark_settings import SPARK_RELEASE_CTE
from sparkxray.rules.spark_settings import skew_join_enabled
from sparkxray.rules.spark_settings import spark_settings_cte
from sparkxray.rules.stage.statistics import STAGE_CTES

GLUE_OBSERVABILITY = 'https://docs.aws.amazon.com/glue/latest/dg/monitor-observability.html'

# A stage's slowest task against its median one, by executor run time. The ratio needs a gap of
# whole seconds beside it, because a stage of tiny tasks scores a high ratio on milliseconds.
_TASK_SKEW_SQL = f"""
WITH {STAGE_CTES}, skewed AS (
  SELECT * FROM stage_statistics
  WHERE task_count >= $min_tasks AND max_run_time_ms - median_run_time_ms >= $min_gap_ms
    AND max_run_time_ms >= $ratio * median_run_time_ms
), worst AS (
  SELECT tasks_effective.stage_id,
    list(tasks_effective.task_id ORDER BY tasks_effective.executor_run_time_ms DESC, tasks_effective.task_id)[1:5] AS worst_task_ids
  FROM tasks_effective JOIN skewed USING (stage_id)
  WHERE tasks_effective.executor_run_time_ms - skewed.median_run_time_ms >= $min_gap_ms
    AND tasks_effective.executor_run_time_ms >= $ratio * skewed.median_run_time_ms
  GROUP BY tasks_effective.stage_id
)
SELECT skewed.stage_id, skewed.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id, worst.worst_task_ids,
  CASE WHEN skewed.max_run_time_ms - skewed.median_run_time_ms >= 300000 THEN 'high' ELSE 'medium' END AS severity,
  round((skewed.max_run_time_ms - skewed.median_run_time_ms) / 1000, 3) AS impact_seconds,
  skewed.task_count, skewed.max_run_time_ms, skewed.median_run_time_ms,
  skewed.max_run_time_ms - skewed.median_run_time_ms AS run_time_gap_ms,
  round(skewed.max_run_time_ms / nullif(skewed.median_run_time_ms, 0), 2) AS run_time_ratio,
  -- AWS Glue's stage skewness, over task duration as Glue takes it: 0 while the ratio is under 5, and 1 at twice that.
  round(greatest(0, skewed.max_duration_ms / nullif(skewed.median_duration_ms, 0) / 5 - 1), 2) AS glue_skewness_ratio
FROM skewed
JOIN worst USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
ORDER BY skewed.stage_id
"""

_TASK_SKEW_LESSON = """\
## What it measures

Each task's `Executor Run Time`: the milliseconds it spent running on an executor, leaving out
scheduling and deserializing it. The rule compares the slowest task of a stage with the median
one, taken over every successful task. `glue_skewness_ratio` is AWS Glue's stage skewness over
task duration: 0 below a ratio of 5, and 1 at a ratio of 10.

## Why it costs time

A stage ends when its last task does. While the slowest task runs on, the cores that finished the
other tasks wait for it, and so does every stage after it. A ratio alone fires on stages of tiny
tasks, where a 400 ms task is 20 times a 20 ms median and nothing is worth fixing. So the rule
also needs the slowest task to run at least {min_gap_ms} past the median.

## Where the Spark UI shows it

Stages tab › the stage › "Summary Metrics for Completed Tasks". Compare the Max column with the
Median column for Duration, and open "Event Timeline" to see the one long bar. Sort the task
table by Duration to find the task this rule names first.

## How to fix it

- Look at the slow task's input and shuffle read. Where it read far more than the median, the
  data is skewed, and `stage.shuffle-skew` says by how much.
- Where it read no more than the rest, the cost is in the work per row: a slow UDF on some
  values, a hot external lookup, or one large file that cannot split.
- Split the hot key by salting it, or repartition on a column that spreads the rows.
"""

TASK_SKEW = Rule(
    rule_id='stage.task-skew',
    title='The slowest task of stage {stage_id} ran {max_run_time_ms:duration}, against a median of {median_run_time_ms:duration}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('ratio', 5, 'x', 'AWS Glue observability: stage skewness factor; sparkDoctor docs/detections.md'),
        Threshold('min_gap_ms', 30000, 'ms', 'proposed: keeps a stage of sub-second tasks quiet'),
        Threshold('min_tasks', 10, 'count', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'the slowest task ran ratio times the median and min_gap_ms past it, across at least min_tasks tasks',
        'high': 'the slowest task ran 5 minutes or more past the median',
    },
    remediation=(
        "compare the slowest task's input and shuffle read with the median task's",
        'salt the hot key, or repartition on a column that spreads the rows',
        'move the slow per-row work out of the UDF the task runs',
    ),
    docs=(GLUE_OBSERVABILITY,),
    lesson=_TASK_SKEW_LESSON,
    sql=_TASK_SKEW_SQL,
)

# Every successful task of a stage counts, those that read nothing included, as in the Stages tab's
# summary metrics and AQE's skew join. The median is the value at sorted index n / 2, as the Stages
# tab takes it. A partition is skewed, as AQE judges one, when it is over both the ratio times the
# median and the size floor. So where most tasks read nothing, the median is 0 and the floor decides.
_SHUFFLE_SKEW_SQL = f"""
WITH {STAGE_CTES}, stage_tasks AS (
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
), {spark_settings_cte(ADAPTIVE, SKEW_JOIN)}, {SPARK_RELEASE_CTE}
SELECT skewed.stage_id, skewed.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  joins.plan_node_id, joins.plan_node_label, worst.worst_task_ids,
  CASE WHEN skewed.max_shuffle_read_bytes > 1073741824 THEN 'high' ELSE 'medium' END AS severity,
  round((skewed.max_task_duration_ms - skewed.median_task_duration_ms) / 1000, 3) AS impact_seconds,
  skewed.task_count,
  skewed.max_shuffle_read_bytes, skewed.median_shuffle_read_bytes,
  round(skewed.max_shuffle_read_bytes / nullif(skewed.median_shuffle_read_bytes, 0), 2) AS shuffle_read_bytes_ratio,
  skewed.max_shuffle_records, skewed.median_shuffle_records,
  round(skewed.max_shuffle_records / nullif(skewed.median_shuffle_records, 0), 2) AS shuffle_records_ratio,
  skewed.max_task_duration_ms, skewed.median_task_duration_ms,
  CASE WHEN stage_owners.sql_execution_id IS NOT NULL THEN {skew_join_enabled('sql_executions.modified_configs')} END
    AS aqe_skew_join_enabled
FROM skewed
JOIN worst USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
LEFT JOIN sql_executions ON sql_executions.execution_id = stage_owners.sql_execution_id
LEFT JOIN joins ON joins.stage_id = skewed.stage_id AND joins.execution_id = stage_owners.sql_execution_id
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

RULES = (TASK_SKEW, SHUFFLE_SKEW)


def rows_outside_skewed_stages(context: RuleContext, sql: str) -> list[dict[str, Any]]:
    """A stage rule's rows, leaving out the stages `stage.shuffle-skew` found skewed.

    A rule whose finding a skewed partition causes reads `stage.shuffle-skew`, because that
    finding names the cause and its fix.
    """
    if context.tables is None:
        return []
    skewed = {finding.scope.stage_id for finding in context.findings.get('stage.shuffle-skew', ())}
    relation = context.tables.query(sql, dict(context.thresholds))
    rows = [dict(zip(relation.columns, row, strict=True)) for row in relation.fetchall()]
    return [row for row in rows if row['stage_id'] not in skewed]
