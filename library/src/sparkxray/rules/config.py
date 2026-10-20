"""Rules over settings that contradict what the run shows: AQE off, its skew join off on a skewed join, and too few shuffle partitions."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold
from sparkxray.rules.spark_settings import ADAPTIVE
from sparkxray.rules.spark_settings import ADAPTIVE_DEFAULT_RELEASE
from sparkxray.rules.spark_settings import ADVISORY_PARTITION_BYTES
from sparkxray.rules.spark_settings import LOG_STAGE_EXECUTOR_METRICS
from sparkxray.rules.spark_settings import SHUFFLE_PARTITIONS
from sparkxray.rules.spark_settings import SKEW_JOIN
from sparkxray.rules.spark_settings import SPARK_RELEASE_CTE
from sparkxray.rules.spark_settings import adaptive_enabled
from sparkxray.rules.spark_settings import setting
from sparkxray.rules.spark_settings import spark_settings_cte
from sparkxray.rules.stage.statistics import STAGE_CTES

_AQE_DOCS = 'https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution'

_AQE_OFF_SQL = f"""
WITH {spark_settings_cte(ADAPTIVE)}, {SPARK_RELEASE_CTE}, off AS (
  SELECT sql_executions.execution_id FROM sql_executions, spark_settings, spark_release
  WHERE NOT {adaptive_enabled('sql_executions.modified_configs')}
)
SELECT 'medium' AS severity, application.spark_version, spark_settings."{ADAPTIVE}" AS adaptive_setting,
  (SELECT count(*) FROM off) AS aqe_off_execution_count, (SELECT count(*) FROM sql_executions) AS sql_execution_count,
  (SELECT list(execution_id ORDER BY execution_id)[1:20] FROM off) AS aqe_off_execution_ids
FROM application, spark_settings, spark_release
WHERE spark_release.number >= {ADAPTIVE_DEFAULT_RELEASE} AND EXISTS (SELECT 1 FROM off)
"""

_AQE_OFF_LESSON = """\
## What it measures

`spark.sql.adaptive.enabled`, as the application set it and as each SQL execution's session
changed it, on a Spark of 3.2 or later, where it is on by default. The rule fires where at least
one SQL execution ran with it off, and `aqe_off_execution_ids` names them.

## Why it costs time

Adaptive query execution replans a query at each shuffle from the sizes the shuffle measured. It
merges small shuffle partitions, splits a skewed join's large ones, and turns a sort-merge join
into a broadcast join where one side proved small. With it off, each of those runs as the plan
guessed before any data was read.

## Where the Spark UI shows it

Environment tab › Spark Properties › `spark.sql.adaptive.enabled`. In the SQL tab, a query run
with AQE on has `AdaptiveSparkPlan isFinalPlan=true` at the root of its plan.

## How to fix it

- Remove the setting that turns it off, from the job's configuration or the code's
  `spark.conf.set`.
- Where it was turned off to work around one query, turn off only the feature that query trips
  on, such as `spark.sql.adaptive.coalescePartitions.enabled`.
"""

AQE_OFF = Rule(
    rule_id='config.aqe-off',
    title='AQE was off for {aqe_off_execution_count} of {sql_execution_count:plural SQL execution}',
    signals=frozenset({'environment'}),
    scope='application',
    thresholds=(),
    severity_ladder={'medium': 'a SQL execution ran with spark.sql.adaptive.enabled=false, on Spark 3.2 or later'},
    remediation=(
        'remove the setting that turns spark.sql.adaptive.enabled off',
        'turn off only the AQE feature a query trips on',
    ),
    docs=(_AQE_DOCS,),
    lesson=_AQE_OFF_LESSON,
    sql=_AQE_OFF_SQL,
)

# The stages each SQL execution ran a shuffled join in, and the AQE settings that execution ran with.
_JOIN_STAGES_SQL = f"""
WITH {spark_settings_cte(ADAPTIVE, SKEW_JOIN)}, {SPARK_RELEASE_CTE}
SELECT DISTINCT node_stages.execution_id, node_stages.stage_id,
  {adaptive_enabled('sql_executions.modified_configs')} AS aqe_enabled,
  {setting(SKEW_JOIN, 'sql_executions.modified_configs')} AS skew_join_setting
FROM node_stages
JOIN sql_executions USING (execution_id)
JOIN plan_nodes ON plan_nodes.execution_id = node_stages.execution_id AND plan_nodes.node_id = node_stages.node_id
  AND plan_nodes.plan_version = sql_executions.plan_version_count - 1
LEFT JOIN spark_settings ON true
LEFT JOIN spark_release ON true
WHERE plan_nodes.operator IN ('SortMergeJoin', 'ShuffledHashJoin')
"""


def _skew_join_off(context: RuleContext) -> Iterable[Mapping[str, Any]]:
    """The `stage.shuffle-skew` findings on a stage running a shuffled join, where the execution ran without AQE's skew join."""
    if context.tables is None:
        return []
    joins = {
        (execution_id, stage_id): (aqe, skew_join)
        for execution_id, stage_id, aqe, skew_join in context.tables.query(_JOIN_STAGES_SQL).fetchall()
    }
    rows = []
    for finding in context.findings.get('stage.shuffle-skew', ()):
        scope = finding.scope
        join = joins.get((scope.sql_execution_id, scope.stage_id))
        if join is None or finding.evidence.get('aqe_skew_join_enabled') is not False:
            continue
        aqe_enabled, skew_join_setting = join
        rows.append(
            {
                'severity': 'medium',
                'stage_id': scope.stage_id,
                'stage_attempt': scope.stage_attempt,
                'job_id': scope.job_id,
                'sql_execution_id': scope.sql_execution_id,
                'plan_node_id': scope.plan_node_id,
                'plan_node_label': scope.plan_node_label,
                'worst_task_ids': scope.worst_task_ids,
                'aqe_enabled': aqe_enabled,
                'skew_join_setting': skew_join_setting,
                'max_shuffle_read_bytes': finding.evidence.get('max_shuffle_read_bytes'),
                'median_shuffle_read_bytes': finding.evidence.get('median_shuffle_read_bytes'),
                'shuffle_read_bytes_ratio': finding.evidence.get('shuffle_read_bytes_ratio'),
                'skew_finding_id': finding.finding_id,
            }
        )
    return rows


_SKEW_JOIN_OFF_LESSON = """\
## What it measures

The stages where `stage.shuffle-skew` found one shuffle partition far larger than the median,
where the stage ran a sort-merge or shuffled hash join, and where its SQL execution ran with
AQE's skew join off: `spark.sql.adaptive.skewJoin.enabled=false`, or AQE itself off.
`aqe_enabled` and `skew_join_setting` say which.

## Why it costs time

AQE's skew join is built for this case. At the join's shuffle it measures each partition, and
splits one over `spark.sql.adaptive.skewJoin.skewedPartitionFactor` times the median and over
`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` into tasks of the median's size,
each joined against a copy of the other side's partition. With it off, one task reads the whole
partition while the rest of the stage's cores wait.

## Where the Spark UI shows it

Environment tab › Spark Properties › `spark.sql.adaptive.skewJoin.enabled`. With the skew join
on, the SQL tab shows the join as `SortMergeJoin(skew=true)` and its stage reads more tasks.

## How to fix it

- Turn the skew join on: `spark.sql.adaptive.enabled=true` and
  `spark.sql.adaptive.skewJoin.enabled=true`.
- Where the skew join is on and still leaves a partition large, lower
  `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`.
"""

SKEW_JOIN_OFF_WITH_SKEW = Rule(
    rule_id='config.skew-join-off-with-skew',
    title="AQE's skew join was off for stage {stage_id}, whose largest join partition read {max_shuffle_read_bytes:bytes}",
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(),
    severity_ladder={'medium': 'stage.shuffle-skew fired on a stage running a shuffled join, with the skew join off'},
    remediation=(
        'set spark.sql.adaptive.enabled=true and spark.sql.adaptive.skewJoin.enabled=true',
        'lower spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes where a partition stays large',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#optimizing-skew-join',),
    lesson=_SKEW_JOIN_OFF_LESSON,
    check=_skew_join_off,
    reads_findings_of=('stage.shuffle-skew',),
)

# A stage counts where its SQL execution ran with spark.sql.shuffle.partitions unset or at its default of 200.
_FEW_PARTITIONS_SQL = f"""
WITH {STAGE_CTES}, {spark_settings_cte(SHUFFLE_PARTITIONS)}, heavy AS (
  SELECT stage_statistics.stage_id, stage_statistics.shuffle_read_bytes,
    stage_statistics.shuffle_read_bytes / stage_statistics.task_count AS average_bytes,
    {setting(SHUFFLE_PARTITIONS, 'sql_executions.modified_configs')} AS partitions_setting
  FROM stage_statistics
  JOIN stage_owners USING (stage_id)
  JOIN sql_executions ON sql_executions.execution_id = stage_owners.sql_execution_id
  LEFT JOIN spark_settings ON true
  WHERE stage_statistics.shuffle_read_bytes / stage_statistics.task_count >= $min_average_bytes
)
SELECT 'medium' AS severity, any_value(partitions_setting) AS shuffle_partitions_setting,
  count(*) AS stage_count, list(stage_id ORDER BY stage_id)[1:20] AS stage_ids,
  max(average_bytes)::BIGINT AS max_average_shuffle_read_bytes, max(shuffle_read_bytes) AS max_stage_shuffle_read_bytes,
  ceil(max(shuffle_read_bytes) / {ADVISORY_PARTITION_BYTES})::BIGINT AS suggested_shuffle_partition_count
FROM heavy
WHERE coalesce(partitions_setting, '200') = '200'
HAVING count(*) > 0
"""

_FEW_PARTITIONS_LESSON = """\
## What it measures

`spark.sql.shuffle.partitions`, as each SQL execution ran with it, against the shuffle bytes each
task of its stages read on average. The rule fires where the setting was unset or 200, its
default, and a stage read at least `min_average_bytes` per task.

## Why it costs time

Every shuffle a query runs writes `spark.sql.shuffle.partitions` partitions, unless AQE merges
them. 200 suits a few gigabytes. Past that each partition grows, and a task reading a gigabyte
or more holds its core for minutes and spills what does not fit its share of memory. The default
left in place on a large job is the common cause.

## Where the Spark UI shows it

Environment tab › Spark Properties › `spark.sql.shuffle.partitions`. Stages tab › the stage in
`stage_ids`: its task count, and Shuffle Read Size / Records in "Summary Metrics for Completed
Tasks".

## How to fix it

- Raise `spark.sql.shuffle.partitions` to `suggested_shuffle_partition_count`, the largest
  stage's shuffle read over 64 MiB, AQE's advisory partition size.
- With AQE on, set it high and let `spark.sql.adaptive.coalescePartitions.enabled` merge the
  partitions a smaller shuffle does not need.
"""

SHUFFLE_PARTITIONS_TOO_FEW = Rule(
    rule_id='config.shuffle-partitions-too-few',
    title='Shuffle partitions stayed at the default 200 while a stage read {max_average_shuffle_read_bytes:bytes} per task',
    signals=frozenset({'tasks', 'environment'}),
    scope='application',
    thresholds=(Threshold('min_average_bytes', 1073741824, 'bytes', 'proposed'),),
    severity_ladder={'medium': 'spark.sql.shuffle.partitions was unset or 200, and a stage read at least min_average_bytes per task'},
    remediation=(
        'raise spark.sql.shuffle.partitions to suggested_shuffle_partition_count',
        'with AQE on, set it high and let spark.sql.adaptive.coalescePartitions.enabled merge partitions',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-performance-tuning.html#tuning-partitions',),
    lesson=_FEW_PARTITIONS_LESSON,
    sql=_FEW_PARTITIONS_SQL,
)

_INCOMPLETE_SQL = f"""
WITH {spark_settings_cte(LOG_STAGE_EXECUTOR_METRICS)}
SELECT 'info' AS severity, spark_settings."{LOG_STAGE_EXECUTOR_METRICS}" AS log_stage_executor_metrics_setting,
  count(*) AS executor_count,
  count(*) FILTER (WHERE executors.peak_memory_source = 'task_peaks') AS task_peak_executor_count,
  count(*) FILTER (WHERE executors.peak_memory_source IS NULL) AS unmeasured_executor_count
FROM executors LEFT JOIN spark_settings ON true
GROUP BY ALL
HAVING count(*) FILTER (WHERE executors.peak_memory_source = 'stage_executor_metrics') = 0
"""

_INCOMPLETE_LESSON = """\
## What it measures

Whether the log holds any `SparkListenerStageExecutorMetrics` event, which Spark writes only
under `spark.eventLog.logStageExecutorMetrics=true`, off by default. Without them each
executor's peak heap is the largest a task reported as it ended, and `unmeasured_executor_count`
counts the executors no task reported one for.

## Why it costs time

It costs none, so the rule reports at `info`. It says how far to trust the memory rules: a task
reports the heap polled while it ran, so a peak between polls, or after the task, is missed.
`executor.memory-over` may then read an executor as emptier than it was.

## Where the Spark UI shows it

Environment tab › Spark Properties › `spark.eventLog.logStageExecutorMetrics`. The Executors
tab's Peak JVM Memory columns read from the same events.

## How to fix it

- Set `spark.eventLog.logStageExecutorMetrics=true` for the next run. It adds one event per
  executor per stage to the log.
- Lower `spark.executor.metrics.pollingInterval` where peaks between heartbeats matter.
"""

EVENT_LOG_INCOMPLETE = Rule(
    rule_id='config.event-log-incomplete',
    title='The log holds no stage executor metrics, so peak memory comes from task-end polls',
    signals=frozenset({'executors'}),
    scope='application',
    thresholds=(),
    severity_ladder={'info': 'no executor peak came from stage executor metrics'},
    remediation=(
        'set spark.eventLog.logStageExecutorMetrics=true',
        'lower spark.executor.metrics.pollingInterval',
    ),
    docs=('https://spark.apache.org/docs/latest/monitoring.html#executor-metrics',),
    lesson=_INCOMPLETE_LESSON,
    sql=_INCOMPLETE_SQL,
)

RULES = (AQE_OFF, SKEW_JOIN_OFF_WITH_SKEW, SHUFFLE_PARTITIONS_TOO_FEW, EVENT_LOG_INCOMPLETE)
