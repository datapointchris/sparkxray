"""Rules over how a stage's data was cut into partitions: too large, too few for the data, or too many tiny ones."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold
from sparkxray.rules.spark_settings import ADVISORY_PARTITION_BYTES
from sparkxray.rules.stage.skew import rows_outside_skewed_stages
from sparkxray.rules.stage.statistics import SLOTS
from sparkxray.rules.stage.statistics import STAGE_CTES

_GIB = 1073741824
_TUNING_DOCS = 'https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions'

_OVERSIZED_SQL = f"""
WITH {STAGE_CTES}, sized AS (
  SELECT *, greatest(max_shuffle_read_bytes, max_input_bytes, max_output_bytes, max_shuffle_write_bytes) AS max_partition_bytes
  FROM stage_statistics
), worst AS (
  SELECT stage_id, list(task_id ORDER BY greatest(
      coalesce(shuffle_remote_read_bytes, 0) + coalesce(shuffle_local_read_bytes, 0),
      input_bytes, output_bytes, shuffle_write_bytes) DESC, task_id)[1:5] AS worst_task_ids
  FROM tasks_effective GROUP BY stage_id
)
SELECT sized.stage_id, sized.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id, worst.worst_task_ids,
  CASE WHEN sized.max_partition_bytes > $critical_bytes THEN 'critical'
    WHEN sized.p95_shuffle_read_bytes >= {_GIB} OR sized.max_shuffle_read_bytes >= $max_bytes THEN 'high'
    ELSE 'medium' END AS severity,
  sized.task_count, sized.max_partition_bytes,
  sized.p95_shuffle_read_bytes, sized.max_shuffle_read_bytes, sized.shuffle_read_bytes AS total_shuffle_read_bytes,
  sized.max_input_bytes, sized.max_output_bytes, sized.max_shuffle_write_bytes,
  CASE WHEN sized.shuffle_read_bytes > 0 THEN ceil(sized.shuffle_read_bytes / {ADVISORY_PARTITION_BYTES})::BIGINT END
    AS suggested_partition_count
FROM sized
JOIN worst USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
WHERE (sized.task_count >= $min_tasks AND (sized.p95_shuffle_read_bytes >= $p95_bytes OR sized.max_shuffle_read_bytes >= $max_bytes))
  OR sized.max_partition_bytes > $critical_bytes
ORDER BY sized.stage_id
"""


def _oversized(context: RuleContext) -> Iterable[Mapping[str, Any]]:
    """The stages whose partitions are too large, leaving out those `stage.shuffle-skew` found skewed."""
    return rows_outside_skewed_stages(context, _OVERSIZED_SQL)


_OVERSIZED_LESSON = """\
## What it measures

The bytes each task of a stage read from the shuffle, at the 95th percentile and at the maximum,
taken as the Stages tab takes them. It also reads each task's input, output and shuffle write, for
one partition past {critical_bytes} in any of the four.

## Why it costs time

One task processes one partition on one core. A partition of gigabytes holds a core for minutes
and, where its rows do not fit the task's share of execution memory, spills them to disk on the
way. A stage whose partitions are all large is slow evenly, so no skew rule fires. A stage with
one large partition is skewed, and `stage.shuffle-skew` reports it instead of this rule.

## Where the Spark UI shows it

Stages tab › the stage › "Summary Metrics for Completed Tasks". Read the 75th percentile and Max
columns for Shuffle Read Size / Records, Input Size / Records and Output Size / Records.

## How to fix it

- Raise `spark.sql.shuffle.partitions`, so each shuffle partition holds less. Total shuffle read
  over 64 MiB, AQE's advisory partition size, gives the count in `suggested_partition_count`.
- With AQE on, leave the partition count high and let
  `spark.sql.adaptive.coalescePartitions.enabled` merge the small ones.
- For a large input partition, lower `spark.sql.files.maxPartitionBytes`. For a large output
  file, `repartition` before the write.
"""

OVERSIZED_PARTITIONS = Rule(
    rule_id='stage.oversized-partitions',
    title='The largest partition of stage {stage_id} holds {max_partition_bytes:bytes}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_tasks', 2, 'count', 'sparkDoctor docs/detections.md'),
        Threshold('p95_bytes', 268435456, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('max_bytes', 2147483648, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('critical_bytes', 5368709120, 'bytes', 'DataFlint MaxPartitionToBigAlert.ts'),
    ),
    severity_ladder={
        'medium': 'across at least min_tasks tasks, the 95th-percentile shuffle read is at least p95_bytes',
        'high': 'the 95th-percentile shuffle read is at least 1 GiB, or the largest is at least max_bytes',
        'critical': "one task's input, output, shuffle read or shuffle write is over critical_bytes",
    },
    remediation=(
        'raise spark.sql.shuffle.partitions to suggested_partition_count',
        'with AQE on, let spark.sql.adaptive.coalescePartitions.enabled merge the small partitions',
        'lower spark.sql.files.maxPartitionBytes for a large input partition',
    ),
    docs=(_TUNING_DOCS,),
    lesson=_OVERSIZED_LESSON,
    check=_oversized,
    reads_findings_of=('stage.shuffle-skew',),
)

# sparkDoctor's two cases, with SD's 10 GiB and 15 tasks for the second: a shuffle of a gigabyte or more
# read by a handful of tasks of moderate size, or one of ten gigabytes or more read by a few more.
_LOW_PARALLELISM_SQL = f"""
WITH {STAGE_CTES}, few AS (
  SELECT *, shuffle_read_bytes >= 10737418240 AND task_count <= 15 AS far_too_few FROM stage_statistics
  WHERE shuffle_read_bytes >= $min_total_bytes
), flagged AS (
  SELECT * FROM few WHERE far_too_few OR (task_count <= $max_tasks AND shuffle_read_bytes / task_count < $max_average_bytes)
), spans AS (
  SELECT stage_id, min(submit_epoch_ms) AS start_epoch_ms, coalesce(max(complete_epoch_ms), min(submit_epoch_ms)) AS end_epoch_ms
  FROM stages GROUP BY stage_id
), stage_cores AS (
  -- The cores of every executor alive while the stage ran, whether or not it ran one of its tasks.
  SELECT spans.stage_id, sum(executors.core_count) AS core_count
  FROM spans JOIN executors ON executors.add_epoch_ms <= spans.end_epoch_ms
    AND coalesce(executors.remove_epoch_ms, spans.start_epoch_ms) >= spans.start_epoch_ms
  GROUP BY spans.stage_id
)
SELECT flagged.stage_id, flagged.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  CASE WHEN flagged.far_too_few THEN 'high' ELSE 'medium' END AS severity,
  flagged.task_count, flagged.shuffle_read_bytes AS total_shuffle_read_bytes,
  (flagged.shuffle_read_bytes / flagged.task_count)::BIGINT AS average_shuffle_read_bytes,
  stage_cores.core_count AS executor_core_count,
  ceil(flagged.shuffle_read_bytes / {ADVISORY_PARTITION_BYTES})::BIGINT AS suggested_partition_count
FROM flagged
LEFT JOIN stage_owners USING (stage_id)
LEFT JOIN stage_cores USING (stage_id)
ORDER BY flagged.stage_id
"""

_LOW_PARALLELISM_LESSON = """\
## What it measures

The bytes a stage read from the shuffle, against the number of tasks that read them. Each task
reads one shuffle partition, so the task count is the partition count the shuffle wrote.

## Why it costs time

A stage runs at most one task per core. A gigabyte read by seven tasks keeps seven cores busy,
however many the application holds, and each runs for as long as its share takes.
`executor_core_count` gives the cores alive while the stage ran, so fewer tasks than cores reads
as cores left idle.

## Where the Spark UI shows it

Stages tab › the stage: the "Tasks" count beside "Shuffle Read". The SQL tab's `Exchange` node
names the partitioning, as in `hashpartitioning(k#2L, 4)`, or a `repartition(n)` the code set.

## How to fix it

- Raise the partition count where the code sets it, as in `repartition(n)` or `coalesce(n)`, or
  raise `spark.sql.shuffle.partitions`. Total shuffle read over 64 MiB, AQE's advisory partition
  size, gives `suggested_partition_count`.
- With AQE on, set `spark.sql.adaptive.coalescePartitions.initialPartitionNum` high and let AQE
  merge what it does not need.
"""

LOW_SHUFFLE_PARALLELISM = Rule(
    rule_id='stage.low-shuffle-parallelism',
    title='Stage {stage_id} read {total_shuffle_read_bytes:bytes} of shuffle in {task_count:plural task}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_total_bytes', 1073741824, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('max_tasks', 7, 'count', 'sparkDoctor docs/detections.md'),
        Threshold('max_average_bytes', 268435456, 'bytes', 'sparkDoctor docs/detections.md, the oversized-partitions floor'),
    ),
    severity_ladder={
        'medium': 'at least min_total_bytes of shuffle read by at most max_tasks tasks, each averaging under max_average_bytes',
        'high': 'at least 10 GiB of shuffle read by at most 15 tasks',
    },
    remediation=(
        'raise the partition count the code sets, or spark.sql.shuffle.partitions, to suggested_partition_count',
        'with AQE on, set spark.sql.adaptive.coalescePartitions.initialPartitionNum high',
    ),
    docs=(_TUNING_DOCS,),
    lesson=_LOW_PARALLELISM_LESSON,
    sql=_LOW_PARALLELISM_SQL,
)

_TINY_TASKS_SQL = f"""
WITH {STAGE_CTES}
SELECT stage_statistics.stage_id, stage_statistics.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  'medium' AS severity,
  round(stage_statistics.overhead_ms / 1000 / {SLOTS}, 3) AS impact_seconds,
  stage_statistics.task_count, round(stage_statistics.mean_duration_ms) AS mean_duration_ms,
  stage_statistics.p95_duration_ms,
  (stage_statistics.shuffle_read_bytes / stage_statistics.task_count)::BIGINT AS mean_shuffle_read_bytes,
  stage_statistics.overhead_ms AS task_overhead_ms,
  ceil(stage_statistics.task_count / 10)::BIGINT AS suggested_partition_count
FROM stage_statistics
LEFT JOIN stage_owners USING (stage_id)
LEFT JOIN stage_slots USING (stage_id)
WHERE stage_statistics.task_count >= $min_tasks AND stage_statistics.mean_duration_ms <= $max_mean_ms
  AND stage_statistics.p95_duration_ms <= $max_p95_ms
  AND stage_statistics.shuffle_read_bytes / stage_statistics.task_count <= $max_mean_read_bytes
ORDER BY stage_statistics.stage_id
"""

_TINY_TASKS_LESSON = """\
## What it measures

A stage's task count, its tasks' mean and 95th-percentile duration, and the shuffle bytes each
read on average. `task_overhead_ms` sums what the tasks spent beyond running: being scheduled,
deserialized, serialized and returning a result.

## Why it costs time

Every task pays a fixed cost to be scheduled, shipped to an executor and reported back, of the
order of milliseconds. A stage of thousands of tasks that each run for milliseconds spends much of
its time on that cost rather than on the data. A write stage of this shape also writes one small
file per task, which every later read pays for again.

## Where the Spark UI shows it

Stages tab › the stage: the task count, and Duration in "Summary Metrics for Completed Tasks".
Turn on "Show Additional Metrics" for Scheduler Delay and Task Deserialization Time.

## How to fix it

- Cut the partition count: `repartition(suggested_partition_count)` or `coalesce` before the
  stage, a tenth of the task count, as DataFlint suggests.
- Lower `spark.sql.shuffle.partitions` for a shuffle this small, or turn on AQE's
  `spark.sql.adaptive.coalescePartitions.enabled` so it merges small partitions.
- Raise `spark.sql.files.maxPartitionBytes` where many small files make many small input tasks.
"""

TINY_TASKS = Rule(
    rule_id='stage.tiny-tasks',
    title='Stage {stage_id} ran {task_count:plural task} averaging {mean_duration_ms:duration}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_tasks', 100, 'count', 'sparkDoctor docs/detections.md'),
        Threshold('max_mean_ms', 500, 'ms', 'sparkDoctor docs/detections.md'),
        Threshold('max_p95_ms', 1000, 'ms', 'sparkDoctor docs/detections.md'),
        Threshold('max_mean_read_bytes', 1048576, 'bytes', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'at least min_tasks tasks with a mean at most max_mean_ms, a 95th percentile at most max_p95_ms, '
        'and at most max_mean_read_bytes of shuffle read each',
    },
    remediation=(
        'repartition to suggested_partition_count, a tenth of the task count',
        'lower spark.sql.shuffle.partitions, or turn on spark.sql.adaptive.coalescePartitions.enabled',
        'raise spark.sql.files.maxPartitionBytes where many small files are read',
    ),
    docs=(_TUNING_DOCS,),
    lesson=_TINY_TASKS_LESSON,
    sql=_TINY_TASKS_SQL,
)

RULES = (OVERSIZED_PARTITIONS, LOW_SHUFFLE_PARALLELISM, TINY_TASKS)
