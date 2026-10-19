"""Rules over the rows a stage's tasks spilled out of execution memory: how much, and whether a few tasks spilled most."""

from __future__ import annotations

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Threshold
from sparkxray.rules.stage.statistics import STAGE_CTES

_TUNING_DOCS = 'https://spark.apache.org/docs/latest/tuning.html#memory-management-overview'

# A single-task stage has no other task to compare with, so sparkDoctor holds it to higher floors.
# The impact is the share of the spilling tasks' run time the disk spill stands for, against what
# the stage read, and never more than the stage's wall time.
_SPILL_SQL = f"""
WITH {STAGE_CTES}, spilled AS (
  SELECT * FROM stage_statistics
  WHERE (task_count >= 2 AND (disk_spilled_bytes >= $disk_bytes OR memory_spilled_bytes >= $memory_bytes))
    OR (task_count = 1 AND (disk_spilled_bytes >= $single_task_disk_bytes OR memory_spilled_bytes >= $single_task_memory_bytes))
), worst AS (
  SELECT tasks_effective.stage_id,
    list(tasks_effective.task_id ORDER BY tasks_effective.disk_spilled_bytes DESC, tasks_effective.memory_spilled_bytes DESC,
      tasks_effective.task_id)[1:5] AS worst_task_ids
  FROM tasks_effective JOIN spilled USING (stage_id)
  WHERE tasks_effective.memory_spilled_bytes > 0 OR tasks_effective.disk_spilled_bytes > 0
  GROUP BY tasks_effective.stage_id
), reads AS (
  SELECT stage_id, shuffle_read_bytes + coalesce(input_bytes, 0) AS read_bytes FROM spilled
)
SELECT spilled.stage_id, spilled.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id, worst.worst_task_ids,
  CASE WHEN spilled.disk_spilled_bytes >= 1073741824 OR spilled.max_disk_spilled_bytes >= 536870912 THEN 'high' ELSE 'medium' END
    AS severity,
  CASE WHEN reads.read_bytes > 0 THEN
    round(least(stage_walls.wall_ms, spilled.spilling_run_time_ms * spilled.disk_spilled_bytes / reads.read_bytes) / 1000, 3)
  END AS impact_seconds,
  spilled.task_count, spilled.spilling_task_count,
  spilled.memory_spilled_bytes, spilled.disk_spilled_bytes, spilled.max_disk_spilled_bytes AS max_task_disk_spilled_bytes,
  reads.read_bytes,
  round(spilled.disk_spilled_bytes / nullif(reads.read_bytes, 0), 4) AS disk_spill_to_read_ratio
FROM spilled
JOIN worst USING (stage_id)
JOIN reads USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
LEFT JOIN stage_walls USING (stage_id)
ORDER BY spilled.stage_id
"""

_SPILL_LESSON = """\
## What it measures

Each task's `Memory Bytes Spilled`, the size in memory of the rows it moved out of execution
memory, and `Disk Bytes Spilled`, the size they took on disk once serialized and compressed. Both
are summed over the stage's tasks. The disk figure is the smaller of the two, and the one that
cost writes and reads.

## Why it costs time

A sort, an aggregation or a join holds its rows in execution memory, the task's share of
`spark.memory.fraction` of the heap. Past that share it writes the rows to disk and merges them
back later. Each spilled byte is serialized, written, read and deserialized again, and the task
runs no faster for it. `disk_spill_to_read_ratio` gives the disk spill against what the stage
read, so a ratio near or past 1 means the stage wrote its whole input to disk again.

## Where the Spark UI shows it

Stages tab › the stage: "Spill (Memory)" and "Spill (Disk)" in the stage's header and in
"Summary Metrics for Completed Tasks". The SQL tab's Sort and aggregate nodes give "spill size".

## How to fix it

- Give each task more data room: more partitions, so each holds less, through
  `spark.sql.shuffle.partitions` or `repartition`.
- Give each task more memory: raise `spark.executor.memory`, or lower `spark.executor.cores` so
  fewer tasks share it.
- Where `spark.memory.fraction` was lowered from its default of 0.6, restore it.
"""

SPILL = Rule(
    rule_id='stage.spill',
    title='Stage {stage_id} spilled {disk_spilled_bytes:bytes} to disk and {memory_spilled_bytes:bytes} from memory',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('disk_bytes', 268435456, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('memory_bytes', 1073741824, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('single_task_disk_bytes', 1073741824, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('single_task_memory_bytes', 4294967296, 'bytes', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'over two tasks or more, at least disk_bytes spilled to disk or memory_bytes from memory; '
        'in a one-task stage, single_task_disk_bytes or single_task_memory_bytes',
        'high': 'at least 1 GiB spilled to disk, or 512 MiB by one task',
    },
    remediation=(
        'raise the partition count, so each task holds less',
        'raise spark.executor.memory, or lower spark.executor.cores',
        'restore spark.memory.fraction where it was lowered',
    ),
    docs=(_TUNING_DOCS,),
    lesson=_SPILL_LESSON,
    sql=_SPILL_SQL,
)

_SPILL_SKEW_SQL = f"""
WITH {STAGE_CTES}, judged AS (
  SELECT *,
    median_memory_spilled_bytes > 0 AND max_memory_spilled_bytes > $memory_bytes
      AND max_memory_spilled_bytes > $ratio * median_memory_spilled_bytes AS memory_skewed,
    median_disk_spilled_bytes > 0 AND max_disk_spilled_bytes > $disk_bytes
      AND max_disk_spilled_bytes > $ratio * median_disk_spilled_bytes AS disk_skewed
  FROM stage_statistics WHERE task_count >= $min_tasks
), skewed AS (
  SELECT * FROM judged WHERE memory_skewed OR disk_skewed
), worst AS (
  SELECT tasks_effective.stage_id,
    list(tasks_effective.task_id ORDER BY tasks_effective.disk_spilled_bytes DESC, tasks_effective.memory_spilled_bytes DESC,
      tasks_effective.task_id)[1:5] AS worst_task_ids
  FROM tasks_effective JOIN skewed USING (stage_id)
  WHERE tasks_effective.memory_spilled_bytes > 0 OR tasks_effective.disk_spilled_bytes > 0
  GROUP BY tasks_effective.stage_id
)
SELECT skewed.stage_id, skewed.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id, worst.worst_task_ids,
  CASE WHEN skewed.disk_skewed THEN 'high' ELSE 'medium' END AS severity,
  round((skewed.max_duration_ms - skewed.median_duration_ms) / 1000, 3) AS impact_seconds,
  skewed.task_count,
  skewed.max_memory_spilled_bytes, skewed.median_memory_spilled_bytes,
  round(skewed.max_memory_spilled_bytes / nullif(skewed.median_memory_spilled_bytes, 0), 2) AS memory_spill_ratio,
  skewed.max_disk_spilled_bytes, skewed.median_disk_spilled_bytes,
  round(skewed.max_disk_spilled_bytes / nullif(skewed.median_disk_spilled_bytes, 0), 2) AS disk_spill_ratio
FROM skewed
JOIN worst USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
ORDER BY skewed.stage_id
"""

_SPILL_SKEW_LESSON = """\
## What it measures

The most any one task of a stage spilled, from memory and to disk, against the median task's
spill. The median is taken over every successful task, so the rule needs most tasks to spill
something and a few to spill far more.

## Why it costs time

Where every task spills a little and a few spill most, those few hold more rows than the rest:
their partitions are skewed. They run longest, and the stage waits for them. Spill skew can
show where bytes read do not, because a few keys can expand far more than others once joined or
grouped.

## Where the Spark UI shows it

Stages tab › the stage › "Summary Metrics for Completed Tasks". Compare the Max column with the
Median column for Spill (Memory) and Spill (Disk), then sort the task table by either.

## How to fix it

- Find the keys the worst tasks held, and salt them, as for any skew.
- With AQE on, `spark.sql.adaptive.skewJoin.enabled` splits a skewed join partition before it
  spills.
- Raise the partition count, so the largest partition holds less.
"""

SPILL_SKEW = Rule(
    rule_id='stage.spill-skew',
    title=(
        'One task of stage {stage_id} spilled {max_memory_spilled_bytes:bytes} from memory, '
        'against a median of {median_memory_spilled_bytes:bytes}'
    ),
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('ratio', 5, 'x', 'sparkDoctor docs/detections.md'),
        Threshold('memory_bytes', 268435456, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('disk_bytes', 134217728, 'bytes', 'sparkDoctor docs/detections.md'),
        Threshold('min_tasks', 10, 'count', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'across at least min_tasks tasks, the most one spilled from memory is over memory_bytes and ratio times the median',
        'high': 'the most one spilled to disk is over disk_bytes and ratio times the median',
    },
    remediation=(
        'salt the keys the worst tasks held',
        'set spark.sql.adaptive.skewJoin.enabled=true',
        'raise the partition count',
    ),
    docs=(_TUNING_DOCS,),
    lesson=_SPILL_SKEW_LESSON,
    sql=_SPILL_SKEW_SQL,
)

RULES = (SPILL, SPILL_SKEW)
