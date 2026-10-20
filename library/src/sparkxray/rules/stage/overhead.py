"""Rules over the time a stage's tasks spent on something other than their rows: GC, scheduling, deserializing and waiting on fetches."""

from __future__ import annotations

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Threshold
from sparkxray.rules.stage.statistics import SLOTS
from sparkxray.rules.stage.statistics import STAGE_CTES

_TUNING_DOCS = 'https://spark.apache.org/docs/latest/tuning.html'
_MONITORING_DOCS = 'https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics'


def _stage_rule_sql(where: str, columns: str) -> str:
    """A query over `stage_statistics`, its owner job and its slots: the scope ids, then `columns`, for the stages `where` holds."""
    return f"""
WITH {STAGE_CTES}
SELECT stage_statistics.stage_id, stage_statistics.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  {columns}
FROM stage_statistics
LEFT JOIN stage_owners USING (stage_id)
LEFT JOIN stage_slots USING (stage_id)
WHERE {where}
ORDER BY stage_statistics.stage_id
"""


# Dr. Elephant's ladder over GC time against run time, from 8% up.
_GC_SHARE = 'stage_statistics.gc_time_ms / nullif(stage_statistics.run_time_ms, 0)'
_GC_PRESSURE_SQL = _stage_rule_sql(
    f'stage_statistics.gc_time_ms >= $min_gc_ms AND {_GC_SHARE} >= $min_share',
    f"""CASE WHEN {_GC_SHARE} >= 0.2 THEN 'critical' WHEN {_GC_SHARE} >= 0.15 THEN 'high'
    WHEN {_GC_SHARE} >= 0.1 THEN 'medium' ELSE 'low' END AS severity,
  stage_statistics.task_count, stage_statistics.gc_time_ms AS jvm_gc_time_ms, stage_statistics.run_time_ms AS executor_run_time_ms,
  round({_GC_SHARE}, 4) AS gc_share""",
)

_GC_PRESSURE_LESSON = """\
## What it measures

Each task's `JVM GC Time`, the milliseconds its executor's JVM spent collecting garbage while the
task ran, summed over the stage, against the stage's summed `Executor Run Time`.

## Why it costs time

A collection pauses the threads that run tasks, so GC time is run time spent on no row. A heap
near its limit collects again and again for little, and the share climbs before an executor runs
out of memory. Dr. Elephant grades the share from 8%, and the rule needs 30 s of GC in all, so a
short stage's few collections stay quiet.

## Where the Spark UI shows it

Stages tab › the stage: "GC Time" in "Summary Metrics for Completed Tasks" and in the task table.
The Executors tab shows each executor's GC time, highlighted red past 10% of its task time.

## How to fix it

- Give each task more heap: raise `spark.executor.memory`, or lower `spark.executor.cores` so
  fewer tasks share it.
- Hold fewer objects: cache in a serialized storage level, or drop a cache no later step reads.
- Raise the partition count, so each task holds fewer rows at once.
"""

GC_PRESSURE = Rule(
    rule_id='stage.gc-pressure',
    title='Stage {stage_id} spent {gc_share:.0%} of its run time in garbage collection',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_share', 0.08, 'fraction', "Dr. Elephant ExecutorGcHeuristic's lowest rung"),
        Threshold('min_gc_ms', 30000, 'ms', 'proposed: keeps a short stage with a few collections quiet'),
    ),
    severity_ladder={
        'low': 'at least min_gc_ms of GC, at least min_share of run time',
        'medium': 'at least 10% of run time',
        'high': 'at least 15% of run time',
        'critical': 'at least 20% of run time',
    },
    remediation=(
        'raise spark.executor.memory, or lower spark.executor.cores',
        'cache in a serialized storage level, or drop a cache nothing reads again',
        'raise the partition count',
    ),
    docs=(_TUNING_DOCS + '#garbage-collection-tuning',),
    lesson=_GC_PRESSURE_LESSON,
    sql=_GC_PRESSURE_SQL,
)

_SCHEDULER_DELAY_SQL = _stage_rule_sql(
    'stage_statistics.median_scheduler_delay_ms > $min_delay_ms '
    'AND stage_statistics.median_scheduler_delay_ms > $min_share * stage_statistics.median_duration_ms '
    f'AND stage_statistics.scheduler_delay_ms / {SLOTS} >= $min_impact_ms',
    f"""'medium' AS severity,
  round(stage_statistics.scheduler_delay_ms / 1000 / {SLOTS}, 3) AS impact_seconds,
  stage_statistics.task_count, stage_statistics.median_scheduler_delay_ms, stage_statistics.median_duration_ms,
  stage_statistics.scheduler_delay_ms,
  round(stage_statistics.median_scheduler_delay_ms / nullif(stage_statistics.median_duration_ms, 0), 4) AS scheduler_delay_share""",
)

_SCHEDULER_DELAY_LESSON = """\
## What it measures

Each task's scheduler delay, derived as the History Server derives it: its duration less its run
time, its deserialization and result serialization, and the time its result took to fetch. The
rule compares the median task's delay with the median task's duration.

## Why it costs time

Scheduler delay is time a task held a slot without running: waiting in the driver to be sent,
traveling to the executor, and reporting back. It grows with a busy driver, a large task to ship,
or a network between driver and executors that is slow. Every task pays it once, so a stage of
short tasks pays it as a large share.

## Where the Spark UI shows it

Stages tab › the stage › "Show Additional Metrics" › Scheduler Delay, and the "Event Timeline",
where it shows as the blue part of each bar.

## How to fix it

- Run fewer, longer tasks: cut the partition count so each task's work outweighs its delay.
- Ship less with each task: broadcast a large lookup rather than capture it in a closure.
- Give the driver more cores or memory where it schedules many stages at once.
"""

SCHEDULER_DELAY = Rule(
    rule_id='stage.scheduler-delay',
    title='The median task of stage {stage_id} waited {median_scheduler_delay_ms:duration} to be scheduled',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_share', 0.1, 'fraction', 'proposed'),
        Threshold('min_delay_ms', 100, 'ms', 'proposed'),
        Threshold('min_impact_ms', 30000, 'ms', 'proposed: the 30 s at stake stage.task-skew and stage.gc-pressure need'),
    ),
    severity_ladder={
        'medium': "the median task's delay is over min_delay_ms and over min_share of the median task's duration, "
        'and the summed delay over concurrent slots is at least min_impact_ms'
    },
    remediation=(
        'cut the partition count, so each task runs longer',
        'broadcast a large lookup rather than capture it in a closure',
        'give the driver more cores or memory',
    ),
    docs=(_MONITORING_DOCS,),
    lesson=_SCHEDULER_DELAY_LESSON,
    sql=_SCHEDULER_DELAY_SQL,
)

# Deserializing comes before a task runs, so it is part of the task's duration and not of its run time.
_DESERIALIZE_SHARE = 'stage_statistics.deserialize_time_ms / nullif(stage_statistics.task_duration_ms, 0)'
_DESERIALIZATION_SQL = _stage_rule_sql(
    f'{_DESERIALIZE_SHARE} > $min_share AND stage_statistics.median_deserialize_time_ms > $min_median_ms '
    f'AND stage_statistics.deserialize_time_ms / {SLOTS} >= $min_impact_ms',
    f"""'medium' AS severity,
  round(stage_statistics.deserialize_time_ms / 1000 / {SLOTS}, 3) AS impact_seconds,
  stage_statistics.task_count, stage_statistics.deserialize_time_ms, stage_statistics.median_deserialize_time_ms,
  stage_statistics.task_duration_ms,
  round({_DESERIALIZE_SHARE}, 4) AS deserialize_share""",
)

_DESERIALIZATION_LESSON = """\
## What it measures

Each task's `Executor Deserialize Time`: the milliseconds its executor spent turning the task it
was sent back into objects, the closure and any broadcast it reads first included. The rule
compares the stage's sum with its tasks' summed duration, and needs the median task's to be long
too.

## Why it costs time

A task is shipped serialized, and every task of a stage carries the same closure. A closure that
captured a large object, such as a lookup table or a model, is deserialized again for every task.
That time is spent before the first row is read. The first stage on fresh executors pays for
loading classes here too, once per core, and a later stage on the same executors does not. So
the rule needs `min_impact_ms` at stake, which that one-time cost rarely reaches.

## Where the Spark UI shows it

Stages tab › the stage › "Show Additional Metrics" › Task Deserialization Time.

## How to fix it

- Broadcast large objects with `spark.sparkContext.broadcast`, so each executor reads one copy.
- Keep closures small: reference what a function needs, not the object holding it.
- Run fewer, longer tasks, so the cost is paid fewer times.
"""

DESERIALIZATION = Rule(
    rule_id='stage.deserialization',
    title='Tasks of stage {stage_id} spent {deserialize_share:.0%} of their time deserializing',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_share', 0.1, 'fraction', 'proposed'),
        Threshold('min_median_ms', 100, 'ms', 'proposed'),
        Threshold('min_impact_ms', 30000, 'ms', 'proposed: the 30 s at stake stage.task-skew and stage.gc-pressure need'),
    ),
    severity_ladder={
        'medium': "deserializing took over min_share of the tasks' time and over min_median_ms for the median task, "
        'and the summed time over concurrent slots is at least min_impact_ms'
    },
    remediation=(
        'broadcast large objects rather than capture them in a closure',
        'reference only what a function needs',
        'cut the partition count',
    ),
    docs=(_TUNING_DOCS + '#broadcasting-large-variables',),
    lesson=_DESERIALIZATION_LESSON,
    sql=_DESERIALIZATION_SQL,
)

_FETCH_WAIT_SHARE = 'stage_statistics.fetch_wait_time_ms / nullif(stage_statistics.run_time_ms, 0)'
_FETCH_WAIT_SQL = _stage_rule_sql(
    f'stage_statistics.fetch_wait_time_ms >= $min_wait_ms AND {_FETCH_WAIT_SHARE} > $min_share',
    f"""'medium' AS severity,
  stage_statistics.task_count, stage_statistics.fetch_wait_time_ms AS shuffle_fetch_wait_time_ms,
  stage_statistics.run_time_ms AS executor_run_time_ms, stage_statistics.shuffle_read_bytes,
  round({_FETCH_WAIT_SHARE}, 4) AS fetch_wait_share""",
)

_FETCH_WAIT_LESSON = """\
## What it measures

Each task's `Fetch Wait Time`: the milliseconds it sat blocked waiting for shuffle blocks to
arrive from other executors, summed over the stage, against the stage's summed run time.

## Why it costs time

A task reading a shuffle fetches its blocks from every executor that wrote one. While none has
arrived it does nothing. A long wait points at the network, at executors or a shuffle service too
busy to serve, or at blocks so many and small that requests dominate.

## Where the Spark UI shows it

Stages tab › the stage › "Show Additional Metrics" › Shuffle Read Blocked Time.

## How to fix it

- Raise `spark.reducer.maxSizeInFlight` and `spark.shuffle.io.numConnectionsPerPeer` where the
  network has room.
- Run the external shuffle service, or more executors, so serving blocks does not compete with
  running tasks.
- Write fewer, larger shuffle blocks: fewer map tasks or fewer shuffle partitions.
"""

FETCH_WAIT = Rule(
    rule_id='stage.fetch-wait',
    title='Tasks of stage {stage_id} waited {shuffle_fetch_wait_time_ms:duration} on shuffle fetches',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_share', 0.2, 'fraction', 'proposed'),
        Threshold('min_wait_ms', 30000, 'ms', 'proposed'),
    ),
    severity_ladder={'medium': 'at least min_wait_ms of fetch wait, over min_share of run time'},
    remediation=(
        'raise spark.reducer.maxSizeInFlight and spark.shuffle.io.numConnectionsPerPeer',
        'run the external shuffle service, or more executors',
        'write fewer, larger shuffle blocks',
    ),
    docs=('https://spark.apache.org/docs/latest/configuration.html#shuffle-behavior',),
    lesson=_FETCH_WAIT_LESSON,
    sql=_FETCH_WAIT_SQL,
)

RULES = (GC_PRESSURE, SCHEDULER_DELAY, DESERIALIZATION, FETCH_WAIT)
