"""Rules over the task attempts a stage threw away: failed and retried, speculated, out of memory, or a stage that failed outright.

These read `tasks`, every attempt, rather than `tasks_effective`, which keeps one success per partition.
"""

from __future__ import annotations

from sparkxray.rules.failure_reasons import failure_line
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Threshold
from sparkxray.rules.stage.statistics import SLOTS
from sparkxray.rules.stage.statistics import STAGE_CTES

_SCHEDULING_DOCS = 'https://spark.apache.org/docs/latest/configuration.html#scheduling'

# Spark's own wording of an attempt that ended other than in success, as JsonProtocol writes its Reason.
_UNSUCCESSFUL = "end_reason IS NOT NULL AND end_reason <> 'Success'"
_HIGHEST_ATTEMPT = '(SELECT max(attempt) FROM stages WHERE stages.stage_id = {table}.stage_id)'

_RETRY_WASTE_SQL = f"""
WITH {STAGE_CTES}, wasted AS (
  SELECT stage_id, count(*) AS failed_attempt_count, sum(duration_ms) AS failed_attempt_ms,
    sum(coalesce(executor_run_time_ms, duration_ms)) AS failed_run_time_ms,
    count(*) FILTER (WHERE end_reason = 'FetchFailed') AS fetch_failed_count,
    count(*) FILTER (WHERE end_reason = 'ExecutorLostFailure') AS executor_lost_count,
    count(*) FILTER (WHERE end_reason = 'TaskKilled') AS task_killed_count,
    count(*) FILTER (WHERE end_reason = 'ExceptionFailure') AS exception_count,
    count(*) FILTER (WHERE end_reason NOT IN ('FetchFailed', 'ExecutorLostFailure', 'TaskKilled', 'ExceptionFailure'))
      AS other_failure_count,
    left(mode({failure_line('end_reason_detail')}), 300) AS most_common_failure,
    list(task_id ORDER BY duration_ms DESC, task_id)[1:5] AS worst_task_ids
  FROM tasks WHERE {_UNSUCCESSFUL}
  GROUP BY stage_id
  HAVING count(*) >= $min_attempts AND sum(duration_ms) >= $min_wasted_ms
)
SELECT wasted.stage_id, {_HIGHEST_ATTEMPT.format(table='wasted')} AS stage_attempt,
  stage_owners.job_id, stage_owners.sql_execution_id, wasted.worst_task_ids,
  CASE WHEN wasted.failed_attempt_ms >= 300000 THEN 'high' ELSE 'medium' END AS severity,
  round(wasted.failed_run_time_ms / 1000 / {SLOTS}, 3) AS impact_seconds,
  wasted.failed_attempt_count, wasted.failed_attempt_ms, wasted.fetch_failed_count, wasted.executor_lost_count,
  wasted.task_killed_count, wasted.exception_count, wasted.other_failure_count, wasted.most_common_failure
FROM wasted
LEFT JOIN stage_statistics USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
LEFT JOIN stage_slots USING (stage_id)
ORDER BY wasted.stage_id
"""

_RETRY_WASTE_LESSON = """\
## What it measures

Every task attempt of a stage that ended other than in success, across all the stage's attempts,
and the time they ran. Spark names each end: `FetchFailed` when a shuffle block could not be
read, `ExecutorLostFailure` when the executor running it went away, `TaskKilled` when Spark
stopped it, as it stops the slower copy of a speculated task, and `ExceptionFailure` when the
task's code threw.

## Why it costs time

A failed attempt's run time bought nothing, and its partition runs again. A `FetchFailed` costs
more than its own attempt: Spark reruns the map stage that wrote the lost blocks, then the
stage that read them. So a few failures on a long stage can cost as much as the stage itself.

## Where the Spark UI shows it

Stages tab › the stage: the "Failed Tasks" count, and the task table's Status and Errors columns.
A stage retried after a `FetchFailed` shows a second row with "(retry 1)".

## How to fix it

- For `ExceptionFailure`, read `most_common_failure` and fix the code or data that throws.
- For `FetchFailed` and `ExecutorLostFailure`, find why executors went away: memory, where
  `executor.lost` names the reason, or preemption by the cluster manager.
- Run the external shuffle service, or turn on `spark.decommission.enabled`, so a lost executor's
  shuffle files outlive it.
"""

RETRY_WASTE = Rule(
    rule_id='stage.retry-waste',
    title='Stage {stage_id} threw away {failed_attempt_count} task attempts, {failed_attempt_ms:duration} of task time',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_attempts', 3, 'count', 'sparkDoctor docs/detections.md'),
        Threshold('min_wasted_ms', 30000, 'ms', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'at least min_attempts unsuccessful attempts, which ran for min_wasted_ms or more',
        'high': 'the unsuccessful attempts ran for 5 minutes or more',
    },
    remediation=(
        'fix the code or data behind most_common_failure',
        'find why executors went away, as executor.lost reports',
        'run the external shuffle service, or set spark.decommission.enabled=true',
    ),
    docs=(_SCHEDULING_DOCS,),
    lesson=_RETRY_WASTE_LESSON,
    sql=_RETRY_WASTE_SQL,
)

_SPECULATION_SQL = f"""
WITH {STAGE_CTES}, speculated AS (
  SELECT stage_id, count(*) FILTER (WHERE speculative) AS speculative_attempt_count,
    count(*) FILTER (WHERE speculative AND end_reason = 'Success') AS speculative_success_count,
    sum(duration_ms) FILTER (WHERE speculative) AS speculative_attempt_ms,
    (list(task_id ORDER BY duration_ms DESC, task_id) FILTER (WHERE speculative AND end_reason = 'Success'))[1:5] AS worst_task_ids
  FROM tasks GROUP BY stage_id
)
SELECT speculated.stage_id, stage_statistics.stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  speculated.worst_task_ids,
  CASE WHEN speculated.speculative_success_count / stage_statistics.task_count >= 0.5 THEN 'high' ELSE 'medium' END AS severity,
  stage_statistics.task_count, speculated.speculative_attempt_count, speculated.speculative_success_count,
  speculated.speculative_attempt_ms,
  round(speculated.speculative_success_count / stage_statistics.task_count, 4) AS speculative_success_share
FROM speculated
JOIN stage_statistics USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
WHERE stage_statistics.task_count >= $min_tasks AND speculated.speculative_success_count >= $min_attempts
  AND speculated.speculative_success_count / stage_statistics.task_count >= $min_share
ORDER BY speculated.stage_id
"""

_SPECULATION_LESSON = """\
## What it measures

The task attempts Spark launched as speculative copies of a slow task, and how many of them
finished first, against the stage's completed tasks. A copy runs on another host while the
original still runs, and whichever finishes first is kept.

## Why it costs time

Each copy holds a core that another task could have used. A copy that wins says the original was
slow because of where it ran: a busy, failing or slow host. When copies win for a large share of
the tasks, the stage is paying twice for much of its work, and the cause is in the cluster
rather than the data.

## Where the Spark UI shows it

Stages tab › the stage › the task table: a task index listed twice, one attempt marked
"(speculative)" and the other killed. Speculation is on where `spark.speculation` is `true`.

## How to fix it

- Find the hosts the losing attempts ran on. A host slow for every stage is a hardware or
  neighbor problem for the cluster manager.
- Where copies win because tasks differ in size, the stage is skewed: speculation copies the
  slow work rather than fixing it.
- Raise `spark.speculation.multiplier` or `spark.speculation.quantile`, so fewer copies launch.
"""

SPECULATION = Rule(
    rule_id='stage.speculation',
    title='Speculative copies won {speculative_success_count} of the {task_count} tasks of stage {stage_id}',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(
        Threshold('min_tasks', 10, 'count', 'sparkDoctor docs/detections.md'),
        Threshold('min_attempts', 3, 'count', 'sparkDoctor docs/detections.md'),
        Threshold('min_share', 0.2, 'fraction', 'sparkDoctor docs/detections.md'),
    ),
    severity_ladder={
        'medium': 'across at least min_tasks tasks, at least min_attempts speculative copies won, min_share of the tasks or more',
        'high': 'speculative copies won half the tasks or more',
    },
    remediation=(
        'find the hosts the losing attempts ran on',
        'look for skew where copies win because the tasks differ in size',
        'raise spark.speculation.multiplier or spark.speculation.quantile',
    ),
    docs=(_SCHEDULING_DOCS,),
    lesson=_SPECULATION_LESSON,
    sql=_SPECULATION_SQL,
)

_TASK_OOM_SQL = f"""
WITH {STAGE_CTES}, out_of_memory AS (
  SELECT stage_id, count(*) AS oom_attempt_count, count(DISTINCT executor_id) AS executor_count,
    list(task_id ORDER BY task_id)[1:5] AS worst_task_ids,
    left(arg_min({failure_line('end_reason_detail')}, task_id), 300) AS first_oom_reason
  FROM tasks WHERE end_reason_detail ILIKE '%OutOfMemoryError%'
  GROUP BY stage_id
)
SELECT out_of_memory.stage_id, {_HIGHEST_ATTEMPT.format(table='out_of_memory')} AS stage_attempt,
  stage_owners.job_id, stage_owners.sql_execution_id, out_of_memory.worst_task_ids, 'high' AS severity,
  out_of_memory.oom_attempt_count, out_of_memory.executor_count, out_of_memory.first_oom_reason
FROM out_of_memory
LEFT JOIN stage_owners USING (stage_id)
ORDER BY out_of_memory.stage_id
"""

_TASK_OOM_LESSON = """\
## What it measures

The task attempts whose end reason names `java.lang.OutOfMemoryError`. Spark reports the error
a task threw as `ExceptionFailure`, with the error's class and message, before the executor exits.

## Why it costs time

A JVM out of heap cannot be trusted to go on, so the executor exits, and every task running on
it is lost with it. Each lost partition runs again, and the map output the executor held is
recomputed for the stages that read it. A partition that fails this way four times fails the
job.

## Where the Spark UI shows it

Stages tab › the stage › the task table's Errors column, and the Executors tab, where the
executor shows as dead with its removal reason.

## How to fix it

- Raise `spark.executor.memory`, or lower `spark.executor.cores`, so each task has more heap.
- Raise the partition count, so each task holds fewer rows.
- Look for one huge value: a `collect_list` over a hot key, an exploded array, or a row built
  from a whole file.
"""

TASK_OOM = Rule(
    rule_id='stage.task-oom',
    title='{oom_attempt_count} task attempts of stage {stage_id} ran out of memory',
    signals=frozenset({'tasks'}),
    scope='stage',
    thresholds=(),
    severity_ladder={'high': 'any task attempt ended with an OutOfMemoryError'},
    remediation=(
        'raise spark.executor.memory, or lower spark.executor.cores',
        'raise the partition count',
        'look for one huge value, such as a collect_list over a hot key',
    ),
    docs=('https://spark.apache.org/docs/latest/tuning.html#memory-management-overview',),
    lesson=_TASK_OOM_LESSON,
    sql=_TASK_OOM_SQL,
)

_FAILED_SQL = f"""
WITH {STAGE_CTES}, failed AS (
  SELECT stage_id, attempt, name, failure_reason FROM stages WHERE is_highest_attempt AND failure_reason IS NOT NULL
), attempts AS (
  SELECT stage_id, count(*) AS stage_attempt_count FROM stages GROUP BY stage_id
), failed_tasks AS (
  SELECT stage_id, count(*) AS failed_attempt_count, sum(duration_ms) AS failed_attempt_ms FROM tasks
  WHERE {_UNSUCCESSFUL} GROUP BY stage_id
)
SELECT failed.stage_id, failed.attempt AS stage_attempt, stage_owners.job_id, stage_owners.sql_execution_id,
  'high' AS severity, failed.name AS stage_name,
  left({failure_line('failed.failure_reason')}, 500) AS failure_reason,
  attempts.stage_attempt_count, coalesce(failed_tasks.failed_attempt_count, 0) AS failed_attempt_count,
  failed_tasks.failed_attempt_ms
FROM failed
JOIN attempts USING (stage_id)
LEFT JOIN failed_tasks USING (stage_id)
LEFT JOIN stage_owners USING (stage_id)
ORDER BY failed.stage_id
"""

_FAILED_LESSON = """\
## What it measures

A stage whose last attempt ended with a failure reason, from its `SparkListenerStageCompleted`
event. A stage retried after a fetch failure and then finished is not failed.

## Why it costs time

A failed stage fails the job that ran it, and the job's work is lost unless the driver catches
the error and runs again. Everything the stage's attempts computed was thrown away.

## Where the Spark UI shows it

Stages tab › "Failed Stages", with the failure reason, and the Jobs tab, where the job shows as
failed.

## How to fix it

- Read `failure_reason`: it names the task that failed its last attempt and the error it threw.
- Where tasks failed four times, `spark.task.maxFailures`, the error is in the code or the data
  rather than in one bad executor.
"""

FAILED = Rule(
    rule_id='stage.failed',
    title='Stage {stage_id} failed: {failure_reason}',
    signals=frozenset({'timeline'}),
    scope='stage',
    thresholds=(),
    severity_ladder={'high': "the stage's last attempt failed"},
    remediation=(
        'read failure_reason for the failing task and its error',
        'fix the code or data where tasks failed spark.task.maxFailures times',
    ),
    docs=(_SCHEDULING_DOCS,),
    lesson=_FAILED_LESSON,
    sql=_FAILED_SQL,
)

RULES = (RETRY_WASTE, SPECULATION, TASK_OOM, FAILED)
