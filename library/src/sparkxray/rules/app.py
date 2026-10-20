"""Rules over the application's job timeline: the driver alone between jobs, jobs too short to schedule, and jobs that failed."""

from __future__ import annotations

from sparkxray.rules.failure_reasons import failure_line
from sparkxray.rules.failure_reasons import is_cancellation
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Threshold
from sparkxray.tables.registry import RUN_SPAN_SQL

_DATABRICKS_GUIDE = 'https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/jobs-timeline'

# A gap runs from the latest end of every job submitted before a job to that job's submission, so
# overlapping jobs leave none. The time before the first job and after the last is no gap.
_DRIVER_GAPS_SQL = f"""
WITH run AS ({RUN_SPAN_SQL}), spans AS (
  SELECT jobs.job_id, jobs.submit_epoch_ms AS start_ms, coalesce(jobs.end_epoch_ms, run.end_epoch_ms) AS end_ms
  FROM jobs, run WHERE jobs.submit_epoch_ms IS NOT NULL
), ordered AS (
  SELECT *, max(end_ms) OVER (ORDER BY start_ms, job_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS covered_until_ms
  FROM spans
), islands AS (
  SELECT start_ms, end_ms,
    sum(CASE WHEN covered_until_ms IS NULL OR start_ms > covered_until_ms THEN 1 ELSE 0 END) OVER (ORDER BY start_ms, job_id) AS island
  FROM ordered
), covered AS (
  SELECT coalesce(sum(island_end_ms - island_start_ms), 0) AS job_ms
  FROM (SELECT island, min(start_ms) AS island_start_ms, max(end_ms) AS island_end_ms FROM islands GROUP BY island)
), gaps AS (
  SELECT job_id AS next_job_id, covered_until_ms AS gap_start_ms, start_ms - covered_until_ms AS gap_ms
  FROM ordered WHERE start_ms - covered_until_ms >= $min_gap_ms
)
SELECT 'medium' AS severity, round(sum(gaps.gap_ms) / 1000, 3) AS impact_seconds,
  count(*) AS gap_count, sum(gaps.gap_ms) AS total_gap_ms, max(gaps.gap_ms) AS longest_gap_ms,
  list(gaps.gap_ms ORDER BY gaps.gap_start_ms)[1:20] AS gap_lengths_ms,
  list(gaps.next_job_id ORDER BY gaps.gap_start_ms)[1:20] AS next_job_ids,
  round(1 - any_value(covered.job_ms) / nullif(any_value(run.end_epoch_ms - run.start_epoch_ms), 0), 4) AS driver_time_share
FROM gaps, covered, run
HAVING count(*) > 0
"""

_DRIVER_GAPS_LESSON = """\
## What it measures

The spans between the first job's submission and the last job's end in which no job ran. Jobs
that overlap count as one span, so a gap is time the driver spent with no job to run. Each gap's
length is in `gap_lengths_ms`, beside the job that ended it in `next_job_ids`.
`driver_time_share` is the share of the whole run no job covered, as Sparklens reports it.

## Why it costs time

While no job runs, every executor is idle and still held. The driver is doing something else:
Python or Scala code on the driver alone, a `collect` and the work on its result, listing files
on object storage, or waiting on a call to another service. None of it is parallel, so adding
executors does not shorten it.

## Where the Spark UI shows it

Jobs tab › "Event Timeline": the stretches with no job bar. The job named in `next_job_ids` is
the first one after each gap, and its call site shows where in the code the driver resumed.

## How to fix it

- Move the driver's work onto the cluster: a Pandas step on collected rows becomes a DataFrame
  step, and a loop over files becomes one read.
- List large object-storage prefixes once, or read a table format that holds its own file list.
- Run independent steps from threads, so their jobs overlap.
"""

DRIVER_GAPS = Rule(
    rule_id='app.driver-gaps',
    title='No job ran for {total_gap_ms:duration} in {gap_count:plural gap} between jobs',
    signals=frozenset({'timeline'}),
    scope='application',
    thresholds=(Threshold('min_gap_ms', 60000, 'ms', 'Databricks Spark UI guide: gaps of a minute or more mid-pipeline'),),
    severity_ladder={'medium': 'no job ran for at least min_gap_ms between two jobs'},
    remediation=(
        "move the driver's own work onto the cluster as DataFrame steps",
        'list large object-storage prefixes once, or read a table format that holds its file list',
        'run independent steps from threads, so their jobs overlap',
    ),
    docs=(_DATABRICKS_GUIDE,),
    lesson=_DRIVER_GAPS_LESSON,
    sql=_DRIVER_GAPS_SQL,
)

# A job without a description is named by its last stage, whose name is its call site.
_MANY_SMALL_JOBS_SQL = """
WITH stage_names AS (
  SELECT stage_id, any_value(name) AS name FROM stages GROUP BY stage_id
), short_jobs AS (
  SELECT jobs.job_id, jobs.end_epoch_ms - jobs.submit_epoch_ms AS job_ms, coalesce(jobs.description, stage_names.name) AS job_name
  FROM jobs LEFT JOIN stage_names ON stage_names.stage_id = list_max(jobs.stage_ids)
  WHERE jobs.end_epoch_ms - jobs.submit_epoch_ms < $max_job_ms
), names AS (
  SELECT job_name, count(*) AS job_count FROM short_jobs GROUP BY job_name
)
SELECT 'low' AS severity, count(*) AS short_job_count, (SELECT count(*) FROM jobs) AS job_count,
  sum(short_jobs.job_ms) AS short_job_time_ms,
  (SELECT left(job_name, 200) FROM names ORDER BY job_count DESC, job_name NULLS LAST LIMIT 1) AS most_common_job_name,
  (SELECT job_count FROM names ORDER BY job_count DESC, job_name NULLS LAST LIMIT 1) AS most_common_job_count
FROM short_jobs
HAVING count(*) >= $min_jobs
"""

_MANY_SMALL_JOBS_LESSON = """\
## What it measures

The jobs that ran for less than `max_job_ms`, from submission to end. `most_common_job_name`
is the description or call site most of them share.

## Why it costs time

Each action starts a job, and each job pays for planning, scheduling its stages and returning
its result before any row is read. Many jobs of a second or two are the shape of an action
called in a loop: a `count` or `collect` per item, a write per partition, or a Python loop
building one DataFrame at a time.

## Where the Spark UI shows it

Jobs tab: the completed jobs list, sorted by Duration, and the Description column, which names
the call site each job ran from.

## How to fix it

- Replace the loop with one job: build one DataFrame over every item and run one action.
- Where the loop checks for emptiness with `count`, use `isEmpty` or `limit(1)` instead.
- Cache what every pass of the loop reads, so each job reads it from memory.
"""

MANY_SMALL_JOBS = Rule(
    rule_id='app.many-small-jobs',
    title='{short_job_count} of {job_count:plural job} ran for {short_job_time_ms:duration} in all',
    signals=frozenset({'timeline'}),
    scope='application',
    thresholds=(
        Threshold('max_job_ms', 2000, 'ms', 'Databricks Spark UI guide: jobs of a few seconds or less'),
        Threshold('min_jobs', 50, 'count', 'proposed'),
    ),
    severity_ladder={'low': 'at least min_jobs jobs each ran for less than max_job_ms'},
    remediation=(
        'build one DataFrame over every item and run one action',
        'check for rows with isEmpty or limit(1), not count',
        'cache what every pass of the loop reads',
    ),
    docs=(_DATABRICKS_GUIDE,),
    lesson=_MANY_SMALL_JOBS_LESSON,
    sql=_MANY_SMALL_JOBS_SQL,
)

# A job Spark cancelled ends as JobFailed too, so it is counted apart and never names the failure.
_FAILED_JOBS_SQL = f"""
WITH ended AS (
  SELECT job_id, failure_reason, {is_cancellation('failure_reason')} AS cancelled FROM jobs WHERE result = 'JobFailed'
)
SELECT 'high' AS severity, count(*) FILTER (WHERE NOT cancelled) AS failed_job_count,
  count(*) FILTER (WHERE cancelled) AS cancelled_job_count, (SELECT count(*) FROM jobs) AS job_count,
  (list(job_id ORDER BY job_id) FILTER (WHERE NOT cancelled))[1:20] AS failed_job_ids,
  left(arg_min({failure_line('failure_reason')}, job_id) FILTER (WHERE NOT cancelled), 500) AS first_failure_reason
FROM ended
HAVING count(*) FILTER (WHERE NOT cancelled) > 0
"""

_FAILED_JOBS_LESSON = """\
## What it measures

The jobs whose `SparkListenerJobEnd` result is `JobFailed`, and the first line of the first
one's exception. Where a Python error failed it, that line names the error its traceback ends
with, rather than the traceback's header. Spark ends a job it cancelled as `JobFailed` too, as
AQE cancels the other query stages of a query one stage failed. Those are counted apart in
`cancelled_job_count`, and the reason is read from the first job that was not cancelled.

## Why it costs time

A failed job threw away the stages it ran. Where the application caught the failure and carried
on, the log still shows it, and the work that job did ran for nothing. Where it did not, the
application ended there.

## Where the Spark UI shows it

Jobs tab › Failed Jobs, with each job's failure reason. The stage that failed it is under the
Stages tab › Failed Stages.

## How to fix it

- Read `first_failure_reason`. A stage failure names the task that failed and its exception,
  which `stage.failed` and `stage.task-oom` report with the stage.
- Where the job failed fetching shuffle output, see `executor.lost`.
"""

FAILED_JOBS = Rule(
    rule_id='app.failed-jobs',
    title='{failed_job_count} of {job_count:plural job} failed',
    signals=frozenset({'timeline'}),
    scope='application',
    thresholds=(),
    severity_ladder={'high': 'any job failed other than by being cancelled'},
    remediation=(
        'read first_failure_reason for the stage and task that failed the job',
        'where it failed fetching shuffle output, check for lost executors',
    ),
    docs=('https://spark.apache.org/docs/latest/web-ui.html#jobs-tab',),
    lesson=_FAILED_JOBS_LESSON,
    sql=_FAILED_JOBS_SQL,
)

RULES = (DRIVER_GAPS, MANY_SMALL_JOBS, FAILED_JOBS)
