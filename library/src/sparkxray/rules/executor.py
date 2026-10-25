"""Rules over the executors and the driver: the cores they held idle, the memory they asked for against what they used, and those lost."""

from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any

from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold
from sparkxray.rules.spark_settings import DEFAULT_MEMORY_BYTES
from sparkxray.rules.spark_settings import DRIVER_MEMORY
from sparkxray.rules.spark_settings import DYNAMIC_ALLOCATION
from sparkxray.rules.spark_settings import EXECUTOR_MEMORY
from sparkxray.rules.spark_settings import memory_setting_bytes
from sparkxray.rules.spark_settings import spark_settings_cte
from sparkxray.tables.registry import CORE_TIME_CTES
from sparkxray.tables.registry import GLUE_DPU_CORES
from sparkxray.tables.registry import glue_shares_cte

if TYPE_CHECKING:
    from sparkxray.tables.relations import Tables

_ALLOCATION_DOCS = 'https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation'
_MEMORY_DOCS = 'https://spark.apache.org/docs/latest/tuning.html#memory-management-overview'
# DataFlint MemoryAlertsReducer.ts: ask for 20% more than the heap that filled, or keep 20% over the peak.
_HEADROOM = 1.2
_TASK_PEAKS_REASON = (
    "peak heap comes from each task's end-of-task poll, which misses the heap between polls; "
    'set spark.eventLog.logStageExecutorMetrics=true for per-stage peaks'
)

_IDLE_CORES_SQL = f"""
WITH {CORE_TIME_CTES}, {spark_settings_cte(DYNAMIC_ALLOCATION)}, {glue_shares_cte('glue.driver.workerUtilization')},
totals AS (SELECT * FROM core_totals)
SELECT 'medium' AS severity, totals.executor_count, totals.driver_ran_tasks,
  round(totals.core_time_ms / 3600000, 4) AS core_hours,
  round(totals.task_core_time_ms / 3600000, 4) AS task_core_hours,
  round((totals.core_time_ms - totals.task_core_time_ms) / 3600000, 4) AS idle_core_hours,
  round(1 - totals.task_core_time_ms / totals.core_time_ms, 4) AS idle_core_share,
  CASE WHEN application.platform = 'glue'
    THEN round((totals.core_time_ms - totals.task_core_time_ms) / 3600000 / {GLUE_DPU_CORES}, 4) END AS idle_dpu_hours,
  (SELECT round(avg(share), 4) FROM glue_shares) AS glue_worker_utilization_share,
  lower(coalesce(spark_settings."{DYNAMIC_ALLOCATION}", 'false')) = 'true' AS dynamic_allocation_enabled
FROM totals, application, spark_settings
WHERE totals.core_time_ms > 0 AND 1 - totals.task_core_time_ms / totals.core_time_ms > $min_idle_share
  AND totals.core_time_ms - totals.task_core_time_ms >= $min_idle_core_ms
"""

_IDLE_CORES_LESSON = """\
## What it measures

Each executor's core time, its lifetime times its cores, against the time its task attempts held
those cores, every attempt counted and each holding `spark.task.cpus` cores. The rule sums both
over every executor, or over the driver where there are none, as in local mode, where the driver
runs the tasks. `idle_core_hours` is the difference, and on Glue `idle_dpu_hours` prices it in
DPUs of 4 vCPUs. Where `logs ingest --glue-run` joined the run's Glue metrics,
`glue_worker_utilization_share` is the mean of Glue's own `glue.driver.workerUtilization` over
the run. It measures the same waste from Glue's side, so the two should roughly agree.

## Why it costs time

A core held and running no task is paid for and does nothing. Cores sit idle while the driver
works alone between jobs, while a stage runs fewer tasks than there are cores, and while every
core waits on the one slow task a stage ends with. More than {min_idle_share} of the core time
idle means the application holds more than it uses. A short run is mostly start-up and shutdown,
so the rule also needs {min_idle_core_ms} of idle core time before the share is worth acting on.

## Where the Spark UI shows it

Executors tab: Cores, Task Time and the duration of each executor. The Jobs tab's "Event
Timeline" shows executors added and removed against the jobs that ran.

## How to fix it

- With dynamic allocation, release idle executors sooner: lower
  `spark.dynamicAllocation.executorIdleTimeout`, raise
  `spark.dynamicAllocation.schedulerBacklogTimeout`, or lower
  `spark.dynamicAllocation.executorAllocationRatio`.
- With a fixed allocation, ask for fewer: lower `spark.executor.instances` or
  `spark.executor.cores`, or on Glue the number of workers.
- Where the idle time is between jobs, see `app.driver-gaps`. Where it is in a stage's tail, see
  `stage.task-skew` and `stage.shuffle-skew`.
"""

IDLE_CORES = Rule(
    rule_id='executor.idle-cores',
    title='Cores sat idle for {idle_core_share:.0%} of the time executors held them',
    signals=frozenset({'executors', 'tasks'}),
    scope='application',
    thresholds=(
        Threshold('min_idle_share', 0.5, 'fraction', 'DataFlint WastedCoresAlertsReducer.ts'),
        Threshold(
            'min_idle_core_ms', 300000, 'ms', 'proposed: five minutes of one core, past what a short run idles starting and stopping'
        ),
    ),
    severity_ladder={'medium': 'over min_idle_share of the core time ran no task, and at least min_idle_core_ms of core time sat idle'},
    remediation=(
        'with dynamic allocation, lower spark.dynamicAllocation.executorIdleTimeout or spark.dynamicAllocation.executorAllocationRatio',
        'with a fixed allocation, lower spark.executor.instances or spark.executor.cores',
        'where the cores idle between jobs or in a stage tail, fix that first',
    ),
    docs=(_ALLOCATION_DOCS,),
    lesson=_IDLE_CORES_LESSON,
    sql=_IDLE_CORES_SQL,
    rule_version=2,
)

_EXECUTOR_MEMORY_SQL = f"""
WITH {spark_settings_cte(EXECUTOR_MEMORY)}
SELECT executors.executor_id, executors.executor_memory_bytes, executors.peak_jvm_heap_bytes, executors.peak_memory_source,
  spark_settings."{EXECUTOR_MEMORY}" AS memory_setting
FROM executors LEFT JOIN spark_settings ON true
WHERE NOT executors.is_driver AND executors.peak_jvm_heap_bytes IS NOT NULL
ORDER BY try_cast(executors.executor_id AS BIGINT), executors.executor_id
"""

# Dr. Elephant ExecutorGcHeuristic: GC under 5% of task time reads as more memory than the tasks need.
_EXECUTOR_GC_SQL = """
SELECT round(sum(tasks.jvm_gc_time_ms) / nullif(sum(tasks.duration_ms), 0), 4)
FROM tasks JOIN executors USING (executor_id) WHERE NOT executors.is_driver
"""


def _fullest_executor(tables: Tables) -> dict[str, Any] | None:
    """The executor whose peak heap came nearest its memory, and where each figure came from. None where no peak was measured.

    An executor's memory is its resource profile's, else `spark.executor.memory`, else Spark's 1g default.
    """
    rows = tables.query(_EXECUTOR_MEMORY_SQL).fetchall()
    fullest: dict[str, Any] | None = None
    for executor_id, profile_bytes, peak_bytes, peak_source, setting in rows:
        configured = memory_setting_bytes(setting)
        if profile_bytes is not None:
            memory_bytes, memory_source = profile_bytes, 'resource_profile'
        elif configured is not None:
            memory_bytes, memory_source = configured, EXECUTOR_MEMORY
        else:
            memory_bytes, memory_source = DEFAULT_MEMORY_BYTES, 'default'
        share = peak_bytes / memory_bytes
        if fullest is None or share > fullest['peak_memory_share']:
            fullest = {
                'peak_executor_id': executor_id,
                'peak_jvm_heap_bytes': peak_bytes,
                'executor_memory_bytes': memory_bytes,
                'peak_memory_share': share,
                'peak_memory_source': peak_source,
                'executor_memory_source': memory_source,
            }
    if fullest is not None:
        fullest['peak_memory_share'] = round(fullest['peak_memory_share'], 4)
        fullest['measured_executor_count'] = len(rows)
    return fullest


def _memory_under(context: RuleContext) -> Iterable[Mapping[str, Any]]:
    fullest = None if context.tables is None else _fullest_executor(context.tables)
    if fullest is None or fullest['peak_memory_share'] <= context.thresholds['max_peak_share']:
        return []
    suggested = round(fullest['executor_memory_bytes'] * _HEADROOM)
    return [{'severity': 'high', **fullest, 'suggested_executor_memory_bytes': suggested}]


def _memory_over(context: RuleContext) -> Iterable[Mapping[str, Any]]:
    """Every executor's peak heap under `min_peak_share` of its memory, unless a task or an executor ran out of memory.

    An allocation that fails never reaches the heap's peak, and a lost executor's last metrics can
    be lost with it. So a run where `stage.task-oom` fired, or where `executor.lost` graded a loss
    high, which it does for a loss to memory, says nothing about memory to spare.
    """
    ran_out = context.findings.get('stage.task-oom', ()) or any(
        finding.base_severity == 'high' for finding in context.findings.get('executor.lost', ())
    )
    fullest = None if context.tables is None or ran_out else _fullest_executor(context.tables)
    if context.tables is None or fullest is None or fullest['peak_memory_share'] >= context.thresholds['min_peak_share']:
        return []
    gc_share = context.tables.first_row(_EXECUTOR_GC_SQL)
    measured_by_task = fullest['peak_memory_source'] == 'task_peaks'
    return [
        {
            'severity': 'low',
            'confidence': 'medium' if measured_by_task else 'high',
            'confidence_reason': _TASK_PEAKS_REASON if measured_by_task else None,
            **fullest,
            'suggested_executor_memory_bytes': round(fullest['peak_jvm_heap_bytes'] * _HEADROOM),
            'gc_share': None if gc_share is None else gc_share[0],
        }
    ]


_MEMORY_SOURCES = """\
Peak heap is each executor's `JVMHeapMemory` peak, the figure the Executors tab shows as Peak JVM
Memory OnHeap. It comes from the stage executor metrics Spark logs under
`spark.eventLog.logStageExecutorMetrics=true`, else from the peaks each task reported as it
ended, and `peak_memory_source` says which. An executor's memory is its resource profile's
request, else `spark.executor.memory`, else Spark's 1g default, and `executor_memory_source` says
which."""

_MEMORY_UNDER_LESSON = f"""\
## What it measures

The executor whose peak JVM heap came nearest the memory it was given, as a share of that
memory. {_MEMORY_SOURCES}

## Why it costs time

A heap that fills collects garbage again and again, spills execution memory to disk, and then
fails a task with `OutOfMemoryError` or has its container killed. Each failure reruns the tasks
the executor held, and a lost executor's shuffle output is computed again.

## Where the Spark UI shows it

Executors tab › "Show Additional Metrics" › Peak JVM Memory OnHeap, against the Environment
tab's `spark.executor.memory`. The Executors tab's GC Time column turns red as the heap fills.

## How to fix it

- Raise `spark.executor.memory`. `suggested_executor_memory_bytes` is 20% over the memory that
  filled, as DataFlint suggests.
- Lower `spark.executor.cores`, so fewer tasks share one heap.
- Raise the partition count, so each task holds fewer rows at once.
"""

MEMORY_UNDER = Rule(
    rule_id='executor.memory-under',
    title='Executor {peak_executor_id} peaked at {peak_memory_share:.0%} of its {executor_memory_bytes:bytes} heap',
    signals=frozenset({'executors'}),
    scope='application',
    thresholds=(Threshold('max_peak_share', 0.95, 'fraction', 'DataFlint MemoryAlertsReducer.ts'),),
    severity_ladder={'high': "an executor's peak heap is over max_peak_share of its memory"},
    remediation=(
        'raise spark.executor.memory to suggested_executor_memory_bytes',
        'lower spark.executor.cores, so fewer tasks share one heap',
        'raise the partition count',
    ),
    docs=(_MEMORY_DOCS,),
    lesson=_MEMORY_UNDER_LESSON,
    check=_memory_under,
)

_MEMORY_OVER_LESSON = f"""\
## What it measures

The executor whose peak JVM heap came nearest the memory it was given, as a share of that
memory, so the rule fires only where no executor came near it. {_MEMORY_SOURCES} `gc_share` is
GC time against task time on the executors, which Dr. Elephant reads as more memory than needed
below 5%. The rule stays silent where `stage.task-oom` fired, or `executor.lost` found an
executor lost to memory. An allocation that fails never reaches the peak, so a low peak there
hides a heap that ran out.

## Why it costs time

Memory asked for and never used is paid for, and on a shared cluster it keeps other work from
being placed. It costs no run time, which is why the rule grades it low. A peak read from task
polls can miss the heap between them, so the finding then carries medium confidence.

## Where the Spark UI shows it

Executors tab › "Show Additional Metrics" › Peak JVM Memory OnHeap, against the Environment
tab's `spark.executor.memory`.

## How to fix it

- Lower `spark.executor.memory`. `suggested_executor_memory_bytes` keeps 20% over the highest
  peak, as DataFlint suggests.
- Or keep the memory and raise `spark.executor.cores`, so more tasks share it.
"""

MEMORY_OVER = Rule(
    rule_id='executor.memory-over',
    title='No executor used more than {peak_memory_share:.0%} of its {executor_memory_bytes:bytes} heap',
    signals=frozenset({'executors'}),
    scope='application',
    thresholds=(Threshold('min_peak_share', 0.7, 'fraction', 'DataFlint MemoryAlertsReducer.ts'),),
    severity_ladder={'low': "every executor's peak heap is under min_peak_share of its memory"},
    remediation=(
        'lower spark.executor.memory to suggested_executor_memory_bytes',
        'or raise spark.executor.cores, so more tasks share the memory',
    ),
    docs=(_MEMORY_DOCS,),
    lesson=_MEMORY_OVER_LESSON,
    check=_memory_over,
    reads_findings_of=('stage.task-oom', 'executor.lost'),
)

_DRIVER_MEMORY_SQL = f"""
WITH {spark_settings_cte(DRIVER_MEMORY)}
SELECT executors.peak_jvm_heap_bytes, executors.peak_memory_source, spark_settings."{DRIVER_MEMORY}" AS memory_setting
FROM executors LEFT JOIN spark_settings ON true
WHERE executors.is_driver AND executors.peak_jvm_heap_bytes IS NOT NULL
"""


def _driver_memory_under(context: RuleContext) -> Iterable[Mapping[str, Any]]:
    row = None if context.tables is None else context.tables.first_row(_DRIVER_MEMORY_SQL)
    if row is None:
        return []
    peak_bytes, peak_source, setting = row
    configured = memory_setting_bytes(setting)
    memory_bytes, memory_source = (DEFAULT_MEMORY_BYTES, 'default') if configured is None else (configured, DRIVER_MEMORY)
    share = peak_bytes / memory_bytes
    if share <= context.thresholds['max_peak_share']:
        return []
    return [
        {
            'severity': 'high',
            'executor_id': 'driver',
            'peak_jvm_heap_bytes': peak_bytes,
            'driver_memory_bytes': memory_bytes,
            'peak_memory_share': round(share, 4),
            'peak_memory_source': peak_source,
            'driver_memory_source': memory_source,
            'suggested_driver_memory_bytes': round(memory_bytes * _HEADROOM),
        }
    ]


_DRIVER_MEMORY_LESSON = """\
## What it measures

The driver's peak JVM heap, as a share of `spark.driver.memory`, or of Spark's 1g default where
the application set none. The peak comes from stage executor metrics where Spark logged them,
else from the peaks of the tasks the driver ran in local mode, and `peak_memory_source` says
which.

## Why it costs time

The driver holds every task's result as it returns, the rows a `collect` or `toPandas` brings
back, each broadcast before it is sent, and the plans and metrics of every query. A driver whose
heap fills pauses to collect garbage while executors wait for their next tasks, and an
`OutOfMemoryError` there ends the application.

## Where the Spark UI shows it

Executors tab › the driver row › "Show Additional Metrics" › Peak JVM Memory OnHeap, against the
Environment tab's `spark.driver.memory`.

## How to fix it

- Raise `spark.driver.memory` on the command line or in the cluster's configuration.
  `suggested_driver_memory_bytes` is 20% over it, as DataFlint suggests. Set in code after the
  driver started, it changes nothing, because the driver's JVM is already running.
- Bring less back to the driver: write results out rather than `collect` them, and `limit`
  before a `toPandas`.
- Broadcast less: lower `spark.sql.autoBroadcastJoinThreshold` where large broadcasts are built.
"""

DRIVER_MEMORY_UNDER = Rule(
    rule_id='executor.driver-memory-under',
    title="The driver's heap peaked at {peak_memory_share:.0%} of its {driver_memory_bytes:bytes}",
    signals=frozenset({'executors'}),
    scope='executor',
    thresholds=(Threshold('max_peak_share', 0.95, 'fraction', 'DataFlint MemoryAlertsReducer.ts'),),
    severity_ladder={'high': "the driver's peak heap is over max_peak_share of spark.driver.memory"},
    remediation=(
        'raise spark.driver.memory to suggested_driver_memory_bytes, where the driver starts',
        'write results out rather than collect them',
        'lower spark.sql.autoBroadcastJoinThreshold',
    ),
    docs=(_MEMORY_DOCS,),
    lesson=_DRIVER_MEMORY_LESSON,
    check=_driver_memory_under,
)

# Removals that lose nothing: dynamic allocation releasing an idle executor, by kill or by
# decommission, YARN returning a container the driver gave back, and the application ending.
_LOST_SQL = r"""
WITH removed AS (
  SELECT executor_id, host, remove_reason,
    nullif(regexp_extract(remove_reason, '(?i)(?:exit status:|exit code|exited with code) ?\(?(-?[0-9]+)', 1), '') AS exit_code
  FROM executors
  WHERE NOT is_driver AND remove_reason IS NOT NULL
    AND remove_reason <> 'Executor killed by driver.'
    AND remove_reason NOT LIKE 'Executor decommission: spark scale down%'
    AND remove_reason NOT LIKE '%exited from explicit termination request%'
    AND remove_reason NOT LIKE '%exited after Application shutdown%'
), task_losses AS (
  SELECT executor_id, count(*) FILTER (WHERE end_reason = 'ExecutorLostFailure') AS lost_task_count,
    count(*) FILTER (WHERE end_reason_detail ILIKE '%OutOfMemoryError%') AS oom_task_count
  FROM tasks GROUP BY executor_id
)
SELECT removed.executor_id,
  CASE WHEN removed.remove_reason ILIKE '%OutOfMemoryError%' OR removed.remove_reason ILIKE '%OOMKilled%'
      OR removed.remove_reason ILIKE '%killed by YARN for exceeding%' OR removed.exit_code IN ('52', '137', '143')
      OR coalesce(task_losses.oom_task_count, 0) > 0
    THEN 'high' ELSE 'medium' END AS severity,
  removed.host, left(split_part(ltrim(removed.remove_reason, chr(10) || ' '), chr(10), 1), 160) AS removal_summary,
  left(removed.remove_reason, 1000) AS remove_reason, removed.exit_code,
  coalesce(task_losses.lost_task_count, 0) AS lost_task_count, coalesce(task_losses.oom_task_count, 0) AS oom_task_count
FROM removed LEFT JOIN task_losses USING (executor_id)
ORDER BY try_cast(removed.executor_id AS BIGINT), removed.executor_id
"""

_LOST_LESSON = """\
## What it measures

Each `SparkListenerExecutorRemoved` event's reason, for executors removed while the application
ran. It leaves out the removals that lose nothing: dynamic allocation releasing an idle executor,
YARN returning a container the driver gave back, and the application ending. `exit_code` is read
from the reason, and `lost_task_count` and `oom_task_count` count the task attempts that ended on
the executor because it was lost, or with an `OutOfMemoryError`.

## Why it costs time

A lost executor fails every task it was running, and the shuffle output it wrote is gone, so the
stages that wrote it run again for whatever still needs it. Exit code 52 is the JVM's
`OutOfMemoryError`. 137 is a kill signal, which on YARN and Kubernetes is most often the container
going over its memory limit, and 143 is YARN's request to stop. Those are graded high.

## Where the Spark UI shows it

Executors tab: the dead executors, with Status and the reason each was removed. The Jobs tab's
"Event Timeline" marks when each was removed.

## How to fix it

- For a container killed over its memory limit, raise `spark.executor.memoryOverhead`, which
  covers memory outside the heap: Python workers, Arrow buffers and native libraries.
- For an `OutOfMemoryError` in the heap, see `executor.memory-under` and `stage.task-oom`.
- For spot or preempted capacity, turn on `spark.decommission.enabled` so executors hand over
  their shuffle and cached blocks before they go.
"""

LOST = Rule(
    rule_id='executor.lost',
    title='Executor {executor_id} was lost: {removal_summary}',
    signals=frozenset({'executors'}),
    scope='executor',
    thresholds=(),
    severity_ladder={
        'medium': 'an executor was removed for a reason other than idling or the application ending',
        'high': 'the removal names OutOfMemoryError, exit code 52, 137 or 143, or a container killed over its memory, '
        'or a task on the executor ran out of memory',
    },
    remediation=(
        'raise spark.executor.memoryOverhead for a container killed over its memory limit',
        'raise spark.executor.memory for an OutOfMemoryError in the heap',
        'turn on spark.decommission.enabled where spot or preempted capacity is lost',
    ),
    docs=('https://spark.apache.org/docs/latest/configuration.html#application-properties',),
    lesson=_LOST_LESSON,
    sql=_LOST_SQL,
)

RULES = (IDLE_CORES, MEMORY_UNDER, MEMORY_OVER, DRIVER_MEMORY_UNDER, LOST)
