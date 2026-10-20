"""Every table the build writes: what it holds, its columns and their types, and the SQL that computes each column.

A numeric column's name ends in its unit, which `column_unit` reads back. An instant ends in
`_epoch_ms`, milliseconds since the epoch, and a duration in `_ms`. Each table's `source` reads
the build's working tables, which `build.py` creates:

- `events`, every event line in log order, with `kind` its event class's short name;
- `ingest_facts`, the ingest manifest's one row;
- `plan_node_rows`, `plan_metric_refs` and `plan_version_rows`, read from every plan version;
- `highest_attempts`, `execution_stages`, `metric_aggregates` and `process_tree`.
"""

from __future__ import annotations

import dataclasses as dc


@dc.dataclass(frozen=True, slots=True)
class Column:
    name: str
    type: str  # a DuckDB type
    expression: str  # SQL over the table's source
    description: str


@dc.dataclass(frozen=True, slots=True)
class Table:
    name: str
    description: str
    source: str
    columns: tuple[Column, ...]
    order_by: str


def _columns(*specs: tuple[str, str, str, str]) -> tuple[Column, ...]:
    return tuple(Column(name, type_, expression, description) for name, type_, expression, description in specs)


_APPLICATION = Table(
    name='application',
    description='The application: its id, name, user and Spark version, when it ran, and where its log came from.',
    source="""
        SELECT facts.*, start."App ID" AS logged_app_id, start."App Attempt ID" AS attempt_id, start."App Name" AS app_name,
          start."User" AS app_user, start."Timestamp" AS started_at, finish."Timestamp" AS ended_at,
          log_start."Spark Version" AS logged_spark_version
        FROM ingest_facts AS facts
        LEFT JOIN (SELECT * FROM events WHERE kind = 'SparkListenerApplicationStart' ORDER BY rowid LIMIT 1) AS start ON true
        LEFT JOIN (SELECT * FROM events WHERE kind = 'SparkListenerApplicationEnd' ORDER BY rowid DESC LIMIT 1) AS finish ON true
        LEFT JOIN (SELECT * FROM events WHERE kind = 'SparkListenerLogStart' ORDER BY rowid LIMIT 1) AS log_start ON true
    """,
    columns=_columns(
        (
            'app_id',
            'VARCHAR',
            'coalesce(logged_app_id, app_id)',
            "The application's id from its start event, or the one the log's name holds where that event is missing.",
        ),
        ('attempt_id', 'VARCHAR', 'attempt_id', 'The application attempt, which only a cluster manager that retries applications writes.'),
        ('app_name', 'VARCHAR', 'app_name', 'The name the application set as spark.app.name.'),
        ('user', 'VARCHAR', 'app_user', 'The user the application ran as.'),
        ('spark_version', 'VARCHAR', 'coalesce(logged_spark_version, spark_version)', 'The Spark version that wrote the log.'),
        ('start_epoch_ms', 'BIGINT', 'started_at', 'When the application started.'),
        ('end_epoch_ms', 'BIGINT', 'ended_at', 'When the application ended. Null where the log ends first.'),
        (
            'platform',
            'VARCHAR',
            'platform',
            'Where the run ran, from its environment and app id: glue, emr-serverless, emr-ec2, databricks, local or other.',
        ),
        ('log_source', 'VARCHAR', 'source', 'The location ingested, as a file:// or s3:// URI.'),
        ('compacted', 'BOOLEAN', 'compacted', 'Whether a History Server compacted the rolling log, which drops finished tasks.'),
        (
            'in_progress',
            'BOOLEAN',
            'in_progress',
            'Whether Spark had not finished writing the log, by its .inprogress name or its rolling appstatus marker.',
        ),
    ),
    order_by='app_id',
)

_ENVIRONMENT_SECTIONS = (
    ('jvm', 'JVM Information'),
    ('spark', 'Spark Properties'),
    ('hadoop', 'Hadoop Properties'),
    ('system', 'System Properties'),
    ('classpath', 'Classpath Entries'),
    ('metrics', 'Metrics Properties'),
)

_ENVIRONMENT = Table(
    name='environment',
    description="The last SparkListenerEnvironmentUpdate's properties, one row per key, by section.",
    source=(
        "WITH latest AS (SELECT * FROM events WHERE kind = 'SparkListenerEnvironmentUpdate' ORDER BY rowid DESC LIMIT 1) "
        + ' UNION ALL '.join(
            f'SELECT \'{section}\' AS section, unnest(map_entries("{key}")) AS entry FROM latest' for section, key in _ENVIRONMENT_SECTIONS
        )
    ),
    columns=_columns(
        ('section', 'VARCHAR', 'section', 'The section the property sits in: jvm, spark, hadoop, system, classpath or metrics.'),
        ('key', 'VARCHAR', 'entry."key"', "The property's name, or a classpath entry's path."),
        ('value', 'VARCHAR', 'entry."value"', "The property's value, or how a classpath entry was added."),
    ),
    order_by='section, "key"',
)

_EXECUTORS = Table(
    name='executors',
    description='Every executor and the driver: where it ran, its cores and memory, and when it was added and removed.',
    source="""
        WITH added AS (
          SELECT "Executor ID" AS executor_id, "Timestamp" AS added_at, "Executor Info" AS info FROM events
          WHERE kind = 'SparkListenerExecutorAdded'
          QUALIFY row_number() OVER (PARTITION BY "Executor ID" ORDER BY rowid DESC) = 1
        ), removed AS (
          SELECT "Executor ID" AS executor_id, "Timestamp" AS removed_at, "Removed Reason" AS removed_reason FROM events
          WHERE kind = 'SparkListenerExecutorRemoved'
          QUALIFY row_number() OVER (PARTITION BY "Executor ID" ORDER BY rowid DESC) = 1
        ), block_managers AS (
          SELECT "Block Manager ID"."Executor ID" AS executor_id, "Block Manager ID"."Host" AS block_manager_host,
            "Maximum Memory" AS maximum_memory, "Timestamp" AS registered_at FROM events
          WHERE kind = 'SparkListenerBlockManagerAdded'
          QUALIFY row_number() OVER (PARTITION BY "Block Manager ID"."Executor ID" ORDER BY rowid DESC) = 1
        ), profiles AS (
          SELECT "Resource Profile Id" AS profile_id, "Executor Resource Requests"."memory"."Amount" AS memory_mib FROM events
          WHERE kind = 'SparkListenerResourceProfileAdded'
          QUALIFY row_number() OVER (PARTITION BY "Resource Profile Id" ORDER BY rowid DESC) = 1
        ), stage_peaks AS (
          SELECT "Executor ID" AS executor_id, max("Executor Metrics"."JVMHeapMemory") AS heap FROM events
          WHERE kind = 'SparkListenerStageExecutorMetrics' GROUP BY "Executor ID"
        ), task_peaks AS (
          SELECT "Task Info"."Executor ID" AS executor_id, max("Task Executor Metrics"."JVMHeapMemory") AS heap FROM events
          WHERE kind = 'SparkListenerTaskEnd' GROUP BY "Task Info"."Executor ID"
        ), ids AS (
          SELECT executor_id FROM added UNION SELECT executor_id FROM block_managers UNION SELECT executor_id FROM removed
        )
        SELECT ids.executor_id, added.added_at, added.info, removed.removed_at, removed.removed_reason,
          block_managers.block_manager_host, block_managers.maximum_memory, block_managers.registered_at, profiles.memory_mib,
          nullif(stage_peaks.heap, 0) AS stage_heap, nullif(task_peaks.heap, 0) AS task_heap
        FROM ids
        LEFT JOIN added USING (executor_id)
        LEFT JOIN removed USING (executor_id)
        LEFT JOIN block_managers USING (executor_id)
        LEFT JOIN profiles ON profiles.profile_id = added.info."Resource Profile Id"
        LEFT JOIN stage_peaks USING (executor_id)
        LEFT JOIN task_peaks USING (executor_id)
    """,
    columns=_columns(
        ('executor_id', 'VARCHAR', 'executor_id', "The executor's id, or driver."),
        ('host', 'VARCHAR', 'coalesce(info."Host", block_manager_host)', 'The host it ran on.'),
        ('core_count', 'BIGINT', 'info."Total Cores"', 'The cores it offered tasks.'),
        ('resource_profile_id', 'BIGINT', 'info."Resource Profile Id"', 'The resource profile it started under, 0 for the default.'),
        # A resource profile holds memory in MiB. The driver's is spark.driver.memory, which no profile holds.
        (
            'executor_memory_bytes',
            'BIGINT',
            "CASE WHEN executor_id = 'driver' THEN NULL ELSE memory_mib * 1048576 END",
            "The heap it started with, its resource profile's memory. Null for the driver, whose heap no profile holds.",
        ),
        (
            'max_storage_memory_bytes',
            'BIGINT',
            'maximum_memory',
            'The memory its block manager could use for cached data, the total the Executors tab prints under Storage Memory.',
        ),
        (
            'add_epoch_ms',
            'BIGINT',
            'coalesce(added_at, registered_at)',
            'When it was added, or when its block manager registered where no add event was logged.',
        ),
        ('remove_epoch_ms', 'BIGINT', 'removed_at', 'When it was removed. Null for one still alive when the log ends.'),
        ('remove_reason', 'VARCHAR', 'removed_reason', 'Why Spark removed it.'),
        ('is_driver', 'BOOLEAN', "executor_id = 'driver'", 'Whether the row is the driver.'),
        # The Executors tab's Peak JVM Memory OnHeap: the most heap any stage's or task's peaks saw.
        # Stage peaks are written only under spark.eventLog.logStageExecutorMetrics, and a task's
        # only where a poll landed while it ran. Spark writes 0 for a peak it never polled.
        (
            'peak_jvm_heap_bytes',
            'BIGINT',
            'greatest(stage_heap, task_heap)',
            "The most JVM heap any stage's or task's peak saw on it, as the Executors tab's Peak JVM Memory OnHeap.",
        ),
        (
            'peak_memory_source',
            'VARCHAR',
            "CASE WHEN stage_heap IS NOT NULL THEN 'stage_executor_metrics' WHEN task_heap IS NOT NULL THEN 'task_peaks' END",
            'Where that peak came from: stage_executor_metrics, which spark.eventLog.logStageExecutorMetrics writes, or task_peaks.',
        ),
    ),
    order_by='is_driver DESC, try_cast(executor_id AS BIGINT), executor_id',
)

_JOBS = Table(
    name='jobs',
    description='Every job: the SQL execution and group it ran for, when it ran, how it ended, and its stages.',
    source="""
        WITH starts AS (
          SELECT "Job ID" AS job_id, "Submission Time" AS submitted_at, "Stage IDs" AS job_stage_ids, "Properties" AS properties
          FROM events WHERE kind = 'SparkListenerJobStart'
        ), finishes AS (
          SELECT "Job ID" AS job_id, "Completion Time" AS completed_at, "Job Result" AS job_result
          FROM events WHERE kind = 'SparkListenerJobEnd'
        )
        SELECT * FROM starts FULL JOIN finishes USING (job_id)
    """,
    columns=_columns(
        ('job_id', 'BIGINT', 'job_id', "The job's id."),
        (
            'sql_execution_id',
            'BIGINT',
            'try_cast(properties."spark.sql.execution.id" AS BIGINT)',
            'The SQL execution it ran for. Null for a job no query or DataFrame action started.',
        ),
        ('job_group', 'VARCHAR', 'properties."spark.jobGroup.id"', 'Its job group, as setJobGroup sets it.'),
        # Spark joins a job's tags with commas. A Spark before 3.5 writes none, which reads as null.
        (
            'job_tags',
            'VARCHAR[]',
            'CASE WHEN properties."spark.job.tags" IS NULL THEN NULL '
            'WHEN properties."spark.job.tags" = \'\' THEN []::VARCHAR[] '
            'ELSE string_split(properties."spark.job.tags", \',\') END',
            'Its tags, as addJobTag sets them. Null in a log from before Spark 3.5.',
        ),
        ('description', 'VARCHAR', 'properties."spark.job.description"', 'Its description, as setJobDescription sets it.'),
        ('submit_epoch_ms', 'BIGINT', 'submitted_at', 'When it was submitted.'),
        ('end_epoch_ms', 'BIGINT', 'completed_at', 'When it ended. Null for one still running when the log ends.'),
        ('result', 'VARCHAR', 'job_result."Result"', 'How it ended: JobSucceeded or JobFailed.'),
        ('failure_reason', 'VARCHAR', 'job_result."Exception"."Message"', 'The message of the exception a failed job ended with.'),
        (
            'stage_ids',
            'BIGINT[]',
            'job_stage_ids',
            'Every stage it planned, including those it skipped because their shuffle output was already written.',
        ),
    ),
    order_by='job_id',
)

_STAGES = Table(
    name='stages',
    description='Every stage attempt. A stage seen only in task events, as in a partial log, has no name or task count.',
    source="""
        WITH infos AS (
          SELECT "Stage Info" AS info FROM events WHERE kind IN ('SparkListenerStageSubmitted', 'SparkListenerStageCompleted')
          QUALIFY row_number() OVER (PARTITION BY "Stage Info"."Stage ID", "Stage Info"."Stage Attempt ID" ORDER BY rowid DESC) = 1
        ), attempts AS (
          SELECT info."Stage ID" AS stage_id, info."Stage Attempt ID" AS attempt FROM infos
          UNION
          SELECT "Stage ID", "Stage Attempt ID" FROM events WHERE kind IN ('SparkListenerTaskStart', 'SparkListenerTaskEnd')
        )
        SELECT attempts.stage_id, attempts.attempt, infos.info, highest_attempts.attempt AS highest_attempt
        FROM attempts
        JOIN highest_attempts ON highest_attempts.stage_id = attempts.stage_id
        LEFT JOIN infos ON infos.info."Stage ID" = attempts.stage_id AND infos.info."Stage Attempt ID" = attempts.attempt
    """,
    columns=_columns(
        ('stage_id', 'BIGINT', 'stage_id', "The stage's id."),
        ('attempt', 'BIGINT', 'attempt', 'The attempt, 0 for the first.'),
        ('name', 'VARCHAR', 'info."Stage Name"', "The stage's name, which is the call site that created it."),
        ('task_count', 'BIGINT', 'info."Number of Tasks"', 'How many tasks the attempt was submitted with.'),
        ('parent_ids', 'BIGINT[]', 'info."Parent IDs"', 'The stages whose output it reads.'),
        ('resource_profile_id', 'BIGINT', 'info."Resource Profile Id"', 'The resource profile its tasks ran under.'),
        ('submit_epoch_ms', 'BIGINT', 'info."Submission Time"', 'When the attempt was submitted. Null for one seen only in task events.'),
        ('complete_epoch_ms', 'BIGINT', 'info."Completion Time"', 'When the attempt completed or failed. Null for one still running.'),
        ('failure_reason', 'VARCHAR', 'info."Failure Reason"', 'Why the attempt failed.'),
        ('is_highest_attempt', 'BOOLEAN', 'attempt = highest_attempt', "Whether this is the stage's latest attempt."),
    ),
    order_by='stage_id, attempt',
)

_FINISH = 'nullif("Task Info"."Finish Time", 0)'
_DURATION = f'({_FINISH} - "Task Info"."Launch Time")'
_GETTING_RESULT = (
    f'(CASE WHEN {_FINISH} IS NULL THEN NULL '
    f'WHEN "Task Info"."Getting Result Time" > 0 THEN {_FINISH} - "Task Info"."Getting Result Time" ELSE 0 END)'
)
# The History Server's scheduler delay: what the task's duration holds beyond running it.
_SCHEDULER_DELAY = (
    f'CASE WHEN "Task Metrics" IS NULL OR {_FINISH} IS NULL THEN NULL ELSE greatest(0, {_DURATION} '
    '- "Task Metrics"."Executor Run Time" - "Task Metrics"."Executor Deserialize Time" '
    f'- "Task Metrics"."Result Serialization Time" - {_GETTING_RESULT}) END'
)
_SHUFFLE_READ = '"Task Metrics"."Shuffle Read Metrics"'
_SHUFFLE_WRITE = '"Task Metrics"."Shuffle Write Metrics"'
_PEAKS = '"Task Executor Metrics"'
_REASON = '"Task End Reason"'

_PARTITION = 'nullif("Task Info"."Partition ID", -1)'

_TASKS = Table(
    name='tasks',
    description=(
        'Every task attempt, in log order. A task still running when the log ends has no finish, reason or metrics. '
        '`effective` marks the first success of each partition of each stage, which `tasks_effective` keeps.'
    ),
    # A stage retry reruns only the partitions it lost, and numbers its tasks' Index from 0 again.
    # So a partition is read from Partition ID, which Spark writes from 3.3 on. An older log has
    # only Index, which names a partition within one stage attempt and no further.
    source=f"""
        WITH ended AS (SELECT DISTINCT "Task Info"."Task ID" AS task_id FROM events WHERE kind = 'SparkListenerTaskEnd'),
        attempts AS (
          SELECT rowid AS position, * FROM events WHERE kind = 'SparkListenerTaskEnd'
          UNION ALL
          SELECT rowid AS position, * FROM events AS started WHERE kind = 'SparkListenerTaskStart'
            AND NOT EXISTS (SELECT 1 FROM ended WHERE ended.task_id = started."Task Info"."Task ID")
        ), keyed AS (
          SELECT *, coalesce("Task End Reason"."Reason" = 'Success', false) AS succeeded,
            coalesce({_PARTITION}, "Task Info"."Index") AS partition_key,
            CASE WHEN {_PARTITION} IS NULL AND "Stage Attempt ID" > 0 THEN "Stage Attempt ID" END AS unnumbered_attempt
          FROM attempts
        )
        SELECT *, succeeded AND row_number() OVER (
            PARTITION BY "Stage ID", partition_key, unnumbered_attempt, succeeded
            ORDER BY nullif("Task Info"."Finish Time", 0), "Task Info"."Task ID"
          ) = 1 AS first_success
        FROM keyed
    """,
    columns=_columns(
        ('stage_id', 'BIGINT', '"Stage ID"', 'The stage the attempt ran in.'),
        ('stage_attempt', 'BIGINT', '"Stage Attempt ID"', 'The stage attempt it ran in, 0 for the first.'),
        ('task_id', 'BIGINT', '"Task Info"."Task ID"', "The attempt's id, unique within the application."),
        (
            'task_index',
            'BIGINT',
            '"Task Info"."Index"',
            'The index of the task within its stage attempt, which is not always the partition it computes.',
        ),
        (
            'partition_id',
            'BIGINT',
            _PARTITION,
            'The RDD partition the attempt computed, the same across its attempts. Null in a log from before Spark 3.3.',
        ),
        ('attempt', 'BIGINT', '"Task Info"."Attempt"', 'How many attempts of the same task index ran before this one, 0 for the first.'),
        ('executor_id', 'VARCHAR', '"Task Info"."Executor ID"', 'The executor the attempt ran on.'),
        ('host', 'VARCHAR', '"Task Info"."Host"', 'The host of that executor.'),
        (
            'locality',
            'VARCHAR',
            '"Task Info"."Locality"',
            'The locality level the scheduler launched it at: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL or ANY.',
        ),
        (
            'speculative',
            'BOOLEAN',
            '"Task Info"."Speculative"',
            'Whether the attempt is a speculative copy of a task whose earlier attempt was still running.',
        ),
        ('launch_epoch_ms', 'BIGINT', '"Task Info"."Launch Time"', 'When the scheduler launched the attempt, by the driver clock.'),
        (
            'finish_epoch_ms',
            'BIGINT',
            _FINISH,
            'When the attempt succeeded, failed or was killed, by the driver clock, after any fetch of its result. Null while it ran.',
        ),
        ('duration_ms', 'BIGINT', _DURATION, 'Wall time from the attempt launching to it finishing.'),
        (
            'getting_result_ms',
            'BIGINT',
            _GETTING_RESULT,
            "Time the driver spent fetching a result too large to return with the task's status, and 0 for one that came back with it.",
        ),
        ('failed', 'BOOLEAN', 'coalesce("Task Info"."Failed", false)', 'Whether the attempt ended failed.'),
        ('killed', 'BOOLEAN', 'coalesce("Task Info"."Killed", false)', 'Whether the attempt ended killed.'),
        (
            'effective',
            'BOOLEAN',
            'first_success',
            "Whether this is its partition's first success across the stage's attempts, the rows tasks_effective keeps.",
        ),
        (
            'end_reason',
            'VARCHAR',
            f'{_REASON}."Reason"',
            'How the attempt ended, as Spark names it: Success, ExceptionFailure, FetchFailed, TaskKilled, ExecutorLostFailure and others.',
        ),
        (
            'end_reason_detail',
            'VARCHAR',
            f'coalesce({_REASON}."Class Name" || coalesce(\': \' || {_REASON}."Description", \'\'), '
            f'{_REASON}."Message", {_REASON}."Kill Reason", {_REASON}."Loss Reason")',
            "What Spark wrote about an attempt that did not succeed: the exception, a message, a kill reason or an executor's loss.",
        ),
        (
            'executor_run_time_ms',
            'BIGINT',
            '"Task Metrics"."Executor Run Time"',
            'Elapsed time the executor spent running this task. This includes time fetching shuffle data.',
        ),
        (
            'executor_cpu_time_ns',
            'BIGINT',
            '"Task Metrics"."Executor CPU Time"',
            'CPU time the executor spent running this task, in nanoseconds. This includes time fetching shuffle data.',
        ),
        (
            'executor_deserialize_time_ms',
            'BIGINT',
            '"Task Metrics"."Executor Deserialize Time"',
            'Elapsed time spent to deserialize this task.',
        ),
        (
            'executor_deserialize_cpu_time_ns',
            'BIGINT',
            '"Task Metrics"."Executor Deserialize CPU Time"',
            'CPU time taken on the executor to deserialize this task, in nanoseconds.',
        ),
        (
            'result_serialization_time_ms',
            'BIGINT',
            '"Task Metrics"."Result Serialization Time"',
            'Elapsed time spent serializing the task result.',
        ),
        (
            'jvm_gc_time_ms',
            'BIGINT',
            '"Task Metrics"."JVM GC Time"',
            'Elapsed time the JVM spent in garbage collection while executing this task.',
        ),
        (
            'result_size_bytes',
            'BIGINT',
            '"Task Metrics"."Result Size"',
            'The number of bytes this task transmitted back to the driver as the TaskResult.',
        ),
        (
            'peak_execution_memory_bytes',
            'BIGINT',
            '"Task Metrics"."Peak Execution Memory"',
            'Peak memory used by internal data structures created during shuffles, aggregations and joins. '
            'For SQL jobs, this only tracks all unsafe operators and ExternalSort.',
        ),
        ('memory_spilled_bytes', 'BIGINT', '"Task Metrics"."Memory Bytes Spilled"', 'The number of in-memory bytes spilled by this task.'),
        ('disk_spilled_bytes', 'BIGINT', '"Task Metrics"."Disk Bytes Spilled"', 'The number of on-disk bytes spilled by this task.'),
        (
            'input_bytes',
            'BIGINT',
            '"Task Metrics"."Input Metrics"."Bytes Read"',
            'Total number of bytes read from a data source or from persisted data.',
        ),
        (
            'input_records',
            'BIGINT',
            '"Task Metrics"."Input Metrics"."Records Read"',
            'Total number of records read from a data source or from persisted data.',
        ),
        (
            'output_bytes',
            'BIGINT',
            '"Task Metrics"."Output Metrics"."Bytes Written"',
            'Total number of bytes written to external storage, such as a distributed filesystem.',
        ),
        (
            'output_records',
            'BIGINT',
            '"Task Metrics"."Output Metrics"."Records Written"',
            'Total number of records written to external storage, such as a distributed filesystem.',
        ),
        (
            'shuffle_remote_read_bytes',
            'BIGINT',
            f'{_SHUFFLE_READ}."Remote Bytes Read"',
            'Number of remote bytes read in shuffle operations.',
        ),
        (
            'shuffle_local_read_bytes',
            'BIGINT',
            f'{_SHUFFLE_READ}."Local Bytes Read"',
            'Number of bytes read in shuffle operations from local disk, as opposed to read from a remote executor.',
        ),
        (
            'shuffle_remote_read_to_disk_bytes',
            'BIGINT',
            f'{_SHUFFLE_READ}."Remote Bytes Read To Disk"',
            'Number of remote bytes read to disk in shuffle operations. Large blocks are fetched to disk rather than read into memory.',
        ),
        (
            'shuffle_fetch_wait_time_ms',
            'BIGINT',
            f'{_SHUFFLE_READ}."Fetch Wait Time"',
            'Time the task spent blocked waiting for shuffle blocks, which counts issuing remote requests and reading host-local blocks.',
        ),
        ('shuffle_read_records', 'BIGINT', f'{_SHUFFLE_READ}."Total Records Read"', 'Number of records read in shuffle operations.'),
        (
            'shuffle_remote_blocks_fetched_count',
            'BIGINT',
            f'{_SHUFFLE_READ}."Remote Blocks Fetched"',
            'Number of remote blocks fetched in shuffle operations.',
        ),
        (
            'shuffle_local_blocks_fetched_count',
            'BIGINT',
            f'{_SHUFFLE_READ}."Local Blocks Fetched"',
            'Number of local blocks fetched in shuffle operations, as opposed to read from a remote executor.',
        ),
        ('shuffle_write_bytes', 'BIGINT', f'{_SHUFFLE_WRITE}."Shuffle Bytes Written"', 'Number of bytes written in shuffle operations.'),
        (
            'shuffle_write_records',
            'BIGINT',
            f'{_SHUFFLE_WRITE}."Shuffle Records Written"',
            'Number of records written in shuffle operations.',
        ),
        (
            'shuffle_write_time_ns',
            'BIGINT',
            f'{_SHUFFLE_WRITE}."Shuffle Write Time"',
            'Time spent blocking on writes to disk or buffer cache, in nanoseconds.',
        ),
        (
            'scheduler_delay_ms',
            'BIGINT',
            _SCHEDULER_DELAY,
            "The part of the attempt's duration spent neither deserializing, running, serializing its result nor fetching it.",
        ),
        (
            'peak_jvm_heap_bytes',
            'BIGINT',
            f'{_PEAKS}."JVMHeapMemory"',
            "The executor's peak JVM heap use polled while the task ran, shared with every task on it then. 0 where no poll saw the task.",
        ),
        (
            'peak_jvm_offheap_bytes',
            'BIGINT',
            f'{_PEAKS}."JVMOffHeapMemory"',
            "The executor's peak JVM non-heap use polled while the task ran, shared with every task on it then. "
            '0 where no poll saw the task.',
        ),
        # Spark writes 0 for every process-tree metric when it measures none, which is no measurement.
        (
            'peak_python_rss_bytes',
            'BIGINT',
            f'CASE WHEN (SELECT measured FROM process_tree) THEN {_PEAKS}."ProcessTreePythonRSSMemory" END',
            "The resident memory of the executor's Python processes at their peak while the task ran. "
            'Null unless spark.executor.processTreeMetrics.enabled was set.',
        ),
        (
            'major_gc_count',
            'BIGINT',
            f'{_PEAKS}."MajorGCCount"',
            "The executor JVM's major collections since it started, as last polled during the task, not those the task caused.",
        ),
        (
            'major_gc_time_ms',
            'BIGINT',
            f'{_PEAKS}."MajorGCTime"',
            "The executor JVM's time in major collections since it started, as last polled during the task.",
        ),
    ),
    order_by='position',
)

_PLAN_EVENTS = "('SparkListenerSQLExecutionStart', 'SparkListenerSQLAdaptiveExecutionUpdate')"

_SQL_EXECUTIONS = Table(
    name='sql_executions',
    description='Every SQL execution: what ran it, when, how it ended, the configs it changed, and its first and last plan text.',
    source=f"""
        WITH starts AS (
          SELECT * FROM events WHERE kind = 'SparkListenerSQLExecutionStart'
          QUALIFY row_number() OVER (PARTITION BY executionId ORDER BY rowid) = 1
        ), finishes AS (
          SELECT executionId, "time" AS ended_at, errorMessage FROM events WHERE kind = 'SparkListenerSQLExecutionEnd'
          QUALIFY row_number() OVER (PARTITION BY executionId ORDER BY rowid DESC) = 1
        ), plan_texts AS (
          SELECT executionId, count(*) AS versions,
            arg_min(physicalPlanDescription, rowid) FILTER (WHERE kind = 'SparkListenerSQLExecutionStart') AS first_text
          FROM events WHERE kind IN {_PLAN_EVENTS} GROUP BY executionId
        ), last_plans AS (
          SELECT executionId, physicalPlanDescription AS last_text, CAST(sparkPlanInfo AS VARCHAR) AS last_info
          FROM events WHERE kind IN {_PLAN_EVENTS}
          QUALIFY row_number() OVER (PARTITION BY executionId ORDER BY rowid DESC) = 1
        ), last_truncation AS (
          SELECT execution_id AS executionId, arg_max(truncated, plan_version) AS last_truncated
          FROM plan_version_rows GROUP BY execution_id
        ), ids AS (
          SELECT executionId FROM starts UNION SELECT executionId FROM finishes UNION SELECT executionId FROM plan_texts
        )
        SELECT ids.executionId AS id, starts.rootExecutionId, starts.description, starts.details, starts.jobTags,
          starts."time" AS started_at, finishes.ended_at, finishes.errorMessage, starts.modifiedConfigs,
          plan_texts.versions, plan_texts.first_text, last_plans.last_text, last_plans.last_info, last_truncation.last_truncated
        FROM ids
        LEFT JOIN starts USING (executionId)
        LEFT JOIN finishes USING (executionId)
        LEFT JOIN plan_texts USING (executionId)
        LEFT JOIN last_plans USING (executionId)
        LEFT JOIN last_truncation USING (executionId)
    """,
    columns=_columns(
        ('execution_id', 'BIGINT', 'id', "The execution's id."),
        # A Spark before 3.4 writes no root, and its listener takes the execution as its own root.
        (
            'root_execution_id',
            'BIGINT',
            'coalesce(rootExecutionId, id)',
            'The execution that ran this one as a nested query, or its own id.',
        ),
        (
            'description',
            'VARCHAR',
            'description',
            'The job description set when it started, cut to spark.sql.event.truncate.length, or else the call site that started it.',
        ),
        ('details', 'VARCHAR', 'details', 'The call stack Spark recorded where the execution started.'),
        ('job_tags', 'VARCHAR[]', 'jobTags', 'The job tags set on the thread that started it.'),
        ('start_epoch_ms', 'BIGINT', 'started_at', 'When the execution started.'),
        ('end_epoch_ms', 'BIGINT', 'ended_at', 'When it ended. Null for one still running when the log ended.'),
        ('error', 'VARCHAR', "nullif(errorMessage, '')", 'The error it ended with. Null for one that succeeded.'),
        (
            'modified_configs',
            'MAP(VARCHAR, VARCHAR)',
            'modifiedConfigs',
            "The session's configs that differ from the application's, less spark.driver.* and spark.executor.*, redacted.",
        ),
        ('plan_version_count', 'BIGINT', 'versions', 'How many plans the log holds for it: 1 from its start, plus one per AQE update.'),
        ('initial_plan_text', 'VARCHAR', 'first_text', 'The formatted plan text logged when it started.'),
        ('final_plan_text', 'VARCHAR', 'last_text', 'The formatted plan text of its last logged plan.'),
        # The last plan event's sparkPlanInfo, which `Tables.last_plans` reads back into a Plan.
        ('final_plan_json', 'VARCHAR', 'last_info', 'The JSON plan tree of its last logged plan.'),
        ('final_plan_truncated', 'BOOLEAN', 'last_truncated', 'Whether Spark cut any part of its last plan.'),
    ),
    order_by='execution_id',
)

_PLAN_VERSIONS = Table(
    name='plan_versions',
    description=(
        "Every version of every SQL execution's plan, as the event that logged it: version 0 from its start, "
        'and one more for each AQE update, in log order. `Tables.plan` reads a version back into a Plan.'
    ),
    source=f"""
        WITH plan_events AS (
          SELECT executionId AS execution_id, row_number() OVER (PARTITION BY executionId ORDER BY rowid) - 1 AS plan_version,
            CAST(sparkPlanInfo AS VARCHAR) AS plan_info, physicalPlanDescription AS plan_text
          FROM events WHERE kind IN {_PLAN_EVENTS} AND executionId IS NOT NULL
        )
        SELECT plan_events.*, plan_version_rows.is_final, plan_version_rows.truncated
        FROM plan_events LEFT JOIN plan_version_rows USING (execution_id, plan_version)
    """,
    columns=_columns(
        ('execution_id', 'BIGINT', 'execution_id', 'The SQL execution the plan is for.'),
        ('plan_version', 'BIGINT', 'plan_version', 'The version: 0 for the plan logged at the start, n for the nth AQE update.'),
        (
            'is_final',
            'BOOLEAN',
            'is_final',
            'Whether AQE marked the plan final. A plan run without AQE is final as written.',
        ),
        ('truncated', 'BOOLEAN', 'truncated', 'Whether Spark cut any part of the plan.'),
        ('plan_json', 'VARCHAR', 'plan_info', 'The JSON plan tree the event carried.'),
        ('plan_text', 'VARCHAR', 'plan_text', 'The formatted plan text the event carried.'),
    ),
    order_by='execution_id, plan_version',
)

_PLAN_NODES = Table(
    name='plan_nodes',
    description=(
        "Every node of every plan version, as sparkxray's plan model reads it. A subquery's nodes follow the plan's, "
        "numbered on from its last node, and its root's parent is the node hosting it."
    ),
    source='SELECT * FROM plan_node_rows',
    columns=_columns(
        ('execution_id', 'BIGINT', 'execution_id', 'The SQL execution the plan is for.'),
        ('plan_version', 'BIGINT', 'plan_version', 'The plan version the node is in.'),
        ('is_final', 'BOOLEAN', 'is_final', 'Whether that plan version is final.'),
        ('node_id', 'BIGINT', 'node_id', "The node's number in the plan, in pre-order from 0 at the root."),
        ('parent_id', 'BIGINT', 'parent_id', "The node above it. Null at the root; a subquery's root has the node hosting it."),
        (
            'operator_id',
            'BIGINT',
            'operator_id',
            'The operator id formatted plan text gives the node, as in `(3) Exchange`. Null where the text shows none.',
        ),
        ('operator', 'VARCHAR', 'operator', "The operator's name, the same whichever form of plan it was read from."),
        ('label', 'VARCHAR', 'label', 'The line Spark printed for the node, ids included.'),
        (
            'normalized_label',
            'VARCHAR',
            'normalized_label',
            'The label with every id Spark generates rewritten, so two builds of one transform compare equal.',
        ),
        ('codegen_stage_id', 'BIGINT', 'codegen_stage_id', 'The whole-stage codegen pipeline the node runs in. Null outside one.'),
        (
            'enclosing_sort_id',
            'BIGINT',
            'enclosing_sort_id',
            "The nearest Sort above the node in the same task, whose `spill size` counts this node's spills too. Null where none.",
        ),
        (
            'subtree_hash',
            'VARCHAR',
            'subtree_hash',
            'A hash of the normalized label and details of this node and everything below it.',
        ),
        (
            'details',
            'MAP(VARCHAR, VARCHAR)',
            'details',
            'The fields formatted plan text lists under the node, such as Join type or Location.',
        ),
        (
            'runtime_size_bytes',
            'BIGINT',
            'runtime_size_bytes',
            'The size AQE measured on a finished query stage, rounded as the text prints it. Null elsewhere.',
        ),
        ('runtime_row_count', 'BIGINT', 'runtime_row_count', 'The rows AQE measured on a finished query stage. Null elsewhere.'),
    ),
    order_by='execution_id, plan_version, node_id',
)

_METRIC_UNITS = "CASE metric_type WHEN 'size' THEN 'bytes' WHEN 'timing' THEN 'ms' WHEN 'nsTiming' THEN 'ms' WHEN 'sum' THEN 'count' END"


def _statistic(column: str, *, total: bool = False) -> str:
    """A statistic in its unit: an nsTiming metric in milliseconds, and an average as Spark shows it, a tenth of what tasks report."""
    average = 'NULL' if total else f'{column} / 10'
    return f"CASE metric_type WHEN 'nsTiming' THEN {column} // 1000000 WHEN 'average' THEN {average} ELSE {column} END"


_PLAN_NODE_METRICS = Table(
    name='plan_node_metrics',
    description=(
        "Every metric a plan node declares, aggregated as the History Server's SQL tab aggregates it. "
        'A metric AQE declared for a subquery plan it never logged has no plan version or node.'
    ),
    source="""
        SELECT refs.*, aggregates.value_count, aggregates.total, aggregates.minimum, aggregates.median, aggregates.maximum,
          aggregates.max_stage_id AS maximum_stage_id, aggregates.max_task_id AS maximum_task_id
        FROM plan_metric_refs AS refs LEFT JOIN metric_aggregates AS aggregates USING (execution_id, accumulator_id)
    """,
    columns=_columns(
        ('execution_id', 'BIGINT', 'execution_id', 'The SQL execution the metric reports for.'),
        ('plan_version', 'BIGINT', 'plan_version', 'The plan version that declares it. Null for an AQE subquery plan never logged.'),
        ('node_id', 'BIGINT', 'node_id', 'The node that declares it, numbered as in plan_nodes. Null where plan_version is.'),
        ('metric_name', 'VARCHAR', 'metric_name', 'The name the Spark UI shows. metrics show describes each.'),
        ('metric_type', 'VARCHAR', 'metric_type', "Spark's kind for it: size, timing, nsTiming, sum or average."),
        ('accumulator_id', 'BIGINT', 'accumulator_id', 'The accumulator tasks and the driver report its values to.'),
        ('value_count', 'BIGINT', 'value_count', 'How many values were reported to it.'),
        ('total', 'DOUBLE', _statistic('total', total=True), 'The sum of every reported value, in its unit. Null for an average.'),
        ('min', 'DOUBLE', _statistic('minimum'), 'The smallest reported value, in its unit.'),
        ('median', 'DOUBLE', _statistic('median'), 'The median reported value, in its unit.'),
        ('max', 'DOUBLE', _statistic('maximum'), 'The largest reported value, in its unit.'),
        ('max_stage_id', 'BIGINT', 'maximum_stage_id', 'The stage of the task that reported the largest value. Null where the driver did.'),
        ('max_task_id', 'BIGINT', 'maximum_task_id', 'The task that reported the largest value. Null where the driver did.'),
        (
            'unit',
            'VARCHAR',
            _METRIC_UNITS,
            'The unit of total, min, median and max: bytes, ms or count. Null for an average, which has none.',
        ),
    ),
    order_by='execution_id, plan_version NULLS LAST, node_id NULLS LAST, accumulator_id',
)

_NODE_STAGES = Table(
    name='node_stages',
    description="The stages each node of an execution's last plan ran in, joined through the accumulators its metrics report to.",
    source="""
        WITH last_versions AS (
          SELECT execution_id, max(plan_version) AS plan_version FROM plan_version_rows GROUP BY execution_id
        ), stage_accumulators AS (
          SELECT stage_id, accumulable."ID" AS accumulator_id FROM (
            SELECT "Stage ID" AS stage_id, unnest("Task Info"."Accumulables") AS accumulable FROM events WHERE kind = 'SparkListenerTaskEnd'
          )
          UNION
          SELECT stage_id, accumulable."ID" FROM (
            SELECT "Stage Info"."Stage ID" AS stage_id, unnest("Stage Info"."Accumulables") AS accumulable FROM events
            WHERE kind = 'SparkListenerStageCompleted'
          )
        )
        SELECT DISTINCT refs.execution_id, refs.node_id, stage_accumulators.stage_id
        FROM plan_metric_refs AS refs
        JOIN last_versions USING (execution_id, plan_version)
        JOIN stage_accumulators USING (accumulator_id)
        JOIN execution_stages
          ON execution_stages.execution_id = refs.execution_id AND execution_stages.stage_id = stage_accumulators.stage_id
    """,
    columns=_columns(
        ('execution_id', 'BIGINT', 'execution_id', 'The SQL execution.'),
        ('node_id', 'BIGINT', 'node_id', "The node's number in the execution's last plan."),
        ('stage_id', 'BIGINT', 'stage_id', "A stage whose tasks reported to one of the node's metrics."),
    ),
    order_by='execution_id, node_id, stage_id',
)

TABLES: tuple[Table, ...] = (
    _APPLICATION,
    _ENVIRONMENT,
    _EXECUTORS,
    _JOBS,
    _STAGES,
    _TASKS,
    _SQL_EXECUTIONS,
    _PLAN_VERSIONS,
    _PLAN_NODES,
    _PLAN_NODE_METRICS,
    _NODE_STAGES,
)

# Each partition of each stage once: its first success across the stage's attempts. Retries and
# speculative copies stay in `tasks`, where they show the work they wasted.
TASKS_EFFECTIVE = 'CREATE VIEW tasks_effective AS SELECT * FROM tasks WHERE effective'
TASKS_EFFECTIVE_DESCRIPTION = (
    'A view of `tasks` where `effective` is true: each partition of each stage once, its first success across the stage attempts.'
)

# When the run started and ended. A run still going when its log ended ends at the last thing the log saw finish.
RUN_SPAN_SQL = """
SELECT start_epoch_ms, coalesce(end_epoch_ms, greatest(
  (SELECT max(finish_epoch_ms) FROM tasks), (SELECT max(complete_epoch_ms) FROM stages),
  (SELECT max(end_epoch_ms) FROM jobs), (SELECT max(end_epoch_ms) FROM sql_executions))) AS end_epoch_ms
FROM application
"""

TASK_CPUS = 'spark.task.cpus'

# A stage's GC time over its run time, aggregated over its effective tasks. `stage.gc-pressure` grades it, and stage summaries carry it.
GC_SHARE = 'sum(jvm_gc_time_ms) / nullif(sum(executor_run_time_ms), 0)'

# Each executor's lifetime and the task time it ran, as common table expressions: `lifetimes`,
# `task_totals`, and `task_cpus`, the cores each task holds. An executor never removed lives to the
# run's end. Task time counts every attempt, failed and speculative ones included.
EXECUTOR_TIME_CTES = f"""run AS ({RUN_SPAN_SQL}), task_cpus AS (
  SELECT coalesce(max(try_cast(value AS BIGINT)) FILTER (WHERE key = '{TASK_CPUS}'), 1) AS cpus
  FROM environment WHERE section = 'spark'
), task_totals AS (
  SELECT executor_id, count(*) AS task_count, count(*) FILTER (WHERE failed) AS failed_task_count,
    sum(duration_ms)::BIGINT AS task_time_ms
  FROM tasks GROUP BY executor_id
), lifetimes AS (
  SELECT executors.*, coalesce(executors.remove_epoch_ms, run.end_epoch_ms) - executors.add_epoch_ms AS lifetime_ms FROM executors, run
)"""

# Each node's `spill size` in every plan version, and the part that is its own, as `own_spills`.
# A Sort's figure counts the spills of the nodes it encloses in its task, so theirs come off it.
# Every other operator records only its own buffer's spills (`SortExec.scala`, `SortMergeJoinExec.scala`).
OWN_SPILLS_CTE = """spill_sizes AS (
  SELECT execution_id, plan_version, node_id, total AS spill_bytes
  FROM plan_node_metrics WHERE metric_name = 'spill size'
  QUALIFY row_number() OVER (PARTITION BY execution_id, plan_version, node_id ORDER BY accumulator_id) = 1
), own_spills AS (
  SELECT spill_sizes.*, greatest(spill_sizes.spill_bytes - coalesce(enclosed.spill_bytes, 0), 0) AS own_spill_bytes
  FROM spill_sizes LEFT JOIN (
    SELECT plan_nodes.execution_id, plan_nodes.plan_version, plan_nodes.enclosing_sort_id AS node_id,
      sum(spill_sizes.spill_bytes) AS spill_bytes
    FROM plan_nodes JOIN spill_sizes USING (execution_id, plan_version, node_id)
    WHERE plan_nodes.enclosing_sort_id IS NOT NULL
    GROUP BY ALL
  ) AS enclosed USING (execution_id, plan_version, node_id)
)"""

# A Glue DPU is 4 vCPUs and 16 GB.
GLUE_DPU_CORES = 4

# The core time the run held and the part its tasks used. The executors hold it, or the driver alone
# where there are none, as in local mode, where it runs the tasks.
CORE_TIME_CTES = f"""{EXECUTOR_TIME_CTES}, workers AS (
  SELECT lifetimes.is_driver, lifetimes.lifetime_ms * lifetimes.core_count AS core_time_ms,
    least(coalesce(task_totals.task_time_ms, 0) * task_cpus.cpus, lifetimes.lifetime_ms * lifetimes.core_count) AS task_core_time_ms
  FROM lifetimes LEFT JOIN task_totals USING (executor_id), task_cpus
  WHERE (NOT lifetimes.is_driver OR NOT EXISTS (SELECT 1 FROM executors WHERE NOT is_driver))
    AND lifetimes.core_count > 0 AND lifetimes.lifetime_ms > 0
), core_totals AS (
  SELECT count(*) AS executor_count, bool_or(is_driver) AS driver_ran_tasks,
    sum(core_time_ms) AS core_time_ms, sum(task_core_time_ms) AS task_core_time_ms
  FROM workers
)"""

# The executors the tasks ran on, as `pool`: those with cores, or the driver alone where there are
# none, as in local mode. `executor_count` is the most alive at once, one added at the instant
# another is removed counting both, as Sparklens's `AppContext.scala` counts them. Their cores are
# the resource profile's most of them ran under, the lowest id on a tie. It follows
# `EXECUTOR_TIME_CTES`, whose `run` and `lifetimes` it reads.
EXECUTOR_POOL_CTE = """pool_members AS (
  SELECT * FROM lifetimes
  WHERE core_count > 0 AND add_epoch_ms IS NOT NULL
    AND (NOT is_driver OR NOT EXISTS (SELECT 1 FROM lifetimes AS others WHERE NOT others.is_driver AND others.core_count > 0))
), pool_profiles AS (
  SELECT coalesce(resource_profile_id, 0) AS resource_profile_id, core_count, count(*) AS member_count FROM pool_members GROUP BY ALL
), pool AS (
  SELECT coalesce((
      SELECT max(alive_count) FROM (
        SELECT count(*) FILTER (WHERE other.add_epoch_ms <= member.add_epoch_ms
          AND coalesce(other.remove_epoch_ms, run.end_epoch_ms) >= member.add_epoch_ms) AS alive_count
        FROM pool_members AS member, pool_members AS other, run
        GROUP BY member.executor_id
      )
    ), 0) AS executor_count,
    (SELECT core_count FROM pool_profiles ORDER BY member_count DESC, resource_profile_id, core_count DESC LIMIT 1) AS executor_core_count,
    (SELECT resource_profile_id FROM pool_profiles ORDER BY member_count DESC, resource_profile_id, core_count DESC LIMIT 1)
      AS resource_profile_id,
    (SELECT count(DISTINCT resource_profile_id) FROM pool_profiles) AS resource_profile_count
)"""

# Each stage's and SQL execution's wall time, and its share of the run's. A stage runs from its
# first attempt's submission to its last completion, or to its last task's finish where it never
# completed. An execution still running when the log ended runs to the run's end.
WALL_SHARES_SQL = f"""
WITH run AS ({RUN_SPAN_SQL}), task_ends AS (
  SELECT stage_id, max(finish_epoch_ms) AS finish_epoch_ms FROM tasks GROUP BY stage_id
), walls AS (
  SELECT 'stage' AS level, stage_id AS id,
    coalesce(max(complete_epoch_ms), max(task_ends.finish_epoch_ms)) - min(submit_epoch_ms) AS wall_ms
  FROM stages LEFT JOIN task_ends USING (stage_id) GROUP BY stage_id
  UNION ALL
  SELECT 'sql_execution', execution_id, coalesce(end_epoch_ms, (SELECT end_epoch_ms FROM run)) - start_epoch_ms FROM sql_executions
)
SELECT walls.level, walls.id, walls.wall_ms,
  CASE WHEN run.end_epoch_ms > run.start_epoch_ms THEN round(walls.wall_ms / (run.end_epoch_ms - run.start_epoch_ms), 4) END AS share
FROM walls, run
WHERE walls.wall_ms IS NOT NULL
"""

# An instant's suffix comes before a duration's, because `_epoch_ms` also ends in `_ms`.
_SUFFIX_UNITS = (
    ('_epoch_ms', 'epoch_ms'),
    ('_ms', 'ms'),
    ('_ns', 'ns'),
    ('_bytes', 'bytes'),
    ('_records', 'records'),
    ('_rows', 'rows'),
    ('_count', 'count'),
    ('_share', 'fraction'),
    ('_ratio', 'x'),
    ('_seconds', 's'),
    ('_hours', 'h'),
    ('_percent', 'percent'),
)


def column_unit(name: str) -> str | None:
    """The unit a column's name ends in: `epoch_ms` for an instant, `ms` for a duration, and so on. None where it names none."""
    return next((unit for suffix, unit in _SUFFIX_UNITS if name.endswith(suffix)), None)
