"""Every table the build writes: what it holds, its columns and their types, and the SQL that computes each column.

A column's name carries its unit: `_ms` and `_ns` for times, `_bytes` and `_mib` for sizes. Times
without one of those suffixes are epoch milliseconds. Each table's `source` reads the build's
working tables, which `build.py` creates:

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


@dc.dataclass(frozen=True, slots=True)
class Table:
    name: str
    description: str
    source: str
    columns: tuple[Column, ...]
    order_by: str


def _columns(*specs: tuple[str, str, str]) -> tuple[Column, ...]:
    return tuple(Column(name, type_, expression) for name, type_, expression in specs)


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
        ('app_id', 'VARCHAR', 'coalesce(logged_app_id, app_id)'),
        ('attempt_id', 'VARCHAR', 'attempt_id'),
        ('app_name', 'VARCHAR', 'app_name'),
        ('user', 'VARCHAR', 'app_user'),
        ('spark_version', 'VARCHAR', 'coalesce(logged_spark_version, spark_version)'),
        ('start_ms', 'BIGINT', 'started_at'),
        ('end_ms', 'BIGINT', 'ended_at'),
        ('platform', 'VARCHAR', 'platform'),
        ('log_source', 'VARCHAR', 'source'),
        ('compacted', 'BOOLEAN', 'compacted'),
        ('in_progress', 'BOOLEAN', 'in_progress'),
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
        ('section', 'VARCHAR', 'section'),
        ('key', 'VARCHAR', 'entry."key"'),
        ('value', 'VARCHAR', 'entry."value"'),
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
        ), ids AS (
          SELECT executor_id FROM added UNION SELECT executor_id FROM block_managers UNION SELECT executor_id FROM removed
        )
        SELECT ids.executor_id, added.added_at, added.info, removed.removed_at, removed.removed_reason,
          block_managers.block_manager_host, block_managers.maximum_memory, block_managers.registered_at, profiles.memory_mib
        FROM ids
        LEFT JOIN added USING (executor_id)
        LEFT JOIN removed USING (executor_id)
        LEFT JOIN block_managers USING (executor_id)
        LEFT JOIN profiles ON profiles.profile_id = added.info."Resource Profile Id"
    """,
    columns=_columns(
        ('executor_id', 'VARCHAR', 'executor_id'),
        ('host', 'VARCHAR', 'coalesce(info."Host", block_manager_host)'),
        ('total_cores', 'BIGINT', 'info."Total Cores"'),
        ('resource_profile_id', 'BIGINT', 'info."Resource Profile Id"'),
        # The driver's memory is spark.driver.memory, which no resource profile holds.
        ('executor_memory_mib', 'BIGINT', "CASE WHEN executor_id = 'driver' THEN NULL ELSE memory_mib END"),
        ('max_storage_memory_bytes', 'BIGINT', 'maximum_memory'),
        ('add_ms', 'BIGINT', 'coalesce(added_at, registered_at)'),
        ('remove_ms', 'BIGINT', 'removed_at'),
        ('remove_reason', 'VARCHAR', 'removed_reason'),
        ('is_driver', 'BOOLEAN', "executor_id = 'driver'"),
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
        ('job_id', 'BIGINT', 'job_id'),
        ('sql_execution_id', 'BIGINT', 'try_cast(properties."spark.sql.execution.id" AS BIGINT)'),
        ('job_group', 'VARCHAR', 'properties."spark.jobGroup.id"'),
        # Spark joins a job's tags with commas. A Spark before 3.5 writes none, which reads as null.
        (
            'job_tags',
            'VARCHAR[]',
            'CASE WHEN properties."spark.job.tags" IS NULL THEN NULL '
            'WHEN properties."spark.job.tags" = \'\' THEN []::VARCHAR[] '
            'ELSE string_split(properties."spark.job.tags", \',\') END',
        ),
        ('description', 'VARCHAR', 'properties."spark.job.description"'),
        ('submit_ms', 'BIGINT', 'submitted_at'),
        ('end_ms', 'BIGINT', 'completed_at'),
        ('result', 'VARCHAR', 'job_result."Result"'),
        ('failure_reason', 'VARCHAR', 'job_result."Exception"."Message"'),
        ('stage_ids', 'BIGINT[]', 'job_stage_ids'),
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
        ('stage_id', 'BIGINT', 'stage_id'),
        ('attempt', 'BIGINT', 'attempt'),
        ('name', 'VARCHAR', 'info."Stage Name"'),
        ('num_tasks', 'BIGINT', 'info."Number of Tasks"'),
        ('parent_ids', 'BIGINT[]', 'info."Parent IDs"'),
        ('resource_profile_id', 'BIGINT', 'info."Resource Profile Id"'),
        ('submit_ms', 'BIGINT', 'info."Submission Time"'),
        ('complete_ms', 'BIGINT', 'info."Completion Time"'),
        ('failure_reason', 'VARCHAR', 'info."Failure Reason"'),
        ('is_highest_attempt', 'BOOLEAN', 'attempt = highest_attempt'),
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
        ('stage_id', 'BIGINT', '"Stage ID"'),
        ('stage_attempt', 'BIGINT', '"Stage Attempt ID"'),
        ('task_id', 'BIGINT', '"Task Info"."Task ID"'),
        ('task_index', 'BIGINT', '"Task Info"."Index"'),
        ('partition_id', 'BIGINT', _PARTITION),
        ('attempt', 'BIGINT', '"Task Info"."Attempt"'),
        ('executor_id', 'VARCHAR', '"Task Info"."Executor ID"'),
        ('host', 'VARCHAR', '"Task Info"."Host"'),
        ('locality', 'VARCHAR', '"Task Info"."Locality"'),
        ('speculative', 'BOOLEAN', '"Task Info"."Speculative"'),
        ('launch_ms', 'BIGINT', '"Task Info"."Launch Time"'),
        ('finish_ms', 'BIGINT', _FINISH),
        ('duration_ms', 'BIGINT', _DURATION),
        ('getting_result_ms', 'BIGINT', _GETTING_RESULT),
        ('failed', 'BOOLEAN', 'coalesce("Task Info"."Failed", false)'),
        ('killed', 'BOOLEAN', 'coalesce("Task Info"."Killed", false)'),
        ('effective', 'BOOLEAN', 'first_success'),
        ('end_reason', 'VARCHAR', f'{_REASON}."Reason"'),
        (
            'end_reason_detail',
            'VARCHAR',
            f'coalesce({_REASON}."Class Name" || coalesce(\': \' || {_REASON}."Description", \'\'), '
            f'{_REASON}."Message", {_REASON}."Kill Reason", {_REASON}."Loss Reason")',
        ),
        ('executor_run_time_ms', 'BIGINT', '"Task Metrics"."Executor Run Time"'),
        ('executor_cpu_time_ns', 'BIGINT', '"Task Metrics"."Executor CPU Time"'),
        ('executor_deserialize_time_ms', 'BIGINT', '"Task Metrics"."Executor Deserialize Time"'),
        ('executor_deserialize_cpu_time_ns', 'BIGINT', '"Task Metrics"."Executor Deserialize CPU Time"'),
        ('result_serialization_time_ms', 'BIGINT', '"Task Metrics"."Result Serialization Time"'),
        ('jvm_gc_time_ms', 'BIGINT', '"Task Metrics"."JVM GC Time"'),
        ('result_size_bytes', 'BIGINT', '"Task Metrics"."Result Size"'),
        ('peak_execution_memory_bytes', 'BIGINT', '"Task Metrics"."Peak Execution Memory"'),
        ('memory_bytes_spilled', 'BIGINT', '"Task Metrics"."Memory Bytes Spilled"'),
        ('disk_bytes_spilled', 'BIGINT', '"Task Metrics"."Disk Bytes Spilled"'),
        ('input_bytes', 'BIGINT', '"Task Metrics"."Input Metrics"."Bytes Read"'),
        ('input_records', 'BIGINT', '"Task Metrics"."Input Metrics"."Records Read"'),
        ('output_bytes', 'BIGINT', '"Task Metrics"."Output Metrics"."Bytes Written"'),
        ('output_records', 'BIGINT', '"Task Metrics"."Output Metrics"."Records Written"'),
        ('shuffle_remote_bytes_read', 'BIGINT', f'{_SHUFFLE_READ}."Remote Bytes Read"'),
        ('shuffle_local_bytes_read', 'BIGINT', f'{_SHUFFLE_READ}."Local Bytes Read"'),
        ('shuffle_remote_bytes_read_to_disk', 'BIGINT', f'{_SHUFFLE_READ}."Remote Bytes Read To Disk"'),
        ('shuffle_fetch_wait_time_ms', 'BIGINT', f'{_SHUFFLE_READ}."Fetch Wait Time"'),
        ('shuffle_records_read', 'BIGINT', f'{_SHUFFLE_READ}."Total Records Read"'),
        ('shuffle_remote_blocks_fetched', 'BIGINT', f'{_SHUFFLE_READ}."Remote Blocks Fetched"'),
        ('shuffle_local_blocks_fetched', 'BIGINT', f'{_SHUFFLE_READ}."Local Blocks Fetched"'),
        ('shuffle_bytes_written', 'BIGINT', f'{_SHUFFLE_WRITE}."Shuffle Bytes Written"'),
        ('shuffle_records_written', 'BIGINT', f'{_SHUFFLE_WRITE}."Shuffle Records Written"'),
        ('shuffle_write_time_ns', 'BIGINT', f'{_SHUFFLE_WRITE}."Shuffle Write Time"'),
        ('scheduler_delay_ms', 'BIGINT', _SCHEDULER_DELAY),
        ('peak_jvm_heap_bytes', 'BIGINT', f'{_PEAKS}."JVMHeapMemory"'),
        ('peak_jvm_offheap_bytes', 'BIGINT', f'{_PEAKS}."JVMOffHeapMemory"'),
        # Spark writes 0 for every process-tree metric when it measures none, which is no measurement.
        (
            'peak_python_rss_bytes',
            'BIGINT',
            f'CASE WHEN (SELECT measured FROM process_tree) THEN {_PEAKS}."ProcessTreePythonRSSMemory" END',
        ),
        ('major_gc_count', 'BIGINT', f'{_PEAKS}."MajorGCCount"'),
        ('major_gc_time_ms', 'BIGINT', f'{_PEAKS}."MajorGCTime"'),
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
        ('execution_id', 'BIGINT', 'id'),
        # A Spark before 3.4 writes no root, and its listener takes the execution as its own root.
        ('root_execution_id', 'BIGINT', 'coalesce(rootExecutionId, id)'),
        ('description', 'VARCHAR', 'description'),
        ('details', 'VARCHAR', 'details'),
        ('job_tags', 'VARCHAR[]', 'jobTags'),
        ('start_ms', 'BIGINT', 'started_at'),
        ('end_ms', 'BIGINT', 'ended_at'),
        ('error', 'VARCHAR', "nullif(errorMessage, '')"),
        ('modified_configs', 'MAP(VARCHAR, VARCHAR)', 'modifiedConfigs'),
        ('plan_versions', 'BIGINT', 'versions'),
        ('initial_plan_text', 'VARCHAR', 'first_text'),
        ('final_plan_text', 'VARCHAR', 'last_text'),
        # The last plan event's sparkPlanInfo, which `Tables.last_plans` reads back into a Plan.
        ('final_plan_json', 'VARCHAR', 'last_info'),
        ('final_plan_truncated', 'BOOLEAN', 'last_truncated'),
    ),
    order_by='execution_id',
)

_PLAN_NODES = Table(
    name='plan_nodes',
    description=(
        "Every node of every plan version, as sparkxray's plan model reads it. A subquery's nodes follow the plan's, "
        "numbered on from its last node, and its root's parent is the node hosting it."
    ),
    source='SELECT * FROM plan_node_rows',
    columns=_columns(
        ('execution_id', 'BIGINT', 'execution_id'),
        ('plan_version', 'BIGINT', 'plan_version'),
        ('is_final', 'BOOLEAN', 'is_final'),
        ('node_id', 'BIGINT', 'node_id'),
        ('parent_id', 'BIGINT', 'parent_id'),
        ('formatted_number', 'BIGINT', 'formatted_number'),
        ('operator', 'VARCHAR', 'operator'),
        ('label', 'VARCHAR', 'label'),
        ('normalized_label', 'VARCHAR', 'normalized_label'),
        ('codegen_stage', 'BIGINT', 'codegen_stage'),
        ('subtree_hash', 'VARCHAR', 'subtree_hash'),
        ('details', 'MAP(VARCHAR, VARCHAR)', 'details'),
        ('runtime_size_bytes', 'BIGINT', 'runtime_size_bytes'),
        ('runtime_row_count', 'BIGINT', 'runtime_row_count'),
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
        ('execution_id', 'BIGINT', 'execution_id'),
        ('plan_version', 'BIGINT', 'plan_version'),
        ('node_id', 'BIGINT', 'node_id'),
        ('metric_name', 'VARCHAR', 'metric_name'),
        ('metric_type', 'VARCHAR', 'metric_type'),
        ('accumulator_id', 'BIGINT', 'accumulator_id'),
        ('value_count', 'BIGINT', 'value_count'),
        ('total', 'DOUBLE', _statistic('total', total=True)),
        ('min', 'DOUBLE', _statistic('minimum')),
        ('median', 'DOUBLE', _statistic('median')),
        ('max', 'DOUBLE', _statistic('maximum')),
        ('max_stage_id', 'BIGINT', 'maximum_stage_id'),
        ('max_task_id', 'BIGINT', 'maximum_task_id'),
        ('unit', 'VARCHAR', _METRIC_UNITS),
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
        ('execution_id', 'BIGINT', 'execution_id'),
        ('node_id', 'BIGINT', 'node_id'),
        ('stage_id', 'BIGINT', 'stage_id'),
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
    _PLAN_NODES,
    _PLAN_NODE_METRICS,
    _NODE_STAGES,
)

# Each partition of each stage once: its first success across the stage's attempts. Retries and
# speculative copies stay in `tasks`, where they show the work they wasted.
TASKS_EFFECTIVE = 'CREATE VIEW tasks_effective AS SELECT * FROM tasks WHERE effective'
