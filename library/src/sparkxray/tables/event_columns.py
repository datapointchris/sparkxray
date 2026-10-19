"""The columns DuckDB's `read_json` reads each event line into.

Every `SparkListener*` event has its own JSON shape. Without this map DuckDB unifies them all into
one struct. With it, DuckDB parses in parallel and drops every key nobody reads. A key one event
lacks is null in that event's row, and a struct reads only the fields it names.
"""

from __future__ import annotations

_ACCUMULABLE = 'STRUCT("ID" BIGINT, "Update" VARCHAR)'

_TASK_INFO = (
    'STRUCT("Task ID" BIGINT, "Index" BIGINT, "Attempt" BIGINT, "Launch Time" BIGINT, "Executor ID" VARCHAR, '
    '"Host" VARCHAR, "Locality" VARCHAR, "Speculative" BOOLEAN, "Getting Result Time" BIGINT, "Finish Time" BIGINT, '
    f'"Failed" BOOLEAN, "Killed" BOOLEAN, "Accumulables" {_ACCUMULABLE}[])'
)

_TASK_METRICS = (
    'STRUCT("Executor Deserialize Time" BIGINT, "Executor Deserialize CPU Time" BIGINT, "Executor Run Time" BIGINT, '
    '"Executor CPU Time" BIGINT, "Peak Execution Memory" BIGINT, "Result Size" BIGINT, "JVM GC Time" BIGINT, '
    '"Result Serialization Time" BIGINT, "Memory Bytes Spilled" BIGINT, "Disk Bytes Spilled" BIGINT, '
    '"Shuffle Read Metrics" STRUCT("Remote Blocks Fetched" BIGINT, "Local Blocks Fetched" BIGINT, '
    '"Fetch Wait Time" BIGINT, "Remote Bytes Read" BIGINT, "Remote Bytes Read To Disk" BIGINT, '
    '"Local Bytes Read" BIGINT, "Total Records Read" BIGINT), '
    '"Shuffle Write Metrics" STRUCT("Shuffle Bytes Written" BIGINT, "Shuffle Write Time" BIGINT, "Shuffle Records Written" BIGINT), '
    '"Input Metrics" STRUCT("Bytes Read" BIGINT, "Records Read" BIGINT), '
    '"Output Metrics" STRUCT("Bytes Written" BIGINT, "Records Written" BIGINT))'
)

# The executor's peaks while the task ran. The process-tree values are zero unless
# spark.executor.processTreeMetrics.enabled was set.
_TASK_EXECUTOR_METRICS = (
    'STRUCT("JVMHeapMemory" BIGINT, "JVMOffHeapMemory" BIGINT, "ProcessTreeJVMRSSMemory" BIGINT, '
    '"ProcessTreePythonRSSMemory" BIGINT, "MajorGCCount" BIGINT, "MajorGCTime" BIGINT)'
)

# One struct for every end reason JsonProtocol writes. Each reason fills its own fields.
_TASK_END_REASON = (
    'STRUCT("Reason" VARCHAR, "Class Name" VARCHAR, "Description" VARCHAR, "Message" VARCHAR, "Kill Reason" VARCHAR, "Loss Reason" VARCHAR)'
)

_STAGE_INFO = (
    'STRUCT("Stage ID" BIGINT, "Stage Attempt ID" BIGINT, "Stage Name" VARCHAR, "Number of Tasks" BIGINT, '
    '"Parent IDs" BIGINT[], "Submission Time" BIGINT, "Completion Time" BIGINT, "Failure Reason" VARCHAR, '
    '"Accumulables" STRUCT("ID" BIGINT)[], "Resource Profile Id" BIGINT)'
)

_JOB_PROPERTIES = (
    'STRUCT("spark.sql.execution.id" VARCHAR, "spark.jobGroup.id" VARCHAR, "spark.job.description" VARCHAR, "spark.job.tags" VARCHAR)'
)

_PROPERTIES = 'MAP(VARCHAR, VARCHAR)'

EVENT_COLUMNS: dict[str, str] = {
    'Event': 'VARCHAR',
    # tasks
    'Stage ID': 'BIGINT',
    'Stage Attempt ID': 'BIGINT',
    'Task Info': _TASK_INFO,
    'Task Metrics': _TASK_METRICS,
    'Task Executor Metrics': _TASK_EXECUTOR_METRICS,
    'Task End Reason': _TASK_END_REASON,
    # stages and jobs
    'Stage Info': _STAGE_INFO,
    'Job ID': 'BIGINT',
    'Stage IDs': 'BIGINT[]',
    'Properties': _JOB_PROPERTIES,
    'Submission Time': 'BIGINT',
    'Completion Time': 'BIGINT',
    'Job Result': 'STRUCT("Result" VARCHAR, "Exception" STRUCT("Message" VARCHAR))',
    # SQL executions
    'executionId': 'BIGINT',
    'rootExecutionId': 'BIGINT',
    'description': 'VARCHAR',
    'details': 'VARCHAR',
    'physicalPlanDescription': 'VARCHAR',
    'sparkPlanInfo': 'JSON',
    'modifiedConfigs': _PROPERTIES,
    'jobTags': 'VARCHAR[]',
    'time': 'BIGINT',
    'errorMessage': 'VARCHAR',
    'accumUpdates': 'BIGINT[][]',
    'sqlPlanMetrics': 'STRUCT("name" VARCHAR, "accumulatorId" BIGINT, "metricType" VARCHAR)[]',
    # executors
    'Executor ID': 'VARCHAR',
    'Executor Info': 'STRUCT("Host" VARCHAR, "Total Cores" BIGINT, "Resource Profile Id" BIGINT)',
    'Removed Reason': 'VARCHAR',
    'Timestamp': 'BIGINT',
    'Block Manager ID': 'STRUCT("Executor ID" VARCHAR, "Host" VARCHAR)',
    'Maximum Memory': 'BIGINT',
    'Resource Profile Id': 'BIGINT',
    'Executor Resource Requests': 'STRUCT("memory" STRUCT("Amount" BIGINT))',
    # the environment
    'JVM Information': _PROPERTIES,
    'Spark Properties': _PROPERTIES,
    'Hadoop Properties': _PROPERTIES,
    'System Properties': _PROPERTIES,
    'Classpath Entries': _PROPERTIES,
    'Metrics Properties': _PROPERTIES,
    # the application
    'Spark Version': 'VARCHAR',
    'App ID': 'VARCHAR',
    'App Name': 'VARCHAR',
    'App Attempt ID': 'VARCHAR',
    'User': 'VARCHAR',
}
