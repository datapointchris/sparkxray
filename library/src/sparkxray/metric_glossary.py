"""What each metric sparkxray reads means, the unit Spark records it in and the unit sparkxray shows, and where the meaning comes from.

The glossary holds every column whose name ends in a unit, and every metric a plan node declares.
A column's description is the one the table registry carries, which `tables list` prints too, so
the two never disagree. A meaning quoted from the Spark 4.2 documentation names its page and
section. One read from the Apache Spark source names its file and line at tag v4.2.0. A column
of the tables Glue's metrics and insights fill names the AWS reference page it is read from.
"""

from __future__ import annotations

import dataclasses as dc
from typing import Any

from sparkxray.errors import UnknownMetricError
from sparkxray.errors import suggest_nearest
from sparkxray.tables.registry import ENRICHMENT_TABLES
from sparkxray.tables.registry import TABLES
from sparkxray.tables.registry import column_unit

_TASK_METRICS_DOC = 'Spark 4.2 docs, Monitoring and Instrumentation › Executor Task Metrics'
_EXECUTOR_METRICS_DOC = 'Spark 4.2 docs, Monitoring and Instrumentation › Executor Metrics'
_SQL_METRICS_DOC = 'Spark 4.2 docs, Web UI › SQL metrics'
_CORE = 'Apache Spark v4.2.0 source, core/src/main/scala/org/apache/spark/'
_SQL = 'Apache Spark v4.2.0 source, sql/core/src/main/scala/org/apache/spark/sql/execution/'
_SPARKXRAY = 'sparkxray, from the event field it reads'
_GLUE_ONLY = "AWS Glue 5.0's Spark build declares it, and no published source says what it measures"

_SHUFFLE_READERS = 'ShuffleExchange, CollectLimit, CollectTail, TakeOrderedAndProject'
_SHUFFLE_READERS_DOC = 'CollectLimit, TakeOrderedAndProject, ShuffleExchange'
_FILE_WRITES = 'any write operation on a file-based table'
_PYTHON = 'Python UDFs, Pandas UDFs, Pandas Functions API and Python Data Source'

# The unit plan_node_metrics shows each kind of plan metric in, and the one Spark records it in.
_SHOWN_UNITS = {'size': 'bytes', 'timing': 'ms', 'nsTiming': 'ms', 'sum': 'count', 'average': None}
_SPARK_UNITS = {'size': 'bytes', 'timing': 'ms', 'nsTiming': 'ns', 'sum': 'count', 'average': None}
_DERIVATIONS = {
    'nsTiming': 'Spark records it in nanoseconds, and plan_node_metrics shows it in milliseconds.',
    'average': (
        "Spark records each task's average times 10. plan_node_metrics divides min, median and max by 10, "
        'as the SQL tab shows them, and gives no total.'
    ),
}


@dc.dataclass(frozen=True, slots=True)
class MetricEntry:
    """One metric: a column of a table, or a metric plan nodes declare, which `plan_node_metrics` holds by `metric_name`.

    `table` is None for a plan metric. `unit` is the unit sparkxray shows it in, and `spark_unit`
    the one Spark records it in, which differ for an `nsTiming` plan metric and for a resource
    profile's memory. For a column CloudWatch fills, `spark_unit` is the one AWS records it in.
    `spark_fields` are the event fields it is read from, each level of a field
    after a `›`. `derivation` says how sparkxray computes it where it is not one field as Spark
    wrote it. `metric_type` is a plan metric's kind in Spark, and `declared_by` the operators that
    declare it, as the cited source names them. `description` is None for a metric no published
    source explains. `source` names where the description comes from.
    """

    name: str
    table: str | None
    unit: str | None
    spark_unit: str | None
    description: str | None
    spark_fields: tuple[str, ...]
    derivation: str | None
    metric_type: str | None
    declared_by: str | None
    source: str

    def to_json(self) -> dict[str, Any]:
        return {**dc.asdict(self), 'spark_fields': list(self.spark_fields)}


@dc.dataclass(frozen=True, slots=True)
class _ColumnSource:
    spark_fields: tuple[str, ...]
    source: str
    derivation: str | None = None
    spark_unit: str | None = None


@dc.dataclass(frozen=True, slots=True)
class _PlanMetric:
    name: str
    metric_type: str
    description: str | None
    declared_by: str
    source: str


def _task_info(field: str) -> str:
    return f'SparkListenerTaskEnd › Task Info › {field}'


def _task_metric(*levels: str) -> str:
    return ' › '.join(('SparkListenerTaskEnd', 'Task Metrics', *levels))


def _task_peak(field: str) -> str:
    return f'SparkListenerTaskEnd › Task Executor Metrics › {field}'


def _stage_info(field: str) -> str:
    return f'SparkListenerStageCompleted › Stage Info › {field}'


def _documented(*levels: str) -> _ColumnSource:
    return _ColumnSource((_task_metric(*levels),), _TASK_METRICS_DOC)


_TASK_PEAKS = f'{_EXECUTOR_METRICS_DOC}; {_CORE}executor/ExecutorMetricsPoller.scala:30'
_GET_METRIC_DATA = 'Amazon CloudWatch API Reference, GetMetricData'
_CLOUDWATCH_CONCEPTS = 'Amazon CloudWatch User Guide, Metrics concepts › Statistics'
_STATISTICS = 'SparkListenerSQLAdaptiveExecutionUpdate › physicalPlanDescription'

_COLUMN_SOURCES: dict[str, _ColumnSource] = {
    'application.start_epoch_ms': _ColumnSource(('SparkListenerApplicationStart › Timestamp',), _SPARKXRAY),
    'application.end_epoch_ms': _ColumnSource(('SparkListenerApplicationEnd › Timestamp',), _SPARKXRAY),
    'executors.core_count': _ColumnSource(('SparkListenerExecutorAdded › Executor Info › Total Cores',), _SPARKXRAY),
    'executors.executor_memory_bytes': _ColumnSource(
        ('SparkListenerResourceProfileAdded › Executor Resource Requests › memory › Amount',),
        _SPARKXRAY,
        derivation='The memory of the resource profile the executor started under, in MiB, times 1,048,576. Null for the driver.',
        spark_unit='MiB',
    ),
    'executors.max_storage_memory_bytes': _ColumnSource(('SparkListenerBlockManagerAdded › Maximum Memory',), _EXECUTOR_METRICS_DOC),
    'executors.add_epoch_ms': _ColumnSource(
        ('SparkListenerExecutorAdded › Timestamp', 'SparkListenerBlockManagerAdded › Timestamp'),
        _SPARKXRAY,
        derivation="The add event's timestamp, or the block manager's where no add event was logged.",
    ),
    'executors.remove_epoch_ms': _ColumnSource(('SparkListenerExecutorRemoved › Timestamp',), _SPARKXRAY),
    'executors.peak_jvm_heap_bytes': _ColumnSource(
        ('SparkListenerStageExecutorMetrics › Executor Metrics › JVMHeapMemory', _task_peak('JVMHeapMemory')),
        _EXECUTOR_METRICS_DOC,
        derivation=(
            "The larger of the most heap any stage's peaks and any task's peaks recorded for the executor. "
            'A peak of 0, which Spark writes where it never polled, counts as none. '
            'Stage peaks are written only under spark.eventLog.logStageExecutorMetrics.'
        ),
    ),
    'jobs.submit_epoch_ms': _ColumnSource(('SparkListenerJobStart › Submission Time',), _SPARKXRAY),
    'jobs.end_epoch_ms': _ColumnSource(('SparkListenerJobEnd › Completion Time',), _SPARKXRAY),
    'stages.task_count': _ColumnSource((_stage_info('Number of Tasks'),), _SPARKXRAY),
    'stages.submit_epoch_ms': _ColumnSource((_stage_info('Submission Time'),), _SPARKXRAY),
    'stages.complete_epoch_ms': _ColumnSource((_stage_info('Completion Time'),), _SPARKXRAY),
    'tasks.launch_epoch_ms': _ColumnSource((_task_info('Launch Time'),), f'{_CORE}scheduler/TaskInfo.scala:44'),
    'tasks.finish_epoch_ms': _ColumnSource(
        (_task_info('Finish Time'),),
        f'{_CORE}scheduler/TaskSetManager.scala:964',
        derivation='Spark writes 0 for an attempt still running, which reads as null.',
    ),
    'tasks.duration_ms': _ColumnSource(
        (_task_info('Finish Time'), _task_info('Launch Time')),
        f'{_CORE}scheduler/TaskInfo.scala:154',
        derivation='Finish Time − Launch Time. Null while the attempt ran.',
    ),
    'tasks.getting_result_ms': _ColumnSource(
        (_task_info('Finish Time'), _task_info('Getting Result Time')),
        f'{_CORE}status/AppStatusUtils.scala:56',
        derivation=(
            'Finish Time − Getting Result Time where Getting Result Time is above 0, and 0 otherwise, as the History Server computes it.'
        ),
    ),
    'tasks.executor_run_time_ms': _documented('Executor Run Time'),
    'tasks.executor_cpu_time_ns': _documented('Executor CPU Time'),
    'tasks.executor_deserialize_time_ms': _documented('Executor Deserialize Time'),
    'tasks.executor_deserialize_cpu_time_ns': _documented('Executor Deserialize CPU Time'),
    'tasks.result_serialization_time_ms': _documented('Result Serialization Time'),
    'tasks.jvm_gc_time_ms': _documented('JVM GC Time'),
    'tasks.result_size_bytes': _documented('Result Size'),
    'tasks.peak_execution_memory_bytes': _documented('Peak Execution Memory'),
    'tasks.memory_spilled_bytes': _documented('Memory Bytes Spilled'),
    'tasks.disk_spilled_bytes': _documented('Disk Bytes Spilled'),
    'tasks.input_bytes': _documented('Input Metrics', 'Bytes Read'),
    'tasks.input_records': _documented('Input Metrics', 'Records Read'),
    'tasks.output_bytes': _documented('Output Metrics', 'Bytes Written'),
    'tasks.output_records': _documented('Output Metrics', 'Records Written'),
    'tasks.shuffle_remote_read_bytes': _documented('Shuffle Read Metrics', 'Remote Bytes Read'),
    'tasks.shuffle_local_read_bytes': _documented('Shuffle Read Metrics', 'Local Bytes Read'),
    'tasks.shuffle_remote_read_to_disk_bytes': _documented('Shuffle Read Metrics', 'Remote Bytes Read To Disk'),
    'tasks.shuffle_fetch_wait_time_ms': _ColumnSource(
        (_task_metric('Shuffle Read Metrics', 'Fetch Wait Time'),),
        f'{_TASK_METRICS_DOC}; {_CORE}storage/ShuffleBlockFetcherIterator.scala:196',
    ),
    'tasks.shuffle_read_records': _documented('Shuffle Read Metrics', 'Total Records Read'),
    'tasks.shuffle_remote_blocks_fetched_count': _documented('Shuffle Read Metrics', 'Remote Blocks Fetched'),
    'tasks.shuffle_local_blocks_fetched_count': _documented('Shuffle Read Metrics', 'Local Blocks Fetched'),
    'tasks.shuffle_write_bytes': _documented('Shuffle Write Metrics', 'Shuffle Bytes Written'),
    'tasks.shuffle_write_records': _documented('Shuffle Write Metrics', 'Shuffle Records Written'),
    'tasks.shuffle_write_time_ns': _documented('Shuffle Write Metrics', 'Shuffle Write Time'),
    'tasks.scheduler_delay_ms': _ColumnSource(
        (
            _task_info('Finish Time'),
            _task_info('Launch Time'),
            _task_metric('Executor Run Time'),
            _task_metric('Executor Deserialize Time'),
            _task_metric('Result Serialization Time'),
            _task_info('Getting Result Time'),
        ),
        f'{_CORE}status/AppStatusUtils.scala:52',
        derivation=(
            'Finish Time − Launch Time − Executor Run Time − Executor Deserialize Time − Result Serialization Time − getting_result_ms, '
            'and 0 where that is below 0, as the History Server computes it. Null while the attempt ran.'
        ),
    ),
    'tasks.peak_jvm_heap_bytes': _ColumnSource((_task_peak('JVMHeapMemory'),), _TASK_PEAKS),
    'tasks.peak_jvm_offheap_bytes': _ColumnSource((_task_peak('JVMOffHeapMemory'),), _TASK_PEAKS),
    'tasks.peak_python_rss_bytes': _ColumnSource(
        (_task_peak('ProcessTreePythonRSSMemory'), _task_peak('ProcessTreeJVMRSSMemory')),
        f'{_TASK_PEAKS}; {_CORE}executor/ProcfsMetricsGetter.scala:108',
        derivation=(
            'Null unless some task has a ProcessTreeJVMRSSMemory above 0, because Spark writes 0 for every '
            'process-tree metric when it measures none. Spark reads RSS in pages and records it in bytes.'
        ),
    ),
    'tasks.major_gc_count': _ColumnSource((_task_peak('MajorGCCount'),), f'{_CORE}metrics/ExecutorMetricType.scala:152'),
    'tasks.major_gc_time_ms': _ColumnSource((_task_peak('MajorGCTime'),), f'{_CORE}metrics/ExecutorMetricType.scala:152'),
    'sql_executions.start_epoch_ms': _ColumnSource(('SparkListenerSQLExecutionStart › time',), _SPARKXRAY),
    'sql_executions.end_epoch_ms': _ColumnSource(('SparkListenerSQLExecutionEnd › time',), _SPARKXRAY),
    'sql_executions.plan_version_count': _ColumnSource(
        (),
        _SPARKXRAY,
        derivation='How many SparkListenerSQLExecutionStart and SparkListenerSQLAdaptiveExecutionUpdate events the log holds for it.',
    ),
    'plan_nodes.runtime_size_bytes': _ColumnSource(
        (_STATISTICS,),
        _SPARKXRAY,
        derivation=(
            'The sizeInBytes of the Statistics(...) formatted plan text prints beside a finished query stage, '
            'rounded to one decimal of its unit. Null where Spark printed 8.0 EiB, its stand-in for a size it does not know.'
        ),
    ),
    'plan_nodes.runtime_row_count': _ColumnSource(
        (_STATISTICS,),
        _SPARKXRAY,
        derivation='The rowCount of the Statistics(...) formatted plan text prints beside a finished query stage.',
    ),
    'plan_node_metrics.value_count': _ColumnSource(
        (_task_info('Accumulables'), 'SparkListenerDriverAccumUpdates › accumUpdates'),
        _SPARKXRAY,
        derivation=(
            "Each task's last report from the stage's latest attempt, a 0 for each of its tasks that reported none, and the driver's "
            'reports. A sum counts every value, an average those above 0, and a size or time those at or above 0, as the SQL tab does.'
        ),
    ),
    'glue_metrics.period_ms': _ColumnSource(
        ('GetMetricData › MetricDataQueries › MetricStat › Period',),
        _GET_METRIC_DATA,
        derivation=(
            'The period sparkxray asks for: 60 seconds for a run that started in the last 15 days, 300 to 63 days, and 3600 after, '
            'the finest CloudWatch keeps at each age.'
        ),
        spark_unit='s',
    ),
    'glue_metrics.period_start_epoch_ms': _ColumnSource(
        ('GetMetricData › MetricDataResults › Timestamps',),
        f'{_GET_METRIC_DATA}; {_CLOUDWATCH_CONCEPTS}',
        derivation='CloudWatch stamps a statistic aggregated over a period with the time the period began.',
    ),
    'glue_insights.logged_epoch_ms': _ColumnSource(
        ('GetLogEvents › events › timestamp',),
        'Amazon CloudWatch Logs API Reference, OutputLogEvent',
    ),
    'driver_log.logged_epoch_ms': _ColumnSource(
        ('ts',),
        'Apache Spark v4.2.0 source, common/utils-java/src/main/resources/org/apache/spark/SparkLayout.json:5',
        derivation="The record's ts, which Spark writes in UTC as yyyy-MM-dd'T'HH:mm:ss.SSS'Z'.",
    ),
}


def _shuffle_read_metric(name: str, metric_type: str, description: str, line: int) -> _PlanMetric:
    return _PlanMetric(name, metric_type, description, _SHUFFLE_READERS, f'{_SQL}metric/SQLShuffleMetricsReporter.scala:{line}')


def _aqe_read_metric(name: str, description: str, line: int) -> _PlanMetric:
    return _PlanMetric(name, 'sum', description, 'AQEShuffleRead', f'{_SQL}adaptive/AQEShuffleReadExec.scala:{line}')


def _file_scan_metric(name: str, metric_type: str, description: str, line: int) -> _PlanMetric:
    return _PlanMetric(name, metric_type, description, 'FileSourceScan', f'{_SQL}DataSourceScanExec.scala:{line}')


def _file_write_metric(name: str, metric_type: str, description: str, line: int) -> _PlanMetric:
    return _PlanMetric(name, metric_type, description, _FILE_WRITES, f'{_SQL}datasources/BasicWriteStatsTracker.scala:{line}')


def _python_metric(name: str, description: str, line: int) -> _PlanMetric:
    return _PlanMetric(name, 'timing', description, _PYTHON, f'{_SQL}python/PythonSQLMetrics.scala:{line}')


def _glue_metric(name: str, metric_type: str, declared_by: str) -> _PlanMetric:
    return _PlanMetric(name, metric_type, None, declared_by, _GLUE_ONLY)


_PLAN_METRICS: tuple[_PlanMetric, ...] = (
    _PlanMetric(
        'number of output rows',
        'sum',
        'The number of output rows of the operator. On a Python evaluation it counts rows received back from Python, '
        'on a file write the rows written, and on BroadcastExchange the rows collected to broadcast.',
        'Aggregate operators, Join operators, Sample, Range, Scan operators, Filter, etc.',
        f'{_SQL_METRICS_DOC}; {_SQL}python/PythonSQLMetrics.scala:55; {_SQL}datasources/BasicWriteStatsTracker.scala:244; '
        f'{_SQL}exchange/BroadcastExchangeExec.scala:187',
    ),
    _PlanMetric(
        'data size',
        'size',
        'The size of broadcast/shuffled/collected data of the operator.',
        'BroadcastExchange, ShuffleExchange, Subquery',
        _SQL_METRICS_DOC,
    ),
    _PlanMetric('time to collect', 'timing', 'The time spent on collecting data.', 'BroadcastExchange, Subquery', _SQL_METRICS_DOC),
    _PlanMetric('scan time', 'timing', 'The time spent on scanning data.', 'ColumnarBatchScan, FileSourceScan', _SQL_METRICS_DOC),
    _PlanMetric(
        'metadata time',
        'timing',
        'The time spent on getting metadata like number of partitions, number of files.',
        'FileSourceScan',
        _SQL_METRICS_DOC,
    ),
    _PlanMetric('shuffle bytes written', 'size', 'The number of bytes written.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric('shuffle records written', 'sum', 'The number of records written.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric('shuffle write time', 'nsTiming', 'The time spent on shuffle writing.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric('remote blocks read', 'sum', 'The number of blocks read remotely.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric('remote bytes read', 'size', 'The number of bytes read remotely.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric(
        'remote bytes read to disk', 'size', 'The number of bytes read from remote to local disk.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC
    ),
    _PlanMetric('local blocks read', 'sum', 'The number of blocks read locally.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric('local bytes read', 'size', 'The number of bytes read locally.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric(
        'fetch wait time',
        'timing',
        'The time spent on fetching data (local and remote). Spark counts the time a task blocks on the next fetched block, '
        'issues remote requests, and reads host-local blocks.',
        _SHUFFLE_READERS_DOC,
        f'{_SQL_METRICS_DOC}; {_CORE}storage/ShuffleBlockFetcherIterator.scala:196',
    ),
    _PlanMetric('records read', 'sum', 'The number of read records.', _SHUFFLE_READERS_DOC, _SQL_METRICS_DOC),
    _PlanMetric('sort time', 'timing', 'The time spent on sorting.', 'Sort', _SQL_METRICS_DOC),
    _PlanMetric('peak memory', 'size', 'The peak memory usage in the operator.', 'Sort, HashAggregate', _SQL_METRICS_DOC),
    _PlanMetric(
        'spill size', 'size', 'Number of bytes spilled to disk from memory in the operator.', 'Sort, HashAggregate', _SQL_METRICS_DOC
    ),
    _PlanMetric(
        'time in aggregation build', 'timing', 'The time spent on aggregation.', 'HashAggregate, ObjectHashAggregate', _SQL_METRICS_DOC
    ),
    _PlanMetric(
        'avg hash probes per key',
        'average',
        'The average bucket list iterations per lookup during aggregation. The docs name it avg hash probe bucket list iters.',
        'HashAggregate',
        f'{_SQL_METRICS_DOC}; {_SQL}aggregate/HashAggregateExec.scala:75',
    ),
    _PlanMetric('data size of build side', 'size', 'The size of built hash map.', 'ShuffledHashJoin', _SQL_METRICS_DOC),
    _PlanMetric('time to build hash map', 'timing', 'The time spent on building hash map.', 'ShuffledHashJoin', _SQL_METRICS_DOC),
    _PlanMetric(
        'task commit time',
        'timing',
        'The time spent on committing the output of a task after the writes succeed.',
        _FILE_WRITES,
        _SQL_METRICS_DOC,
    ),
    _PlanMetric(
        'job commit time',
        'timing',
        'The time spent on committing the output of a job after the writes succeed.',
        _FILE_WRITES,
        _SQL_METRICS_DOC,
    ),
    _PlanMetric(
        'data sent to Python workers',
        'size',
        'The number of bytes of serialized data sent to the Python workers.',
        _PYTHON,
        _SQL_METRICS_DOC,
    ),
    _PlanMetric(
        'data returned from Python workers',
        'size',
        'The number of bytes of serialized data received back from the Python workers.',
        _PYTHON,
        _SQL_METRICS_DOC,
    ),
    _PlanMetric(
        'duration',
        'timing',
        "Time from a task creating a whole-stage-codegen stage's generated iterator until that iterator ran out of rows, "
        'summed across tasks. It covers every operator fused into the stage, and the Spark UI prints it on the stage, not an operator.',
        'WholeStageCodegen',
        f'{_SQL}WholeStageCodegenEvaluatorFactory.scala:51',
    ),
    _shuffle_read_metric(
        'remote reqs duration',
        'timing',
        'Time from issuing each remote shuffle fetch request until its last block arrived, summed over every request, '
        'merged-chunk requests included.',
        178,
    ),
    _shuffle_read_metric(
        'remote merged reqs duration',
        'timing',
        'Time from issuing each remote request for merged shuffle chunks until its last chunk arrived, summed over every request. '
        'Merged chunks come from push-based shuffle.',
        179,
    ),
    _shuffle_read_metric(
        'corrupt merged block chunks',
        'sum',
        'Merged shuffle block chunks, from push-based shuffle, that a task found corrupt while reading them.',
        168,
    ),
    _shuffle_read_metric(
        'merged fetch fallback count',
        'sum',
        'Times a task gave up on a merged shuffle block chunk, from push-based shuffle, and fetched the original blocks instead.',
        169,
    ),
    _shuffle_read_metric('remote merged blocks fetched', 'sum', 'Merged shuffle blocks, from push-based shuffle, fetched remotely.', 170),
    _shuffle_read_metric('local merged blocks fetched', 'sum', 'Merged shuffle blocks, from push-based shuffle, read locally.', 171),
    _shuffle_read_metric(
        'remote merged chunks fetched', 'sum', 'Chunks of merged shuffle blocks, from push-based shuffle, fetched remotely.', 172
    ),
    _shuffle_read_metric(
        'local merged chunks fetched', 'sum', 'Chunks of merged shuffle blocks, from push-based shuffle, read locally.', 173
    ),
    _shuffle_read_metric(
        'remote merged bytes read', 'size', 'Bytes of merged shuffle data, from push-based shuffle, fetched remotely.', 175
    ),
    _shuffle_read_metric('local merged bytes read', 'size', 'Bytes of merged shuffle data, from push-based shuffle, read locally.', 177),
    _PlanMetric(
        'number of partitions',
        'sum',
        "Partitions a shuffle produces, set once on the driver: an Exchange's partition count, or the partitions an AQE shuffle "
        'read leaves after coalescing and splitting skewed ones.',
        'ShuffleExchange, AQEShuffleRead',
        f'{_SQL}exchange/ShuffleExchangeExec.scala:201; {_SQL}adaptive/AQEShuffleReadExec.scala:227',
    ),
    _aqe_read_metric(
        'number of empty partitions',
        'Shuffle partitions the map stage wrote no bytes to, counted once on the driver when AQE plans the read.',
        229,
    ),
    _PlanMetric(
        'partition data size',
        'size',
        'Shuffle bytes in each partition the read produces, one value per partition, recorded on the driver. A local read declares none.',
        'AQEShuffleRead',
        f'{_SQL}adaptive/AQEShuffleReadExec.scala:236',
    ),
    _aqe_read_metric('number of skewed partitions', 'Shuffle partitions AQE found skewed and split, counted once on the driver.', 241),
    _aqe_read_metric('number of skewed partition splits', 'Pieces AQE split the skewed partitions into, counted once on the driver.', 243),
    _aqe_read_metric(
        'number of coalesced partitions',
        'Partitions of the read that each cover more than one shuffle partition, counted once on the driver.',
        250,
    ),
    _file_scan_metric(
        'number of files read',
        'sum',
        'Files the scan selected after partition pruning, counted once on the driver. Under dynamic partition pruning, '
        'the count after the dynamic filters.',
        341,
    ),
    _file_scan_metric(
        'size of files read',
        'size',
        'Total bytes of the files the scan selected after partition pruning, counted once on the driver. Under dynamic '
        'partition pruning, the size after the dynamic filters.',
        343,
    ),
    _file_scan_metric(
        'number of partitions read',
        'sum',
        'Table partitions the scan selected after pruning, counted once on the driver. Only a partitioned table declares it.',
        347,
    ),
    _file_scan_metric(
        'dynamic partition pruning time',
        'timing',
        "Time on the driver to apply the scan's dynamic partition filters to its statically pruned file listing.",
        349,
    ),
    _file_scan_metric(
        'static number of files read',
        'sum',
        'Files a scan under dynamic partition pruning listed after its static filters alone, counted once on the driver.',
        583,
    ),
    _file_scan_metric(
        'static size of files read',
        'size',
        'Total bytes of the files a scan under dynamic partition pruning listed after its static filters alone, '
        'counted once on the driver.',
        584,
    ),
    _file_write_metric(
        'number of written files', 'sum', "Files the write's tasks closed, summed on the driver after the job commits.", 242
    ),
    _file_write_metric(
        'written output', 'size', "Total bytes of the files the write's tasks closed, summed on the driver after the job commits.", 243
    ),
    _file_write_metric(
        'number of dynamic part',
        'sum',
        "Distinct partition values the write's tasks opened, counted on the driver after the job commits.",
        245,
    ),
    _python_metric(
        'time to start Python workers',
        'Time from a task starting its Python runner until the Python worker began work on the task, summed across tasks.',
        47,
    ),
    _python_metric(
        'time to initialize Python workers',
        'Time the Python worker spent before running the UDFs, reading the task context, files and broadcasts and loading the '
        'functions, summed across tasks.',
        48,
    ),
    _python_metric(
        'time to run Python workers',
        'Time from a task starting its Python runner until the Python worker finished the task, summed across tasks. '
        'It holds the start, initialize and execute times.',
        49,
    ),
    _python_metric(
        'time to execute Python code',
        'Time the Python worker spent streaming rows through the UDFs and writing the results back, summed across tasks.',
        50,
    ),
    _PlanMetric(
        'number of input batches',
        'sum',
        'Columnar batches ColumnarToRow read from its child.',
        'ColumnarToRow',
        f'{_SQL}Columnar.scala:81',
    ),
    _PlanMetric(
        'number of sort fallback tasks',
        'sum',
        'Tasks whose hash aggregation could not hold its hash map in memory and finished by sort-based aggregation.',
        'HashAggregate',
        f'{_SQL}aggregate/HashAggregateExec.scala:76',
    ),
    _PlanMetric(
        'number of segment-tree frames prepared',
        'sum',
        'Window partitions whose moving frame was computed with a segment tree, one per partition and frame.',
        'Window',
        f'{_SQL}window/WindowExec.scala:96',
    ),
    _PlanMetric(
        'number of segment-tree fallback frames prepared',
        'sum',
        'Window partitions below spark.sql.window.segmentTree.minPartitionRows whose moving frame fell back to the sliding-window '
        'path, one per partition and frame.',
        'Window',
        f'{_SQL}window/WindowExec.scala:99',
    ),
    _PlanMetric(
        'time to build',
        'timing',
        'Time on the driver to turn the collected rows into the relation it broadcasts, such as a hash relation.',
        'BroadcastExchange',
        f'{_SQL}exchange/BroadcastExchangeExec.scala:132',
    ),
    _PlanMetric(
        'time to broadcast',
        'timing',
        'Time on the driver to broadcast the built relation to the executors.',
        'BroadcastExchange',
        f'{_SQL}exchange/BroadcastExchangeExec.scala:133',
    ),
    _glue_metric('cumulative time for AQE to re-optimize', 'timing', 'AdaptiveSparkPlan'),
    _glue_metric('cumulative time for effective re-optimization', 'timing', 'AdaptiveSparkPlan'),
    _glue_metric('number of times re-optimization was applied', 'sum', 'AdaptiveSparkPlan'),
    _glue_metric('number of effective re-optimization was applied', 'sum', 'AdaptiveSparkPlan'),
    _glue_metric('number of unique hash keys', 'sum', 'BroadcastExchange, ShuffledHashJoin'),
    _glue_metric('number of values in hashed relation', 'sum', 'BroadcastExchange, ShuffledHashJoin'),
)


def metric_glossary() -> tuple[MetricEntry, ...]:
    """Every column whose name ends in a unit, table by table in build order and then the enrichment's, then every plan metric by name."""
    columns = []
    for table in (*TABLES, *ENRICHMENT_TABLES):
        for column in table.columns:
            unit = column_unit(column.name)
            if unit is None:
                continue
            source = _COLUMN_SOURCES[f'{table.name}.{column.name}']
            columns.append(
                MetricEntry(
                    name=column.name,
                    table=table.name,
                    unit=unit,
                    spark_unit=source.spark_unit or unit,
                    description=column.description,
                    spark_fields=source.spark_fields,
                    derivation=source.derivation,
                    metric_type=None,
                    declared_by=None,
                    source=source.source,
                )
            )
    plan_metrics = [
        MetricEntry(
            name=metric.name,
            table=None,
            unit=_SHOWN_UNITS[metric.metric_type],
            spark_unit=_SPARK_UNITS[metric.metric_type],
            description=metric.description,
            spark_fields=(),
            derivation=_DERIVATIONS.get(metric.metric_type),
            metric_type=metric.metric_type,
            declared_by=metric.declared_by,
            source=metric.source,
        )
        for metric in sorted(_PLAN_METRICS, key=lambda metric: metric.name.lower())
    ]
    return (*columns, *plan_metrics)


def metric_entries(name: str) -> tuple[MetricEntry, ...]:
    """Every entry `name` names, in any case: a column as `column` or `table.column`, a plan metric, or a Spark field it reads.

    A Spark field matches by its whole path or its last level, so `Executor CPU Time` finds the
    column holding it and every column derived from it. A name nothing matches raises
    `UnknownMetricError`, listing the column and plan metric names the glossary holds.
    """
    wanted = name.strip().casefold()
    entries = metric_glossary()
    matched = tuple(entry for entry in entries if wanted in _names(entry))
    if not matched:
        held = sorted({entry.name for entry in entries}, key=str.casefold)
        raise UnknownMetricError(
            suggest_nearest(
                f'{name!r} is no table column, plan metric or Spark field sparkxray describes. '
                "A summary's own numbers, as wall_ms in stages list, are defined on that command's --help",
                name,
                held,
            ),
            field='name',
            valid=held,
            hint='sparkxray metrics list',
        )
    return matched


def _names(entry: MetricEntry) -> set[str]:
    names = {entry.name.casefold()}
    if entry.table is not None:
        names.add(f'{entry.table}.{entry.name}'.casefold())
    for field in entry.spark_fields:
        names.add(field.casefold())
        names.add(field.rpartition(' › ')[2].casefold())
    return names
