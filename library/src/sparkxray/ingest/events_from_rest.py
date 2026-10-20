"""Rewrite one application attempt's REST JSON as the event log Spark would have written, so the normal build reads it.

A server with no event log to give still serves what its listener replayed: the application, its
environment and executors, its jobs, stages and every task, and each SQL execution's plan and
metrics. Each of those becomes the listener event the build reads, with the field names and units
`JsonProtocol` writes. So one build serves a downloaded log and a rebuilt one.

REST carries less than the log did, and the rebuilt log holds only what it carries:

- no stage names its parents, because `StageData` has no parent ids;
- no task carries the executor's peaks while it ran, or any accumulator update, because Spark
  serves neither per task. So no plan node's metric aggregates from tasks, and no node reaches the
  stages that ran it;
- each plan node's metrics are the totals and spread the SQL tab displays, read back from their
  display strings onto the nodes of the plan's text;
- only each execution's last plan, which is what the server keeps;
- each executor's peaks over the whole run, as one event naming no stage;
- no execution's call stack, job tags or the settings it changed;
- a task's end reason, read back from the error string Spark made of it, and a failed job's
  failure, read from its failed stage's.

The build names what is missing in its manifest's `lacks`, and each rule needing it is skipped.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from collections.abc import Iterable
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

from sparkxray.plan.last_plans import plan_for_event
from sparkxray.plan.rest_graph import parse_rest_graph
from sparkxray.plan.tree import Plan
from sparkxray.plan.tree import PlanNode

if TYPE_CHECKING:
    from sparkxray.ingest.rest_client import RestClient

# The events carrying one execution's plan metrics as the server displayed them, and one
# executor's peaks where the run logged no stage's. No Spark writes either.
PLAN_METRICS_EVENT = 'sparkxray.RestPlanMetrics'
EXECUTOR_PEAKS_EVENT = 'sparkxray.RestExecutorPeaks'
_TASK_PAGE = 1000
_SQL_PAGE = 100
# A stage in any other state never started, as Spark logs no event for a skipped or pending stage.
_STARTED_STAGES = frozenset({'ACTIVE', 'COMPLETE', 'FAILED'})
_ENDED_STAGES = frozenset({'COMPLETE', 'FAILED'})
_SQL_EXECUTION = 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecution'
# Where an event sorts among those at the same millisecond, in the order Spark posts them.
_RANKS = {
    'SparkListenerExecutorAdded': 0,
    'SparkListenerBlockManagerAdded': 0,
    'SparkListenerSQLExecutionStart': 1,
    PLAN_METRICS_EVENT.rpartition('.')[2]: 1,
    'SparkListenerJobStart': 2,
    'SparkListenerStageSubmitted': 3,
    'SparkListenerTaskStart': 4,
    'SparkListenerTaskEnd': 5,
    'SparkListenerStageCompleted': 6,
    'SparkListenerJobEnd': 7,
    'SparkListenerSQLExecutionEnd': 8,
    'SparkListenerExecutorRemoved': 9,
    'SparkListenerStageExecutorMetrics': 10,
    EXECUTOR_PEAKS_EVENT.rpartition('.')[2]: 10,
    'SparkListenerApplicationEnd': 11,
}
# The error string `TaskEndReason.toErrorString` writes for each reason but an exception, and the
# field `JsonProtocol` logs its `detail` under.
_REASON_STRINGS = (
    (re.compile(r'FetchFailed\(.*?, message=\n(?P<detail>.*)\n\)', re.DOTALL), 'FetchFailed', 'Message'),
    (
        re.compile(
            r'ExecutorLostFailure \(executor .*? exited (?:caused by one of|unrelated to) the running tasks\)(?: Reason: (?P<detail>.*))?',
            re.DOTALL,
        ),
        'ExecutorLostFailure',
        'Loss Reason',
    ),
    (re.compile(r'TaskKilled \((?P<detail>.*)\)', re.DOTALL), 'TaskKilled', 'Kill Reason'),
    (re.compile(r'TaskResultLost \(.*', re.DOTALL), 'TaskResultLost', None),
    (re.compile(r'TaskCommitDenied \(.*', re.DOTALL), 'TaskCommitDenied', None),
    (re.compile(r'Resubmitted \(.*', re.DOTALL), 'Resubmitted', None),
    (re.compile(r'UnknownReason'), 'UnknownReason', None),
)


def write_rebuilt_log(client: RestClient, app_path: str, application: Mapping[str, Any], attempt: Mapping[str, Any], out: Path) -> None:
    """Write the events one application attempt's REST JSON holds to `out`, one JSON line each, in the order Spark logs them.

    `app_path` is the attempt's path below the API root, `applications/<app>[/<attempt>]`.
    `application` and `attempt` are the server's record of it.
    """
    found = client.in_parallel(lambda resource: client.json(f'{app_path}/{resource}'), ['environment', 'allexecutors', 'jobs', 'stages'])
    environment: dict[str, Any] = found[0] or {}
    executors: list[dict[str, Any]] = found[1] or []
    jobs: list[dict[str, Any]] = found[2] or []
    stages: list[dict[str, Any]] = found[3] or []
    executions = _pages(client, f'{app_path}/sql', _SQL_PAGE, {'details': 'true', 'planDescription': 'true'})
    started = [stage for stage in stages if stage.get('status') in _STARTED_STAGES]
    tasks = _tasks(client, app_path, started)
    version = attempt.get('appSparkVersion') or (client.json('version') or {}).get('spark')

    events = _Events()
    events.head({'Event': 'SparkListenerLogStart', 'Spark Version': version})
    for profile in environment.get('resourceProfiles') or ():
        events.head(_resource_profile(profile))
    events.head(_environment(environment))
    start = {
        'Event': 'SparkListenerApplicationStart',
        'App Name': application.get('name'),
        'App ID': application.get('id'),
        'Timestamp': _epoch(attempt, 'startTime'),
        'User': attempt.get('sparkUser'),
    }
    if attempt.get('attemptId') is not None:
        start['App Attempt ID'] = str(attempt['attemptId'])
    events.head(start)
    end = _epoch(attempt, 'endTime') if attempt.get('completed') else None
    stage_peaks_logged = _pairs(environment.get('sparkProperties')).get('spark.eventLog.logStageExecutorMetrics', '').lower() == 'true'
    for executor in executors:
        _add_executor(events, executor, end, stage_peaks_logged)
    execution_of_job = {
        int(job_id): int(execution['id'])
        for execution in executions
        for key in ('successJobIds', 'runningJobIds', 'failedJobIds')
        for job_id in execution.get(key) or ()
    }
    failures = {
        int(stage['stageId']): stage['failureReason'] for stage in started if stage.get('status') == 'FAILED' and stage.get('failureReason')
    }
    for job in jobs:
        failure = next((failures[stage_id] for stage_id in sorted(job.get('stageIds') or ()) if stage_id in failures), None)
        _add_job(events, job, execution_of_job.get(int(job['jobId'])), failure)
    for stage in started:
        _add_stage(events, stage)
    for stage, stage_tasks in zip(started, tasks, strict=True):
        for task in stage_tasks:
            _add_task(events, stage, task)
    for execution in executions:
        _add_execution(events, execution)
    if end is not None:
        events.add(end, {'Event': 'SparkListenerApplicationEnd', 'Timestamp': end})
    with out.open('w') as written:
        for event in events.ordered():
            written.write(json.dumps(event, separators=(',', ':')) + '\n')


class _Events:
    """Events gathered in any order, written as Spark posts them: the head first, then by time."""

    def __init__(self) -> None:
        self._head: list[dict[str, Any]] = []
        self._timed: list[tuple[float, int, int, dict[str, Any]]] = []

    def head(self, event: dict[str, Any]) -> None:
        self._head.append(event)

    def add(self, time: int | None, event: dict[str, Any]) -> None:
        when = math.inf if time is None else time
        self._timed.append((when, _RANKS[event['Event'].rpartition('.')[2]], len(self._timed), event))

    def ordered(self) -> Iterable[dict[str, Any]]:
        yield from self._head
        for *_, event in sorted(self._timed, key=lambda entry: entry[:3]):
            yield event


def _pages(client: RestClient, path: str, length: int, parameters: Mapping[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    while True:
        page = client.json(path, {**parameters, 'offset': len(rows), 'length': length}) or []
        rows.extend(page)
        if len(page) < length:
            return rows


def _tasks(client: RestClient, app_path: str, stages: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Every task attempt of each stage attempt, a page of 1,000 a request, the pages of every stage fetched together."""
    pages: list[tuple[int, int]] = []
    for position, stage in enumerate(stages):
        count = sum(int(stage.get(key) or 0) for key in ('numActiveTasks', 'numCompleteTasks', 'numFailedTasks', 'numKilledTasks'))
        pages.extend((position, offset) for offset in range(0, max(count, 1), _TASK_PAGE))

    def page(entry: tuple[int, int]) -> list[dict[str, Any]]:
        position, offset = entry
        stage = stages[position]
        path = f'{app_path}/stages/{stage["stageId"]}/{stage["attemptId"]}/taskList'
        return client.json(path, {'offset': offset, 'length': _TASK_PAGE}) or []

    tasks: list[list[dict[str, Any]]] = [[] for _ in stages]
    last: dict[int, tuple[int, int]] = {}
    for entry, rows in zip(pages, client.in_parallel(page, pages), strict=True):
        tasks[entry[0]].extend(rows)
        last[entry[0]] = (entry[1], len(rows))
    # A live stage can run more tasks than it had counted, so a full last page is followed on.
    for position, (offset, count) in last.items():
        while count == _TASK_PAGE:
            offset += _TASK_PAGE
            rows = page((position, offset))
            tasks[position].extend(rows)
            count = len(rows)
    return tasks


def _epoch_ms(text: object) -> int | None:
    """An instant as the REST API writes it, `2026-10-19T10:50:48.617GMT`, in ms since the epoch. None for none, or Spark's -1."""
    if not isinstance(text, str) or not text.endswith('GMT'):
        return None
    moment = dt.datetime.strptime(text.removesuffix('GMT'), '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=dt.UTC)
    epoch = round(moment.timestamp() * 1000)
    return epoch if epoch >= 0 else None


def _epoch(record: Mapping[str, Any], name: str) -> int | None:
    """An attempt's instant, from its `<name>Epoch` where the server writes one."""
    epoch = record.get(f'{name}Epoch')
    if isinstance(epoch, int):
        return epoch if epoch >= 0 else None
    return _epoch_ms(record.get(name))


def _pairs(rows: object) -> dict[str, str]:
    return {str(key): str(value) for key, value in rows or ()} if isinstance(rows, list) else {}


def _environment(environment: Mapping[str, Any]) -> dict[str, Any]:
    runtime = environment.get('runtime') or {}
    jvm = {'Java Home': runtime.get('javaHome'), 'Java Version': runtime.get('javaVersion'), 'Scala Version': runtime.get('scalaVersion')}
    return {
        'Event': 'SparkListenerEnvironmentUpdate',
        'JVM Information': {key: value for key, value in jvm.items() if value is not None},
        'Spark Properties': _pairs(environment.get('sparkProperties')),
        'Hadoop Properties': _pairs(environment.get('hadoopProperties')),
        'System Properties': _pairs(environment.get('systemProperties')),
        'Metrics Properties': _pairs(environment.get('metricsProperties')),
        'Classpath Entries': _pairs(environment.get('classpathEntries')),
    }


def _resource_profile(profile: Mapping[str, Any]) -> dict[str, Any]:
    executor_requests = {
        name: {
            'Resource Name': request.get('resourceName'),
            'Amount': request.get('amount'),
            'Discovery Script': request.get('discoveryScript'),
            'Vendor': request.get('vendor'),
        }
        for name, request in (profile.get('executorResources') or {}).items()
    }
    task_requests = {
        name: {'Resource Name': request.get('resourceName'), 'Amount': request.get('amount')}
        for name, request in (profile.get('taskResources') or {}).items()
    }
    return {
        'Event': 'SparkListenerResourceProfileAdded',
        'Resource Profile Id': profile.get('id'),
        'Executor Resource Requests': executor_requests,
        'Task Resource Requests': task_requests,
    }


def _add_executor(events: _Events, executor: Mapping[str, Any], end: int | None, stage_peaks_logged: bool) -> None:
    """The executor's add, its removal, and its peaks over the run.

    A History Server's peaks are the largest any stage's or task's event logged. So where the run
    logged no stage peaks they are the tasks', and go out as `EXECUTOR_PEAKS_EVENT`, which the build
    reads as task peaks.
    """
    executor_id = str(executor['id'])
    host, _, port = str(executor.get('hostPort') or '').rpartition(':')
    added = _epoch_ms(executor.get('addTime'))
    cores = int(executor.get('totalCores') or 0)
    # A driver outside local mode runs no tasks, and Spark logs it only through its block manager.
    if executor_id != 'driver' or cores > 0:
        info = {'Host': host or None, 'Total Cores': cores, 'Resource Profile Id': executor.get('resourceProfileId')}
        events.add(added, {'Event': 'SparkListenerExecutorAdded', 'Timestamp': added, 'Executor ID': executor_id, 'Executor Info': info})
    manager = {'Executor ID': executor_id, 'Host': host or None, 'Port': int(port) if port.isdigit() else None}
    events.add(
        added,
        {
            'Event': 'SparkListenerBlockManagerAdded',
            'Block Manager ID': manager,
            'Maximum Memory': executor.get('maxMemory'),
            'Timestamp': added,
        },
    )
    removed = _epoch_ms(executor.get('removeTime'))
    if removed is not None:
        removal = {'Event': 'SparkListenerExecutorRemoved', 'Timestamp': removed, 'Executor ID': executor_id}
        removal['Removed Reason'] = executor.get('removeReason')
        events.add(removed, removal)
    peaks = executor.get('peakMemoryMetrics')
    if peaks:
        kind = 'SparkListenerStageExecutorMetrics' if stage_peaks_logged else EXECUTOR_PEAKS_EVENT
        events.add(end, {'Event': kind, 'Executor ID': executor_id, 'Executor Metrics': peaks})


def _add_job(events: _Events, job: Mapping[str, Any], execution_id: int | None, failure: str | None) -> None:
    """The job's start, and its end where it ended.

    `JobData` holds no failure, but Spark ends each running stage only the failed job used with the
    message the job failed with. So `failure` is the first such stage's.
    """
    properties: dict[str, str] = {}
    if execution_id is not None:
        properties['spark.sql.execution.id'] = str(execution_id)
    if job.get('jobGroup') is not None:
        properties['spark.jobGroup.id'] = str(job['jobGroup'])
    if job.get('description') is not None:
        properties['spark.job.description'] = str(job['description'])
    if job.get('jobTags'):
        properties['spark.job.tags'] = ','.join(str(tag) for tag in job['jobTags'])
    submitted = _epoch_ms(job.get('submissionTime'))
    start = {
        'Event': 'SparkListenerJobStart',
        'Job ID': job['jobId'],
        'Submission Time': submitted,
        'Stage IDs': list(job.get('stageIds') or ()),
        'Properties': properties,
    }
    events.add(submitted, start)
    status = job.get('status')
    if status in ('SUCCEEDED', 'FAILED'):
        completed = _epoch_ms(job.get('completionTime'))
        result: dict[str, Any] = {'Result': 'JobSucceeded' if status == 'SUCCEEDED' else 'JobFailed'}
        if status == 'FAILED' and failure is not None:
            result['Exception'] = {'Message': failure}
        events.add(completed, {'Event': 'SparkListenerJobEnd', 'Job ID': job['jobId'], 'Completion Time': completed, 'Job Result': result})


def _add_stage(events: _Events, stage: Mapping[str, Any]) -> None:
    submitted = _epoch_ms(stage.get('submissionTime'))
    info: dict[str, Any] = {
        'Stage ID': stage['stageId'],
        'Stage Attempt ID': stage['attemptId'],
        'Stage Name': stage.get('name'),
        'Number of Tasks': stage.get('numTasks'),
        'Submission Time': submitted,
        'Resource Profile Id': stage.get('resourceProfileId'),
    }
    events.add(submitted, {'Event': 'SparkListenerStageSubmitted', 'Stage Info': info})
    if stage.get('status') in _ENDED_STAGES:
        completed = _epoch_ms(stage.get('completionTime'))
        ended = {**info, 'Completion Time': completed}
        if stage.get('failureReason'):
            ended['Failure Reason'] = stage['failureReason']
        events.add(completed, {'Event': 'SparkListenerStageCompleted', 'Stage Info': ended})


def _add_task(events: _Events, stage: Mapping[str, Any], task: Mapping[str, Any]) -> None:
    launched = _epoch_ms(task.get('launchTime'))
    status = task.get('status')
    info: dict[str, Any] = {
        'Task ID': task['taskId'],
        'Index': task.get('index'),
        'Attempt': task.get('attempt'),
        'Launch Time': launched,
        'Executor ID': task.get('executorId'),
        'Host': task.get('host'),
        'Locality': task.get('taskLocality'),
        'Speculative': task.get('speculative'),
    }
    if task.get('partitionId') is not None:
        info['Partition ID'] = task['partitionId']
    ids = {'Stage ID': stage['stageId'], 'Stage Attempt ID': stage['attemptId']}
    duration = task.get('duration')
    if status == 'RUNNING' or launched is None or duration is None:
        events.add(launched, {'Event': 'SparkListenerTaskStart', **ids, 'Task Info': info})
        return
    finished = launched + int(duration)
    fetch_started = _epoch_ms(task.get('resultFetchStart'))
    info.update(
        {
            'Finish Time': finished,
            'Getting Result Time': fetch_started or 0,
            'Failed': status == 'FAILED',
            'Killed': status == 'KILLED',
        }
    )
    end = {'Event': 'SparkListenerTaskEnd', **ids, 'Task End Reason': _end_reason(status, task.get('errorMessage')), 'Task Info': info}
    metrics = task.get('taskMetrics')
    # The server serves zeros for a task that ended reporting no metrics, as one lost with its executor does.
    if metrics and (status == 'SUCCESS' or _reported(metrics)):
        end['Task Metrics'] = _task_metrics(metrics)
    events.add(finished, end)


def _reported(metrics: Mapping[str, Any]) -> bool:
    return any(_reported(value) if isinstance(value, Mapping) else bool(value) for value in metrics.values())


def _end_reason(status: object, message: object) -> dict[str, Any]:
    """The end reason Spark wrote `message` from, as `JsonProtocol` logs it, read back from the string `toErrorString` made of it."""
    if status == 'SUCCESS':
        return {'Reason': 'Success'}
    text = str(message or '')
    for pattern, reason, field in _REASON_STRINGS:
        if matched := pattern.fullmatch(text):
            return {'Reason': reason, field: matched['detail']} if field else {'Reason': reason}
    if status == 'KILLED':
        return {'Reason': 'TaskKilled', 'Kill Reason': text or None}
    # An exception's string is its class and message, then its stack, one frame a line from `\tat `.
    first, _, rest = text.partition('\n')
    class_name, _, description = first.partition(': ')
    message_lines = [description, *(line for line in _until_stack(rest.split('\n')))]
    return {'Reason': 'ExceptionFailure', 'Class Name': class_name or None, 'Description': '\n'.join(message_lines) or None}


def _until_stack(lines: list[str]) -> Iterable[str]:
    for line in lines:
        if line.startswith('\tat '):
            return
        yield line


def _task_metrics(metrics: Mapping[str, Any]) -> dict[str, Any]:
    shuffle_read = metrics.get('shuffleReadMetrics') or {}
    shuffle_write = metrics.get('shuffleWriteMetrics') or {}
    read = metrics.get('inputMetrics') or {}
    written = metrics.get('outputMetrics') or {}
    return {
        'Executor Deserialize Time': metrics.get('executorDeserializeTime'),
        'Executor Deserialize CPU Time': metrics.get('executorDeserializeCpuTime'),
        'Executor Run Time': metrics.get('executorRunTime'),
        'Executor CPU Time': metrics.get('executorCpuTime'),
        'Peak Execution Memory': metrics.get('peakExecutionMemory'),
        'Result Size': metrics.get('resultSize'),
        'JVM GC Time': metrics.get('jvmGcTime'),
        'Result Serialization Time': metrics.get('resultSerializationTime'),
        'Memory Bytes Spilled': metrics.get('memoryBytesSpilled'),
        'Disk Bytes Spilled': metrics.get('diskBytesSpilled'),
        'Shuffle Read Metrics': {
            'Remote Blocks Fetched': shuffle_read.get('remoteBlocksFetched'),
            'Local Blocks Fetched': shuffle_read.get('localBlocksFetched'),
            'Fetch Wait Time': shuffle_read.get('fetchWaitTime'),
            'Remote Bytes Read': shuffle_read.get('remoteBytesRead'),
            'Remote Bytes Read To Disk': shuffle_read.get('remoteBytesReadToDisk'),
            'Local Bytes Read': shuffle_read.get('localBytesRead'),
            'Total Records Read': shuffle_read.get('recordsRead'),
        },
        'Shuffle Write Metrics': {
            'Shuffle Bytes Written': shuffle_write.get('bytesWritten'),
            'Shuffle Write Time': shuffle_write.get('writeTime'),
            'Shuffle Records Written': shuffle_write.get('recordsWritten'),
        },
        'Input Metrics': {'Bytes Read': read.get('bytesRead'), 'Records Read': read.get('recordsRead')},
        'Output Metrics': {'Bytes Written': written.get('bytesWritten'), 'Records Written': written.get('recordsWritten')},
    }


def _add_execution(events: _Events, execution: Mapping[str, Any]) -> None:
    execution_id = int(execution['id'])
    submitted = _epoch_ms(execution.get('submissionTime'))
    start: dict[str, Any] = {
        'Event': f'{_SQL_EXECUTION}Start',
        'executionId': execution_id,
        'description': execution.get('description'),
        'physicalPlanDescription': execution.get('planDescription'),
        'time': submitted,
    }
    if execution.get('rootExecutionId') is not None:
        start['rootExecutionId'] = execution['rootExecutionId']
    events.add(submitted, start)
    metrics = _plan_metrics(execution)
    if metrics:
        events.add(submitted, {'Event': PLAN_METRICS_EVENT, 'executionId': execution_id, 'restPlanMetrics': metrics})
    if execution.get('status') in ('COMPLETED', 'FAILED'):
        ended = _epoch_ms(execution.get('completionTime'))
        if ended is None and submitted is not None and execution.get('duration') is not None:
            ended = submitted + int(execution['duration'])
        events.add(
            ended,
            {'Event': f'{_SQL_EXECUTION}End', 'executionId': execution_id, 'time': ended, 'errorMessage': execution.get('errorMessage')},
        )


def _plan_metrics(execution: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Each metric the graph displays, on the node of the text plan in its place, numbered as `plan_nodes` numbers it.

    The graph leaves out every query stage the text prints, so those are stepped over. A graph
    whose operators differ from the text's gives no metrics, and a subquery that differs gives none
    for its own nodes.
    """
    text = execution.get('planDescription')
    if not text or not execution.get('nodes'):
        return []
    plan = plan_for_event({'sparkPlanInfo': None, 'physicalPlanDescription': text}, int(execution['id']), 0)
    try:
        graph = parse_rest_graph(execution)
    except ValueError:
        return []
    if plan is None:
        return []
    rows = []
    for number, node in _paired_nodes(plan, graph):
        named: set[str] = set()
        for metric in node.metrics:
            value = metric.display_value
            if value is None or metric.name in named:
                continue
            named.add(metric.name)
            # Only an average prints a spread with no total, and an average has no unit.
            unit = None if value.total is None and value.maximum is not None else value.unit
            rows.append(
                {
                    'node': number,
                    'name': metric.name,
                    'unit': unit,
                    'total': value.total,
                    'min': value.minimum,
                    'median': value.median,
                    'max': value.maximum,
                    'maxStageId': value.max_stage_id,
                    'maxTaskId': value.max_task_id,
                }
            )
    return rows


def _paired_nodes(text: Plan, graph: Plan) -> list[tuple[int, PlanNode]]:
    """Each text node's number in `text.numbered()` beside the graph node in its place, the main plan's first."""
    text_parts, graph_parts = _numbered_parts(text), _numbered_parts(graph)
    pairs = _paired_part(*text_parts[0], graph_parts[0][0])
    if pairs is None:
        return []
    if len(text_parts) == len(graph_parts):
        for (text_part, offset), (graph_part, _) in zip(text_parts[1:], graph_parts[1:], strict=True):
            pairs.extend(_paired_part(text_part, offset, graph_part) or ())
    return pairs


def _numbered_parts(plan: Plan) -> list[tuple[Plan, int]]:
    """The plan and each subquery, depth first, with the number its first node takes, as `Plan.numbered` numbers them."""
    parts: list[tuple[Plan, int]] = []

    def add(part: Plan, offset: int) -> int:
        parts.append((part, offset))
        position = offset + len(part.nodes)
        for subquery in part.subqueries:
            position = add(subquery, position)
        return position

    add(plan, 0)
    return parts


def _paired_part(text: Plan, offset: int, graph: Plan) -> list[tuple[int, PlanNode]] | None:
    if not text.nodes or not graph.nodes:
        return None
    pairs: list[tuple[int, PlanNode]] = []
    pending = [(0, 0)]
    while pending:
        text_id, graph_id = pending.pop()
        node = text.nodes[text_id]
        while node.operator.endswith('QueryStage') and len(node.children) == 1:
            node = text.nodes[node.children[0]]
        graph_node = graph.nodes[graph_id]
        if node.operator != graph_node.operator or len(node.children) != len(graph_node.children):
            return None
        pairs.append((node.node_id + offset, graph_node))
        pending.extend(zip(node.children, graph_node.children, strict=True))
    return pairs
