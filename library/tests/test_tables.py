"""The tables select each partition's effective task and keep Spark's units."""

import copy
import itertools
import json
import pathlib
from typing import Any

from fixture_logs import FIXTURES
from fixture_logs import plain_lines
from sparkxray.ingest import EventLog


def test_tasks_effective_keeps_the_first_success_of_each_partition_of_each_stage(tmp_path: pathlib.Path) -> None:
    events = [json.loads(line) for line in plain_lines(FIXTURES / 'logs' / 'spark-3.5.9' / 'local-1792406949080.zstd')]
    task_ids = itertools.count(1 + max(event['Task Info']['Task ID'] for event in events if 'Task Info' in event))

    def attempt_of(task_end: dict[str, Any], *, records_factor: int = 1, **info: Any) -> dict[str, Any]:
        copied = copy.deepcopy(task_end)
        copied['Task Info'].update({'Task ID': next(task_ids), **info})
        copied['Task Metrics']['Shuffle Read Metrics']['Total Records Read'] *= records_factor
        return copied

    skewed = [event for event in events if event['Event'] == 'SparkListenerTaskEnd' and event['Stage ID'] == 4]
    longest = max(skewed, key=lambda event: event['Task Metrics']['Executor Run Time'])
    failed = attempt_of(longest, records_factor=2, Failed=True)
    failed['Task End Reason'] = {'Reason': 'ExceptionFailure', 'Class Name': 'java.lang.RuntimeException', 'Description': 'injected'}
    duplicate = attempt_of(longest, records_factor=3, Speculative=True)
    # Stage 99 appears only in task events, as in a partial log. Partition 1 fails to fetch, and attempt 1 reruns it as Index 0.
    retried = [
        attempt_of(event, Index=index, **{'Partition ID': partition}) | {'Stage ID': 99, 'Stage Attempt ID': attempt}
        for attempt, index, partition, event in [(0, 0, 0, skewed[0]), (0, 1, 1, skewed[1]), (0, 2, 2, skewed[2]), (1, 0, 1, skewed[3])]
    ]
    retried[1]['Task Info']['Failed'] = True
    retried[1]['Task End Reason'] = {'Reason': 'FetchFailed', 'Message': 'injected'}
    position = events.index(longest)
    events[position : position + 1] = [failed, longest, duplicate]
    events.extend(retried)
    log = tmp_path / 'local-1792406949080'
    log.write_text(''.join(json.dumps(event) + '\n' for event in events))

    succeeded = {
        (event['Stage ID'], event['Task Info']['Partition ID'])
        for event in events
        if event['Event'] == 'SparkListenerTaskEnd' and event['Task End Reason']['Reason'] == 'Success'
    }

    tables = EventLog.open(log, cache_dir=tmp_path / 'cache').tables
    assert tables.query('select stage_id, partition_id from tasks_effective order by all').fetchall() == sorted(succeeded)
    assert tables.query('select attempt, name from stages where stage_id = 99 and is_highest_attempt').fetchall() == [(1, None)]
    stage = tables.query(
        'select max(shuffle_records_read), median(shuffle_records_read), max(executor_run_time_ms) from tasks_effective where stage_id = 4'
    )
    assert stage.fetchone() == (32_536_313, 506_215, 25_607)
    cpu = tables.query('select executor_cpu_time_ns from tasks_effective where task_id = ?', [longest['Task Info']['Task ID']])
    assert cpu.fetchone() == (longest['Task Metrics']['Executor CPU Time'],)
