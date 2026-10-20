"""The tables select each partition's effective task, keep Spark's units, and aggregate as the SQL tab does."""

import collections
import copy
import itertools
import json
import pathlib
import struct
from decimal import ROUND_HALF_EVEN
from decimal import ROUND_HALF_UP
from decimal import Decimal
from typing import Any

import cramjam
import pytest
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
        'select max(shuffle_read_records), median(shuffle_read_records), max(executor_run_time_ms) from tasks_effective where stage_id = 4'
    )
    assert stage.fetchone() == (32_536_313, 506_215, 25_607)
    cpu = tables.query('select executor_cpu_time_ns from tasks_effective where task_id = ?', [longest['Task Info']['Task ID']])
    assert cpu.fetchone() == (longest['Task Metrics']['Executor CPU Time'],)


SQL_TAB_CAPTURES = sorted((FIXTURES / 'history-server-sql').glob('*/*.json.zstd'))


@pytest.mark.parametrize('capture', SQL_TAB_CAPTURES, ids=lambda capture: capture.parent.name)
def test_plan_node_metrics_print_as_the_history_servers_sql_tab(capture: pathlib.Path, tmp_path: pathlib.Path) -> None:
    log = FIXTURES / 'logs' / capture.parent.name / capture.name.replace('.json.zstd', '.zstd')
    tables = EventLog.open(log, cache_dir=tmp_path).tables
    attempts = dict(tables.query('select stage_id, attempt from stages where is_highest_attempt').fetchall())
    rows = tables.query(
        """
        select execution_id, metric_name, metric_type, value_count, total, min, median, max, max_stage_id, max_task_id
        from plan_node_metrics join sql_executions using (execution_id)
        where plan_version = plan_version_count - 1 and (total is not null or min is not null)
        """
    )
    ours: dict[int, collections.Counter[tuple[str, str]]] = collections.defaultdict(collections.Counter)
    for execution_id, name, kind, count, total, low, middle, high, stage, task in rows.fetchall():
        show = _bytes if kind == 'size' else _duration
        if kind == 'sum':
            shown = f'{int(total):,}'
        elif kind == 'average':
            shown = _average(low)  # every captured average holds one value
        elif count <= 1:
            shown = show(high)
        else:
            spread = f'{show(low)}, {show(middle)}, {show(high)} (stage {stage}.{attempts[stage]}: task {task})'
            shown = f'total (min, med, max (stageId: taskId))\n{show(total)} ({spread})'
        ours[execution_id][name.strip(), shown] += 1

    served = json.loads(bytes(cramjam.zstd.decompress(capture.read_bytes())))
    theirs = {
        execution['id']: collections.Counter((metric['name'], metric['value']) for node in execution['nodes'] for metric in node['metrics'])
        for execution in served
    }
    assert ours == {execution_id: shown for execution_id, shown in theirs.items() if shown}


def _average(value: float) -> str:
    """Java's NumberFormat as Spark prints an average: grouped, at most three decimals, half-even."""
    text = f'{Decimal(repr(value)).quantize(Decimal("0.001"), ROUND_HALF_EVEN):,}'
    return text.rstrip('0').rstrip('.')


def _bytes(size: float) -> str:
    """Spark's Utils.bytesToString."""
    units = ((1 << 30, 'GiB'), (1 << 20, 'MiB'), (1 << 10, 'KiB'))
    scale, unit = next(((scale, unit) for scale, unit in units if size >= 2 * scale), (1, 'B'))
    return f'{(Decimal(int(size)) / scale).quantize(Decimal("0.1"), ROUND_HALF_UP)} {unit}'


def _duration(ms: float) -> str:
    """Spark's Utils.msDurationToString for under a minute, which divides as a Java float."""
    if ms < 1000:
        return f'{int(ms)} ms'
    (seconds,) = struct.unpack('f', struct.pack('f', ms / 1000))
    return f'{Decimal(repr(seconds)).quantize(Decimal("0.1"), ROUND_HALF_UP)} s'
