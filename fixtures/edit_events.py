"""Edit the plain log `job/edited_events_job.py` writes, so each job seeds the rules its description names.

    python3 edit_events.py < plain-log > edited-log

Two executors on one host cannot fetch shuffle blocks slowly, wait on a busy driver, or run a
speculative copy, which Spark never places on a host already running the task. So each job runs
a stage shaped for one problem, and the edit for each rule its description lists under `fires=`
changes the fields that rule reads: a task's `Task Metrics` and `Task Info`, or an executor's
stage peaks. Accumulator updates stay as Spark wrote them. Executor 1 also moves to a host of
its own, for every event that names its host.
"""

import copy
import json
import sys
from collections.abc import Callable
from typing import Any

APPLICATION_PROPERTY = 'spark.sparkxray.fixture.application'
GIB = 1 << 30
# Both the executors' spark.executor.memory and the driver's default memory.
MEMORY_BYTES = GIB
SPECULATED_RUN_MS = 5000
Event = dict[str, Any]


def kind(event: Event) -> str:
    return event['Event'].rsplit('.', 1)[-1]


def fires(description: str) -> set[str]:
    """The rules a `fires=<ids>; silent=<ids>; <what it does>` description seeds."""
    if not description.startswith('fires='):
        return set()
    return set(description.split('; ', 1)[0].removeprefix('fires=').split(',')) - {''}


def succeeded(events: list[Event], stage_ids: set[int]) -> list[Event]:
    return [
        event
        for event in events
        if kind(event) == 'SparkListenerTaskEnd' and event['Stage ID'] in stage_ids and event['Task End Reason']['Reason'] == 'Success'
    ]


def shuffle_read_bytes(end: Event) -> int:
    read = end['Task Metrics']['Shuffle Read Metrics']
    return read['Remote Bytes Read'] + read['Local Bytes Read']


def gc_pressure(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """Each task's JVM GC time becomes a quarter of its run time."""
    for end in succeeded(events, stage_ids):
        end['Task Metrics']['JVM GC Time'] = end['Task Metrics']['Executor Run Time'] // 4
    return events


def scheduler_delay(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """Each task ends 700 ms later, outside its run time, which is how the History Server derives scheduler delay."""
    for end in succeeded(events, stage_ids):
        end['Task Info']['Finish Time'] += 700
    return events


def deserialization(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """Each task spends 700 ms more deserializing, and ends that much later."""
    for end in succeeded(events, stage_ids):
        end['Task Metrics']['Executor Deserialize Time'] += 700
        end['Task Info']['Finish Time'] += 700
    return events


def fetch_wait(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """Each task that read a shuffle spent 40% of its run time waiting for blocks."""
    for end in succeeded(events, stage_ids):
        if shuffle_read_bytes(end) > 0:
            end['Task Metrics']['Shuffle Read Metrics']['Fetch Wait Time'] = round(0.4 * end['Task Metrics']['Executor Run Time'])
    return events


def oversized_reads(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """Each task that read a shuffle read 1.1 GiB of it."""
    for end in succeeded(events, stage_ids):
        read = end['Task Metrics']['Shuffle Read Metrics']
        if shuffle_read_bytes(end) > 0:
            read['Remote Bytes Read'] = round(1.1 * GIB) - read['Local Bytes Read']
    return events


def executor_hosts(events: list[Event]) -> dict[str, str]:
    return {event['Executor ID']: event['Executor Info']['Host'] for event in events if kind(event) == 'SparkListenerExecutorAdded'}


def other_executor(executor_id: str) -> str:
    return '1' if executor_id == '0' else '0'


def speculation(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """Each task that ran 5 s or more gets a speculative copy on the other executor, launched 1.5 s after it.

    The copy runs like the stage's fast tasks and succeeds, and the original is killed as it does,
    as Spark kills the slower attempt.
    """
    ends = succeeded(events, stage_ids)
    fast = next(end for end in ends if end['Task Metrics']['Executor Run Time'] < SPECULATED_RUN_MS)
    fast_ms = fast['Task Info']['Finish Time'] - fast['Task Info']['Launch Time']
    hosts = executor_hosts(events)
    next_id = max(event['Task Info']['Task ID'] for event in events if kind(event) == 'SparkListenerTaskEnd') + 1
    copies: dict[int, list[Event]] = {}
    for original in ends:
        info = original['Task Info']
        if original['Task Metrics']['Executor Run Time'] < SPECULATED_RUN_MS:
            continue
        executor_id = other_executor(info['Executor ID'])
        launched = info['Launch Time'] + 1500
        winner = copy.deepcopy(fast)
        winner['Stage ID'], winner['Stage Attempt ID'] = original['Stage ID'], original['Stage Attempt ID']
        winner['Task Info'].update(
            {
                'Task ID': next_id,
                'Index': info['Index'],
                'Partition ID': info['Partition ID'],
                'Attempt': 1,
                'Speculative': True,
                'Executor ID': executor_id,
                'Host': hosts[executor_id],
                'Launch Time': launched,
                'Finish Time': launched + fast_ms,
            }
        )
        started = {
            'Event': 'SparkListenerTaskStart',
            'Stage ID': original['Stage ID'],
            'Stage Attempt ID': original['Stage Attempt ID'],
            'Task Info': {**copy.deepcopy(winner['Task Info']), 'Finish Time': 0, 'Accumulables': []},
        }
        killed_at = launched + fast_ms
        original['Task End Reason'] = {'Reason': 'TaskKilled', 'Kill Reason': 'another attempt succeeded', 'Accumulator Updates': []}
        info.update({'Killed': True, 'Finish Time': killed_at})
        metrics = original['Task Metrics']
        metrics['Executor Run Time'] = min(
            metrics['Executor Run Time'], killed_at - info['Launch Time'] - metrics['Executor Deserialize Time']
        )
        copies[id(original)] = [started, winner]
        next_id += 1
    return [spliced for event in events for spliced in (event, *copies.get(id(event), ()))]


def imbalance(events: list[Event], stage_ids: set[int]) -> list[Event]:
    """The first 80% of the stage's tasks to launch run on executor 0, and the rest on executor 1."""
    ends = sorted(succeeded(events, stage_ids), key=lambda end: (end['Task Info']['Launch Time'], end['Task Info']['Task ID']))
    hosts = executor_hosts(events)
    placed = {end['Task Info']['Task ID']: '0' if position < 0.8 * len(ends) else '1' for position, end in enumerate(ends)}
    for event in events:
        if kind(event) in ('SparkListenerTaskStart', 'SparkListenerTaskEnd') and event['Task Info']['Task ID'] in placed:
            executor_id = placed[event['Task Info']['Task ID']]
            event['Task Info'].update({'Executor ID': executor_id, 'Host': hosts[executor_id]})
    return events


def heap_peak(executor_id: str) -> Callable[[list[Event]], list[Event]]:
    """An edit raising `executor_id`'s first stage peak of JVM heap to 97% of its memory."""

    def edit(events: list[Event]) -> list[Event]:
        peaks = next(
            event for event in events if kind(event) == 'SparkListenerStageExecutorMetrics' and event['Executor ID'] == executor_id
        )
        peaks['Executor Metrics']['JVMHeapMemory'] = round(0.97 * MEMORY_BYTES)
        return events

    return edit


def second_host(events: list[Event]) -> list[Event]:
    """Executor 1 moves to a host of its own: the first host's address with its last number one higher."""
    first = executor_hosts(events)['1']
    head, _, last = first.rpartition('.')
    moved = f'{head}.{int(last) + 1}' if last.isdigit() else f'{first}-2'
    for event in events:
        name = kind(event)
        if name == 'SparkListenerExecutorAdded' and event['Executor ID'] == '1':
            event['Executor Info']['Host'] = moved
            event['Executor Info']['Log Urls'] = {key: url.replace(first, moved) for key, url in event['Executor Info']['Log Urls'].items()}
        elif name == 'SparkListenerBlockManagerAdded' and event['Block Manager ID']['Executor ID'] == '1':
            event['Block Manager ID']['Host'] = moved
        elif name in ('SparkListenerTaskStart', 'SparkListenerTaskEnd') and event['Task Info']['Executor ID'] == '1':
            event['Task Info']['Host'] = moved
    return events


STAGE_EDITS: dict[str, Callable[[list[Event], set[int]], list[Event]]] = {
    'stage.gc-pressure': gc_pressure,
    'stage.scheduler-delay': scheduler_delay,
    'stage.deserialization': deserialization,
    'stage.fetch-wait': fetch_wait,
    'stage.speculation': speculation,
    'stage.executor-imbalance': imbalance,
    'stage.oversized-partitions': oversized_reads,
}
RUN_EDITS: dict[str, Callable[[list[Event]], list[Event]]] = {
    'executor.memory-under': heap_peak('0'),
    'executor.driver-memory-under': heap_peak('driver'),
}


def main() -> None:
    events = second_host([json.loads(line) for line in sys.stdin])
    seeded: dict[str, set[int]] = {}
    for event in events:
        if kind(event) == 'SparkListenerJobStart':
            for rule_id in fires(event['Properties'].get('spark.job.description', '')) & set(STAGE_EDITS):
                seeded.setdefault(rule_id, set()).update(event['Stage IDs'])
    environment = next(event for event in events if kind(event) == 'SparkListenerEnvironmentUpdate')
    run_seeds = fires(environment['Spark Properties'].get(APPLICATION_PROPERTY, '')) & set(RUN_EDITS)
    unseeded = sorted((set(STAGE_EDITS) - set(seeded)) | (set(RUN_EDITS) - run_seeds))
    if unseeded:
        sys.exit(f"edit_events.py: no job or run property names {', '.join(unseeded)}, so the log is not the edited-events job's")
    for rule_id, stage_ids in seeded.items():
        events = STAGE_EDITS[rule_id](events, stage_ids)
    for rule_id in sorted(run_seeds):
        events = RUN_EDITS[rule_id](events)
    sys.stdout.writelines(json.dumps(event, separators=(',', ':'), ensure_ascii=False) + '\n' for event in events)


if __name__ == '__main__':
    main()
