"""`sparkxray executors`: each executor's lifetime, cores, memory, tasks and idle share, and the run at other executor counts."""

from __future__ import annotations

from typing import Annotated

import typer
from sparkxray.simulation import MODEL_LIMITS
from sparkxray.simulation import Simulation
from sparkxray.simulation import simulate
from sparkxray.summaries import executor_summaries

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(
    no_args_is_help=True,
    help='See whether the run had more executors than work, which ones failed tasks, and how it would run on more or fewer.',
)

_REFERENCE_KEYS = ('wall_ms', 'driver_ms', 'executor_ms', 'critical_path_ms', 'perfect_parallel_ms', 'single_core_ms')
_CURVE_COLUMNS = ('executor_count', 'estimated_wall_ms', 'utilization_share', 'core_hours')
_MODEL_KEYS = (
    'executor_count',
    'executor_core_count',
    'model_estimate_ms',
    'wall_ms',
    'model_error_share',
    'job_group_count',
    'overlap_merge_count',
    'retry_run_ms',
)
_CORE_HOUR_KEYS = (
    'available_core_hours',
    'executor_lifetime_core_hours',
    'used_core_hours',
    'driver_wasted_core_hours',
    'executor_wasted_core_hours',
)


class ExecutorCounts(tuple[int, ...]):
    """The executor counts `--executors` names."""


def executor_counts(text: str) -> ExecutorCounts:
    """`2..64` as every count from 2 to 64, or `4,8,16` as those three."""
    try:
        if '..' in text:
            low, high = (int(bound) for bound in text.split('..', 1))
            counts = ExecutorCounts(range(low, high + 1))
        else:
            counts = ExecutorCounts(int(count) for count in text.split(','))
    except ValueError:
        raise typer.BadParameter('give a range as 2..64 or a list as 4,8,16') from None
    if not counts or min(counts) < 1:
        raise typer.BadParameter('a range runs from a low count to a high one, and every count is at least 1')
    return counts


_LIST_COLUMNS = (
    'executor_id',
    'host',
    'core_count',
    'executor_memory_bytes',
    'lifetime_ms',
    'task_count',
    'failed_task_count',
    'task_time_ms',
    'idle_share',
    'peak_jvm_heap_bytes',
    'peak_memory_source',
    'remove_reason',
)


@app.command('list')
def list_executors(log: LogArgument, limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List the driver and every executor: lifetime, cores, memory, task time, idle share and peak heap.

    The idle share is the part of an executor's core time, lifetime times cores, that no task held,
    each task holding spark.task.cpus cores. The peak heap's source says whether it came from
    per-stage executor metrics or only from task-end polls.
    """
    rows = executor_summaries(open_tables(log), limit=limit)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.none_found('executors')
    else:
        render.print_rows(rows, _LIST_COLUMNS)


@app.command('simulate')
def simulate_executors(
    log: LogArgument,
    executors: Annotated[
        ExecutorCounts | None,
        typer.Option(
            '--executors',
            metavar='COUNTS',
            parser=executor_counts,
            help='The executor counts to estimate, a range as 2..64 or a list as 4,8,16. '
            'By default 10% to 500% of the most the run had alive at once.',
            show_default=False,
        ),
    ] = None,
    cores: Annotated[
        int | None,
        typer.Option(
            '--cores',
            min=1,
            help="Each executor's cores. By default the log's, those of the resource profile most executors ran under.",
            show_default=False,
        ),
    ] = None,
    as_json: JsonOption = False,
) -> None:
    """Estimate the run's wall time at other executor counts, and split its time between the driver and the executors.

    Each group of jobs that ran together is replayed onto the executors' task slots, every task
    taking the run time it took, as Sparklens's model replays them. An executor has its cores over
    spark.task.cpus slots. The time no job ran stays the same at every count. The reference times
    frame the curve: the run on unlimited executors, at today's size with no skew, and one task at
    a time. The model's error is its estimate at the run's own size against the run's wall time.

    Exits 0 when it ran and 2 on a usage error. Exits 3 when the log could not be read, or its stages could not be replayed.
    """
    simulation = simulate(open_log(log), executors=executors, cores=cores)
    if as_json:
        render.emit_json(simulation.to_json())
    else:
        _print_simulation(simulation)
    if simulation.resource_profile_count > 1:
        render.note(
            f'executors ran under {simulation.resource_profile_count} resource profiles, '
            f'and the cores are those of profile {simulation.resource_profile_id}, which most ran under'
        )
    if simulation.curve:
        return
    curve_cores = simulation.curve_executor_core_count
    if curve_cores is None or curve_cores >= simulation.task_cpu_count:
        render.note("no curve was estimated, because the log names no executor's cores; --executors and --cores set them")
    else:
        render.note(
            f'no curve was estimated, because {simulation.curve_executor_core_count} cores an executor are fewer than '
            f'the {simulation.task_cpu_count} spark.task.cpus each task holds; --cores sets them'
        )


def _print_simulation(simulation: Simulation) -> None:
    document = simulation.to_json()
    render.print_record(document, _REFERENCE_KEYS, title='Reference times')
    if document['curve']:
        columns = (*_CURVE_COLUMNS, 'dpu_hours') if simulation.platform == 'glue' else _CURVE_COLUMNS
        render.print_records(
            document['curve'], columns, title=f'Estimated wall time at {simulation.curve_executor_core_count} cores an executor'
        )
    render.print_record(document, _MODEL_KEYS, title='Model check')
    render.print_record(document, _CORE_HOUR_KEYS, title='Core-hours')
    render.print_list('What the model cannot see', MODEL_LIMITS)
