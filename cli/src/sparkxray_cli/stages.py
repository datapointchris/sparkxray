"""`sparkxray stages`: every stage's tasks, time, shuffle, spill and GC, and one stage's task quantiles."""

from __future__ import annotations

import shlex
from typing import Annotated

import typer
from rich.text import Text
from sparkxray.summaries import stage_detail
from sparkxray.summaries import stage_summaries

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import SortOption

app = typer.Typer(no_args_is_help=True, help='Find the stages that took the time, and see how evenly their tasks shared it.')

_LIST_COLUMNS = (
    'stage_id',
    'name',
    'status',
    'task_count',
    'failed_task_count',
    'wall_ms',
    'wall_share',
    'median_task_duration_ms',
    'max_task_duration_ms',
    'shuffle_read_bytes',
    'shuffle_write_bytes',
    'disk_spilled_bytes',
    'gc_time_ms',
    'wasted_core_hours',
)


@app.command('list', no_args_is_help=True)
def list_stages(log: LogArgument, sort: SortOption = None, limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List every stage: its tasks, wall time and share of the run, task durations, shuffle, spill, GC and idle cores.

    wall_ms runs from the stage's first submission to its last completion, and wall_share is
    that time over the run's. Totals count each partition once, from its first successful task.
    disk_spilled_bytes is Spark's on-disk bytes spilled. A stage's wasted_core_hours are the core
    time no task of any stage held, from its first task's launch to its end, so `--sort
    wasted_core_hours` puts the stage that idled the most first.

    `--json` and `stages show` add the in-memory bytes spilled, the core-hours available and
    used, and the parallelism ratio: the stage's partitions over the run's task slots, so one
    under 1 left slots idle.
    """
    rows = stage_summaries(open_tables(log), sort=sort, limit=limit)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.empty('stages', limit)
    else:
        render.print_rows(rows, _LIST_COLUMNS)
        render.note(f'one in full: {shlex.join(["sparkxray", "stages", "show", log, str(rows.to_json()[0]["stage_id"])])}')


@app.command('show', no_args_is_help=True)
def show(
    log: LogArgument,
    stage_id: Annotated[int, typer.Argument(metavar='STAGE_ID', help='A stage id, as stages list prints it.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Show one stage, and the min, p25, p50 (the median), p75, p95 and max of every metric over its successful tasks.

    The quantiles are taken as the Stages tab takes its summary metrics, so a max far above the
    p50 is the skew the Spark UI would show. A stage a job skipped, because an earlier stage had
    written its shuffle output, ran no tasks and has none to show.
    """
    detail = stage_detail(open_tables(log), stage_id)
    if as_json:
        render.emit_json(detail.to_json())
        return
    render.print_record(detail.stage, title=f'Stage {stage_id}')
    quantiles = detail.task_quantiles
    table = render.table(title='Task metrics')
    table.add_column('metric')
    labels = [row[0] for row in quantiles.values]
    for label in labels:
        table.add_column(label, justify='right')
    for position, metric in enumerate(quantiles.columns[1:], start=1):
        table.add_row(metric, *(Text(render.shown(metric, row[position])) for row in quantiles.values))
    render.print_table(table)
    tasks = ['sparkxray', 'tasks', 'list', log, '--stage', str(stage_id), '--sort', 'duration_ms', '-n', '5']
    render.note(f'its slowest tasks: {shlex.join(tasks)}')
