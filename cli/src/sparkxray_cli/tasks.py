"""`sparkxray tasks`: the task attempts of one stage."""

from __future__ import annotations

import shlex
from typing import Annotated

import typer
from sparkxray.summaries import stage_tasks

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import SortOption

app = typer.Typer(no_args_is_help=True, help="Read one stage's tasks, to find the partition or executor a stage waited on.")

_LIST_COLUMNS = (
    'task_id',
    'partition_id',
    'stage_attempt',
    'attempt',
    'executor_id',
    'duration_ms',
    'executor_run_time_ms',
    'jvm_gc_time_ms',
    'input_bytes',
    'shuffle_read_bytes',
    'shuffle_write_bytes',
    'disk_spilled_bytes',
    'end_reason',
)


@app.command('list', no_args_is_help=True)
def list_tasks(
    log: LogArgument,
    stage: Annotated[int, typer.Option('--stage', metavar='STAGE_ID', help='The stage whose tasks to list.', show_default=False)],
    sort: SortOption = None,
    limit: LimitOption = None,
    as_json: JsonOption = False,
) -> None:
    """List every task attempt of one stage, retries and speculative copies included.

    `--sort duration_ms -n 5` gives the five slowest. `--json` carries every column of the
    tasks table, and `tables query` reads them all.
    """
    rows = stage_tasks(open_tables(log), stage, sort=sort, limit=limit)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.empty(f'tasks in stage {stage}', limit)
    else:
        render.print_rows(rows, _LIST_COLUMNS)
        render.note(f'every column: {shlex.join(["sparkxray", "tasks", "list", log, "--stage", str(stage), "--json"])}')
