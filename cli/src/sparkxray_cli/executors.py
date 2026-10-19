"""`sparkxray executors`: each executor's lifetime, cores and memory, the tasks it ran, and its idle share."""

from __future__ import annotations

import typer
from sparkxray.summaries import executor_summaries

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(no_args_is_help=True, help='See whether the run had more executors than work, and which ones failed tasks.')

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
    'remove_reason',
)


@app.command('list')
def list_executors(log: LogArgument, limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List the driver and every executor: lifetime, cores, memory, task time, idle share and peak heap.

    The idle share is the part of an executor's core time, lifetime times cores, that no task ran in.
    """
    rows = executor_summaries(open_tables(log), limit=limit)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.none_found('executors')
    else:
        render.print_rows(rows, _LIST_COLUMNS)
