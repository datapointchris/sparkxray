"""`sparkxray sql`: every SQL execution's duration, jobs, stages and operators, and one execution in full."""

from __future__ import annotations

from typing import Annotated

import typer
from sparkxray.summaries import sql_detail
from sparkxray.summaries import sql_summaries

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(no_args_is_help=True, help='Find the query or DataFrame action that took the time, and what its plan holds.')

_LIST_COLUMNS = (
    'execution_id',
    'description',
    'status',
    'duration_ms',
    'wall_share',
    'job_count',
    'stage_count',
    'exchange_count',
    'join_count',
    'scan_count',
)
_SHOW_KEYS = (
    'execution_id',
    'root_execution_id',
    'description',
    'status',
    'error',
    'start_ms',
    'duration_ms',
    'wall_share',
    'job_count',
    'stage_count',
    'plan_version_count',
    'node_count',
    'exchange_count',
    'join_count',
    'scan_count',
    'final_plan_truncated',
    'modified_configs',
)


@app.command('list')
def list_executions(log: LogArgument, limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List every SQL execution: its duration and share of the run, jobs, stages, and the operators of its last plan."""
    rows = sql_summaries(open_tables(log), limit=limit)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.none_found('SQL executions; the run used only RDDs')
    else:
        render.print_rows(rows, _LIST_COLUMNS)


@app.command('show')
def show(
    log: LogArgument,
    execution_id: Annotated[int, typer.Argument(metavar='ID', help='A SQL execution id, as sql list prints it.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Show one SQL execution, the jobs it ran, and how many of each operator its last plan holds."""
    detail = sql_detail(open_tables(log), execution_id)
    if as_json:
        render.emit_json(detail.to_json())
        return
    render.print_record(detail.execution, _SHOW_KEYS, title=f'SQL execution {execution_id}')
    render.print_rows(detail.jobs, title='Jobs')
    render.print_rows(detail.operator_counts, title='Operators in the last plan')
