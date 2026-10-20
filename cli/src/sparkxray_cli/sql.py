"""`sparkxray sql`: every SQL execution's duration, jobs, stages and operators, and one execution in full."""

from __future__ import annotations

import shlex
from typing import Annotated
from typing import Any

import typer
from sparkxray.plan import without_ids
from sparkxray.summaries import sql_detail
from sparkxray.summaries import sql_summaries

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(
    no_args_is_help=True,
    help='Find the SQL execution, a query or DataFrame action, that took the time, and what its plan holds. '
    'To run SQL of your own, use tables query.',
)

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
    'start_epoch_ms',
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
_JOIN_CHANGE_KEYS = ('node', 'initial_operator', 'final_operator', 'skew_split', 'join_type', 'left_keys', 'right_keys')
_OPERATOR_CHANGE_KEYS = ('operator', 'initial_count', 'final_count')
# Each shuffle read's --json name under the one-word header its table prints, which sql show --help defines.
_SHUFFLE_READ_HEADERS = {
    'exchange_partition_count': 'written',
    'partition_count': 'read',
    'coalesced_partition_count': 'coalesced',
    'skewed_partition_count': 'skewed',
    'skewed_split_count': 'splits',
    'empty_partition_count': 'empty',
    'data_bytes': 'data',
}


@app.command('list', no_args_is_help=True)
def list_executions(log: LogArgument, limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List every SQL execution: its duration and share of the run, jobs, stages, and the operators of its last plan."""
    rows = sql_summaries(open_tables(log), limit=limit)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.empty('SQL executions; the run used only RDDs', limit)
    else:
        render.print_rows(rows, _LIST_COLUMNS)
        render.note(f'one in full: {shlex.join(["sparkxray", "sql", "show", log, str(rows.to_json()[0]["execution_id"])])}')


@app.command('show', no_args_is_help=True)
def show(
    log: LogArgument,
    execution_id: Annotated[int, typer.Argument(metavar='ID', help='A SQL execution id, as sql list prints it.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Show one SQL execution, the jobs it ran, how many of each operator its last plan holds, and what AQE changed in it.

    AQE is adaptive query execution, which re-plans a query as its stages finish. A job's
    skipped_stage_ids are the stages it skipped, because an earlier stage had written their
    shuffle output. A node is numbered as `plans show` numbers the last plan, such as n9.

    In the shuffle reads AQE planned, `written` is the partitions the exchange below wrote, and
    `read` the partitions AQE read them as. `coalesced` counts the reads that merged small
    partitions, `skewed` the partitions split for skew, `splits` the reads those became, and
    `empty` the partitions that held no rows. `data` is the bytes read.
    """
    detail = sql_detail(open_tables(log), execution_id)
    if as_json:
        render.emit_json(detail.to_json())
        return
    render.print_record(detail.execution, _SHOW_KEYS, title=f'SQL execution {execution_id}')
    render.print_rows(detail.jobs, title='Jobs')
    render.print_rows(detail.operator_counts, title='Operators in the last plan')
    changes = detail.aqe_changes
    if changes is not None:
        _print_aqe_changes(changes.to_json())
    render.note(f'its plan as a tree: {shlex.join(["sparkxray", "plans", "show", log, "--sql", str(execution_id)])}')


def _print_aqe_changes(printed: dict[str, Any]) -> None:
    render.print_record(printed, ['update_count'], title='AQE')
    if printed['joins']:
        joins = [
            {
                **join,
                'node': f'n{join["final_node_id"]}',
                **{side: [without_ids(key) for key in join[side]] for side in ('left_keys', 'right_keys')},
            }
            for join in printed['joins']
        ]
        render.print_records(joins, _JOIN_CHANGE_KEYS, title='Joins AQE changed')
    if printed['operators']:
        render.print_records(printed['operators'], _OPERATOR_CHANGE_KEYS, title='Operators AQE added or removed')
    if printed['shuffle_reads']:
        reads = [
            {
                'node': f'n{read["node_id"]}',
                'kind': read['kind'],
                **{header: render.shown(name, read[name]) for name, header in _SHUFFLE_READ_HEADERS.items()},
            }
            for read in printed['shuffle_reads']
        ]
        headers = tuple(_SHUFFLE_READ_HEADERS.values())
        render.print_records(reads, ('node', 'kind', *headers), title='Shuffle reads AQE planned', numbers=headers)
    if not (printed['joins'] or printed['operators'] or printed['shuffle_reads']):
        render.none_found('join, operator or shuffle read AQE changed')
