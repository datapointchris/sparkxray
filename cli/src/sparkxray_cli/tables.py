"""`sparkxray tables`: the Parquet tables a log reads into, and SQL over them."""

from __future__ import annotations

from typing import Annotated

import typer

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(no_args_is_help=True, help='Ask the run anything with SQL over its tables, which any Parquet reader also opens.')

_COLUMN_KEYS = ('name', 'type', 'unit', 'description')


@app.command('list', no_args_is_help=True)
def list_tables(log: LogArgument, as_json: JsonOption = False) -> None:
    """List the tables and views tables query reads in one log: row counts, each Parquet file, and every column's type, unit and meaning.

    findings appears once `findings list` or `reports write` has run on the log. `metrics list`
    gives every column with a unit, and every plan metric, with no log needed.
    """
    listed = open_tables(log).listing()
    if as_json:
        render.emit_json([table.to_json() for table in listed])
        return
    for entry in listed:
        render.print_described(
            f'{entry.name}  {entry.row_count:,} {"row" if entry.row_count == 1 else "rows"}',
            [column.to_json() for column in entry.columns],
            _COLUMN_KEYS,
            summary=entry.description,
            location=entry.path,
        )
    render.note('what a metric column means in Spark, and its unit there: sparkxray metrics show NAME')


@app.command('query', no_args_is_help=True)
def query(
    log: LogArgument,
    sql: Annotated[str, typer.Argument(metavar='SQL', help='DuckDB SQL naming the tables tables list prints.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Run DuckDB SQL over the log's tables and print its rows.

    The slowest task of each stage, counting each partition once:

        sparkxray tables query LOG "select stage_id, max(duration_ms) from tasks_effective group by all"

    tasks_effective holds each partition's first successful attempt, and tasks holds every
    attempt, retries and speculative copies included.
    """
    rows = open_tables(log).rows(sql)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.none_found('rows')
    else:
        render.print_rows(rows)
