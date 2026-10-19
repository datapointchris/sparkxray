"""`sparkxray tables`: the Parquet tables a log reads into, and SQL over them."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.text import Text

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(no_args_is_help=True, help='Ask the run anything with SQL over its tables, which any Parquet reader also opens.')


@app.command('list')
def list_tables(log: LogArgument, as_json: JsonOption = False) -> None:
    """List the tables and views a query reads: rows, columns with their types and units, and each Parquet file."""
    listed = open_tables(log).listing()
    if as_json:
        render.emit_json([table.to_json() for table in listed])
        return
    table = render.table()
    table.add_column('name', style='bold')
    table.add_column(render.header('row_count'), justify='right')
    table.add_column('description and columns', overflow='fold')
    for entry in listed:
        columns = ', '.join(f'{column.name} ({column.unit})' if column.unit else column.name for column in entry.columns)
        table.add_row(entry.name, f'{entry.row_count:,}', Text.assemble(entry.description, '\n', (columns, 'dim')))
    render.print_table(table)


@app.command('query')
def query(
    log: LogArgument,
    sql: Annotated[str, typer.Argument(metavar='SQL', help='DuckDB SQL naming the tables tables list prints.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Run DuckDB SQL over the log's tables and print its rows.

    `sparkxray tables query LOG "select stage_id, max(duration_ms) from tasks_effective group by all"`
    """
    rows = open_tables(log).rows(sql)
    if as_json:
        render.emit_json(rows.to_json())
    elif not rows:
        render.none_found('rows')
    else:
        render.print_rows(rows)
