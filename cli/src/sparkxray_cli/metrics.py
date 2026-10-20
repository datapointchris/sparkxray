"""`sparkxray metrics`: what each metric means, the Spark field it comes from, and its unit in Spark and in sparkxray."""

from __future__ import annotations

import itertools
from typing import Annotated

import typer
from sparkxray.metric_glossary import metric_entries
from sparkxray.metric_glossary import metric_glossary

from sparkxray_cli import render
from sparkxray_cli.shared_parameters import JsonOption

app = typer.Typer(no_args_is_help=True, help='Look up what a number means and which unit it is in, before reading anything into it.')

_LIST_KEYS = ('name', 'unit', 'spark_unit', 'description')
_SHOW_KEYS = ('table', 'unit', 'spark_unit', 'metric_type', 'description', 'spark_fields', 'derivation', 'declared_by', 'source')
_PLAN_METRICS_TITLE = 'plan node metrics, held in plan_node_metrics by metric_name'


@app.command('list')
def list_metrics(as_json: JsonOption = False) -> None:
    """List every column whose name ends in a unit, table by table, then every metric a plan node declares."""
    entries = metric_glossary()
    if as_json:
        render.emit_json([entry.to_json() for entry in entries])
        return
    for table, group in itertools.groupby(entries, key=lambda entry: entry.table):
        # A metric no published source explains shows that source where its description would be.
        records = [{**entry.to_json(), 'description': entry.description or entry.source} for entry in group]
        render.print_described(table or _PLAN_METRICS_TITLE, records, _LIST_KEYS)
    render.note('one in full, with its Spark fields and source: sparkxray metrics show NAME')


@app.command('show')
def show(
    name: Annotated[
        str,
        typer.Argument(
            metavar='NAME',
            help='A column, alone or as table.column; a plan metric as sql show prints it; or a Spark field such as "Executor CPU Time".',
            show_default=False,
        ),
    ],
    as_json: JsonOption = False,
) -> None:
    """Show every metric NAME names: its unit in sparkxray and in Spark, the Spark fields it reads, how it is derived, and its source."""
    entries = metric_entries(name)
    if as_json:
        render.emit_json([entry.to_json() for entry in entries])
        return
    for entry in entries:
        record = {**entry.to_json(), 'spark_fields': '\n'.join(entry.spark_fields) or None}
        held = {key: value for key, value in record.items() if value is not None}
        render.print_record(held, _SHOW_KEYS, title=entry.name if entry.table is None else f'{entry.table}.{entry.name}')
