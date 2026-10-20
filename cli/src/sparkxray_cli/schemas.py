"""`sparkxray schemas`: the JSON Schemas sparkxray ships, so a program can check a document it reads."""

from __future__ import annotations

from typing import Annotated

import typer
from sparkxray.schemas import schema_document
from sparkxray.schemas import schema_summaries

from sparkxray_cli import render
from sparkxray_cli.shared_parameters import JsonOption

app = typer.Typer(
    no_args_is_help=True,
    help='Check a document sparkxray printed: the JSON Schemas of its documents, its error envelope and a row of each table.',
)


@app.command('list')
def list_schemas(as_json: JsonOption = False) -> None:
    """List every schema sparkxray ships, by the name schemas show takes, with what the document is and the command printing it."""
    summaries = schema_summaries()
    if as_json:
        render.emit_json([summary.to_json() for summary in summaries])
        return
    render.print_described('Schemas', [{'name': summary.name, 'what': summary.summary} for summary in summaries], ('name', 'what'))
    render.note(f'one in full: sparkxray schemas show {summaries[0].name}')


@app.command('show', no_args_is_help=True)
def show(
    name: Annotated[str, typer.Argument(metavar='NAME', help='A schema name, as schemas list prints it.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Print one JSON Schema, draft 2020-12. It is JSON with or without --json.

    A `$ref` to another document names its file: `findings.json` is `sparkxray schemas show findings`.
    """
    render.emit_json(schema_document(name))
