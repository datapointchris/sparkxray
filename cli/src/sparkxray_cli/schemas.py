"""`sparkxray schemas`: the JSON Schema of everything sparkxray prints as JSON, so a program can check what it reads."""

from __future__ import annotations

from typing import Annotated

import typer
from sparkxray.schemas import schema_document
from sparkxray.schemas import schema_summaries

from sparkxray_cli import render
from sparkxray_cli.shared_parameters import JsonOption

app = typer.Typer(no_args_is_help=True, help='The JSON Schema of every JSON sparkxray prints, the error envelope and every table included.')


@app.command('list')
def list_schemas(as_json: JsonOption = False) -> None:
    """List every schema sparkxray ships, by the name schemas show takes."""
    summaries = schema_summaries()
    if as_json:
        render.emit_json([summary.to_json() for summary in summaries])
        return
    render.print_records([summary.to_json() for summary in summaries], ('name', 'title'))
    render.note('one in full: sparkxray schemas show NAME')


@app.command('show')
def show(
    name: Annotated[str, typer.Argument(metavar='NAME', help='A schema name, as schemas list prints it.', show_default=False)],
    as_json: JsonOption = False,
) -> None:
    """Print one JSON Schema, draft 2020-12. It is JSON with or without --json.

    A `$ref` to another document names its file: `findings.json` is `sparkxray schemas show findings`.
    """
    render.emit_json(schema_document(name))
