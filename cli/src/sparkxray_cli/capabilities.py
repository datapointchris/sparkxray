"""`sparkxray capabilities`: what a program or an agent can call, read from the running command and library."""

from __future__ import annotations

import shlex

import typer
from sparkxray.capabilities import capabilities

from sparkxray_cli import render
from sparkxray_cli.command_surface import command_surface
from sparkxray_cli.shared_parameters import JsonOption

app = typer.Typer(no_args_is_help=True, help='What a program or an agent can call: every command, rule, table, schema and error code.')


@app.command('show')
def show(ctx: typer.Context, as_json: JsonOption = False) -> None:
    """Show every command, and with --json its flags and exit codes, every rule's thresholds, and the tables, schemas and codes.

    It is built from the installed sparkxray, so it matches this version exactly. `--json` prints
    the document `sparkxray schemas show capabilities` describes.
    """
    held = capabilities(command_surface(ctx))
    if as_json:
        render.emit_json(held)
        return
    render.print_records(
        [{'command': shlex.join(['sparkxray', *command['path']]), 'summary': command['summary']} for command in held['commands']],
        ('command', 'summary'),
    )
    render.note(
        f'{len(held["rules"])} rules, {len(held["tables"])} tables, {len(held["schemas"])} schemas and '
        f'{len(held["error_codes"])} error codes; each in full: sparkxray capabilities show --json'
    )
