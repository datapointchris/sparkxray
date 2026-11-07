"""`sparkxray capabilities`: what a program or an agent can call, read from the running command and library."""

from __future__ import annotations

import shlex

import typer
from sparkxray.capabilities import capabilities

from sparkxray_cli import render
from sparkxray_cli.command_surface import command_surface
from sparkxray_cli.shared_parameters import JsonOption

CAPABILITIES_HELP = """What a program or an agent can call: every command, rule, table, schema and error code.

Examples:

```text
sparkxray capabilities show --json    everything a program can call, read before driving sparkxray
sparkxray capabilities show           every command and what it does, to scan by eye
```
"""

app = typer.Typer(no_args_is_help=True, help=CAPABILITIES_HELP)


@app.command('show')
def show(ctx: typer.Context, as_json: JsonOption = False) -> None:
    """Show every command, and with --json its flags and exit codes, every rule's thresholds, and the tables, schemas and error codes.

    It is built from the installed sparkxray, so it matches this version exactly. `--json` prints
    the document `sparkxray schemas show capabilities` describes.

    Examples:

    ```text
    sparkxray capabilities show --json    every command's flags and exit codes, every rule, table and error code
    sparkxray capabilities show           every command and what it does, to scan by eye
    ```
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
