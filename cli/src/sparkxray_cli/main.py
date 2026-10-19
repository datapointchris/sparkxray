"""The sparkxray root command and its `--version`."""

import importlib.metadata
import json
from typing import Annotated

import typer

DISTRIBUTION = 'sparkxray-cli'

app = typer.Typer(name='sparkxray', no_args_is_help=True, rich_markup_mode='markdown')


def installed_commit() -> str | None:
    """The git commit the CLI was installed from, when it was installed from a ref.

    uv records the resolved commit in `direct_url.json` for any git install. A wheel
    from an index carries no such file and gets None."""
    try:
        text = importlib.metadata.distribution(DISTRIBUTION).read_text('direct_url.json')
    except importlib.metadata.PackageNotFoundError:
        return None
    if not text:
        return None
    return json.loads(text).get('vcs_info', {}).get('commit_id')


def version_callback(asked: bool) -> None:
    """`sparkxray --version`, as one line naming the tool."""
    if not asked:
        return
    try:
        version = importlib.metadata.version(DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        version = 'unknown'
    commit = installed_commit()
    print(f'sparkxray {version}{f" @ {commit[:8]}" if commit else ""}')
    raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option('--version', callback=version_callback, is_eager=True, help='Show the installed version and exit.'),
    ] = None,
) -> None:
    """**sparkxray** reads a Spark event log and finds why the run was slow."""
