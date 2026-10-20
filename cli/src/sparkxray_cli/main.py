"""The sparkxray root command: its resources, its `--version`, and how a library refusal reaches the terminal."""

import importlib.metadata
import json
from typing import Annotated
from typing import Any

import typer
from sparkxray.errors import SparkxrayError
from typer.core import TyperGroup

from sparkxray_cli import executors
from sparkxray_cli import findings
from sparkxray_cli import logs
from sparkxray_cli import metrics
from sparkxray_cli import plans
from sparkxray_cli import render
from sparkxray_cli import rules
from sparkxray_cli import sql
from sparkxray_cli import stages
from sparkxray_cli import tables
from sparkxray_cli import tasks

DISTRIBUTION = 'sparkxray-cli'
_READING = 'Reading a run'
_FINDING = 'Finding problems'
_LEARNING = 'Learning'


class SparkxrayGroup(TyperGroup):
    """The root group. A library refusal prints on stderr and exits with the error's own status."""

    def invoke(self, ctx: Any) -> Any:
        try:
            return super().invoke(ctx)
        except SparkxrayError as error:
            render.report_error(error)
            raise typer.Exit(error.exit_code) from error


app = typer.Typer(name='sparkxray', cls=SparkxrayGroup, no_args_is_help=True, rich_markup_mode='markdown')
app.add_typer(logs.app, name='logs', rich_help_panel=_READING)
app.add_typer(stages.app, name='stages', rich_help_panel=_READING)
app.add_typer(tasks.app, name='tasks', rich_help_panel=_READING)
app.add_typer(executors.app, name='executors', rich_help_panel=_READING)
app.add_typer(sql.app, name='sql', rich_help_panel=_READING)
app.add_typer(plans.app, name='plans', rich_help_panel=_READING)
app.add_typer(tables.app, name='tables', rich_help_panel=_READING)
app.add_typer(findings.app, name='findings', rich_help_panel=_FINDING)
app.add_typer(rules.app, name='rules', rich_help_panel=_LEARNING)
app.add_typer(metrics.app, name='metrics', rich_help_panel=_LEARNING)


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
    """**sparkxray** reads a Spark event log and finds why the run was slow.

    Every command is `sparkxray <resource> <verb> LOG`. The resource comes first, so walking
    from `stages list` to `stages show` to `tasks list` changes one word at a time. Each verb
    means the same on every resource: `list` a set, `show` one, `query` with SQL, `ingest` a log.

    `LOG` is a log file or directory, an `s3://` URI, or the app id of a log already read. The
    first command on a log reads it into the cache, and every later one opens the cache.

    `sparkxray findings list LOG` runs the rules, worst first, and exits 3 when a finding is
    at or above `--fail-on`. Run any partial command with no arguments or `--help` to see what
    comes next.
    """
