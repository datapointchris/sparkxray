"""The sparkxray root command: its resources, its `--version`, and how a refusal reaches the terminal."""

import importlib.metadata
import itertools
import json
from collections.abc import Sequence
from typing import Annotated
from typing import Any

import typer
from sparkxray.errors import CommandLineError
from sparkxray.errors import SparkxrayError
from typer._click.exceptions import NoSuchOption  # Typer vendors click, and exports BadParameter alone of its errors
from typer._click.exceptions import UsageError
from typer.core import TyperGroup

from sparkxray_cli import capabilities
from sparkxray_cli import executors
from sparkxray_cli import findings
from sparkxray_cli import logs
from sparkxray_cli import metrics
from sparkxray_cli import plans
from sparkxray_cli import render
from sparkxray_cli import reports
from sparkxray_cli import rules
from sparkxray_cli import schemas
from sparkxray_cli import sql
from sparkxray_cli import stages
from sparkxray_cli import tables
from sparkxray_cli import tasks
from sparkxray_cli.command_surface import flag_names

DISTRIBUTION = 'sparkxray-cli'
_READING = 'Reading a run'
_FINDING = 'Finding problems'
_LEARNING = 'Learning'
_PROGRAMS = 'For programs and agents'
# Set in the root context's meta, which every context of one invocation shares, before any parameter is parsed.
_JSON_REQUESTED = 'sparkxray.json'


class SparkxrayGroup(TyperGroup):
    """The root group. A refusal prints on stderr, as one line of JSON where the command asked for JSON, and exits with its status.

    A library refusal exits with the error's own status. Under JSON, a command line the parser
    refuses prints as `E-USAGE-001` and exits 2. Without it, the parser's own text prints.
    """

    def parse_args(self, ctx: Any, args: list[str]) -> list[str]:
        ctx.meta[_JSON_REQUESTED] = _asks_for_json(args)
        return super().parse_args(ctx, args)

    def invoke(self, ctx: Any) -> Any:
        try:
            return super().invoke(ctx)
        except SparkxrayError as error:
            render.report_error(error, as_json=bool(ctx.meta.get(_JSON_REQUESTED)))
            raise typer.Exit(error.exit_code) from error
        except UsageError as error:
            if not ctx.meta.get(_JSON_REQUESTED):
                raise
            refused = _command_line_error(error)
            render.report_error(refused, as_json=True)
            raise typer.Exit(refused.exit_code) from error


def _asks_for_json(args: Sequence[str]) -> bool:
    """Whether the command line asks for JSON, by `--json` or by `--format json`, read from its words before any `--`."""
    words = list(itertools.takewhile(lambda word: word != '--', args))
    return '--json' in words or '--format=json' in words or ('--format', 'json') in itertools.pairwise(words)


def _command_line_error(error: UsageError) -> CommandLineError:
    """The parser's refusal as sparkxray's, naming the parameter and the values it takes where the parser knows them."""
    context = error.ctx
    field: str | None = None
    valid: Sequence[str] = ()
    if isinstance(error, typer.BadParameter) and error.param is not None:
        parameter = error.param
        field = flag_names(parameter)[0] if parameter.param_type_name == 'option' else parameter.metavar or (parameter.name or '').upper()
        valid = list(getattr(parameter.type, 'choices', None) or ())
    elif isinstance(error, NoSuchOption):
        field = error.option_name
        if context is not None:
            valid = sorted(name for parameter in context.command.params for name in parameter.opts if name.startswith('--'))
    elif context is not None and (list_commands := getattr(context.command, 'list_commands', None)) is not None:
        field = 'command'
        valid = list_commands(context)
    hint = None if context is None else f'{context.command_path} --help'
    return CommandLineError(error.format_message(), field=field, valid=valid, hint=hint)


app = typer.Typer(name='sparkxray', cls=SparkxrayGroup, no_args_is_help=True, rich_markup_mode='markdown')
app.add_typer(findings.app, name='findings', rich_help_panel=_FINDING)
app.add_typer(reports.app, name='reports', rich_help_panel=_FINDING)
app.add_typer(logs.app, name='logs', rich_help_panel=_READING)
app.add_typer(stages.app, name='stages', rich_help_panel=_READING)
app.add_typer(tasks.app, name='tasks', rich_help_panel=_READING)
app.add_typer(executors.app, name='executors', rich_help_panel=_READING)
app.add_typer(sql.app, name='sql', rich_help_panel=_READING)
app.add_typer(plans.app, name='plans', rich_help_panel=_READING)
app.add_typer(tables.app, name='tables', rich_help_panel=_READING)
app.add_typer(rules.app, name='rules', rich_help_panel=_LEARNING)
app.add_typer(metrics.app, name='metrics', rich_help_panel=_LEARNING)
app.add_typer(capabilities.app, name='capabilities', rich_help_panel=_PROGRAMS)
app.add_typer(schemas.app, name='schemas', rich_help_panel=_PROGRAMS)


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
    render.emit_text(f'sparkxray {version}{f" @ {commit[:8]}" if commit else ""}\n')
    raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool | None,
        typer.Option('--version', callback=version_callback, is_eager=True, help='Show the installed version and exit.'),
    ] = None,
) -> None:
    """**sparkxray** reads a Spark event log offline, with no JVM, and finds why the run was slow.

    A command is `sparkxray <resource> <verb>`, and one that reads a run takes `LOG` first. The
    resource comes first, so `stages list`, then `stages show LOG 4`, then `tasks list LOG --stage 4`
    walks from the run down to one stage's tasks. `list` prints a set and `show` prints one, wherever
    a resource has them. The other verbs belong to one resource each: `logs ingest`, `logs diff`,
    `executors simulate`, `tables query` and `reports write`.

    Start with `sparkxray findings list ./eventlog.zstd`. It reads the log into the cache, then
    lists what slowed the run, the most wall time a fix could recover first.

    Each finding has an id. `findings show LOG ID` gives its evidence and its fix, and
    `rules show RULE_ID` explains its rule. Where a stage took the time, read it with
    `stages show LOG STAGE_ID`, then `tasks list LOG --stage STAGE_ID`. Where a query did, read it
    with `sql show LOG ID`, then `plans show LOG --sql ID`. `logs diff GOOD_LOG SLOW_LOG` explains a
    slow run against a good one.

    `findings list` exits 3 when a finding is high or worse, so it gates a CI step as it stands.
    `--fail-on` moves the bar.

    `LOG` is a log file or directory, an `s3://` URI, a History Server's or driver UI's `http(s)://`
    address, or the app id of a log already read. The first command on a log reads it into the cache
    under `$XDG_CACHE_HOME/sparkxray/`, and every later one opens the cache. Deleting that directory
    empties it.

    A table names each column as `--json` does, with spaces for underscores. It prints each value in
    the unit that reads best, as `8 s` under `wall ms`, and `--json` prints the raw number in the
    unit its name ends in.

    A program starts at `sparkxray capabilities show --json`, which lists every command, rule,
    table, schema and error code. `sparkxray schemas show NAME` prints the schema of one document.
    The `--json` of a listing, such as `stages list --json`, has no schema of its own.

    A resource with no verb, such as `sparkxray stages`, lists its verbs. A command missing its
    arguments, or given `--help`, shows what it takes.
    """
