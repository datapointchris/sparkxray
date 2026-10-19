"""`sparkxray logs`: read a log into the cache, list the logs the cache holds, and show one."""

from __future__ import annotations

import typer
from sparkxray.summaries import application_detail
from sparkxray.tables.cached_logs import CachedLog
from sparkxray.tables.cached_logs import cached_logs

from sparkxray_cli import render
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument

app = typer.Typer(
    no_args_is_help=True,
    help='Read a log into the cache, list the logs it holds, and show one. A cached log opens by its app id.',
)

_LIST_COLUMNS = ('app_id', 'app_name', 'spark_version', 'platform', 'log_size_bytes', 'cached_ms', 'location')
_APPLICATION_KEYS = ('app_id', 'attempt_id', 'app_name', 'user', 'spark_version', 'platform', 'start_ms', 'end_ms', 'duration_ms')


@app.command('ingest')
def ingest(log: LogArgument, as_json: JsonOption = False) -> None:
    """Read a log, build its tables, and print the directory that holds them.

    Reading an unchanged log again reads the tables already built, so every other command is
    as fast on the second run as the first was slow.
    """
    cached = CachedLog.of(open_tables(log))
    if as_json:
        render.emit_json(cached.to_json())
    else:
        print(cached.tables_path)


@app.command('list')
def list_logs(limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List the logs the cache holds tables for, newest first."""
    logs = cached_logs(limit=limit)
    if as_json:
        render.emit_json([cached.to_json() for cached in logs])
    elif not logs:
        render.none_found('logs are cached yet; sparkxray logs ingest LOG reads one')
    else:
        render.print_records([cached.to_json() for cached in logs], _LIST_COLUMNS)


@app.command('show')
def show(log: LogArgument, as_json: JsonOption = False) -> None:
    """Show the application a log holds, the files it was read from, and what the log was written without."""
    detail = application_detail(open_tables(log))
    if as_json:
        render.emit_json(detail.to_json())
        return
    render.print_record(detail.application, _APPLICATION_KEYS, title='Application')
    ingest_manifest = detail.manifest.ingest
    render.print_record(
        {
            'source': ingest_manifest.source,
            'layout': ingest_manifest.layout,
            'in_progress': ingest_manifest.in_progress,
            'truncated_tail': ingest_manifest.truncated_tail,
            'compacted': ingest_manifest.compacted,
            'lacks': list(detail.manifest.lacks),
        },
        title='Log',
    )
    files = [{'uri': file.uri, 'codec': file.codec, 'size_bytes': file.size_bytes} for file in ingest_manifest.files]
    render.print_records(files, ('uri', 'codec', 'size_bytes'), title='Files')
