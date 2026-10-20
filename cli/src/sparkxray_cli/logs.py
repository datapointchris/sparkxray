"""`sparkxray logs`: read a log into the cache, list the logs the cache holds, show one, and diff two runs of one job."""

from __future__ import annotations

from typing import Annotated
from typing import Any

import typer
from sparkxray.rules import Severity
from sparkxray.run_diff import RunDiff
from sparkxray.run_diff import diff_logs
from sparkxray.summaries import application_detail
from sparkxray.tables.cached_logs import CachedLog
from sparkxray.tables.cached_logs import cached_logs

from sparkxray_cli import render
from sparkxray_cli.findings import exit_if_gated
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.shared_parameters import LOG_HELP
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(
    no_args_is_help=True,
    help='Read a log into the cache, list the logs it holds, show one, and diff two runs. A cached log opens by its app id.',
)

_RUN_COLUMNS = ('run', 'app_id', 'app_name', 'spark_version', 'wall_ms', 'succeeded')

_LIST_COLUMNS = ('app_id', 'app_name', 'spark_version', 'platform', 'log_size_bytes', 'cached_epoch_ms', 'location')
_APPLICATION_KEYS = (
    'app_id',
    'attempt_id',
    'app_name',
    'user',
    'spark_version',
    'platform',
    'start_epoch_ms',
    'end_epoch_ms',
    'duration_ms',
)


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


@app.command('diff')
def diff(
    log_a: Annotated[str, typer.Argument(metavar='LOG_A', help=f'The reference run, usually the good or older one. {LOG_HELP}')],
    log_b: Annotated[str, typer.Argument(metavar='LOG_B', help=f'The run to explain. {LOG_HELP}')],
    baseline: Annotated[
        list[str] | None,
        typer.Option(
            '--baseline',
            metavar='LOG',
            help='Another run of the job that succeeded. An exception it threw too is routine. Repeat for more.',
            show_default=False,
        ),
    ] = None,
    ignore_config: Annotated[
        list[str] | None,
        typer.Option(
            '--ignore-config',
            metavar='GLOB',
            help='Leave out the settings whose key matches, as spark.yarn.*, beside those Spark sets anew each run. Repeat for more.',
            show_default=False,
        ),
    ] = None,
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    fail_on: Annotated[
        Severity | None,
        typer.Option(
            '--fail-on',
            help='Exit 3 when a finding of LOG_B is at or above this severity, and is new or was below it in LOG_A.',
            show_default=False,
        ),
    ] = None,
    as_json: JsonOption = False,
) -> None:
    """Compare LOG_B with LOG_A: what got slower or faster, which settings differ, which findings and exceptions are new.

    Ids shift between runs, so SQL executions pair by their description and plan, and stages by
    the plan nodes they ran, never by id. Both runs are analyzed at the same thresholds. An
    exception LOG_B shares with a run that succeeded, LOG_A or a `--baseline`, is routine. The
    view prints the notable changes and counts the rest, and `--json` carries every row.

    Exits 0 when it ran, whatever changed, and 2 on a usage error. Exits 3 when `--fail-on` is
    given and LOG_B has a finding at or above it that is new or was below it in LOG_A, or when a
    log could not be read.
    """
    opened_a, opened_b = open_log(log_a), open_log(log_b)
    opened_baselines = [open_log(location) for location in baseline or ()]
    with render.status(f'Comparing {opened_b.manifest.app_id} with {opened_a.manifest.app_id}'):
        compared = diff_logs(
            opened_a,
            opened_b,
            baselines=opened_baselines,
            threshold_file=thresholds,
            threshold_flags=threshold or (),
            ignore_config=ignore_config or (),
        )
    if as_json:
        document = compared.to_json()
        if fail_on is not None:
            document['gate'] = compared.gate(fail_on).to_json()
        render.emit_json(document)
    else:
        _print_diff(compared)
    for run in compared.baselines:
        if not run.succeeded:
            render.note(f'baseline {run.app_id} did not succeed, so it marks no exception routine')
    if fail_on is not None:
        exit_if_gated(compared.gate(fail_on))


def _print_diff(compared: RunDiff) -> None:
    runs = [{'run': 'A', **compared.a.to_json()}, {'run': 'B', **compared.b.to_json()}]
    runs += [{'run': f'baseline {number}', **run.to_json()} for number, run in enumerate(compared.baselines, start=1)]
    render.print_records(runs, _RUN_COLUMNS, title='Runs')

    moved: list[dict[str, Any]] = [
        {'part': f'SQL {execution.a_execution_id} → {execution.b_execution_id}', 'matched_by': execution.matched_by, **metric.to_json()}
        for execution in compared.sql.matched
        for metric in execution.metrics
        if metric.notable
    ]
    moved += [
        {'part': f'stage {stage.a_stage_id} → {stage.b_stage_id}', 'matched_by': stage.matched_by, **metric.to_json()}
        for stage in compared.stages.matched
        for metric in stage.metrics
        if metric.notable
    ]
    if moved:
        render.print_metric_changes(moved, title='What moved by at least 10% and its floor')
    render.print_list(
        'Last plans that differ',
        [
            f'SQL {execution.a_execution_id} → {execution.b_execution_id}: '
            + (
                ', '.join(f'{change.operator} {change.a_count} → {change.b_count}' for change in execution.operator_changes)
                or 'the same operators'
            )
            for execution in compared.sql.matched
            if execution.final_plan_changed
        ],
    )
    for run, executions in (('A', compared.sql.only_a), ('B', compared.sql.only_b)):
        if executions:
            records = [execution.to_json() for execution in executions]
            render.print_records(records, ('execution_id', 'description', 'wall_ms'), title=f'SQL executions only run {run} holds')

    config = compared.config
    render.print_list(
        'Settings that differ',
        [
            f'{setting.section} {setting.key}: {_setting_value(setting.a)} → {_setting_value(setting.b)}'
            for change in ('added', 'removed', 'changed')
            for setting in getattr(config, change)
        ],
    )

    findings = compared.findings
    for title, listed in (('Findings new in run B', findings.new), ('Findings resolved since run A', findings.resolved)):
        if listed:
            render.print_records([finding.to_json() for finding in listed], ('finding_id', 'severity', 'title'), title=title)
    render.print_list(
        'Findings in both runs whose severity moved',
        [
            f'{pair.a.finding_id} → {pair.b.finding_id}: {pair.a.severity} → {pair.b.severity}'
            + ('' if pair.finished_in_b else ', on a part run B did not finish')
            for pair in findings.severity_moved
        ],
    )

    unusual = [found.to_json() for found in compared.exceptions if found.status in ('new', 'recurring')]
    if unusual:
        render.print_records(unusual, ('status', 'source', 'a_count', 'b_count', 'example'), title='Exceptions no succeeded run threw')

    still = sum(not any(metric.notable for metric in execution.metrics) for execution in compared.sql.matched)
    still_stages = sum(not any(metric.notable for metric in stage.metrics) for stage in compared.stages.matched)
    counted = {
        'paired SQL executions with no notable change': still,
        'paired stages with no notable change': still_stages,
        'stages only run A ran': len(compared.stages.only_a),
        'stages only run B ran': len(compared.stages.only_b),
        'findings in both runs at the same severity': len(findings.persisting) - len(findings.severity_moved),
        'findings of run A on parts run B did not finish, so not resolved': len(findings.on_parts_b_did_not_finish),
        'settings a run redacted, so unknown': len(config.unknown),
        'routine exceptions': sum(found.status == 'routine' for found in compared.exceptions),
        'exceptions resolved since run A': sum(found.status == 'resolved' for found in compared.exceptions),
    }
    render.print_list('Not shown, and in --json', [f'{what}: {count}' for what, count in counted.items() if count])


def _setting_value(value: str | None) -> str:
    """A setting's value in one run, or `unset` where that run did not set it."""
    return 'unset' if value is None else value
