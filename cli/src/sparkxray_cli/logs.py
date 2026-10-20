"""`sparkxray logs`: read a log into the cache, list the logs the cache holds, show one, and diff two runs of one job."""

from __future__ import annotations

from typing import Annotated
from typing import Any

import typer
from sparkxray.enrichment import GlueRun
from sparkxray.rules import Severity
from sparkxray.run_diff import RunDiff
from sparkxray.run_diff import diff_logs
from sparkxray.summaries import application_detail
from sparkxray.tables.cached_logs import CachedLog
from sparkxray.tables.cached_logs import cached_logs
from sparkxray.tables.manifest import LACK_MEANINGS

from sparkxray_cli import render
from sparkxray_cli.findings import exit_if_gated
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.log_opening import open_tables
from sparkxray_cli.log_opening import tables_of
from sparkxray_cli.shared_parameters import LOG_HELP
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(
    no_args_is_help=True,
    help='Diff two runs of one job, list and show the logs the cache holds, and join a Glue run or a driver log to one.',
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


def glue_run(text: str) -> GlueRun:
    """`JOB_NAME:RUN_ID`, split at the last colon, because a job's name may hold one and a run id never does."""
    job_name, _, run_id = text.rpartition(':')
    if not job_name or not run_id.startswith('jr_'):
        raise typer.BadParameter(f"{text!r} is not JOB_NAME:RUN_ID, as my-job:jr_0123abcd, and a Glue run's id starts jr_")
    return GlueRun(job_name, run_id)


@app.command('ingest', no_args_is_help=True)
def ingest(
    log: LogArgument,
    app_id: Annotated[
        str | None,
        typer.Option(
            '--app',
            metavar='APP_ID',
            help="The application to read from a server holding several. Its newest attempt, unless LOG's address names one.",
            show_default=False,
        ),
    ] = None,
    glue: Annotated[
        GlueRun | None,
        typer.Option(
            '--glue-run',
            metavar='JOB_NAME:RUN_ID',
            parser=glue_run,
            help='The Glue job run LOG is from. Joins its observability metrics from CloudWatch and its job run insights '
            "from CloudWatch Logs, read with boto3's credentials, in the region AWS_DEFAULT_REGION or the profile names.",
            show_default=False,
        ),
    ] = None,
    driver_logs: Annotated[
        list[str] | None,
        typer.Option(
            '--driver-log',
            metavar='LOCATION',
            help='A log Spark 4 wrote with spark.log.structuredLogging.enabled=true: a file, an s3:// object, '
            'or cloudwatch:GROUP:STREAM. Repeat it for each log.',
            show_default=False,
        ),
    ] = None,
    as_json: JsonOption = False,
) -> None:
    """Join a Glue run's metrics or a driver's log to a log, or read a log ahead of time, and print where its tables are.

    Every other command reads a log on first use, so a plain run needs no ingest. A log already
    read is not read again while it is unchanged, so only the first command on it pays for the
    read. A log is cached by the location it was opened from, so a file and the directory holding
    it are read apart.

    A History Server's address downloads the event log it serves. A live driver UI serves none, so
    sparkxray rebuilds one from its REST JSON. That rebuild lacks each stage's parents, each task's
    accumulator updates and executor peaks, and the settings a query changed. `logs show` names
    what a log lacks, and `findings list` names each rule it skipped for want of it.

    --glue-run and --driver-log join what the event log does not hold, each as a table of its own
    that rules read. Giving one again replaces its table. An upgrade of sparkxray rebuilds the
    tables without the joined ones, so give the flags again after one. With --json, `enrichment`
    gives each joined table's rows and each warning, and is null where neither flag was given.
    """
    opened = open_log(log, app=app_id)
    tables = tables_of(opened)
    app = opened.manifest.app_id
    enrichment = None
    if glue is not None or driver_logs:
        with render.status(f'Joining sources to {app}'):
            enrichment = opened.enrich(glue_run=glue, driver_logs=driver_logs or ())
        render.enrichment_notes(app, enrichment)
    cached = CachedLog.of(tables)
    if as_json:
        render.emit_json({**cached.to_json(), 'enrichment': None if enrichment is None else enrichment.to_json()})
    else:
        render.emit_text(f'{cached.tables_path}\n')
        render.note(f'{app} is read, and opens by that id; what slowed it: sparkxray findings list {app}')


@app.command('list')
def list_logs(limit: LimitOption = None, as_json: JsonOption = False) -> None:
    """List the logs the cache under $XDG_CACHE_HOME/sparkxray/ holds tables for, newest first, each by the app id that opens it."""
    logs = cached_logs(limit=limit)
    if as_json:
        render.emit_json([cached.to_json() for cached in logs])
    elif not logs:
        render.empty('logs are cached yet; any command given a LOG reads one', limit)
    else:
        render.print_records([cached.to_json() for cached in logs], _LIST_COLUMNS)
        render.note(f'what slowed one: sparkxray findings list {logs[0].app_id}')


@app.command('show', no_args_is_help=True)
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
        },
        title='Log',
    )
    files = [{'uri': file.uri, 'codec': file.codec, 'size_bytes': file.size_bytes} for file in ingest_manifest.files]
    render.print_records(files, ('uri', 'codec', 'size_bytes'), title='Files')
    lacks = [{'lack': lack, 'missing': LACK_MEANINGS[lack]} for lack in detail.manifest.lacks]
    if lacks:
        render.print_described('What the log was written without', lacks, ('lack', 'missing'))
    render.note(f'what slowed it: sparkxray findings list {ingest_manifest.app_id}')


@app.command('diff', no_args_is_help=True)
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

    Ids shift between runs, so no pair is made by id. SQL executions pair by their description
    and the plan before AQE changed it, then by weaker keys where those leave one unpaired. A stage
    pairs by the plan nodes it ran, and a stage of no SQL execution by name, its call site in the
    code. Each pair says how it was made. Both runs are analyzed at the same thresholds. An
    exception LOG_B shares with a run that succeeded, LOG_A or a `--baseline`, is routine.

    The view prints each metric that moved by at least 10% and by its floor: 5 s for a time,
    64 MiB for a size, and 1 for a ratio. Any other metric's floor is the 10% alone. The view
    counts the rest, and `--json` carries every row.

    Exits 0 when it ran, whatever changed, and 2 on a usage error. Exits 3 when `--fail-on` is
    given and LOG_B has a finding at or above it that is new or was below it in LOG_A, or when a
    log could not be read.
    """
    opened_a, opened_b = open_log(log_a), open_log(log_b)
    opened_baselines = [open_log(location) for location in baseline or ()]
    for opened in (opened_a, opened_b, *opened_baselines):
        tables_of(opened)
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
        exit_if_gated(compared.gate(fail_on), which=' that run B added or raised')


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
        render.print_metric_changes(moved, title='What moved by at least 10% and by at least its floor (logs diff --help gives each floor)')
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
