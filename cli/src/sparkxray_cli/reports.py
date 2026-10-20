"""`sparkxray reports`: write one run's findings, and the run behind them, as one Markdown or JSON document."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from sparkxray.reports import ReportFormat
from sparkxray.reports import build_report
from sparkxray.reports import render_report
from sparkxray.reports import write_report

from sparkxray_cli import render
from sparkxray_cli.findings import exit_if_gated
from sparkxray_cli.log_opening import analyze_log
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.shared_parameters import FailOnOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(
    no_args_is_help=True,
    help="Write a run's report: Markdown to read or attach to a ticket, JSON for a program. Exits 3 when a finding reaches --fail-on.",
)


@app.command('write')
def write(
    log: LogArgument,
    report_format: Annotated[ReportFormat, typer.Option('--format', help='Markdown to read, or JSON to keep and diff.')] = 'markdown',
    out: Annotated[
        Path | None,
        typer.Option(
            '--out', metavar='FILE', help='Write to FILE, whole or not at all, and print its path. Stdout by default.', show_default=False
        ),
    ] = None,
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    fail_on: FailOnOption = 'high',
) -> None:
    """Write one report of a log: its findings worst first with their evidence, then its longest stages, SQL plans and settings.

    The same log, thresholds and sparkxray version write the same bytes, so a report diffs and
    commits cleanly. Exits 0 when no finding reaches `--fail-on`, 2 on a usage error, and 3 when
    one does or the log could not be read. The report is written either way.
    """
    opened = open_log(log)
    analysis = analyze_log(opened, None, threshold, thresholds)
    with render.status(f'Writing the report of {opened.manifest.app_id}'):
        text = render_report(build_report(opened, analysis), report_format)
    render.emit_text(text if out is None else f'{write_report(text, out)}\n')
    exit_if_gated(analysis, fail_on)
