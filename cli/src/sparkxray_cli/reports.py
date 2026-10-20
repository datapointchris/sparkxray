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
            '--out',
            metavar='FILE',
            dir_okay=False,
            help='Write to FILE, whole or not at all, creating its directory, and print its path. Stdout by default.',
            show_default=False,
        ),
    ] = None,
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    fail_on: FailOnOption = 'high',
) -> None:
    """Write one report of a log: its findings with their evidence, then its longest stages, SQL plans and settings.

    The findings come in the order `findings list` gives them, the most wall time a fix could
    recover first. A report names a local log by its file name and holds no time it was made. So
    the same log file, thresholds and sparkxray version write the same bytes on any machine, and a
    report diffs and commits cleanly.

    Exits 0 when no finding reaches `--fail-on` and 3 when one does, and the report is written in
    both cases, stating the verdict. Exits 2 on a usage error, and 3 when the log could not be read.
    """
    opened = open_log(log)
    analysis = analyze_log(opened, None, threshold, thresholds)
    with render.status(f'Writing the report of {opened.manifest.app_id}'):
        text = render_report(build_report(opened, analysis, fail_on), report_format)
    render.emit_text(text if out is None else f'{write_report(text, out)}\n')
    exit_if_gated(analysis, fail_on)
