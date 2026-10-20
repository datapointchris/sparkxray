"""One report per analyzed log, as Markdown for a person and versioned JSON for a program."""

from sparkxray.reports.markdown import plan_lines
from sparkxray.reports.markdown import render_markdown
from sparkxray.reports.report import AppSummary
from sparkxray.reports.report import ConfigEntry
from sparkxray.reports.report import LogFile
from sparkxray.reports.report import LogSummary
from sparkxray.reports.report import Report
from sparkxray.reports.report import ReportFormat
from sparkxray.reports.report import SqlSummary
from sparkxray.reports.report import Usage
from sparkxray.reports.report import build_report
from sparkxray.reports.report import render_json
from sparkxray.reports.report import write_report

__all__ = [
    'AppSummary',
    'ConfigEntry',
    'LogFile',
    'LogSummary',
    'Report',
    'ReportFormat',
    'SqlSummary',
    'Usage',
    'build_report',
    'plan_lines',
    'render_json',
    'render_markdown',
    'render_report',
    'write_report',
]


def render_report(report: Report, report_format: ReportFormat) -> str:
    """The report as Markdown or JSON, ending in one newline."""
    return render_markdown(report) if report_format == 'markdown' else render_json(report)
