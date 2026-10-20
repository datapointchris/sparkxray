"""`sparkxray findings`: run the rules over a log, list what they found, and show one finding in full."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Annotated

import typer
from rich.text import Text
from sparkxray.rules import Analysis
from sparkxray.rules import Severity

from sparkxray_cli import render
from sparkxray_cli.log_opening import analyze_log
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(
    no_args_is_help=True,
    help='Find what slowed a run, worst first. findings list exits 3 when a finding reaches --fail-on, so it gates CI.',
)

RuleOption = Annotated[
    list[str] | None,
    typer.Option(
        '--rule',
        metavar='RULE_ID',
        help=(
            'List and gate only this rule. A rule reading the findings of another runs that one first, unlisted. '
            'Repeat for more. Every rule by default.'
        ),
        show_default=False,
    ),
]


@app.command('list')
def list_findings(
    log: LogArgument,
    rule: RuleOption = None,
    min_severity: Annotated[
        Severity | None, typer.Option('--min-severity', help='List only findings at or above this severity.', show_default=False)
    ] = None,
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    fail_on: Annotated[
        Severity, typer.Option('--fail-on', help='Exit 3 when a finding is at or above this severity, listed or not.')
    ] = 'high',
    limit: LimitOption = None,
    as_json: JsonOption = False,
) -> None:
    """Run the rules over a log and list what they found, the most wall time a fix could recover first.

    Exits 0 when no finding reaches `--fail-on`, 2 on a usage error, and 3 when one does or the
    log could not be read. So `sparkxray findings list LOG --fail-on critical` gates a CI job,
    and a scheduled report stays green while it lists medium findings. `--json` carries the
    gate it was judged by.
    """
    analysis = analyze_log(open_log(log), rule, threshold, thresholds)
    gate = analysis.gate(fail_on)
    listed = analysis.narrowed(min_severity=min_severity, limit=limit)
    if as_json:
        render.emit_json({**listed.to_json(), 'gate': gate.to_json()})
    else:
        _print_findings(listed, len(analysis.findings))
        if listed.findings:
            render.note(f'one in full: {show_command(log, listed.findings[0].finding_id, threshold, thresholds)}')
    for skipped in analysis.skipped:
        if 'plan_estimates' in skipped.missing_signals:
            render.note(f"skipped {skipped.rule_id}: it runs on explain(mode='cost') text only")
        else:
            render.note(f'skipped {skipped.rule_id}: the log holds no {", ".join(skipped.missing_signals)}')
    if not gate.passed:
        render.note(f'{len(gate.blocking)} at or above {fail_on}: {", ".join(gate.blocking)}')
        raise typer.Exit(3)


@app.command('show')
def show(
    log: LogArgument,
    finding_id: Annotated[str, typer.Argument(metavar='FINDING_ID', help='A finding id, as findings list prints it.', show_default=False)],
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    as_json: JsonOption = False,
) -> None:
    """Show one finding: its evidence, the thresholds it was judged by and where each came from, and how to fix it."""
    finding = analyze_log(open_log(log), None, threshold, thresholds).finding(finding_id)
    if as_json:
        render.emit_json(finding.to_json())
        return
    render.stdout.print(Text(finding.title, style='bold'))
    render.print_record(
        {
            'finding_id': finding.finding_id,
            'rule_id': finding.rule_id,
            'rule_version': finding.rule_version,
            'severity': finding.severity,
            'base_severity': finding.base_severity,
            'scope': finding.scope.describe(),
            'worst_task_ids': list(finding.scope.worst_task_ids),
            'impact_seconds': finding.impact_seconds,
            'confidence': finding.confidence,
            'confidence_reason': finding.confidence_reason,
        }
    )
    render.print_records(
        [{'key': key, 'value': render.shown(key, value)} for key, value in finding.evidence.items()], ('key', 'value'), title='Evidence'
    )
    applied = [
        {
            'name': name,
            'value': render.shown_in(threshold.unit, threshold.value),
            'source': threshold.source,
            'default': render.shown_in(threshold.unit, threshold.default),
            'default_source': threshold.default_source,
        }
        for name, threshold in finding.thresholds.items()
    ]
    render.print_records(applied, ('name', 'value', 'source', 'default', 'default_source'), title='Thresholds')
    render.print_list('How to fix it', finding.remediation)
    render.print_links(finding.docs)
    render.note(f'how the rule decides: {rule_command(finding.rule_id, threshold, thresholds)}')


def _print_findings(listed: Analysis, found: int) -> None:
    if not found:
        render.none_found('findings')
        return
    if not listed.findings:
        render.note(f'none listed; {found} finding{"s fall" if found > 1 else " falls"} below --min-severity or past --limit')
        return
    render.print_records(
        [
            {
                'severity': finding.severity,
                'finding_id': finding.finding_id,
                'scope': finding.scope.describe(),
                'title': finding.title,
                'impact_seconds': finding.impact_seconds,
            }
            for finding in listed.findings
        ],
        ('severity', 'finding_id', 'scope', 'title', 'impact_seconds'),
    )


def show_command(log: str, finding_id: str, flags: list[str] | None, file: Path | None) -> str:
    """`findings show` for one finding with the same thresholds, so it shows the finding as the command printing this judged it."""
    return _with_thresholds(['sparkxray', 'findings', 'show', log, finding_id], flags, file)


def rule_command(rule_id: str, flags: list[str] | None, file: Path | None) -> str:
    """`rules show` for one rule with the same thresholds, so its lesson states the values the finding was judged by."""
    return _with_thresholds(['sparkxray', 'rules', 'show', rule_id], flags, file)


def _with_thresholds(command: list[str], flags: list[str] | None, file: Path | None) -> str:
    for flag in flags or ():
        command += ['--threshold', flag]
    if file is not None:
        command += ['--thresholds', str(file)]
    return shlex.join(command)
