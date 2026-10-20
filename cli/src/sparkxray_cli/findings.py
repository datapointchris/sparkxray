"""`sparkxray findings`: run the rules over a log, list what they found, and show one finding in full."""

from __future__ import annotations

import shlex
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated

import typer
from rich.text import Text
from sparkxray.rules import Analysis
from sparkxray.rules import AppliedThreshold
from sparkxray.rules import Gate
from sparkxray.rules import Severity

from sparkxray_cli import render
from sparkxray_cli.log_opening import analyze_log
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.shared_parameters import FailOnOption
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LimitOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(
    no_args_is_help=True,
    help='Find what slowed a run, costliest first. findings list exits 3 on a high finding or worse, so it gates a CI step.',
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


@app.command('list', no_args_is_help=True)
def list_findings(
    log: LogArgument,
    rule: RuleOption = None,
    min_severity: Annotated[
        Severity | None, typer.Option('--min-severity', help='List only findings at or above this severity.', show_default=False)
    ] = None,
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    fail_on: FailOnOption = 'high',
    limit: LimitOption = None,
    as_json: JsonOption = False,
) -> None:
    """Run the rules over a log and list what they found, the most wall time a fix could recover first.

    A rule that cannot estimate that time leaves impact blank, and those findings follow, by
    severity. A finding id is the rule id, a colon, and what the finding is about: `s4` is stage
    4, `e0` is SQL execution 0, `e0n9` is node n9 of that execution's last plan, `x3` is executor
    3, and `app` is the whole application.

    A finding's severity is its rule's grade, moved one step by the share of the run's wall time
    its stage or SQL execution took: down below 5%, up above 50%. info never moves, and nothing
    drops below low. `findings show` gives the rule's own grade as base_severity.

    Exits 0 when no finding reaches `--fail-on`, 2 on a usage error, and 3 when one does or the
    log could not be read. Under `--json`, an exit 3 with nothing on stdout means the log was not
    read. So `sparkxray findings list LOG` gates a CI step on high findings as it stands, and
    `--fail-on critical` gates on critical ones alone. A finding below the bar is listed and never
    fails the step. `--json` carries the gate it was judged by.
    """
    analysis = analyze_log(open_log(log), rule, threshold, thresholds)
    gate = analysis.gate(fail_on)
    listed = analysis.narrowed(min_severity=min_severity, limit=limit)
    if as_json:
        render.emit_json({**listed.to_json(), 'gate': gate.to_json()})
    else:
        _print_findings(listed, len(analysis.findings), limit)
        if listed.findings:
            render.note(f'one in full: {show_command(log, listed.findings[0].finding_id, threshold, thresholds)}')
    for skipped in analysis.skipped:
        render.note(f'skipped {skipped.rule_id}: {_why_skipped(skipped.missing_signals, log)}')
    exit_if_gated(gate)


def _why_skipped(missing: Sequence[str], log: str) -> str:
    """Why a rule did not run, and the command that feeds it where one does."""
    if 'plan_estimates' in missing:
        return "it runs on explain(mode='cost') text only"
    if 'driver_log' in missing:
        return f'no driver log is joined; {shlex.join(["sparkxray", "logs", "ingest", log])} --driver-log FILE joins one'
    if 'glue_metrics' in missing or 'glue_insights' in missing:
        return f'no Glue run is joined; {shlex.join(["sparkxray", "logs", "ingest", log])} --glue-run JOB_NAME:RUN_ID joins one'
    return f'the log holds no {", ".join(missing)}; what it lacks: {shlex.join(["sparkxray", "logs", "show", log])}'


def exit_if_gated(gate: Gate, *, which: str = '') -> None:
    """Exit 3, naming the findings, when the gate blocks. `which` says which findings the gate counts, where not all of them."""
    if not gate.passed:
        render.note(f'{len(gate.blocking)} at or above {gate.fail_on}{which}: {", ".join(gate.blocking)}')
        raise typer.Exit(3)


@app.command('show', no_args_is_help=True)
def show(
    log: LogArgument,
    finding_id: Annotated[str, typer.Argument(metavar='FINDING_ID', help='A finding id, as findings list prints it.', show_default=False)],
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    as_json: JsonOption = False,
) -> None:
    """Show one finding: its evidence, the thresholds it was judged by and who set each, and how to fix it.

    severity is base_severity, the rule's own grade, moved one step by the wall time its scope
    took, as `findings list --help` says. confidence says how sure the rule is that its evidence
    shows the problem it names. Where it is below high, confidence_reason says why, as when peak
    memory came only from task-end polls.
    """
    finding = analyze_log(open_log(log), None, threshold, thresholds).finding(finding_id)
    if as_json:
        render.emit_json(finding.to_json())
        return
    render.stdout.print(Text(finding.title, style='bold'))
    fields = {
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
    render.print_record({key: value for key, value in fields.items() if value not in (None, [])})
    render.print_records(
        [{'key': key, 'value': render.shown(key, value)} for key, value in finding.evidence.items()], ('key', 'value'), title='Evidence'
    )
    print_thresholds(finding.thresholds)
    render.print_list('How to fix it', finding.remediation)
    render.print_links(finding.docs)
    render.note(f'how the rule decides: {rule_command(finding.rule_id, threshold, thresholds)}')


def print_thresholds(applied: Mapping[str, AppliedThreshold]) -> None:
    """The thresholds a rule judged by: each one's value and who set it, beside the rule's own value and where that came from."""
    if not applied:
        render.stdout.print(Text('Thresholds: none, so nothing tunes what this rule finds'))
        render.stdout.print()
        return
    records = [
        {
            'name': name,
            'value': render.shown_in(threshold.unit, threshold.value),
            'set by': threshold.source,
            'default': render.shown_in(threshold.unit, threshold.default),
            'default from': threshold.default_source,
        }
        for name, threshold in applied.items()
    ]
    render.print_records(records, ('name', 'value', 'set by', 'default', 'default from'), title='Thresholds')


def _print_findings(listed: Analysis, found: int, limit: int | None) -> None:
    if not found:
        render.none_found('findings')
        return
    if not listed.findings:
        if limit == 0:
            render.empty('findings', limit)
        else:
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
        yields=('scope',),
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
