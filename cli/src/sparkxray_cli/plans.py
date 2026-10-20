"""`sparkxray plans`: one SQL execution's plan as a tree, with the SQL tab's numbers on each node and the findings that name it."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.text import Text
from rich.tree import Tree
from sparkxray.rules import Finding
from sparkxray.summaries import AnnotatedNode
from sparkxray.summaries import AnnotatedPlan
from sparkxray.summaries import NodeMetric
from sparkxray.summaries import annotated_plan

from sparkxray_cli import render
from sparkxray_cli.findings import show_command
from sparkxray_cli.log_opening import analyze_log
from sparkxray_cli.log_opening import open_log
from sparkxray_cli.log_opening import tables_of
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import LogArgument
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(no_args_is_help=True, help='See where in a query the rows, bytes and time went, and which node a finding is about.')

_UNPLACED_KEYS = ('severity', 'finding_id', 'scope', 'title')


@app.command('show')
def show(
    log: LogArgument,
    execution_id: Annotated[
        int, typer.Option('--sql', metavar='ID', help='The SQL execution, by the id sql list prints.', show_default=False)
    ],
    initial: Annotated[
        bool, typer.Option('--initial', help='Show the plan the execution started from, version 0, before AQE re-planned it.')
    ] = False,
    version: Annotated[
        int | None,
        typer.Option(
            '--version',
            metavar='N',
            min=0,
            help='Show plan version N: 0 is the plan the execution started from, and each AQE re-plan adds one. The last by default.',
            show_default=False,
        ),
    ] = None,
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    as_json: JsonOption = False,
) -> None:
    """Show one SQL execution's plan as a tree: what each node does, its metrics and stages, and the findings that name it.

    The last plan carries the findings on the nodes they name. An earlier version, from
    `--initial` or `--version`, lists them below the tree, because findings number the last
    plan's nodes. A node AQE kept carries its metrics into every later version, so an earlier
    version shows the values of the nodes it shares with the last.
    """
    if initial and version not in (None, 0):
        raise typer.BadParameter('--initial is version 0, so give one of --initial and --version', param_hint='--initial')
    event_log = open_log(log)
    plan = annotated_plan(tables_of(event_log), execution_id, version=0 if initial else version)
    plan = plan.with_findings(analyze_log(event_log, None, threshold, thresholds).findings)
    if as_json:
        render.emit_json(plan.to_json())
        return
    _print_plan(plan)
    if plan.findings:
        render.note(f'one finding in full: {show_command(log, plan.findings[0].finding_id, threshold, thresholds)}')


def _print_plan(plan: AnnotatedPlan) -> None:
    findings = {finding.finding_id: finding for finding in plan.findings}
    render.stdout.print(Text(_title(plan), style='bold'))
    for node in plan.nodes:
        if node.parent is None:
            tree = Tree(_label(node, findings), guide_style='dim')
            _add_branches(tree, plan, node, findings)
            render.stdout.print(tree)
    render.stdout.print()
    if plan.truncated:
        render.note('Spark cut this plan short, so a node past the cut is not in it')
    unplaced = plan.unplaced_findings()
    if unplaced:
        records = [{'severity': f.severity, 'finding_id': f.finding_id, 'scope': f.scope.describe(), 'title': f.title} for f in unplaced]
        render.print_records(records, _UNPLACED_KEYS, title='Findings on no node shown')
    render.note('each metric with its min, median and max, and the task that reported the max: --json')


def _title(plan: AnnotatedPlan) -> str:
    execution = plan.execution
    if plan.version == plan.last_version:
        version = f'final plan, version {plan.version}' if plan.is_final else f'last plan, version {plan.version}, not final'
    elif plan.version == 0:
        version = 'initial plan, version 0'
    else:
        version = f'plan version {plan.version} of {plan.last_version}'
    parts = [
        f'SQL {plan.execution_id}',
        execution['description'],
        render.shown('duration_ms', execution['duration_ms']),
        f'stages {render.shown_in(None, list(plan.stage_ids))}' if plan.stage_ids else '',
        version,
    ]
    return ' · '.join(part for part in parts if part)


def _add_branches(tree: Tree, plan: AnnotatedPlan, node: AnnotatedNode, findings: dict[str, Finding]) -> None:
    for number in node.children:
        child = plan.nodes[number]
        _add_branches(tree.add(_label(child, findings)), plan, child, findings)
    for subquery in node.subqueries:
        root = plan.nodes[subquery.root]
        named = Text(f'subquery {subquery.subquery_id}  ' if subquery.subquery_id is not None else 'subquery  ', style='dim')
        _add_branches(tree.add(named + _label(root, findings)), plan, root, findings)


def _label(node: AnnotatedNode, findings: dict[str, Finding]) -> Text:
    """The node's headline, its runtime statistics and finding marks, then a line of its stages and metrics."""
    headline = node.headline()
    label = Text(node.operator, style='bold') + Text(headline[len(node.operator) :])
    runtime = []
    if node.runtime_size_bytes is not None:
        runtime.append(render.shown_in('bytes', node.runtime_size_bytes))
    if node.runtime_row_count is not None:
        runtime.append(f'{render.shown_in("rows", node.runtime_row_count)} rows')
    if runtime:
        label.append(f'  {" · ".join(runtime)} (runtime)', style='cyan')
    for finding_id in node.finding_ids:
        finding = findings[finding_id]
        stage = f'stage {finding.scope.stage_id}, ' if finding.scope.stage_id is not None else ''
        label.append(f'  ◀ {finding.rule_id} ({stage}{finding.severity})', style=render.SEVERITY_STYLES[finding.severity])
    measured = [f'stage{"s" if len(node.stage_ids) > 1 else ""} {render.shown_in(None, list(node.stage_ids))}'] if node.stage_ids else []
    measured += [shown for shown in map(_shown_metric, node.metrics) if shown]
    if measured:
        label.append('\n' + ' · '.join(measured), style='dim')
    return label


def _shown_metric(metric: NodeMetric) -> str | None:
    """A metric as its name and total, or for an average its min, median and max. Zero, and an average nothing reported, print nothing."""
    if metric.total is None:
        if not metric.max:
            return None
        spread = ', '.join(render.shown_in(metric.unit, value) for value in (metric.min, metric.median, metric.max))
        return f'{metric.name} ({spread})'
    return f'{metric.name} {render.shown_in(metric.unit, metric.total)}' if metric.total else None
