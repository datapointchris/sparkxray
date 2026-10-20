"""`sparkxray plans`: one SQL execution's plan as a tree, with the SQL tab's numbers on each node and the findings that name it."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.text import Text
from rich.tree import Tree
from sparkxray.summaries import AnnotatedNode
from sparkxray.summaries import AnnotatedPlan
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
            '--plan-version',
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

    Each node opens with its number, as finding ids and `sql show` name it, such as `n9`. A node
    Spark compiled into a codegen pipeline carries the pipeline's `*(N)`, and the top one its
    `duration`: each task's time from the pipeline's start to its last row, reading its inputs
    included. A Sort's `spill size` counts the spills of the nodes it reads from in its task, so
    it also states its own part.

    The last plan carries the findings on the nodes they name. An earlier version, from
    `--initial` or `--plan-version`, lists them below the tree, because findings number the last
    plan's nodes. A node AQE kept carries its metrics into every later version, so an earlier
    version shows the values of the nodes it shares with the last.
    """
    if initial and version not in (None, 0):
        raise typer.BadParameter('--initial is version 0, so give one of --initial and --plan-version', param_hint='--initial')
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
    render.stdout.print(Text(_title(plan), style='bold'))
    for node in plan.nodes:
        if node.parent is None:
            tree = Tree(_label(plan, node), guide_style='dim')
            _add_branches(tree, plan, node)
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


def _add_branches(tree: Tree, plan: AnnotatedPlan, node: AnnotatedNode) -> None:
    for number in node.children:
        child = plan.nodes[number]
        _add_branches(tree.add(_label(plan, child)), plan, child)
    for subquery in node.subqueries:
        root = plan.nodes[subquery.root]
        named = Text(f'subquery {subquery.subquery_id}  ' if subquery.subquery_id is not None else 'subquery  ', style='dim')
        _add_branches(tree.add(named + _label(plan, root)), plan, root)


def _label(plan: AnnotatedPlan, node: AnnotatedNode) -> Text:
    """The node's line as the report draws it, styled: number and pipeline dim, operator bold, runtime cyan, marks by severity."""
    line = plan.node_line(node)
    label = Text(f'{line.number} ', style='dim')
    if line.codegen:
        label.append(f'{line.codegen} ', style='dim')
    label += Text(line.operator, style='bold') + Text(line.rest)
    if line.runtime:
        label.append(f'  {line.runtime}', style='cyan')
    for mark in line.marks:
        label.append(f'  {mark.text}', style=render.SEVERITY_STYLES[mark.severity])
    if line.measured:
        label.append('\n' + line.measured, style='dim')
    return label
