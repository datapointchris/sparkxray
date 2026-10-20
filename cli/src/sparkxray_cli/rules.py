"""`sparkxray rules`: the catalog of rules findings list runs, and one rule's lesson."""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Annotated

import typer
from rich.markdown import Markdown
from rich.text import Text
from sparkxray.rules import LessonExample
from sparkxray.rules import catalog
from sparkxray.rules import rule_lesson

from sparkxray_cli import render
from sparkxray_cli.findings import print_thresholds
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(no_args_is_help=True, help='Learn what each rule looks for, the thresholds it judges by, and how to fix what it finds.')

# A title's `{name}` or `{name:spec}`, where a `plural` spec carries the noun, and a second form after `|`.
_PLACEHOLDER = re.compile(r'\{[^{}:]*(?::plural ([^{}|]*)(?:\|([^{}]*))?|:[^{}]*)?\}')


@app.command('list')
def list_rules(as_json: JsonOption = False) -> None:
    """List every rule: what it finds, what it is about, the signals a log must hold for it to run, and its thresholds.

    A signal is a part of a run a rule reads. plan, plan_metrics and plan_stages are a query's
    plan, its nodes' metrics, and the stages each node ran in. tasks, executors, environment and
    timeline are the run's tasks, executors, settings and stage times. whole_run means the server
    dropped none of the run. glue_metrics, glue_insights and driver_log hold rows once `logs ingest`
    joins a Glue run or a driver log. plan_estimates is printed by explain(mode='cost') alone. A
    log lacking a rule's signal skips the rule, and `findings list` names each skip.
    """
    rules = catalog()
    if as_json:
        render.emit_json([rule.to_json() for rule in rules])
        return
    records = [
        {
            'rule_id': rule.rule_id,
            'finds': _in_words(rule.title),
            'scope': rule.scope,
            'signals': record['signals'],
            'thresholds': [f'{threshold.name}={render.shown_in(threshold.unit, threshold.default)}' for threshold in rule.thresholds],
        }
        for rule, record in ((rule, rule.to_json()) for rule in rules)
    ]
    render.print_records(records, ('rule_id', 'finds', 'scope', 'signals', 'thresholds'), yields=('signals', 'thresholds'))
    render.note(f'one in full: sparkxray rules show {rules[0].rule_id}')


def _in_words(title: str) -> str:
    """A finding title's template as a reader takes it in, each value as `…`: `… of … jobs failed`."""

    def word(match: re.Match[str]) -> str:
        singular, plural = match.group(1), match.group(2)
        return '…' if singular is None else f'… {plural or singular + "s"}'

    return _PLACEHOLDER.sub(word, title)


@app.command('show', no_args_is_help=True)
def show(
    rule_id: Annotated[str, typer.Argument(metavar='RULE_ID', help='A rule id, as rules list prints it.', show_default=False)],
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    as_json: JsonOption = False,
) -> None:
    """Show one rule: its thresholds and who set each, its severity ladder, its lesson, and what it found on a test run.

    A threshold's default comes from the tool or document its `default from` names, and
    `proposed` marks one sparkxray chose itself, where no published tool gives one. The example
    is what the rule found on one of the runs sparkxray is tested against.
    """
    lesson = rule_lesson(rule_id, threshold_file=thresholds, threshold_flags=threshold or ())
    if as_json:
        render.emit_json(lesson.to_json())
        return
    record = lesson.rule.to_json()
    render.print_record({key: record[key] for key in ('rule_id', 'rule_version', 'scope', 'signals')})
    print_thresholds(lesson.thresholds)
    ladder = [{'severity': severity, 'when': when} for severity, when in record['severity_ladder'].items()]
    render.print_records(ladder, ('severity', 'when'), title='Severity')
    if lesson.rule.scope in ('stage', 'sql_execution', 'plan_node'):
        render.stdout.print(
            Text(
                'A finding moves one step from this grade by the share of the wall time its stage or SQL execution took: '
                'down below 5%, up above 50%. info never moves, and nothing drops below low.'
            )
        )
        render.stdout.print()
    render.stdout.print(Markdown(lesson.text))
    if lesson.example is not None:
        _print_example(lesson.example, at_defaults=lesson.overridden)
    render.print_links(lesson.rule.docs)


def _print_example(example: LessonExample, *, at_defaults: bool) -> None:
    run = PurePosixPath(example.source).parent.name
    render.stdout.print()
    render.stdout.print(
        Markdown(f"## What it found on sparkxray's test run {run}" + (', at the default thresholds' if at_defaults else ''))
    )
    render.stdout.print(Text(example.title, style='bold'))
    fields = {'description': example.description, 'finding_id': example.finding_id, 'severity': example.severity, 'scope': example.scope}
    render.print_record({key: value for key, value in fields.items() if value})
    render.print_records(
        [{'key': key, 'value': render.shown(key, value)} for key, value in example.evidence.items()], ('key', 'value'), title='Evidence'
    )
