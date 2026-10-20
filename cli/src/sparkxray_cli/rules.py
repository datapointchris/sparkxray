"""`sparkxray rules`: the catalog of rules findings list runs, and one rule's lesson."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.markdown import Markdown
from rich.text import Text
from sparkxray.rules import LessonExample
from sparkxray.rules import catalog
from sparkxray.rules import rule_lesson

from sparkxray_cli import render
from sparkxray_cli.shared_parameters import JsonOption
from sparkxray_cli.shared_parameters import ThresholdFileOption
from sparkxray_cli.shared_parameters import ThresholdOption

app = typer.Typer(no_args_is_help=True, help='Learn what each rule looks for, the thresholds it judges by, and how to fix what it finds.')


@app.command('list')
def list_rules(as_json: JsonOption = False) -> None:
    """List every rule: what it is about, the signals a log must hold for it to run, and its thresholds."""
    rules = catalog()
    if as_json:
        render.emit_json([rule.to_json() for rule in rules])
        return
    records = [
        {
            'rule_id': rule.rule_id,
            'rule_version': rule.rule_version,
            'scope': rule.scope,
            'signals': record['signals'],
            'thresholds': [f'{threshold.name}={render.shown_in(threshold.unit, threshold.default)}' for threshold in rule.thresholds],
        }
        for rule, record in ((rule, rule.to_json()) for rule in rules)
    ]
    render.print_records(records, ('rule_id', 'rule_version', 'scope', 'signals', 'thresholds'))
    render.note('one in full: sparkxray rules show RULE_ID')


@app.command('show')
def show(
    rule_id: Annotated[str, typer.Argument(metavar='RULE_ID', help='A rule id, as rules list prints it.', show_default=False)],
    threshold: ThresholdOption = None,
    thresholds: ThresholdFileOption = None,
    as_json: JsonOption = False,
) -> None:
    """Show one rule: its thresholds and who set each, its severity ladder, its lesson, and what it found on a fixture log."""
    lesson = rule_lesson(rule_id, threshold_file=thresholds, threshold_flags=threshold or ())
    if as_json:
        render.emit_json(lesson.to_json())
        return
    record = lesson.rule.to_json()
    render.print_record({key: record[key] for key in ('rule_id', 'rule_version', 'scope', 'signals')})
    applied = [
        {
            'name': name,
            'value': render.shown_in(value.unit, value.value),
            'source': value.source,
            'default': render.shown_in(value.unit, value.default),
            'default_source': value.default_source,
        }
        for name, value in lesson.thresholds.items()
    ]
    render.print_records(applied, ('name', 'value', 'source', 'default', 'default_source'), title='Thresholds')
    ladder = [{'severity': severity, 'when': when} for severity, when in record['severity_ladder'].items()]
    render.print_records(ladder, ('severity', 'when'), title='Severity')
    render.stdout.print(Markdown(lesson.text))
    if lesson.example is not None:
        _print_example(lesson.example, at_defaults=lesson.overridden)
    render.print_links(lesson.rule.docs)


def _print_example(example: LessonExample, *, at_defaults: bool) -> None:
    render.stdout.print()
    render.stdout.print(Markdown('## What it found on a fixture log' + (', at the default thresholds' if at_defaults else '')))
    render.stdout.print(Text(example.title, style='bold'))
    render.print_record(
        {
            'source': f'fixtures/{example.source}',
            'description': example.description,
            'finding_id': example.finding_id,
            'severity': example.severity,
            'scope': example.scope,
        }
    )
    render.print_records(
        [{'key': key, 'value': render.shown(key, value)} for key, value in example.evidence.items()], ('key', 'value'), title='Evidence'
    )
