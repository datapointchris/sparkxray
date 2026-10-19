"""`sparkxray rules`: the catalog of rules findings list runs, and one rule's lesson."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.markdown import Markdown
from sparkxray.rules import catalog
from sparkxray.rules import rule_by_id

from sparkxray_cli import render
from sparkxray_cli.shared_parameters import JsonOption

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
    as_json: JsonOption = False,
) -> None:
    """Show one rule: its thresholds and where each default came from, its severity ladder, and its lesson."""
    rule = rule_by_id(rule_id)
    if as_json:
        render.emit_json(rule.to_json())
        return
    record = rule.to_json()
    render.print_record({key: record[key] for key in ('rule_id', 'rule_version', 'scope', 'signals')})
    thresholds = [
        {
            'name': threshold.name,
            'default': render.shown_in(threshold.unit, threshold.default),
            'unit': threshold.unit,
            'source': threshold.source,
        }
        for threshold in rule.thresholds
    ]
    render.print_records(thresholds, ('name', 'default', 'unit', 'source'), title='Thresholds')
    ladder = [{'severity': severity, 'when': when} for severity, when in record['severity_ladder'].items()]
    render.print_records(ladder, ('severity', 'when'), title='Severity')
    render.stdout.print(Markdown(rule.lesson))
    render.print_links(rule.docs)
