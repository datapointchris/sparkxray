"""An agent drives sparkxray from what it prints: a refusal it can branch on, and a capabilities document that holds to its schema."""

import json
from pathlib import Path

import jsonschema
from sparkxray.schemas import schema_document
from sparkxray_cli.main import app
from typer.testing import CliRunner


def test_an_unknown_threshold_refuses_on_stderr_as_json_naming_the_field_that_failed(tmp_path: Path) -> None:
    runner = CliRunner(env={'XDG_CACHE_HOME': str(tmp_path)})

    result = runner.invoke(app, ['rules', 'show', 'stage.shuffle-skew', '--threshold', 'stage.shuffle-skew.ratoi=3', '--json'])

    assert (result.exit_code, result.stdout) == (2, '')
    envelope = json.loads(result.stderr)
    jsonschema.validate(envelope, schema_document('errors'))
    refused = envelope['error']
    assert (refused['code'], refused['field'], refused['hint']) == (
        'E-RULE-002',
        'threshold.stage.shuffle-skew.ratoi',
        'sparkxray rules show stage.shuffle-skew',
    )
    assert 'ratio' in refused['valid']


def test_capabilities_hold_to_their_schema_and_carry_each_commands_flags_and_exit_codes(tmp_path: Path) -> None:
    runner = CliRunner(env={'XDG_CACHE_HOME': str(tmp_path)})

    result = runner.invoke(app, ['capabilities', 'show', '--json'])

    assert result.exit_code == 0, result.stderr
    document = json.loads(result.stdout)
    jsonschema.validate(document, schema_document('capabilities'))
    findings_list = next(command for command in document['commands'] if command['path'] == ['findings', 'list'])
    fail_on = next(flag for flag in findings_list['flags'] if flag['name'] == '--fail-on')
    assert (fail_on['type'], fail_on['default'], 'critical' in fail_on['choices']) == ('choice', 'high', True)
    assert set(findings_list['exit_codes']) == {'0', '2', '3'}
