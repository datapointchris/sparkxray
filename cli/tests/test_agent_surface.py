"""An agent drives sparkxray from what it prints: a refusal it can branch on, and a capabilities document that holds to its schema."""

import json
import shlex
from pathlib import Path

import jsonschema
import pytest
from sparkxray.schemas import schema_document
from sparkxray_cli.main import app
from typer.testing import CliRunner


@pytest.mark.parametrize(
    ('arguments', 'code', 'field', 'one_valid'),
    [
        (
            ['rules', 'show', 'stage.shuffle-skew', '--threshold', 'stage.shuffle-skew.ratoi=3', '--json'],
            'E-RULE-002',
            'threshold.stage.shuffle-skew.ratoi',
            'ratio',
        ),
        (['schemas', 'show', 'findigns', '--json'], 'E-SCHEMA-001', 'name', 'findings'),
        (['findings', 'list', 'no-such-log', '--fail-on', 'hgih', '--json'], 'E-USAGE-001', '--fail-on', 'high'),
    ],
)
def test_a_refusal_under_json_is_one_envelope_on_stderr_naming_the_field_and_a_hint_that_runs(
    arguments: list[str], code: str, field: str, one_valid: str, tmp_path: Path
) -> None:
    runner = CliRunner(env={'XDG_CACHE_HOME': str(tmp_path)})

    result = runner.invoke(app, arguments)

    assert (result.exit_code, result.stdout) == (2, '')
    envelope = json.loads(result.stderr)
    jsonschema.validate(envelope, schema_document('errors'))
    refused = envelope['error']
    assert (refused['code'], refused['field'], one_valid in refused['valid']) == (code, field, True)
    program, *hint = shlex.split(refused['hint'])
    hinted = runner.invoke(app, hint)
    assert (program, hinted.exit_code) == ('sparkxray', 0), hinted.stderr


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
