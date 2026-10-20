"""Rules run wherever their signals exist, fire on the fixture queries naming them, and record who set each threshold."""

import importlib.resources
import json
import pathlib

import jsonschema
import pytest
from fixture_logs import FIXTURES
from fixture_logs import plain_lines
from sparkxray.ingest import EventLog
from sparkxray.plan import parse_plan_text
from sparkxray.rules import analyze_plan

FINDINGS_SCHEMA = jsonschema.Draft202012Validator(
    json.loads(importlib.resources.files('sparkxray').joinpath('schemas', 'findings.json').read_text())
)


def _listed(description: str) -> set[str]:
    """The plan rules a fixture query's description says fire: `fires=a,b; silent=c; what the query does`."""
    return {rule for rule in description.split('; ', 1)[0].removeprefix('fires=').split(',') if rule.startswith('plan.')}


def _plan_firings(path: pathlib.Path, cache_dir: pathlib.Path) -> list[tuple[str, set[str]]]:
    """Each query's description beside the plan rules that fired on it, from a log or from captured cost-mode text."""
    if path.suffix == '.json':
        captured = []
        for capture in json.loads(path.read_text()):
            plan_only = analyze_plan(parse_plan_text(capture['text']))
            FINDINGS_SCHEMA.validate(plan_only.to_json())
            captured.append((capture['description'], {finding.rule_id for finding in plan_only.findings}))
        return captured
    log = EventLog.open(path, cache_dir=cache_dir)
    analysis = log.analyze()
    FINDINGS_SCHEMA.validate(analysis.to_json())
    fired: dict[int, set[str]] = {}
    for finding in analysis.findings:
        if finding.rule_id.startswith('plan.') and finding.scope.sql_execution_id is not None:
            fired.setdefault(finding.scope.sql_execution_id, set()).add(finding.rule_id)
    executions = log.tables.query('SELECT execution_id, description FROM sql_executions ORDER BY execution_id').fetchall()
    return [(description or '', fired.get(execution_id, set())) for execution_id, description in executions]


@pytest.mark.parametrize(
    'source',
    [
        'logs/spark-3.5.9/local-1792406949080.zstd',
        'logs/spark-4.2.0/eventlog_v2_local-1792407003364',
        'logs/spark-3.5.9-plan-rules/local-1792441743480.zstd',
        'logs/spark-4.2.0-plan-rules/local-1792441776528.zstd',
        'explain/spark-3.5.9-plan-rules/cost-mode.json',
        'explain/spark-4.2.0-plan-rules/cost-mode.json',
    ],
)
def test_each_plan_rule_fires_on_the_fixture_queries_that_list_it_and_on_no_other(source: str, tmp_path: pathlib.Path) -> None:
    fired = _plan_firings(FIXTURES / source, tmp_path)

    assert fired == [(description, _listed(description)) for description, _ in fired]


def test_a_plan_without_metrics_runs_the_plan_rules_and_names_what_the_others_lack() -> None:
    events = [json.loads(line) for line in plain_lines(FIXTURES / 'logs' / 'spark-3.5.9-plan-shapes' / 'local-1792412428407.zstd')]
    text = next(
        event['physicalPlanDescription'] for event in events if event['Event'].endswith('SQLExecutionStart') and event['executionId'] == 15
    )

    analysis = analyze_plan(parse_plan_text(text))

    assert [(finding.rule_id, finding.severity) for finding in analysis.findings] == [('plan.cartesian', 'info')]
    missing = {skipped.rule_id: skipped.missing_signals for skipped in analysis.skipped}
    assert (missing['stage.shuffle-skew'], missing['plan.broadcast-too-large']) == (('tasks',), ('plan_metrics',))
    FINDINGS_SCHEMA.validate(analysis.to_json())


def test_a_threshold_from_a_file_applies_and_records_the_file_as_its_source(tmp_path: pathlib.Path) -> None:
    log = EventLog.open(FIXTURES / 'logs' / 'spark-3.5.9' / 'local-1792406949080.zstd', cache_dir=tmp_path / 'cache')
    raised, silenced = tmp_path / 'raised.toml', tmp_path / 'silenced.toml'
    raised.write_text('["stage.shuffle-skew"]\nratio = 8\nmin_bytes = "512MiB"\n')
    silenced.write_text('["stage.shuffle-skew"]\nratio = 60\n')

    analysis = log.analyze(rules=['stage.shuffle-skew'], threshold_file=raised)

    (finding,) = analysis.findings
    applied = {name: (threshold.value, threshold.source) for name, threshold in finding.thresholds.items()}
    assert applied == {'ratio': (8, 'file'), 'min_bytes': (536_870_912, 'file'), 'min_tasks': (10, 'default')}
    FINDINGS_SCHEMA.validate(analysis.to_json())
    assert log.analyze(rules=['stage.shuffle-skew'], threshold_file=silenced).findings == ()
    assert log.tables.stored_analysis()['rules_run'][0]['thresholds']['ratio']['source'] == 'file'
