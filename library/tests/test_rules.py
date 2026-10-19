"""Rules run wherever their signals exist, a finding records the layer that set each threshold it applied, and both fit the schema."""

import importlib.resources
import json
import pathlib

import jsonschema
from fixture_logs import FIXTURES
from fixture_logs import plain_lines
from sparkxray.ingest import EventLog
from sparkxray.plan import parse_plan_text
from sparkxray.rules import analyze_plan

FINDINGS_SCHEMA = jsonschema.Draft202012Validator(
    json.loads(importlib.resources.files('sparkxray').joinpath('schemas', 'findings.json').read_text())
)


def test_a_plan_without_metrics_runs_the_plan_rules_and_names_what_the_others_lack() -> None:
    events = [json.loads(line) for line in plain_lines(FIXTURES / 'logs' / 'spark-3.5.9-plan-shapes' / 'local-1792412428407.zstd')]
    text = next(
        event['physicalPlanDescription'] for event in events if event['Event'].endswith('SQLExecutionStart') and event['executionId'] == 15
    )

    analysis = analyze_plan(parse_plan_text(text))

    assert [(finding.rule_id, finding.severity) for finding in analysis.findings] == [('plan.cartesian', 'info')]
    assert [(skipped.rule_id, skipped.missing_signals) for skipped in analysis.skipped] == [('stage.shuffle-skew', ('tasks',))]
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
