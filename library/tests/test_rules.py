"""Rules run wherever their signals exist, fire on the fixture queries naming them, teach in four parts, and record who set thresholds."""

import dataclasses as dc
import importlib.resources
import json
import pathlib

import jsonschema
import pytest
from fixture_logs import FIXTURES
from fixture_logs import driver_logs
from fixture_logs import plain_lines
from sparkxray.ingest import EventLog
from sparkxray.plan import parse_plan_text
from sparkxray.rules import Finding
from sparkxray.rules import LessonExample
from sparkxray.rules import analyze_plan
from sparkxray.rules.lessons import lesson_examples

FINDINGS_SCHEMA = jsonschema.Draft202012Validator(
    json.loads(importlib.resources.files('sparkxray').joinpath('schemas', 'findings.json').read_text())
)


APPLICATION_PROPERTY = 'spark.sparkxray.fixture.application'


def _expected(description: str) -> tuple[set[str], set[str]]:
    """The rules a fixture description says fire and stay silent, from `fires=a,b; silent=c; what it does`."""
    if not description.startswith('fires='):
        return set(), set()
    fires, silent, _ = description.split('; ', 2)
    return {rule for rule in fires.removeprefix('fires=').split(',') if rule}, {
        rule for rule in silent.removeprefix('silent=').split(',') if rule
    }


def _firings(path: pathlib.Path, cache_dir: pathlib.Path) -> tuple[dict[str, set[str]], str, set[str], list[Finding]]:
    """The rules that fired under each job or query description, the run's own description, the rules fired on the run, and every finding.

    A finding scoped to a job counts under the job's description, one scoped to a SQL execution under the execution's,
    and any other under the run, whose description is the `spark.sparkxray.fixture.application` property.
    """
    if path.suffix == '.json':
        captured: dict[str, set[str]] = {}
        every: list[Finding] = []
        for capture in json.loads(path.read_text()):
            plan_only = analyze_plan(parse_plan_text(capture['text']))
            FINDINGS_SCHEMA.validate(plan_only.to_json())
            captured[capture['description']] = {finding.rule_id for finding in plan_only.findings}
            every.extend(plan_only.findings)
        return captured, '', set(), every
    log = EventLog.open(path, cache_dir=cache_dir)
    if captured_driver_logs := driver_logs(path):
        log.enrich(driver_logs=captured_driver_logs)
    analysis = log.analyze()
    FINDINGS_SCHEMA.validate(analysis.to_json())
    jobs = dict(log.tables.query("SELECT job_id, coalesce(description, '') FROM jobs").fetchall())
    executions = dict(log.tables.query("SELECT execution_id, coalesce(description, '') FROM sql_executions").fetchall())
    declared = log.tables.query(
        "SELECT value FROM environment WHERE section = 'spark' AND key = $key", {'key': APPLICATION_PROPERTY}
    ).fetchone()
    fired: dict[str, set[str]] = {description: set() for description in (*jobs.values(), *executions.values())}
    on_run = set()
    for finding in analysis.findings:
        if finding.scope.job_id is not None:
            fired[jobs[finding.scope.job_id]].add(finding.rule_id)
        elif finding.scope.sql_execution_id is not None:
            fired[executions[finding.scope.sql_execution_id]].add(finding.rule_id)
        else:
            on_run.add(finding.rule_id)
    return fired, declared[0] if declared else '', on_run, list(analysis.findings)


def _misfired(description: str, fired: set[str], families: tuple[str, ...]) -> tuple[list[str], list[str]]:
    """The rules a description lists as firing that did not, then those that fired unwanted.

    A rule fired unwanted where the description lists it silent, or where it is of one of `families` and not listed.
    """
    fires, silent = _expected(description)
    unwanted = (fired & silent) | {rule for rule in fired - fires if rule.startswith(families)}
    return sorted(fires - fired), sorted(unwanted)


# Each source holds every rule of the families beside it to its descriptions' lists, and every
# other rule only to the rules a description names.
@pytest.mark.parametrize(
    ('source', 'families'),
    [
        ('logs/spark-3.5.9/local-1792406949080.zstd', ('plan.',)),
        ('logs/spark-4.2.0/eventlog_v2_local-1792407003364', ('plan.',)),
        ('logs/spark-3.5.9-warmup/local-1792454673559.zstd', ('plan.',)),
        ('logs/spark-3.5.9-plan-rules/local-1792441743480.zstd', ('plan.',)),
        ('logs/spark-4.2.0-plan-rules/local-1792441776528.zstd', ('plan.',)),
        ('explain/spark-3.5.9-plan-rules/cost-mode.json', ('plan.',)),
        ('explain/spark-4.2.0-plan-rules/cost-mode.json', ('plan.',)),
        ('logs/spark-3.5.9-stage-rules/app-20261019220120-0000.zstd', ('stage.', 'config.')),
        ('logs/spark-4.2.0-stage-rules/eventlog_v2_app-20261019215829-0000', ('stage.', 'config.')),
        ('logs/spark-3.5.9-edited-events/app-20261019235634-0000.zstd', ('stage.', 'config.')),
        ('logs/spark-4.2.0-driver-oom/eventlog_v2_local-1792509790683', ()),
        ('logs/apache-spark-events/application_1553914137147_0018.zstd', ()),
    ],
)
def test_each_rule_fires_where_a_fixture_lists_it_and_each_lesson_example_is_what_its_source_reports(
    source: str, families: tuple[str, ...], tmp_path: pathlib.Path
) -> None:
    fired, run_description, on_run, findings = _firings(FIXTURES / source, tmp_path)

    misfired = {description: _misfired(description, rules, families) for description, rules in fired.items()}
    # The run's findings are held only to what its description names: an executor's memory peak depends on when it was polled.
    misfired[f'the run: {run_description}'] = _misfired(run_description, on_run, ())
    assert {description: wrong for description, wrong in misfired.items() if wrong != ([], [])} == {}
    # `fixtures/write_lesson_examples.py` writes the examples again after a rule or a fixture changes.
    reported = [LessonExample.of_finding(finding, source, None) for finding in findings]
    examples = {rule_id: example for rule_id, example in lesson_examples().items() if example.source == source}
    assert sorted(rule_id for rule_id, example in examples.items() if dc.replace(example, description=None) not in reported) == []


def test_a_plan_without_metrics_runs_the_plan_rules_and_names_what_the_others_lack() -> None:
    events = [json.loads(line) for line in plain_lines(FIXTURES / 'logs' / 'spark-3.5.9-plan-shapes' / 'local-1792455590972.zstd')]
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
