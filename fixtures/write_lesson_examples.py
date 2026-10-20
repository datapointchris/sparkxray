"""Write the finding each rule's lesson ends with, from the committed fixture logs.

    uv run python fixtures/write_lesson_examples.py

Each rule gets the first finding it reports on a job or query whose description seeds it, as
`fires=<rule ids>; silent=<rule ids>; <what it shows>`, and otherwise the first finding it reports
at all. Sources are read in the order below, so a log beats a captured explain, and a log is
analyzed with the driver logs its run wrote joined. The result goes
to the library's `sparkxray/rules/lesson_examples.json`. The firing test checks each example
against the finding its source still produces.
"""

import json
import pathlib
import sys
import tempfile

from sparkxray.ingest import EventLog
from sparkxray.plan import parse_plan_text
from sparkxray.rules import Finding
from sparkxray.rules import LessonExample
from sparkxray.rules import analyze_plan
from sparkxray.rules import catalog

FIXTURES = pathlib.Path(__file__).resolve().parent
OUTPUT = FIXTURES.parent / 'library' / 'src' / 'sparkxray' / 'rules' / 'lesson_examples.json'
SOURCES = (
    'logs/spark-3.5.9/local-1792406949080.zstd',
    'logs/spark-3.5.9-plan-rules/local-1792441743480.zstd',
    'logs/spark-3.5.9-stage-rules/app-20261019220120-0000.zstd',
    'logs/spark-4.2.0/eventlog_v2_local-1792407003364',
    'logs/spark-4.2.0-plan-rules/local-1792441776528.zstd',
    'logs/spark-4.2.0-stage-rules/eventlog_v2_app-20261019215829-0000',
    'logs/spark-3.5.9-edited-events/app-20261019235634-0000.zstd',
    'logs/spark-3.5.9-warmup/local-1792454673559.zstd',
    'logs/spark-4.2.0-driver-oom/eventlog_v2_local-1792509790683',
    'logs/apache-spark-events/application_1553914137147_0018.zstd',
    'explain/spark-3.5.9-plan-rules/cost-mode.json',
    'explain/spark-4.2.0-plan-rules/cost-mode.json',
)
APPLICATION_PROPERTY = 'spark.sparkxray.fixture.application'


def seeded_and_shown(description: str) -> tuple[set[str], str | None]:
    """The rules a description seeds, then what it says it shows."""
    if not description.startswith('fires='):
        return set(), description or None
    fires, _, shown = description.split('; ', 2)
    return set(fires.removeprefix('fires=').split(',')) - {''}, shown


def findings_of(source: str, cache_dir: pathlib.Path) -> list[tuple[Finding, str]]:
    """Each finding on one source, beside the description of the job, query or run it is about."""
    path = FIXTURES / source
    if path.suffix == '.json':
        return [
            (finding, capture['description'])
            for capture in json.loads(path.read_text())
            for finding in analyze_plan(parse_plan_text(capture['text'])).findings
        ]
    log = EventLog.open(path, cache_dir=cache_dir)
    if captured := sorted(str(driver_log) for driver_log in (FIXTURES / 'driver-logs' / pathlib.Path(source).parts[1]).glob('*')):
        log.enrich(driver_logs=captured)
    jobs = dict(log.tables.query("SELECT job_id, coalesce(description, '') FROM jobs").fetchall())
    executions = dict(log.tables.query("SELECT execution_id, coalesce(description, '') FROM sql_executions").fetchall())
    declared = log.tables.query(
        "SELECT value FROM environment WHERE section = 'spark' AND key = $key", {'key': APPLICATION_PROPERTY}
    ).fetchone()
    run = declared[0] if declared else ''
    described = []
    for finding in log.analyze().findings:
        if finding.scope.job_id is not None:
            described.append((finding, jobs[finding.scope.job_id]))
        elif finding.scope.sql_execution_id is not None:
            described.append((finding, executions[finding.scope.sql_execution_id]))
        else:
            described.append((finding, run))
    return described


def main() -> None:
    seeded: dict[str, LessonExample] = {}
    incidental: dict[str, LessonExample] = {}
    with tempfile.TemporaryDirectory() as cache_dir:
        for source in SOURCES:
            for finding, description in findings_of(source, pathlib.Path(cache_dir)):
                seeds, shown = seeded_and_shown(description)
                example = LessonExample.of_finding(finding, source, shown)
                (seeded if finding.rule_id in seeds else incidental).setdefault(finding.rule_id, example)
    examples = {**incidental, **seeded}
    rule_ids = [rule.rule_id for rule in catalog()]
    OUTPUT.write_text(json.dumps({rule_id: examples[rule_id].to_json() for rule_id in rule_ids if rule_id in examples}, indent=2) + '\n')
    missing = [rule_id for rule_id in rule_ids if rule_id not in examples]
    print(f'{len(examples)} examples written to {OUTPUT.relative_to(FIXTURES.parent)}', file=sys.stderr)
    for rule_id in missing:
        print(f'no fixture finding: {rule_id}', file=sys.stderr)
    for rule_id in sorted(set(incidental) - set(seeded)):
        print(f'incidental only: {rule_id} on {examples[rule_id].source}', file=sys.stderr)


if __name__ == '__main__':
    main()
