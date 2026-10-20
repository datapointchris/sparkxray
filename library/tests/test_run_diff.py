"""Two runs of one job diff with their SQL executions and stages paired by what they ran, though every id of the second is shifted."""

from pathlib import Path

import jsonschema
import referencing
from fixture_logs import FIXTURES
from sparkxray.ingest import EventLog
from sparkxray.run_diff import diff_logs
from sparkxray.schemas import schema_document

BASE = FIXTURES / 'logs/spark-3.5.9/local-1792406949080.zstd'
DIFF_SCHEMA = jsonschema.Draft202012Validator(
    schema_document('diff'),
    registry=referencing.Registry().with_resource('findings.json', referencing.Resource.from_contents(schema_document('findings'))),
)


def test_the_warmup_run_pairs_every_execution_and_stage_of_the_base_run_and_the_base_run_fails_the_gate_on_its_risen_spill(
    tmp_path: Path,
) -> None:
    base = EventLog.open(BASE, cache_dir=tmp_path)
    warmup = EventLog.open(FIXTURES / 'logs/spark-3.5.9-warmup/local-1792454673559.zstd', cache_dir=tmp_path)

    diff = diff_logs(base, warmup)

    DIFF_SCHEMA.validate(diff.to_json())
    assert [(pair.a_execution_id, pair.b_execution_id) for pair in diff.sql.matched] == [(n, n + 1) for n in range(7)]
    assert [execution.execution_id for execution in diff.sql.only_b] == [0]
    assert diff.stages.only_a == ()
    assert [stage for stage in diff.stages.matched if stage.b_stage_id != stage.a_stage_id + 6] == []
    assert 'stage.shuffle-skew:s4' in {finding.finding_id for finding in diff.findings.resolved}
    assert ('spark', 'spark.sql.adaptive.skewJoin.enabled', 'false', 'true') in {
        (setting.section, setting.key, setting.a, setting.b) for setting in diff.config.changed
    }
    # The warmup run's stage.spill:s10 is high, and the base run's stage.spill:s4 critical.
    assert diff_logs(warmup, base).gate('critical').blocking == ('stage.spill:s4',)


def test_a_run_killed_partway_resolves_none_of_the_findings_on_the_parts_it_never_finished(tmp_path: Path) -> None:
    base = EventLog.open(BASE, cache_dir=tmp_path)
    killed = EventLog.open(FIXTURES / 'logs/spark-3.5.9-killed/local-1792407096495.zstd.inprogress', cache_dir=tmp_path)

    findings = diff_logs(base, killed).findings

    assert findings.resolved == ()
    assert {'stage.shuffle-skew:s4', 'stage.spill:s9'} <= {finding.finding_id for finding in findings.on_parts_b_did_not_finish}
