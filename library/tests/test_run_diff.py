"""Two runs of one job diff with their SQL executions and stages paired by what they ran, though every id of the second is shifted."""

from pathlib import Path

from fixture_logs import FIXTURES
from sparkxray.ingest import EventLog
from sparkxray.run_diff import diff_logs


def test_the_warmup_run_pairs_every_execution_and_stage_of_the_base_run_and_resolves_its_shuffle_skew(tmp_path: Path) -> None:
    base = EventLog.open(FIXTURES / 'logs/spark-3.5.9/local-1792406949080.zstd', cache_dir=tmp_path)
    warmup = EventLog.open(FIXTURES / 'logs/spark-3.5.9-warmup/local-1792454673559.zstd', cache_dir=tmp_path)

    diff = diff_logs(base, warmup)

    assert sorted((pair.a_execution_id, pair.b_execution_id) for pair in diff.sql.matched) == [(n, n + 1) for n in range(7)]
    assert [execution.execution_id for execution in diff.sql.only_b] == [0]
    assert diff.stages.only_a == ()
    assert [stage for stage in diff.stages.matched if stage.b_stage_id != stage.a_stage_id + 6] == []
    assert 'stage.shuffle-skew:s4' in {finding.finding_id for finding in diff.findings.resolved}
    assert ('spark', 'spark.sql.adaptive.skewJoin.enabled', 'false', 'true') in {
        (setting.section, setting.key, setting.a, setting.b) for setting in diff.config.changed
    }
