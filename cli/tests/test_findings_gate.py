"""`findings list` exits by `--fail-on`, so a CI job gates on the severity it names, over the rules it names."""

import json
from pathlib import Path

from sparkxray_cli.main import app
from typer.testing import CliRunner

SKEWED_JOIN_LOG = Path(__file__).parents[2] / 'fixtures/logs/spark-3.5.9/local-1792406949080.zstd'


def test_findings_list_exits_3_at_or_above_fail_on_and_0_below_over_the_named_rules_alone(tmp_path: Path) -> None:
    runner = CliRunner(env={'XDG_CACHE_HOME': str(tmp_path)})

    skew_only = ['findings', 'list', str(SKEWED_JOIN_LOG), '--rule', 'stage.shuffle-skew', '--json']
    at_default = runner.invoke(app, skew_only)
    at_critical = runner.invoke(app, [*skew_only, '--fail-on', 'critical'])
    # stage.oversized-partitions reads stage.shuffle-skew's findings, so the high s4 finding runs unlisted beneath it.
    reading_skew = runner.invoke(app, ['findings', 'list', str(SKEWED_JOIN_LOG), '--rule', 'stage.oversized-partitions', '--json'])

    assert at_default.exit_code == 3, at_default.stderr
    gate = json.loads(at_default.stdout)['gate']
    assert (gate['fail_on'], gate['passed'], 'stage.shuffle-skew:s4' in gate['blocking']) == ('high', False, True)
    assert at_critical.exit_code == 0, at_critical.stderr
    assert json.loads(at_critical.stdout)['gate'] == {'fail_on': 'critical', 'passed': True, 'blocking': []}
    assert reading_skew.exit_code == 0, reading_skew.stderr
    assert json.loads(reading_skew.stdout)['gate'] == {'fail_on': 'high', 'passed': True, 'blocking': []}
