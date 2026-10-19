"""`findings list` exits by `--fail-on`, so a CI job gates on the severity it names."""

import json
from pathlib import Path

from sparkxray_cli.main import app
from typer.testing import CliRunner

SKEWED_JOIN_LOG = Path(__file__).parents[2] / 'fixtures/logs/spark-3.5.9/local-1792406949080.zstd'


def test_findings_list_exits_3_at_or_above_fail_on_and_0_below(tmp_path: Path) -> None:
    runner = CliRunner(env={'XDG_CACHE_HOME': str(tmp_path)})

    at_default = runner.invoke(app, ['findings', 'list', str(SKEWED_JOIN_LOG), '--json'])
    at_critical = runner.invoke(app, ['findings', 'list', str(SKEWED_JOIN_LOG), '--json', '--fail-on', 'critical'])

    assert at_default.exit_code == 3, at_default.stderr
    assert json.loads(at_default.stdout)['gate'] == {'fail_on': 'high', 'passed': False, 'blocking': ['stage.shuffle-skew:s4']}
    assert at_critical.exit_code == 0, at_critical.stderr
    assert json.loads(at_critical.stdout)['gate'] == {'fail_on': 'critical', 'passed': True, 'blocking': []}
