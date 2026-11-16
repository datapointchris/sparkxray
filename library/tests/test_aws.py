"""S3, CloudWatch and CloudWatch Logs reads, against moto's stand-in for each."""

import datetime as dt
import pathlib
from collections.abc import Iterator

import boto3
import pytest
from fixture_logs import FIXTURES
from sparkxray.enrichment import GlueRun
from sparkxray.errors import NoGlueMetricsError
from sparkxray.ingest import EventLog
from sparkxray.tables.registry import RUN_SPAN_SQL

moto = pytest.importorskip('moto', reason='moto is in the dev group, which the unlocked library install leaves out')

_LOG = FIXTURES / 'logs' / 'spark-3.5.9' / 'local-1792406949080.zstd'
_JOB = 'nightly-orders'
_RUN = 'jr_0123456789abcdef'


@pytest.fixture(autouse=True)
def _aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A region and stand-in credentials, with every real AWS endpoint replaced by moto's."""
    for name in (
        'AWS_PROFILE',
        'AWS_ENDPOINT_URL',
        'AWS_ENDPOINT_URL_S3',
        'AWS_ENDPOINT_URL_CLOUDWATCH',
        'AWS_ENDPOINT_URL_CLOUDWATCH_LOGS',
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    with moto.mock_aws():
        yield


def test_a_log_in_s3_reads_as_the_file_does(tmp_path: pathlib.Path) -> None:
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='spark-logs')
    s3.upload_file(str(_LOG), 'spark-logs', f'events/{_LOG.name}')

    from_s3 = EventLog.open(f's3://spark-logs/events/{_LOG.name}', cache_dir=tmp_path / 's3')
    from_file = EventLog.open(_LOG, cache_dir=tmp_path / 'file')

    tasks = 'SELECT * FROM tasks ORDER BY ALL'
    assert from_s3.tables.query(tasks).fetchall() == from_file.tables.query(tasks).fetchall()
    assert from_s3.manifest.source == f's3://spark-logs/events/{_LOG.name}'


def test_a_glue_run_s_metrics_and_insights_join_the_log(tmp_path: pathlib.Path) -> None:
    log = EventLog.open(_LOG, cache_dir=tmp_path)
    span = log.tables.first_row(RUN_SPAN_SQL)
    assert span is not None
    start_epoch_ms, _ = span
    sampled = dt.datetime.fromtimestamp(start_epoch_ms / 1000, tz=dt.UTC) + dt.timedelta(minutes=1)
    boto3.client('cloudwatch').put_metric_data(
        Namespace='Glue',
        MetricData=[
            {
                'MetricName': 'glue.driver.workerUtilization',
                'Dimensions': [
                    {'Name': 'JobName', 'Value': _JOB},
                    {'Name': 'JobRunId', 'Value': _RUN},
                    {'Name': 'Type', 'Value': 'gauge'},
                    {'Name': 'ObservabilityGroup', 'Value': 'resource_utilization'},
                ],
                'Timestamp': sampled,
                'Value': 0.25,
            }
        ],
    )
    logs = boto3.client('logs')
    logs.create_log_group(logGroupName='/aws-glue/jobs/logs-v2')
    logs.create_log_stream(logGroupName='/aws-glue/jobs/logs-v2', logStreamName=f'{_RUN}-job-insights-rule-driver')
    logs.put_log_events(
        logGroupName='/aws-glue/jobs/logs-v2',
        logStreamName=f'{_RUN}-job-insights-rule-driver',
        # CloudWatch Logs refuses an event over 14 days old, and the insights are read with no window.
        logEvents=[{'timestamp': int(dt.datetime.now(dt.UTC).timestamp() * 1000), 'message': 'Failure Reason: out of memory'}],
    )

    enrichment = log.enrich(glue_run=GlueRun(job_name=_JOB, run_id=_RUN))

    assert enrichment.warnings == ()
    assert enrichment.row_counts == {'glue_metrics': 1, 'glue_insights': 1}
    assert log.tables.query('SELECT metric, statistic, value FROM glue_metrics').fetchall() == [
        ('glue.driver.workerUtilization', 'Average', 0.25)
    ]
    assert log.tables.query('SELECT message FROM glue_insights').fetchall() == [('Failure Reason: out of memory',)]


def test_a_glue_run_cloudwatch_lists_no_metric_for_warns_and_writes_no_rows(tmp_path: pathlib.Path) -> None:
    log = EventLog.open(_LOG, cache_dir=tmp_path)

    enrichment = log.enrich(glue_run=GlueRun(job_name=_JOB, run_id=_RUN))

    assert [type(warning) for warning in enrichment.warnings] == [NoGlueMetricsError]
    assert enrichment.row_counts == {'glue_metrics': 0, 'glue_insights': 0}
