"""Read a Glue job run's observability metrics from CloudWatch, one row per datapoint.

Glue publishes them every 30 seconds in the `Glue` namespace when the job runs with
`--enable-observability-metrics true`, on Glue 4.0 and later. Each carries the dimensions
`JobName`, `JobRunId`, `Type` and `ObservabilityGroup`, and a throughput metric `Source` or `Sink`
as well. ListMetrics names every metric the run published, and GetMetricData reads each one over
the run. ListMetrics returns no metric without data in the last two weeks, so an older run reads
as having none. A job run with `--enable-metrics` also publishes Glue's older job metrics, whose
names start the same way. They carry no `ObservabilityGroup`, so they are left out.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

import pyarrow as pa

from sparkxray.ingest.aws import CloudWatch

NAMESPACE = 'Glue'
# GetMetricData takes at most 500 queries a call.
_QUERIES_PER_CALL = 500
# CloudWatch keeps 1-minute datapoints for 15 days, 5-minute ones for 63 days and hourly ones for 455.
_PERIODS = ((dt.timedelta(days=15), 60), (dt.timedelta(days=63), 300))
_LONGEST_PERIOD = 3600
# Metrics may land after the run ends, so the window reads past it.
_AFTER_END = dt.timedelta(minutes=2)
# The unit AWS's metric reference, "Monitoring with AWS Glue Observability metrics", gives each metric, by the end of its name.
_UNITS = (
    (re.compile(r'\.(workerUtilization|percentage)$'), 'Percentage'),
    (re.compile(r'\.memory\.(heap|non-heap|total)\.(available|used)$'), 'Bytes'),
    (re.compile(r'\.disk\.(available_GB|used_GB)$'), 'Gigabytes'),
    (re.compile(r'\.(bytesRead|bytesWritten|bytesWrittten)$'), 'Bytes'),
    (re.compile(r'\.(recordsRead|filesRead|partitionsRead|recordsWritten|filesWritten|skewness\.stage|skewness\.job)$'), 'Count'),
    (re.compile(r'^glue\.(succeed|error|driver\.error|executor\.error)\.'), 'Count'),
)
SAMPLE_SCHEMA = pa.schema(
    [
        ('metric', pa.string()),
        ('observability_group', pa.string()),
        ('metric_type', pa.string()),
        ('source', pa.string()),
        ('sink', pa.string()),
        ('statistic', pa.string()),
        ('unit', pa.string()),
        ('period_ms', pa.int64()),
        ('period_start_epoch_ms', pa.int64()),
        ('value', pa.float64()),
    ]
)


def glue_metric_samples(cloudwatch: CloudWatch, job_name: str, run_id: str, start_epoch_ms: int, end_epoch_ms: int) -> pa.Table:
    """Every datapoint of every metric the run published, from a period before `start_epoch_ms` to two minutes past `end_epoch_ms`.

    The period is the shortest CloudWatch still keeps for a run that old. A count is read as its
    Sum and a gauge as its Average, the statistics AWS's reference lists for each. A throughput
    gauge, one with a `Source` or `Sink`, is read as its Maximum, as AWS's Grafana dashboard reads it.
    """
    listed = [
        metric
        for metric in cloudwatch.list_metrics(NAMESPACE, {'JobName': job_name, 'JobRunId': run_id})
        if 'ObservabilityGroup' in _dimensions(metric)
    ]
    metrics = list({(metric['MetricName'], tuple(sorted(_dimensions(metric).items()))): metric for metric in listed}.values())
    start = dt.datetime.fromtimestamp(start_epoch_ms / 1000, tz=dt.UTC)
    period = _period(start)
    end = dt.datetime.fromtimestamp(end_epoch_ms / 1000, tz=dt.UTC) + _AFTER_END
    rows: list[dict[str, Any]] = []
    for first in range(0, len(metrics), _QUERIES_PER_CALL):
        batch = metrics[first : first + _QUERIES_PER_CALL]
        queries = [_query(f'm{index}', metric, period) for index, metric in enumerate(batch)]
        merged: dict[str, tuple[list[dt.datetime], list[float]]] = {}
        resource = f'{NAMESPACE} metrics of {job_name} run {run_id}'
        for result in cloudwatch.metric_data(queries, start - dt.timedelta(seconds=period), end, resource):
            timestamps, values = merged.setdefault(str(result['Id']), ([], []))
            timestamps.extend(result.get('Timestamps', []))
            values.extend(result.get('Values', []))
        for query, metric in zip(queries, batch, strict=True):
            rows.extend(_rows(metric, query['MetricStat']['Stat'], period, *merged.get(query['Id'], ([], []))))
    return pa.Table.from_pylist(rows, schema=SAMPLE_SCHEMA)


def _period(start: dt.datetime) -> int:
    age = dt.datetime.now(dt.UTC) - start
    return next((seconds for kept_for, seconds in _PERIODS if age < kept_for), _LONGEST_PERIOD)


def _query(query_id: str, metric: Mapping[str, Any], period: int) -> dict[str, Any]:
    dimensions = _dimensions(metric)
    statistic = 'Average'
    if dimensions.get('Type') == 'count':
        statistic = 'Sum'
    elif 'Source' in dimensions or 'Sink' in dimensions:
        statistic = 'Maximum'
    return {
        'Id': query_id,
        'MetricStat': {
            'Metric': {'Namespace': NAMESPACE, 'MetricName': metric['MetricName'], 'Dimensions': list(metric.get('Dimensions', []))},
            'Period': period,
            'Stat': statistic,
        },
        'ReturnData': True,
    }


def _rows(
    metric: Mapping[str, Any], statistic: str, period: int, timestamps: Sequence[dt.datetime], values: Sequence[float]
) -> list[dict[str, Any]]:
    name = str(metric['MetricName'])
    dimensions = _dimensions(metric)
    unit = next((unit for pattern, unit in _UNITS if pattern.search(name)), None)
    return [
        {
            'metric': name,
            'observability_group': dimensions.get('ObservabilityGroup'),
            'metric_type': dimensions.get('Type'),
            'source': dimensions.get('Source'),
            'sink': dimensions.get('Sink'),
            'statistic': statistic,
            'unit': unit,
            'period_ms': period * 1000,
            'period_start_epoch_ms': int(timestamp.timestamp() * 1000),
            'value': float(value),
        }
        for timestamp, value in zip(timestamps, values, strict=True)
    ]


def _dimensions(metric: Mapping[str, Any]) -> dict[str, str]:
    return {str(dimension['Name']): str(dimension['Value']) for dimension in metric.get('Dimensions', [])}
