"""Read a failed Glue job run's job run insights from CloudWatch Logs, one row per event.

A job run with `--enable-job-insights true`, on Glue 2.0 and later, writes two streams when it
fails, and none when it succeeds: `<run id>-job-insights-rca-driver`, its exception analysis, and
`<run id>-job-insights-rule-driver`, a root cause and an action. Both go to the job's continuous
logging group, `/aws-glue/jobs/logs-v2` unless the job sets its own. This reads only that group.
"""

from __future__ import annotations

import pyarrow as pa

from sparkxray.ingest.aws import CloudWatchLogs

LOG_GROUP = '/aws-glue/jobs/logs-v2'
STREAM_SUFFIXES = ('-job-insights-rca-driver', '-job-insights-rule-driver')
EVENT_SCHEMA = pa.schema([('log_stream', pa.string()), ('logged_epoch_ms', pa.int64()), ('message', pa.string())])


def glue_insight_events(logs: CloudWatchLogs, run_id: str) -> pa.Table:
    """Every event of both insights streams of the run, none where a stream does not exist."""
    rows = []
    for suffix in STREAM_SUFFIXES:
        stream = f'{run_id}{suffix}'
        for event in logs.log_events(LOG_GROUP, stream) or []:
            rows.append({'log_stream': stream, 'logged_epoch_ms': int(event['timestamp']), 'message': str(event['message'])})
    return pa.Table.from_pylist(rows, schema=EVENT_SCHEMA)
