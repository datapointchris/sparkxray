"""Join a Glue job run's metrics and insights, and Spark's structured logs, to one log's tables as tables of their own.

Each source is optional and writes its own Parquet file beside the log's tables:
`glue_metrics.parquet` and `glue_insights.parquet` from a Glue run, `driver_log.parquet` from the
structured logs. Giving a source again replaces its table, and leaves the others as they were.
Tables a later sparkxray builds for the same log start without them.
"""

from __future__ import annotations

import dataclasses as dc
import os
import secrets
import tempfile
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.enrichment.driver_logs import RECORDS_SQL
from sparkxray.enrichment.driver_logs import driver_log_files
from sparkxray.enrichment.driver_logs import driver_log_lines
from sparkxray.errors import NoGlueMetricsError
from sparkxray.errors import SparkxrayError
from sparkxray.tables.build import write_table
from sparkxray.tables.registry import ENRICHMENT_TABLES
from sparkxray.tables.registry import RUN_SPAN_SQL
from sparkxray.tables.registry import Table
from sparkxray.tables.relations import Tables


@dc.dataclass(frozen=True, slots=True)
class GlueRun:
    """One Glue job run: its job's name, and its run id, as `jr_` and 64 hex digits, with `_attempt_1` on a retry."""

    job_name: str
    run_id: str

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class Enrichment:
    """What one enrichment wrote: each table it replaced and the rows it now holds, and the warnings it carried on past."""

    row_counts: Mapping[str, int]
    warnings: tuple[SparkxrayError, ...]

    def to_json(self) -> dict[str, Any]:
        return {'row_counts': dict(self.row_counts), 'warnings': [warning.to_json() for warning in self.warnings]}


def enrich(
    tables: Tables,
    *,
    cache: Path,
    glue_run: GlueRun | None = None,
    driver_logs: Sequence[str] = (),
    profile: str | None = None,
    region: str | None = None,
) -> Enrichment:
    """Read each source given, then write its table beside `tables`, and point their views at it.

    `glue_run` reads the run's metrics, over the window the log's application ran in, and its job
    run insights. CloudWatch listing no observability metric for the run is the `NoGlueMetricsError` warning,
    and the insights are still read. `driver_logs` are the locations `driver_log_files` takes.
    Every source is read before any table is written, so a refusal leaves the tables as they were.
    `cache` holds S3 downloads. `profile` and `region` narrow boto3's credential chain.
    """
    read: set[str] = set()
    warnings: list[SparkxrayError] = []
    connection = duckdb.connect()
    try:
        with tempfile.TemporaryDirectory(prefix='.enrichment-', dir=tables.path) as scratch:
            if driver_logs:
                files = driver_log_files(driver_logs, cache=cache, scratch=Path(scratch), profile=profile, region=region)
                connection.register('driver_log_lines', driver_log_lines(files))
                connection.execute(f'CREATE TEMP TABLE driver_log_records AS {RECORDS_SQL}')
                read.add('driver_log')
            if glue_run is not None:
                from sparkxray.enrichment.glue_insights import glue_insight_events  # boto3 is imported only when AWS is read
                from sparkxray.enrichment.glue_metrics import SAMPLE_SCHEMA
                from sparkxray.enrichment.glue_metrics import glue_metric_samples
                from sparkxray.ingest.aws import CloudWatch
                from sparkxray.ingest.aws import CloudWatchLogs

                span = tables.first_row(RUN_SPAN_SQL)
                start_epoch_ms, end_epoch_ms = span if span is not None else (None, None)
                samples = SAMPLE_SCHEMA.empty_table()
                if start_epoch_ms is not None and end_epoch_ms is not None:
                    cloudwatch = CloudWatch(profile=profile, region=region)
                    samples = glue_metric_samples(cloudwatch, glue_run.job_name, glue_run.run_id, start_epoch_ms, end_epoch_ms)
                if samples.num_rows == 0:
                    warnings.append(_no_metrics(glue_run, start_epoch_ms))
                connection.register('glue_metric_samples', samples)
                read.add('glue_metrics')
                connection.register(
                    'glue_insight_events', glue_insight_events(CloudWatchLogs(profile=profile, region=region), glue_run.run_id)
                )
                read.add('glue_insights')
            row_counts = {table.name: _replace(connection, table, tables.path) for table in ENRICHMENT_TABLES if table.name in read}
    finally:
        connection.close()
    tables.register_enrichment()
    return Enrichment(row_counts, tuple(warnings))


def _no_metrics(glue_run: GlueRun, start_epoch_ms: int | None) -> NoGlueMetricsError:
    if start_epoch_ms is None:
        return NoGlueMetricsError(
            f'the log records no application start, so there is no window to read the metrics of {glue_run.run_id} over',
            field='glue_run',
        )
    return NoGlueMetricsError(
        f'CloudWatch lists no Glue observability metric for {glue_run.job_name} run {glue_run.run_id}, so glue_metrics holds no rows. '
        'The job may have run without --enable-observability-metrics true, or over two weeks ago, '
        'past when CloudWatch lists a metric',
        field='glue_run',
        hint='check the job and run names with aws glue get-job-run',
    )


def _replace(connection: duckdb.DuckDBPyConnection, table: Table, directory: Path) -> int:
    """Write one table beside the log's, replacing the one there only once the new file is whole."""
    path = directory / f'{table.name}.parquet'
    partial = directory / f'{path.name}{PARTIAL_MARK}{os.getpid()}-{secrets.token_hex(4)}'
    try:
        written = write_table(connection, table, partial)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
    return written
