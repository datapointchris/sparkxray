"""Build one log's Parquet tables in a single DuckDB pass over its events, and keep them in the cache.

The tables go under `<cache>/tables/<source hash>/<key>/`. The key covers the log's files, the
tables schema version and the installed sparkxray version, so an unchanged log is parsed once, and
any upgrade rebuilds. Tables built for an older version of a log stay until they have gone a day
unused, because a `Tables` opened before the log changed still reads them.

A plan node's metrics are aggregated the way Spark's `SQLAppStatusListener` aggregates them for
the SQL tab:

- only each stage's highest attempt counts;
- a task index keeps the value its tasks last reported, up to and including its first success;
- an index that never reported counts as 0, because Spark logs no update for a zero;
- values a driver reported count beside the tasks', and attribute the maximum to the driver when
  they hold it.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import re
import secrets
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.cache_directory import installed_version
from sparkxray.cache_directory import remove_superseded
from sparkxray.cache_directory import used
from sparkxray.errors import CorruptLogError
from sparkxray.ingest import decoders
from sparkxray.ingest.events_from_rest import EVICTED_EVENT
from sparkxray.ingest.manifest import IngestedFile
from sparkxray.ingest.manifest import IngestManifest
from sparkxray.tables.event_columns import EVENT_COLUMNS
from sparkxray.tables.manifest import SCHEMA_VERSION
from sparkxray.tables.manifest import Lack
from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.plan_rows import plan_rows
from sparkxray.tables.registry import TABLES
from sparkxray.tables.registry import Table
from sparkxray.tables.relations import Tables

if TYPE_CHECKING:
    from sparkxray.ingest import EventLog

# One SQLExecutionStart with a wide plan is larger than DuckDB's default object size.
_MAXIMUM_OBJECT_SIZE = 64 << 20

_COMPRESSION = {'none': 'uncompressed', 'zstd': 'zstd', 'gzip': 'gzip'}

_MALFORMED = re.compile(r'Malformed JSON in file "(?P<path>.+?)", at byte \d+ in line (?P<line>\d+): (?P<reason>[^.\n]+)')

_WORKING_TABLES = (
    """
    CREATE TEMP TABLE highest_attempts AS
    SELECT stage_id, max(attempt) AS attempt FROM (
      SELECT "Stage Info"."Stage ID" AS stage_id, "Stage Info"."Stage Attempt ID" AS attempt FROM events
      WHERE kind IN ('SparkListenerStageSubmitted', 'SparkListenerStageCompleted')
      UNION ALL
      SELECT "Stage ID", "Stage Attempt ID" FROM events WHERE kind IN ('SparkListenerTaskStart', 'SparkListenerTaskEnd')
    ) GROUP BY stage_id
    """,
    """
    CREATE TEMP TABLE execution_stages AS
    SELECT DISTINCT execution_id, stage_id FROM (
      SELECT try_cast("Properties"."spark.sql.execution.id" AS BIGINT) AS execution_id, unnest("Stage IDs") AS stage_id
      FROM events WHERE kind = 'SparkListenerJobStart'
    ) WHERE execution_id IS NOT NULL
    """,
    """
    CREATE TEMP TABLE metric_definitions AS
    SELECT execution_id, accumulator_id, any_value(metric_type) AS metric_type FROM plan_metric_refs
    WHERE accumulator_id IS NOT NULL
    GROUP BY execution_id, accumulator_id
    """,
    """
    CREATE TEMP TABLE highest_task_ends AS
    SELECT task_ends.* FROM (
      SELECT rowid AS position, "Stage ID" AS stage_id, "Stage Attempt ID" AS stage_attempt, "Task Info"."Index" AS task_index,
        "Task Info"."Task ID" AS task_id, "Task Info"."Accumulables" AS accumulables,
        NOT coalesce("Task Info"."Failed", false) AND NOT coalesce("Task Info"."Killed", false) AS succeeded
      FROM events WHERE kind = 'SparkListenerTaskEnd'
    ) AS task_ends
    JOIN highest_attempts ON highest_attempts.stage_id = task_ends.stage_id AND highest_attempts.attempt = task_ends.stage_attempt
    """,
    """
    CREATE TEMP TABLE metric_values AS
    WITH frozen AS (
      SELECT stage_id, task_index, min(position) FILTER (WHERE succeeded) AS frozen_at
      FROM highest_task_ends GROUP BY stage_id, task_index
    ), updates AS (
      SELECT position, stage_id, task_index, task_id, accumulable."ID" AS accumulator_id,
        try_cast(accumulable."Update" AS BIGINT) AS value
      FROM (SELECT position, stage_id, task_index, task_id, unnest(accumulables) AS accumulable FROM highest_task_ends)
    ), task_values AS (
      SELECT definitions.execution_id, definitions.accumulator_id, definitions.metric_type, updates.stage_id, updates.task_index,
        arg_max(updates.value, updates.position) AS value, arg_max(updates.task_id, updates.position) AS task_id,
        max(updates.position) AS position
      FROM updates
      JOIN frozen ON frozen.stage_id = updates.stage_id AND frozen.task_index = updates.task_index
      JOIN execution_stages ON execution_stages.stage_id = updates.stage_id
      JOIN metric_definitions AS definitions
        ON definitions.execution_id = execution_stages.execution_id AND definitions.accumulator_id = updates.accumulator_id
      WHERE updates.value IS NOT NULL AND (frozen.frozen_at IS NULL OR updates.position <= frozen.frozen_at)
      GROUP BY definitions.execution_id, definitions.accumulator_id, definitions.metric_type, updates.stage_id, updates.task_index
    ), stage_task_counts AS (
      SELECT "Stage Info"."Stage ID" AS stage_id, max("Stage Info"."Number of Tasks") AS num_tasks FROM events
      WHERE kind IN ('SparkListenerStageSubmitted', 'SparkListenerStageCompleted')
      GROUP BY "Stage Info"."Stage ID"
    ), padding AS (
      SELECT reported.execution_id, reported.accumulator_id, reported.metric_type, reported.stage_id,
        unnest(range(greatest(stage_task_counts.num_tasks - reported.indices, 0))) AS pad
      FROM (
        SELECT execution_id, accumulator_id, metric_type, stage_id, count(*) AS indices FROM task_values
        GROUP BY execution_id, accumulator_id, metric_type, stage_id
      ) AS reported
      JOIN stage_task_counts ON stage_task_counts.stage_id = reported.stage_id
    ), driver_values AS (
      SELECT definitions.execution_id, definitions.accumulator_id, definitions.metric_type, driver.pair[2] AS value, driver.position
      FROM (
        SELECT rowid AS position, executionId, unnest(accumUpdates) AS pair FROM events
        WHERE kind = 'SparkListenerDriverAccumUpdates'
      ) AS driver
      JOIN metric_definitions AS definitions
        ON definitions.execution_id = driver.executionId AND definitions.accumulator_id = driver.pair[1]
    )
    SELECT execution_id, accumulator_id, metric_type, stage_id, task_id, value, position FROM task_values
    UNION ALL
    SELECT execution_id, accumulator_id, metric_type, stage_id, NULL, 0, NULL FROM padding
    UNION ALL
    SELECT execution_id, accumulator_id, metric_type, NULL, NULL, value, position FROM driver_values
    """,
    # Spark's MetricUtils.stringValue: a sum counts every value, an average only those above 0,
    # and a size or time those at or above 0, which drops the -1 a metric starts at. The median is
    # the value at index n / 2 of the sorted values.
    """
    CREATE TEMP TABLE metric_aggregates AS
    WITH counted AS (
      SELECT * FROM metric_values
      WHERE CASE metric_type WHEN 'sum' THEN true WHEN 'average' THEN value > 0
        WHEN 'size' THEN value >= 0 WHEN 'timing' THEN value >= 0 WHEN 'nsTiming' THEN value >= 0 ELSE false END
    ), ranked AS (
      SELECT execution_id, accumulator_id, value,
        row_number() OVER (PARTITION BY execution_id, accumulator_id ORDER BY value) - 1 AS value_rank,
        count(*) OVER (PARTITION BY execution_id, accumulator_id) AS value_count
      FROM counted
    ), summaries AS (
      SELECT execution_id, accumulator_id, sum(value) AS total, min(value) AS minimum, max(value) AS maximum,
        max(value) FILTER (WHERE value_rank = value_count // 2) AS median, any_value(value_count) AS value_count
      FROM ranked GROUP BY execution_id, accumulator_id
    ), task_maxima AS (
      SELECT execution_id, accumulator_id, value AS task_maximum, stage_id, task_id FROM counted WHERE task_id IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY execution_id, accumulator_id ORDER BY value DESC, position) = 1
    )
    SELECT summaries.*,
      CASE WHEN task_maxima.task_maximum = summaries.maximum THEN task_maxima.stage_id END AS max_stage_id,
      CASE WHEN task_maxima.task_maximum = summaries.maximum THEN task_maxima.task_id END AS max_task_id
    FROM summaries LEFT JOIN task_maxima USING (execution_id, accumulator_id)
    """,
    # An executor JVM's RSS is above 0 whenever Spark measured the process tree at all.
    """
    CREATE TEMP TABLE process_tree AS
    SELECT count(*) FILTER (WHERE kind = 'SparkListenerTaskEnd' AND "Task Executor Metrics"."ProcessTreeJVMRSSMemory" > 0) > 0
      AS measured
    FROM events
    """,
)

_EVICTED_KIND = EVICTED_EVENT.rpartition('.')[2]
_LACKS = f"""
    SELECT
      count(*) FILTER (WHERE kind = 'SparkListenerStageExecutorMetrics') = 0,
      count(*) FILTER (WHERE kind = 'SparkListenerBlockUpdated') = 0,
      NOT (SELECT measured FROM process_tree),
      count(*) FILTER (
        WHERE kind IN ('SparkListenerStageSubmitted', 'SparkListenerStageCompleted') AND "Stage Info"."Parent IDs" IS NULL
      ) > 0,
      count(*) FILTER (WHERE kind = 'SparkListenerTaskEnd') > 0
        AND count(*) FILTER (WHERE kind = 'SparkListenerTaskEnd' AND "Task Executor Metrics" IS NOT NULL) = 0,
      count(*) FILTER (WHERE kind = 'SparkListenerTaskEnd') > 0
        AND count(*) FILTER (WHERE kind = 'SparkListenerTaskEnd' AND len("Task Info"."Accumulables") > 0) = 0,
      count(*) FILTER (WHERE kind = 'SparkListenerSQLExecutionStart') > 0
        AND count(*) FILTER (WHERE kind = 'SparkListenerSQLExecutionStart' AND modifiedConfigs IS NOT NULL) = 0,
      count(*) FILTER (WHERE kind = '{_EVICTED_KIND}') > 0
    FROM events
"""
_LACK_ORDER: tuple[Lack, ...] = (
    'stage_executor_metrics',
    'block_updates',
    'process_tree_metrics',
    'stage_parents',
    'task_executor_metrics',
    'task_accumulators',
    'execution_configs',
    'evicted_records',
)


def build_tables(log: EventLog) -> Tables:
    """The log's tables, read from the cache when this build already wrote them, and built otherwise."""
    manifest = log.manifest
    key = tables_key(manifest)
    subject = log.cache_directory / 'tables' / hashlib.sha256(manifest.source.encode()).hexdigest()[:32]
    remove_superseded(subject, key)
    directory = subject / key
    if (directory / 'manifest.json').is_file():
        used(directory)
        return Tables(directory)
    partial = subject / f'{key}{PARTIAL_MARK}{os.getpid()}-{secrets.token_hex(4)}'
    partial.mkdir(parents=True)
    try:
        _build(manifest, key, partial)
        try:
            partial.rename(directory)
        except OSError:
            if not (directory / 'manifest.json').is_file():
                raise
            # another process built the same tables first, and its directory is kept
    finally:
        shutil.rmtree(partial, ignore_errors=True)
    return Tables(directory)


def tables_key(manifest: IngestManifest) -> str:
    """The SHA-256 over the log's files, the tables schema version and the installed sparkxray version."""
    files = [[file.uri, file.etag, file.mtime_ns, file.size_bytes] for file in manifest.files]
    identity = [files, SCHEMA_VERSION, installed_version()]
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def _build(manifest: IngestManifest, key: str, out: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(f'SET temp_directory = {_literal(str(out / ".spill"))}')
        connection.execute('SET preserve_insertion_order = true')
        _load_events(connection, manifest)
        rows = plan_rows(connection)
        facts = pa.table(
            {
                'app_id': pa.array([manifest.app_id], pa.string()),
                'spark_version': pa.array([manifest.spark_version], pa.string()),
                'platform': pa.array([manifest.platform], pa.string()),
                'source': pa.array([manifest.source], pa.string()),
                'compacted': pa.array([manifest.compacted], pa.bool_()),
                'in_progress': pa.array([manifest.in_progress], pa.bool_()),
            }
        )
        connection.register('ingest_facts', facts)
        connection.register('plan_node_rows', rows.nodes)
        connection.register('plan_metric_refs', rows.metrics)
        connection.register('plan_version_rows', rows.versions)
        for statement in _WORKING_TABLES:
            connection.execute(statement)
        row_counts = {table.name: write_table(connection, table, out / f'{table.name}.parquet') for table in TABLES}
        found = connection.execute(_LACKS).fetchone()
        lacks = tuple(lack for lack, missing in zip(_LACK_ORDER, found or (), strict=True) if missing)
    finally:
        connection.close()
    shutil.rmtree(out / '.spill', ignore_errors=True)
    record = TablesManifest(key=key, ingest=manifest, row_counts=row_counts, lacks=lacks)
    (out / 'manifest.json').write_text(json.dumps(record.to_json(), indent=2) + '\n')


def _load_events(connection: duckdb.DuckDBPyConnection, manifest: IngestManifest) -> None:
    """Read every event line into `events`, in log order, which `rowid` keeps.

    One `read_json` reads each run of files that share a codec, because it takes one.
    """
    columns = '{' + ', '.join(f'{_literal(name)}: {_literal(kind)}' for name, kind in EVENT_COLUMNS.items()) + '}'
    statement = 'CREATE TEMP TABLE events AS'
    for codec, files in itertools.groupby(manifest.files, key=lambda file: file.read_codec):
        paths = '[' + ', '.join(_literal(file.read_path) for file in files) + ']'
        try:
            connection.execute(
                f"""{statement} SELECT *, regexp_extract("Event", '([^.]*)$', 1) AS kind
                FROM read_json({paths}, format='newline_delimited', compression={_literal(_COMPRESSION[codec])},
                  maximum_object_size={_MAXIMUM_OBJECT_SIZE}, columns={columns})"""
            )
        except duckdb.InvalidInputException as error:
            corrupt = _malformed(str(error), manifest)
            if corrupt is None:
                raise
            raise corrupt from error
        statement = 'INSERT INTO events'


def _malformed(message: str, manifest: IngestManifest) -> CorruptLogError | None:
    """The error naming the file and line DuckDB could not parse, or None when `message` names none of the log's files."""
    match = _MALFORMED.search(message)
    file = next((file for file in manifest.files if match and file.read_path == match['path']), None)
    if match is None or file is None:
        return None
    line = _unparsable_line(file, int(match['line']))
    where = f'line {line}' if line is not None else f'near line {match["line"]}'
    return CorruptLogError(
        f'{file.uri} {where} is not a JSON event: {match["reason"].strip()}',
        field='location',
        hint='read an undamaged copy of the log',
    )


def _unparsable_line(file: IngestedFile, reported: int) -> int | None:
    """The number of the first line from just before `reported` on that does not parse.

    DuckDB can name the line after the one that fails, so the line before is tried too.
    """
    with open(file.read_path, 'rb') as raw:
        split = decoders.Lines(decoders.decoded_chunks(raw, file.read_codec))
        number = 0
        try:
            for number, line in enumerate(split, start=1):
                if number >= reported - 1 and not decoders.is_complete_line(line):
                    return number
        except (decoders.StreamCut, decoders.StreamCorrupt):
            return None
        if split.tail and not decoders.is_complete_line(split.tail):
            return number + 1
    return None


def write_table(connection: duckdb.DuckDBPyConnection, table: Table, path: Path) -> int:
    """Write one table's Parquet file from its source on `connection`, and return its row count."""
    select = ', '.join(f'CAST({column.expression} AS {column.type}) AS "{column.name}"' for column in table.columns)
    written = connection.execute(
        f'COPY (SELECT {select} FROM ({table.source}) AS source ORDER BY {table.order_by}) '
        f'TO {_literal(str(path))} (FORMAT parquet, COMPRESSION zstd)'
    ).fetchone()
    return int(written[0]) if written else 0


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
