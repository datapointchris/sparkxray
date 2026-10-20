"""Every codec Spark writes a log in decodes to the lines Spark wrote, a server's log reads as the file does, a plain driver log fails."""

import contextlib
import http.server
import io
import json
import pathlib
import threading
import zipfile
from collections.abc import Iterator

import pytest
from fixture_logs import FIXTURES
from fixture_logs import plain_bytes
from fixture_logs import plain_lines
from fixture_logs import rolled_files
from sparkxray.enrichment.driver_logs import driver_log_files
from sparkxray.errors import UnstructuredDriverLogError
from sparkxray.ingest import EventLog
from sparkxray.tables import TABLES

# What a rebuilt log's tables hold differently, by table. REST carries no trace of the first four's
# columns. A final plan is read from its text, so its nodes are labeled as a text plan's are, and a
# plan metric is the value the SQL tab displayed, so only which node measured what is compared.
_REBUILD_LOSES = {
    'application': {'log_source'},
    'stages': {'parent_ids'},
    'tasks': {'peak_jvm_heap_bytes', 'peak_jvm_offheap_bytes', 'major_gc_count', 'major_gc_time_ms'},
    'sql_executions': {'details', 'job_tags', 'modified_configs', 'plan_version_count', 'initial_plan_text', 'final_plan_json'},
    'plan_versions': {'plan_json'},
    'plan_nodes': {'label', 'normalized_label', 'subtree_hash', 'details'},
    'plan_node_metrics': {'metric_type', 'accumulator_id', 'value_count', 'total', 'min', 'median', 'max', 'max_stage_id', 'max_task_id'},
    'node_stages': {'execution_id', 'node_id', 'stage_id'},
}
# The rows each table is compared on: a final plan's, and a plan metric some task reported to.
_COMPARED_ROWS = {
    'plan_versions': '(execution_id, plan_version) IN (SELECT execution_id, plan_version_count - 1 FROM sql_executions)',
    'plan_nodes': '(execution_id, plan_version) IN (SELECT execution_id, plan_version_count - 1 FROM sql_executions)',
    'plan_node_metrics': '(execution_id, plan_version) IN (SELECT execution_id, plan_version_count - 1 FROM sql_executions) '
    'AND (total IS NOT NULL OR max IS NOT NULL)',
}


def _log(variant: str) -> pathlib.Path:
    """The one application log in a fixture variant."""
    (entry,) = (FIXTURES / 'logs' / variant).iterdir()
    return entry


@pytest.mark.parametrize(
    ('codec', 'variant'),
    [
        ('none', 'spark-3.5.9'),
        ('zstd', 'spark-4.2.0'),
        ('gzip', 'spark-3.5.9-gzip'),
        ('lz4', 'spark-3.5.9-lz4'),
        ('snappy', 'spark-3.5.9-snappy'),
    ],
)
def test_every_codec_decodes_to_the_lines_spark_wrote(codec: str, variant: str, tmp_path: pathlib.Path) -> None:
    log = _log(variant)
    files = rolled_files(log) if log.is_dir() else [log]
    expected = [line for file in files for line in plain_lines(file)]
    if codec == 'none':
        # A plain log is committed zstd'd, so the plain file is written out here.
        log = tmp_path / log.name.removesuffix('.zstd')
        log.write_bytes(plain_bytes(files[0]))

    opened = EventLog.open(log, cache_dir=tmp_path / 'cache')

    assert [file.codec for file in opened.manifest.files] == [codec] * len(files)
    assert list(opened.lines()) == expected


def test_an_app_id_read_while_spark_wrote_the_log_opens_the_log_spark_finished_and_renamed(tmp_path: pathlib.Path) -> None:
    lines = plain_lines(FIXTURES / 'logs' / 'spark-3.5.9' / 'local-1792406949080.zstd')
    running = tmp_path / 'spark-events' / 'local-1792406949080.inprogress'
    running.parent.mkdir()
    running.write_bytes(b'\n'.join(lines[: len(lines) // 2]) + b'\n')
    cache = tmp_path / 'cache'
    read_while_running = _task_count(EventLog.open(running, cache_dir=cache))

    finished = running.with_suffix('')
    finished.write_bytes(b'\n'.join(lines) + b'\n')
    running.unlink()

    by_app_id = _task_count(EventLog.open('local-1792406949080', cache_dir=cache))
    by_path = _task_count(EventLog.open(finished, cache_dir=cache))
    assert read_while_running < by_app_id == by_path
    # Read by its finished path as well, the log is still one log under its app id.
    assert EventLog.open('local-1792406949080', cache_dir=cache).manifest.source == finished.as_uri()


def _task_count(log: EventLog) -> int:
    count: int = log.tables.query('SELECT count(*) FROM tasks').fetchall()[0][0]
    return count


@pytest.mark.parametrize('serves_logs', [True, False], ids=['downloaded', 'rebuilt'])
def test_a_history_server_s_log_reads_as_the_file_does(serves_logs: bool, tmp_path: pathlib.Path) -> None:
    log = FIXTURES / 'logs' / 'spark-3.5.9-stage-rules' / 'app-20261019220120-0000.zstd'
    recording = FIXTURES / 'history-server-rest' / 'spark-3.5.9-stage-rules' / 'app-20261019220120-0000.jsonl.zstd'
    from_file = EventLog.open(log, cache_dir=tmp_path / 'file')
    with _history_server(recording, log, serves_logs=serves_logs) as address:
        from_server = EventLog.open(f'{address}/history/app-20261019220120-0000/jobs/', cache_dir=tmp_path / 'server')

    loses = _REBUILD_LOSES if not serves_logs else {'application': {'log_source'}}
    for table in TABLES:
        names = [column.name for column in table.columns if column.name not in loses.get(table.name, ()) and column.name != 'plan_version']
        query = f'SELECT {", ".join(f"{chr(34)}{name}{chr(34)}" for name in names) or "1"} FROM {table.name}'
        if not serves_logs and table.name in _COMPARED_ROWS:
            query += f' WHERE {_COMPARED_ROWS[table.name]}'
        query += ' ORDER BY ALL'
        expected = from_file.tables.query(query).fetchall() if names else []
        assert from_server.tables.query(query).fetchall() == expected, table.name

    file_found, server_found = from_file.analyze(), from_server.analyze()
    assert from_server.manifest.layout == ('single_file' if serves_logs else 'rest_rebuild')
    # Which stages ran a join, and the settings a query changed, are what REST does not carry.
    lost = set() if serves_logs else {'config.skew-join-off-with-skew', 'config.aqe-off'}
    assert {finding.finding_id for finding in server_found.findings} == {
        finding.finding_id for finding in file_found.findings if finding.rule_id not in lost
    }


@contextlib.contextmanager
def _history_server(recording: pathlib.Path, log: pathlib.Path, *, serves_logs: bool) -> Iterator[str]:
    """A History Server answering what `recording` saved, and `log` zipped at `/logs` where it serves logs."""
    answers = {answer['path']: (answer['status'], answer['body'].encode()) for answer in map(json.loads, plain_lines(recording))}
    zipped = io.BytesIO()
    with zipfile.ZipFile(zipped, 'w') as archive:
        archive.write(log, log.name)

    class Replay(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.endswith('/logs'):
                status, body = (200, zipped.getvalue()) if serves_logs else (404, b'')
            else:
                status, body = answers.get(self.path, (404, b''))
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: object) -> None:
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Replay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f'http://127.0.0.1:{server.server_address[1]}'
    finally:
        server.shutdown()
        server.server_close()


def test_a_driver_log_without_structured_logging_is_refused_naming_its_first_line(tmp_path: pathlib.Path) -> None:
    plain = tmp_path / 'stderr.log'
    plain.write_text('26/10/20 10:00:00 INFO SparkContext: Running Spark version 4.2.0\n26/10/20 10:00:01 INFO Utils: started\n')

    with pytest.raises(UnstructuredDriverLogError, match='Running Spark version 4.2.0') as refused:
        driver_log_files([str(plain)], cache=tmp_path, scratch=tmp_path, profile=None, region=None)

    assert (refused.value.code, refused.value.exit_code) == ('E-ENRICH-002', 3)
