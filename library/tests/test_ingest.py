"""Every codec Spark writes a log in decodes to the lines Spark wrote."""

import pathlib

import pytest
from fixture_logs import FIXTURES
from fixture_logs import plain_bytes
from fixture_logs import plain_lines
from fixture_logs import rolled_files
from sparkxray.ingest import EventLog


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
