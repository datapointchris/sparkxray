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
