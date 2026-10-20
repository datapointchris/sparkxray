"""Find each structured log a caller names, and refuse one Spark did not write with structured logging.

With `spark.log.structuredLogging.enabled=true`, on Spark 4.0 and later, log4j writes one JSON
record a line, with the fields `SPARK_LOG_SCHEMA` lists in Spark's
`common/utils/src/main/scala/org/apache/spark/util/LogUtils.scala:34`. A driver's stderr holds
other lines too: the JVM's own warnings, and a stack trace the JVM printed for a thread that died.
So a log is structured when a record appears in its first 64 KiB, and the table keeps only the
records. Each line is numbered here as it is decoded, blank lines included, so a record's number
is the line `rg -n` shows it on.
"""

from __future__ import annotations

import dataclasses as dc
import json
from collections.abc import Iterator
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa

from sparkxray.errors import NoDriverLogError
from sparkxray.errors import NoEventLogError
from sparkxray.errors import UnstructuredDriverLogError
from sparkxray.ingest import decoders
from sparkxray.ingest.manifest import ReadCodec

CLOUDWATCH_SCHEME = 'cloudwatch:'
_PROBE_BYTES = 64 << 10
_SHOWN_CHARACTERS = 200
_BATCH_LINES = 8192
_STARTS: dict[bytes, ReadCodec] = {b'\x28\xb5\x2f\xfd': 'zstd', b'\x1f\x8b': 'gzip'}
_LINE_SCHEMA = pa.schema([('log_file', pa.string()), ('line_number', pa.int64()), ('line', pa.string())])
# Spark's SPARK_LOG_SCHEMA in DuckDB's types, as json_transform takes it. `ts` stays text, which the table parses.
_RECORD_STRUCTURE = json.dumps(
    {
        'ts': 'VARCHAR',
        'level': 'VARCHAR',
        'msg': 'VARCHAR',
        'context': 'MAP(VARCHAR, VARCHAR)',
        'exception': {
            'class': 'VARCHAR',
            'msg': 'VARCHAR',
            'stacktrace': [{'class': 'VARCHAR', 'method': 'VARCHAR', 'file': 'VARCHAR', 'line': 'VARCHAR'}],
        },
        'logger': 'VARCHAR',
    }
)
# Each line of `driver_log_lines` that is a JSON object, as Spark's record fields. Any other line reads as no row.
RECORDS_SQL = (
    'SELECT log_file, line_number, record.* FROM ('
    f"SELECT log_file, line_number, json_transform(line, '{_RECORD_STRUCTURE}') AS record FROM driver_log_lines "
    "WHERE json_valid(line) AND json_type(line) = 'OBJECT')"
)
_REFUSAL_HINT = 'run the job with --conf spark.log.structuredLogging.enabled=true, on Spark 4.0 or later'


@dc.dataclass(frozen=True, slots=True)
class DriverLogFile:
    """One structured log, readable here. `location` names it as `driver_log.log_file` does, and `path` is its local copy."""

    location: str
    path: Path
    codec: ReadCodec


def driver_log_files(
    locations: Sequence[str], *, cache: Path, scratch: Path, profile: str | None = None, region: str | None = None
) -> list[DriverLogFile]:
    """Each log a location names, downloaded where it is on S3 or in CloudWatch Logs, and each checked for structured records.

    A location is a local path, an `s3://` URI to one object, or `cloudwatch:<group>:<stream>`.
    One that names nothing raises `NoDriverLogError`, and one holding no structured record
    raises `UnstructuredDriverLogError`. A stream's events are written to `scratch`, one a line.
    """
    files = [_located(location, number, cache, scratch, profile, region) for number, location in enumerate(locations)]
    for file in files:
        _require_structured(file)
    return files


def driver_log_lines(files: Sequence[DriverLogFile]) -> pa.RecordBatchReader:
    """Every line of every log that is not blank, beside its log's location and its line number from 1, which `RECORDS_SQL` reads."""
    return pa.RecordBatchReader.from_batches(_LINE_SCHEMA, _line_batches(files))


def _line_batches(files: Sequence[DriverLogFile]) -> Iterator[pa.RecordBatch]:
    for file in files:
        numbers: list[int] = []
        lines: list[str] = []
        for number, line in enumerate(_lines(file), start=1):
            if line.strip():
                numbers.append(number)
                lines.append(line.decode('utf-8', 'replace'))
            if len(lines) == _BATCH_LINES:
                yield _line_batch(file.location, numbers, lines)
                numbers, lines = [], []
        if lines:
            yield _line_batch(file.location, numbers, lines)


def _line_batch(location: str, numbers: list[int], lines: list[str]) -> pa.RecordBatch:
    columns = [pa.array([location] * len(lines), pa.string()), pa.array(numbers, pa.int64()), pa.array(lines, pa.string())]
    return pa.record_batch(columns, schema=_LINE_SCHEMA)


def _lines(file: DriverLogFile) -> Iterator[bytes]:
    """Each line of one log, the last too where no newline ends it. A log cut partway through keeps what came before the cut."""
    with open(file.path, 'rb') as raw:
        lines = decoders.Lines(decoders.decoded_chunks(raw, file.codec))
        try:
            yield from lines
        except decoders.StreamCut:
            return
        if lines.tail:
            yield lines.tail


def _located(location: str, number: int, cache: Path, scratch: Path, profile: str | None, region: str | None) -> DriverLogFile:
    if location.startswith(CLOUDWATCH_SCHEME):
        return _from_cloudwatch(location, scratch / f'{number}.jsonl', profile, region)
    if location.startswith('s3://'):
        return _from_s3(location, cache, profile, region)
    path = Path(location).expanduser()
    if not path.is_file():
        raise NoDriverLogError(f'{path} is no file', field='driver_log')
    return DriverLogFile(path.resolve().as_uri(), path, _codec(path))


def _from_cloudwatch(location: str, path: Path, profile: str | None, region: str | None) -> DriverLogFile:
    from sparkxray.ingest.aws import CloudWatchLogs  # boto3 is imported only when a log is in AWS

    # A group name never holds a colon, so the first one after the scheme ends it.
    group, _, stream = location.removeprefix(CLOUDWATCH_SCHEME).partition(':')
    if not group or not stream:
        raise NoDriverLogError(f'{location} names no log stream', field='driver_log', hint='name one as cloudwatch:<group>:<stream>')
    events = CloudWatchLogs(profile=profile, region=region).log_events(group, stream)
    if events is None:
        raise NoDriverLogError(f'CloudWatch Logs holds no stream {stream} in the group {group}', field='driver_log')
    path.write_text(''.join(str(event['message']).rstrip('\n') + '\n' for event in events), encoding='utf-8')
    return DriverLogFile(location, path, 'none')


def _from_s3(location: str, cache: Path, profile: str | None, region: str | None) -> DriverLogFile:
    from sparkxray.ingest.locations import S3Location  # boto3 is imported only when a log is on S3

    source = S3Location(location, profile=profile, region=region)
    try:
        listing, named = source.list()
    except NoEventLogError as error:
        raise NoDriverLogError(f'{location} holds no object', field='driver_log') from error
    if named is None:
        raise NoDriverLogError(
            f'{location} names no one object',
            field='driver_log',
            valid=[listed.uri for listed in listing],
            hint='pass one of the listed objects',
        )
    path = source.local_path(named, cache)
    return DriverLogFile(location, path, _codec(path))


def _codec(path: Path) -> ReadCodec:
    with open(path, 'rb') as raw:
        start = raw.read(4)
    return next((codec for magic, codec in _STARTS.items() if start.startswith(magic)), 'none')


def _require_structured(file: DriverLogFile) -> None:
    first = None
    with open(file.path, 'rb') as raw:
        try:
            for line in decoders.Lines(_capped(decoders.decoded_chunks(raw, file.codec))):
                if first is None and line.strip():
                    first = line
                if _is_record(line):
                    return
        except (decoders.StreamCut, decoders.StreamCorrupt):
            pass
    shown = 'it holds no line' if first is None else f'its first line is {first[:_SHOWN_CHARACTERS].decode("utf-8", "replace")!r}'
    raise UnstructuredDriverLogError(
        f'{file.location} holds no record of Spark structured logging in its first 64 KiB, and {shown}',
        field='driver_log',
        hint=_REFUSAL_HINT,
    )


def _capped(chunks: Iterator[bytes]) -> Iterator[bytes]:
    read = 0
    for chunk in chunks:
        yield chunk[: _PROBE_BYTES - read]
        read += len(chunk)
        if read >= _PROBE_BYTES:
            return


def _is_record(line: bytes) -> bool:
    try:
        record = json.loads(line)
    except ValueError:
        return False
    return isinstance(record, dict) and {'ts', 'level', 'msg'} <= record.keys()
