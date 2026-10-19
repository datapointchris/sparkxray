"""Open one Spark application's event log from a local path or S3, and read its events in order."""

from __future__ import annotations

import dataclasses as dc
import hashlib
import json
import os
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import TypeVar

from sparkxray.cache_directory import cache_root
from sparkxray.errors import CorruptLogError
from sparkxray.errors import NoEventLogError
from sparkxray.errors import NotAnEventLogError
from sparkxray.errors import SeveralApplicationsError
from sparkxray.errors import SparkxrayError
from sparkxray.errors import UnsupportedCodecError
from sparkxray.ingest import decoders
from sparkxray.ingest import layouts
from sparkxray.ingest.locations import Location
from sparkxray.ingest.locations import location_for
from sparkxray.ingest.manifest import IngestedFile
from sparkxray.ingest.manifest import IngestManifest
from sparkxray.ingest.manifest import ReadCodec
from sparkxray.ingest.platforms import infer_platform

# Enough of a file to decode its first block under any codec Spark writes.
_HEAD_BYTES = 1 << 20
# The events to read for the app id, Spark version and environment, which Spark writes first.
_HEAD_EVENTS = 1000
_FOUND_SHOWN = 10
# Concurrent S3 reads for one ingest: first lines, and the files of a rolling log.
_THREADS = 8

_Item = TypeVar('_Item')
_Result = TypeVar('_Result')


@dc.dataclass(frozen=True, slots=True)
class EventLog:
    """One application's event log, resolved, downloaded and decoded where it needed to be."""

    manifest: IngestManifest

    @classmethod
    def open(
        cls,
        location: str | os.PathLike[str],
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        profile: str | None = None,
        region: str | None = None,
    ) -> EventLog:
        """Resolve `location` to one application's log and make every file of it readable.

        `location` is a log file, a directory, a rolling `eventlog_v2_*` directory, or an `s3://`
        URI to any of those. It must hold exactly one application. `cache_dir` overrides
        `$XDG_CACHE_HOME/sparkxray`. `profile` and `region` narrow boto3's credential chain.
        """
        source = location_for(os.fspath(location), profile=profile, region=region)
        candidate = _resolve(source)
        cache = cache_root(cache_dir)
        local_paths = _in_parallel(lambda log_file: source.local_path(log_file.listed, cache), candidate.files)
        files = []
        truncated_tail = False
        for position, (log_file, local) in enumerate(zip(candidate.files, local_paths, strict=True)):
            is_last = position == len(candidate.files) - 1
            read_path, read_codec, cut = _readable(local, log_file, cache, is_last=is_last, in_progress=candidate.in_progress)
            truncated_tail = truncated_tail or cut
            listed = log_file.listed
            files.append(
                IngestedFile(listed.uri, listed.etag, listed.mtime_ns, listed.size_bytes, log_file.codec, str(read_path), read_codec)
            )
        provisional = IngestManifest(
            source=source.uri,
            app_id=candidate.app_id,
            layout=candidate.layout,
            files=tuple(files),
            compacted=candidate.compacted,
            in_progress=candidate.in_progress,
            truncated_tail=truncated_tail,
            spark_version=None,
            platform='other',
        )
        head = _head_facts(EventLog(provisional).lines())
        app_id = head.app_id or candidate.app_id
        platform = infer_platform(app_id, head.spark_version, head.properties, head.classpath)
        return cls(dc.replace(provisional, app_id=app_id, spark_version=head.spark_version, platform=platform))

    def lines(self) -> Iterator[bytes]:
        """Every event line of the log in order, without its newline.

        The cut last line of a log Spark had not finished is left out.
        """
        for file in self.manifest.files:
            with open(file.read_path, 'rb') as raw:
                split = decoders.Lines(decoders.decoded_chunks(raw, file.read_codec))
                yield from split
                if split.tail:
                    yield split.tail


def _resolve(source: Location) -> layouts.Candidate:
    listing, named = source.list()
    unrecognized: list[str] = []
    if named is not None:
        candidates = [layouts.named_file(named)]
    else:
        candidates, unrecognized = layouts.group(listing)
    if not candidates:
        raise NoEventLogError(f'{source.uri} holds no Spark event log. {_found(unrecognized)}', field='location')

    rejections = _in_parallel(lambda candidate: _rejection(source, candidate), candidates)
    confirmed = [candidate for candidate, rejection in zip(candidates, rejections, strict=True) if rejection is None]
    if not confirmed:
        # The location named one log, so the refusal is about that log rather than what a directory holds.
        if len(candidates) == 1 and candidates[0].uri == source.uri and rejections[0] is not None:
            raise rejections[0].error
        rejected = [
            f'{"/".join(candidate.files[0].listed.parts)} ({rejection.reason})'
            for candidate, rejection in zip(candidates, rejections, strict=True)
            if rejection is not None
        ]
        raise NoEventLogError(f'{source.uri} holds no Spark event log. {_found(rejected + unrecognized)}', field='location')
    if len(confirmed) > 1:
        raise SeveralApplicationsError(
            f"{source.uri} holds {len(confirmed)} applications' logs, and ingest reads one",
            field='location',
            valid=[candidate.uri for candidate in confirmed],
            hint='pass one of the listed locations',
        )
    candidate = confirmed[0]
    if any(log_file.codec == 'lzf' for log_file in candidate.files):
        raise UnsupportedCodecError(
            f'{candidate.uri} is lzf-compressed, which sparkxray cannot decode',
            field='location',
            hint='run the job with spark.eventLog.compression.codec=zstd',
        )
    return candidate


def _in_parallel(function: Callable[[_Item], _Result], items: Sequence[_Item]) -> list[_Result]:
    """`function` over `items` in order, on threads when there is more than one, since each waits on S3."""
    if len(items) <= 1:
        return [function(item) for item in items]
    with ThreadPoolExecutor(max_workers=_THREADS) as pool:
        return list(pool.map(function, items))


@dc.dataclass(frozen=True, slots=True)
class _Rejection:
    reason: str
    error: SparkxrayError


def _rejection(source: Location, candidate: layouts.Candidate) -> _Rejection | None:
    """Why a candidate is not a Spark event log, or None when its first line is SparkListenerLogStart."""
    first = candidate.files[0]
    if first.codec == 'lzf':
        return None  # unreadable, so the codec refusal names it rather than this check
    uri = first.listed.uri
    try:
        line = decoders.first_line(source.leading_bytes(first.listed, _HEAD_BYTES), first.codec)
    except decoders.StreamCorrupt as error:
        return _Rejection(f'its first {first.codec} block does not decode', CorruptLogError(f'{uri}: {error}', field='location'))
    try:
        event = json.loads(line)
    except ValueError:
        event = None
    if isinstance(event, dict) and str(event.get('Event', '')).rpartition('.')[2] == 'SparkListenerLogStart':
        return None
    shown = line[:80].decode('utf-8', 'replace')
    return _Rejection(
        'no SparkListenerLogStart',
        NotAnEventLogError(
            f'{uri} is not a Spark event log: its first line is {shown!r}, where Spark writes SparkListenerLogStart', field='location'
        ),
    )


def _found(paths: list[str]) -> str:
    if not paths:
        return 'It holds no files.'
    shown = ', '.join(paths[:_FOUND_SHOWN])
    more = f', and {len(paths) - _FOUND_SHOWN} more' if len(paths) > _FOUND_SHOWN else ''
    return f'It holds {shown}{more}.'


def _readable(local: Path, log_file: layouts.LogFile, cache: Path, *, is_last: bool, in_progress: bool) -> tuple[Path, ReadCodec, bool]:
    """The local file to read one log file's events from, its codec, and whether its tail was cut.

    lz4 and snappy have no standard framing, so they decode into the cache as plain lines. So
    does the last file of a log Spark is still writing, whose end may be cut. A plain last file is
    checked from its end, and copied only when its last line is cut.
    """
    codec = log_file.codec
    if codec in ('lz4', 'snappy') or (is_last and in_progress and codec in ('zstd', 'gzip')):
        path, cut = _decoded_copy(local, log_file, cache, is_last=is_last)
        return path, 'none', cut
    if codec == 'none' and is_last:
        whole_end = _end_of_whole_lines(local)
        if whole_end is not None:
            return _trimmed_copy(local, log_file, cache, whole_end), 'none', True
        return local, 'none', False
    if codec == 'none' or codec == 'zstd' or codec == 'gzip':
        return local, codec, False
    raise ValueError(f'no reader for {codec}')


def _cache_paths(log_file: layouts.LogFile, cache: Path, kind: str) -> tuple[Path, Path]:
    """The decoded copy and its record for one version of a file. Older versions' copies are removed."""
    listed = log_file.listed
    directory = cache / 'logs' / 'decoded' / hashlib.sha256(listed.uri.encode()).hexdigest()[:32]
    identity = [listed.uri, listed.etag, listed.mtime_ns, listed.size_bytes, decoders.DECODER_VERSION, kind]
    stem = hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:32]
    if directory.is_dir():
        for stale in directory.iterdir():
            # A `.part-<pid>` file is another process's copy still being written.
            if not stale.name.startswith(stem) and '.part-' not in stale.name:
                stale.unlink(missing_ok=True)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f'{stem}.jsonl', directory / f'{stem}.json'


def _cached(copy: Path, record: Path) -> bool | None:
    """The recorded cut of a finished copy, or None when there is no finished copy."""
    if copy.is_file() and record.is_file():
        cut: bool = json.loads(record.read_text())['truncated_tail']
        return cut
    return None


def _finish(copy: Path, record: Path, partial: Path, cut: bool) -> None:
    partial.replace(copy)
    record_partial = record.with_name(f'{record.name}.part-{os.getpid()}')
    record_partial.write_text(json.dumps({'truncated_tail': cut}))
    record_partial.replace(record)


def _decoded_copy(local: Path, log_file: layouts.LogFile, cache: Path, *, is_last: bool) -> tuple[Path, bool]:
    copy, record = _cache_paths(log_file, cache, 'last' if is_last else 'decoded')
    if (cached := _cached(copy, record)) is not None:
        return copy, cached
    uri = log_file.listed.uri
    partial = copy.with_name(f'{copy.name}.part-{os.getpid()}')
    stream_cut = False

    def chunks(raw: Any) -> Iterator[bytes]:
        nonlocal stream_cut
        try:
            yield from decoders.decoded_chunks(raw, log_file.codec)
        except decoders.StreamCut:
            stream_cut = True

    try:
        with local.open('rb') as raw, partial.open('wb') as out:
            lines = decoders.Lines(chunks(raw))
            for line in lines:
                out.write(line)
                out.write(b'\n')
            line_cut = bool(lines.tail) and not decoders.is_complete_line(lines.tail)
            if lines.tail and not line_cut:
                out.write(lines.tail)
                out.write(b'\n')
        cut = stream_cut or line_cut
        if cut and not is_last:
            raise CorruptLogError(f"{uri} ends partway through a block or a line, which only a log's last file may", field='location')
        _finish(copy, record, partial, cut)
    except decoders.StreamCorrupt as error:
        raise CorruptLogError(f'{uri}: {error}', field='location') from error
    finally:
        partial.unlink(missing_ok=True)
    return copy, cut


def _end_of_whole_lines(path: Path) -> int | None:
    """Where the last whole line of a plain file ends, or None when the file ends on a whole line.

    A last line with no newline after it is whole when it parses, as in logs Spark finished
    without a final newline.
    """
    position = path.stat().st_size
    later: list[bytes] = []
    with path.open('rb') as file:
        while position > 0:
            start = max(0, position - (1 << 20))
            file.seek(start)
            block = file.read(position - start)
            newline = block.rfind(b'\n')
            if newline != -1:
                tail = block[newline + 1 :] + b''.join(reversed(later))
                end = start + newline + 1
                break
            later.append(block)
            position = start
        else:
            tail = b''.join(reversed(later))
            end = 0
    if not tail or decoders.is_complete_line(tail):
        return None
    return end


def _trimmed_copy(local: Path, log_file: layouts.LogFile, cache: Path, end: int) -> Path:
    copy, record = _cache_paths(log_file, cache, 'trimmed')
    if _cached(copy, record) is not None:
        return copy
    partial = copy.with_name(f'{copy.name}.part-{os.getpid()}')
    try:
        with local.open('rb') as source, partial.open('wb') as out:
            remaining = end
            while remaining > 0:
                block = source.read(min(remaining, 1 << 20))
                if not block:
                    break
                out.write(block)
                remaining -= len(block)
        _finish(copy, record, partial, True)
    finally:
        partial.unlink(missing_ok=True)
    return copy


@dc.dataclass(slots=True)
class _HeadFacts:
    app_id: str | None = None
    spark_version: str | None = None
    properties: dict[str, str] = dc.field(default_factory=dict)
    classpath: tuple[str, ...] = ()


def _head_facts(lines: Iterator[bytes]) -> _HeadFacts:
    """The app id, Spark version, Spark properties and classpath from the events Spark writes first."""
    facts = _HeadFacts()
    seen_environment = False
    for index, line in enumerate(lines):
        if index >= _HEAD_EVENTS or (facts.app_id is not None and seen_environment):
            break
        try:
            event = json.loads(line)
        except ValueError:
            continue
        kind = str(event.get('Event', '')).rpartition('.')[2] if isinstance(event, dict) else ''
        if kind == 'SparkListenerLogStart':
            facts.spark_version = event.get('Spark Version')
        elif kind == 'SparkListenerEnvironmentUpdate':
            seen_environment = True
            facts.properties = {str(key): str(value) for key, value in (event.get('Spark Properties') or {}).items()}
            facts.classpath = tuple(str(entry) for entry in (event.get('Classpath Entries') or {}))
        elif kind == 'SparkListenerApplicationStart' and event.get('App ID'):
            facts.app_id = str(event['App ID'])
    close = getattr(lines, 'close', None)
    if close is not None:
        close()
    return facts
