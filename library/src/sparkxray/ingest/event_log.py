"""Open one Spark application's event log from a local path, S3 or a Spark server, and read its events in order."""

from __future__ import annotations

import dataclasses as dc
import functools
import hashlib
import json
import os
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING
from typing import BinaryIO
from typing import TypeVar

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.cache_directory import cache_root
from sparkxray.cache_directory import installed_version
from sparkxray.cache_directory import remove_superseded
from sparkxray.cache_directory import used
from sparkxray.errors import AmbiguousLocationError
from sparkxray.errors import CorruptLogError
from sparkxray.errors import NoEventLogError
from sparkxray.errors import NotAnEventLogError
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

if TYPE_CHECKING:
    from sparkxray.enrichment import Enrichment
    from sparkxray.enrichment import GlueRun
    from sparkxray.rules.analysis import Analysis
    from sparkxray.tables.relations import Tables

# How much of a file is read for its first line. Spark's own blocks fit the first size, and the
# second holds the 128 KiB block the zstd tool writes.
_HEAD_BYTES = (64 << 10, 1 << 20)
# The events to read for the app id, Spark version and environment, which Spark writes first.
_HEAD_EVENTS = 1000
_FOUND_SHOWN = 10
# Concurrent S3 reads for one ingest: first lines, and the files of a rolling log.
_THREADS = 8
_COPY_BLOCK = 1 << 20
# How each compressed form a log arrives in starts, and the suffix that declares it.
_COMPRESSED_STARTS = {
    b'\x28\xb5\x2f\xfd': ('zstd', '.zstd'),
    b'\x1f\x8b': ('gzip', '.gz'),
    b'LZ4Block': ('lz4', '.lz4'),
    b'\x82SNAPPY\x00': ('snappy', '.snappy'),
}

_Item = TypeVar('_Item')
_Result = TypeVar('_Result')


@dc.dataclass(frozen=True)
class EventLog:
    """One application's event log, resolved, downloaded and decoded where it needed to be.

    `cache_directory` holds its downloads, decoded copies and tables.
    """

    manifest: IngestManifest
    cache_directory: Path

    @functools.cached_property
    def tables(self) -> Tables:
        """The log's events as Parquet tables, built on first use and kept in the cache.

        A second open of an unchanged log reads the tables already built.
        """
        from sparkxray.tables.build import build_tables  # DuckDB is imported only when tables are read

        return build_tables(self)

    def analyze(
        self,
        *,
        rules: Iterable[str] | None = None,
        thresholds: Mapping[str, Mapping[str, object]] | None = None,
        threshold_file: str | os.PathLike[str] | None = None,
        threshold_flags: Iterable[str] = (),
    ) -> Analysis:
        """Run the rules over this log, as `sparkxray.rules.analyze` runs them."""
        from sparkxray.rules.analysis import analyze

        return analyze(self, rules=rules, thresholds=thresholds, threshold_file=threshold_file, threshold_flags=threshold_flags)

    def enrich(
        self,
        *,
        glue_run: GlueRun | None = None,
        driver_logs: Sequence[str] = (),
        profile: str | None = None,
        region: str | None = None,
    ) -> Enrichment:
        """Join a Glue job run's metrics and insights, and Spark's structured logs, to this log's tables.

        `sparkxray.enrichment.enrich` describes the sources, and a later `analyze` reads them.
        """
        from sparkxray.enrichment import enrich

        return enrich(self.tables, cache=self.cache_directory, glue_run=glue_run, driver_logs=driver_logs, profile=profile, region=region)

    @classmethod
    def open(
        cls,
        location: str | os.PathLike[str],
        *,
        cache_dir: str | os.PathLike[str] | None = None,
        profile: str | None = None,
        region: str | None = None,
        app: str | None = None,
    ) -> EventLog:
        """Resolve `location` to one application's log and make every file of it readable.

        `location` is a log file, a directory, a rolling `eventlog_v2_*` directory, or an `s3://`
        URI to any of those. It must hold exactly one application. It may also be the
        `http(s)://` address of a Spark History Server or driver UI, which `app` names an
        application on where the address names none, and which `history_server` describes. It
        may also be the app id of a log the cache holds tables for, where it names no file.
        Finished tables the installed sparkxray built then open without reading the log's source
        again. A log Spark was still writing, or tables an earlier sparkxray built, send the open
        to the log's location, so it reads the log as it stands now. `cache_dir` overrides
        `$XDG_CACHE_HOME/sparkxray`. `profile` and `region` narrow boto3's credential chain.
        """
        text = os.fspath(location)
        cache = cache_root(cache_dir)
        if app is None and _may_be_app_id(text):
            from sparkxray.tables.cached_logs import cached_ingest  # DuckDB is imported only when an app id is opened

            cached = cached_ingest(text, cache)
            if isinstance(cached, IngestManifest):
                return cls(cached, cache)
            text = cached
        source = location_for(text, profile=profile, region=region, app=app, cache=cache)
        candidate = _resolve(source)
        local_paths = _in_parallel(lambda log_file: source.local_path(log_file.listed, cache), candidate.files)
        files = []
        truncated_tail = False
        for position, (log_file, local) in enumerate(zip(candidate.files, local_paths, strict=True)):
            read_path, read_codec, cut = _readable(
                local,
                log_file,
                cache,
                is_last=position == len(candidate.files) - 1,
                in_progress=candidate.in_progress,
                may_grow=source.files_may_grow,
            )
            truncated_tail = truncated_tail or cut
            listed = log_file.listed
            files.append(
                IngestedFile(listed.uri, listed.etag, listed.mtime_ns, listed.size_bytes, log_file.codec, str(read_path), read_codec)
            )
        provisional = IngestManifest(
            source=source.uri,
            app_id=candidate.app_id,
            layout='rest_rebuild' if source.rest_rebuild else candidate.layout,
            files=tuple(files),
            compacted=candidate.compacted,
            in_progress=candidate.in_progress,
            truncated_tail=truncated_tail,
            spark_version=None,
            platform='other',
        )
        head = _head_facts(EventLog(provisional, cache).lines())
        app_id = head.app_id or candidate.app_id
        platform = infer_platform(app_id, head.spark_version, head.properties, head.classpath)
        return cls(dc.replace(provisional, app_id=app_id, spark_version=head.spark_version, platform=platform), cache)

    def lines(self) -> Iterator[bytes]:
        """Every event line of the log in order, without its newline.

        The cut last line of a log Spark had not finished is left out.
        """
        for file in self.manifest.files:
            with open(file.read_path, 'rb') as raw:
                split = decoders.Lines(decoders.decoded_chunks(raw, file.read_codec))
                try:
                    yield from split
                except (decoders.StreamCut, decoders.StreamCorrupt) as error:
                    # `open` decoded this file whole, so it changed since.
                    raise CorruptLogError(
                        f'{file.read_path} no longer decodes as it did when the log was opened: {error}',
                        field='location',
                        hint='open the log again',
                    ) from error
                if split.tail:
                    yield split.tail


def _may_be_app_id(text: str) -> bool:
    """Whether a location could only be an app id: it holds no path separator, so no URI, and names nothing that exists."""
    separators = [separator for separator in ('/', os.sep, os.altsep) if separator]
    return bool(text) and not any(separator in text for separator in separators) and not Path(text).expanduser().exists()


def _resolve(source: Location) -> layouts.Candidate:
    listing, named = source.list()
    unrecognized: list[str] = []
    if named is not None:
        candidates = [layouts.named_file(named)]
    else:
        candidates, unrecognized = layouts.group(listing)
    if not candidates:
        raise NoEventLogError(f'{source.uri} holds no Spark event log. {_found(unrecognized)}', field='location')

    confirmed, rejected, unchecked = _confirm(source, candidates)
    if not confirmed:
        # The location named one log, so the refusal is about that log rather than what a directory holds.
        if len(candidates) == 1 and candidates[0].uri == source.uri.rstrip('/'):
            raise rejected[0][1].error
        passed_over = [f'{_path(candidate)} ({rejection.reason})' for candidate, rejection in rejected]
        raise NoEventLogError(f'{source.uri} holds no Spark event log. {_found(passed_over + unrecognized)}', field='location')
    if len(confirmed) > 1:
        logs = confirmed + unchecked
        by_name = f' {len(unchecked)} of them are known only by their names.' if unchecked else ''
        raise AmbiguousLocationError(
            f'{source.uri} holds {len(logs)} logs, and ingest reads one.{by_name}',
            field='location',
            valid=[candidate.uri for candidate in logs],
            hint='pass one of the listed locations',
        )
    candidate = confirmed[0]
    pieces = [_path(piece) for piece, rejection in rejected if rejection.first_event is not None]
    if pieces:
        raise AmbiguousLocationError(
            f'{source.uri} holds a log and {len(pieces)} files of Spark events that start no log, which may be pieces of it: '
            f'{_shown(pieces)}. sparkxray reads a log whole and cannot order pieces',
            field='location',
            valid=[candidate.uri],
            hint='pass the log alone to read it without them',
        )
    if any(log_file.codec == 'lzf' for log_file in candidate.files):
        raise UnsupportedCodecError(
            f'{candidate.uri} is lzf-compressed, which sparkxray cannot decode',
            field='location',
            hint='run the job with spark.eventLog.compression.codec=zstd',
        )
    return candidate


def _confirm(
    source: Location, candidates: Sequence[layouts.Candidate]
) -> tuple[list[layouts.Candidate], list[tuple[layouts.Candidate, _Rejection]], list[layouts.Candidate]]:
    """The candidates that start with SparkListenerLogStart, those that do not with why, and those left unchecked.

    Candidates are checked a batch at a time, and checking stops at a second log, which is
    enough to refuse the location. So a prefix holding every run of a job reads one batch.
    """
    confirmed: list[layouts.Candidate] = []
    rejected: list[tuple[layouts.Candidate, _Rejection]] = []
    for start in range(0, len(candidates), _THREADS):
        batch = candidates[start : start + _THREADS]
        for candidate, rejection in zip(batch, _in_parallel(lambda candidate: _rejection(source, candidate), batch), strict=True):
            if rejection is None:
                confirmed.append(candidate)
            else:
                rejected.append((candidate, rejection))
        if len(confirmed) > 1:
            return confirmed, rejected, list(candidates[start + _THREADS :])
    return confirmed, rejected, []


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
    # The kind of the Spark event the file starts with, when it starts with one.
    first_event: str | None = None


def _rejection(source: Location, candidate: layouts.Candidate) -> _Rejection | None:
    """Why a candidate is not a Spark event log, or None when its first line is SparkListenerLogStart."""
    first = candidate.files[0]
    if first.codec == 'lzf':
        return None  # unreadable, so the codec refusal names it rather than this check
    uri = first.listed.uri
    try:
        line = _first_line(source, first)
    except decoders.StreamCorrupt as error:
        return _Rejection(f'its first {first.codec} block does not decode', CorruptLogError(f'{uri}: {error}', field='location'))
    try:
        event = json.loads(line)
    except ValueError:
        event = None
    kind = None
    if isinstance(event, dict) and 'Event' in event:
        kind = str(event['Event']).rpartition('.')[2] or None
    if kind == 'SparkListenerLogStart':
        return None
    if first.codec == 'none':
        for start, (codec, suffix) in _COMPRESSED_STARTS.items():
            if line.startswith(start):
                hint = f'rename it to end in {suffix}, the suffix that declares {codec}'
                compressed = NotAnEventLogError(f'{uri} is {codec}-compressed, but its name declares no codec', field='location', hint=hint)
                return _Rejection(f'it is {codec}-compressed under a name that declares no codec', compressed)
    shown = line[:80].decode('utf-8', 'replace')
    refusal = NotAnEventLogError(
        f'{uri} is not a Spark event log: its first line is {shown!r}, where Spark writes SparkListenerLogStart', field='location'
    )
    if kind is None:
        return _Rejection('its first line is not a Spark event', refusal)
    return _Rejection(f'its first event is {kind}, not SparkListenerLogStart', refusal, kind)


def _first_line(source: Location, log_file: layouts.LogFile) -> bytes:
    for size in _HEAD_BYTES:
        head = source.leading_bytes(log_file.listed, size)
        line, ended = decoders.first_line(head, log_file.codec)
        if ended or len(head) < size:
            break
    return line


def _path(candidate: layouts.Candidate) -> str:
    return '/'.join(candidate.files[0].listed.parts)


def _shown(paths: Sequence[str]) -> str:
    more = f', and {len(paths) - _FOUND_SHOWN} more' if len(paths) > _FOUND_SHOWN else ''
    return ', '.join(paths[:_FOUND_SHOWN]) + more


def _found(paths: Sequence[str]) -> str:
    return f'It holds {_shown(paths)}.' if paths else 'It holds no files.'


def _readable(
    local: Path, log_file: layouts.LogFile, cache: Path, *, is_last: bool, in_progress: bool, may_grow: bool
) -> tuple[Path, ReadCodec, bool]:
    """The local file to read one log file's events from, its codec, and whether its end was cut.

    lz4 and snappy have no standard framing, so they decode into the cache as plain lines. So
    does the last file of a log Spark is still writing, when that file can still grow. A plain
    one is copied at the size it was listed with. Every other file is read in place once its
    end is checked. A compressed file's end is found by decoding all of it, once per version.

    A cut end is dropped from a copy when the file is the log's last, and refused otherwise.
    """
    codec = log_file.codec
    version = _Version.of(log_file, cache)
    growing = is_last and in_progress and may_grow
    if codec == 'none':
        size = min(log_file.listed.size_bytes, local.stat().st_size)
        end = _whole_lines_end(local, size)
        cut = end < size
        if cut and not is_last:
            raise _cut_before_last(log_file)
        if cut or growing:
            return _plain_copy(local, version, end, cut=cut), 'none', cut
        return local, 'none', False
    if codec in ('lz4', 'snappy') or growing:
        cut = _decoded_copy(local, log_file, version)
        if cut and not is_last:
            raise _cut_before_last(log_file)
        return version.copy, 'none', cut
    if codec == 'zstd' or codec == 'gzip':
        recorded = version.recorded_cut()
        if recorded is None:
            cut = _decode(local, log_file, None)
            version.record_cut(cut)
        else:
            cut = recorded
        if not cut:
            return local, codec, False
        if not is_last:
            raise _cut_before_last(log_file)
        return version.copy, 'none', _decoded_copy(local, log_file, version)
    raise ValueError(f'no reader for {codec}')


def _cut_before_last(log_file: layouts.LogFile) -> CorruptLogError:
    return CorruptLogError(
        f"{log_file.listed.uri} ends partway through a block, a frame or a line, which only a log's last file may", field='location'
    )


@dc.dataclass(frozen=True, slots=True)
class _Version:
    """The cache's entries for one version of one file: a copy of its whole lines, and whether its end was cut.

    A version is the file's URI, ETag, modification time and size, read by the installed
    sparkxray. Entries for older versions of the file are removed once they go a day unused.
    """

    copy: Path
    record: Path

    @classmethod
    def of(cls, log_file: layouts.LogFile, cache: Path) -> _Version:
        listed = log_file.listed
        directory = cache / 'logs' / 'decoded' / hashlib.sha256(listed.uri.encode()).hexdigest()[:32]
        identity = [listed.uri, listed.etag, listed.mtime_ns, listed.size_bytes, installed_version()]
        stem = hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:32]
        remove_superseded(directory, stem)
        return cls(directory / f'{stem}.jsonl', directory / f'{stem}.json')

    def recorded_cut(self) -> bool | None:
        """Whether this version's end was cut, or None when it was never checked."""
        try:
            cut: bool = json.loads(self.record.read_text())['cut']
        except FileNotFoundError:
            return None
        used(self.record)
        return cut

    def copied_cut(self) -> bool | None:
        """Whether this version's end was cut, or None when its copy was never made."""
        if not self.copy.is_file():
            return None
        cut = self.recorded_cut()
        if cut is not None:
            used(self.copy)
        return cut

    def record_cut(self, cut: bool) -> None:
        self.record.parent.mkdir(parents=True, exist_ok=True)
        partial = self.record.with_name(f'{self.record.name}{PARTIAL_MARK}{os.getpid()}')
        partial.write_text(json.dumps({'cut': cut}))
        partial.replace(self.record)

    def partial_copy(self) -> Path:
        """Where this process writes the copy before it is whole."""
        self.copy.parent.mkdir(parents=True, exist_ok=True)
        return self.copy.with_name(f'{self.copy.name}{PARTIAL_MARK}{os.getpid()}')


def _decoded_copy(local: Path, log_file: layouts.LogFile, version: _Version) -> bool:
    """Decode a file's whole lines into its version's copy, and return whether its end was cut."""
    if (cut := version.copied_cut()) is not None:
        return cut
    partial = version.partial_copy()
    try:
        with partial.open('wb') as out:
            cut = _decode(local, log_file, out)
        partial.replace(version.copy)
        version.record_cut(cut)
    finally:
        partial.unlink(missing_ok=True)
    return cut


def _decode(local: Path, log_file: layouts.LogFile, out: BinaryIO | None) -> bool:
    """Decode a file, writing its whole lines to `out` when given. True when it ends partway through a block, a frame or a line."""
    stream_cut = False
    tail = bytearray()
    try:
        with local.open('rb') as raw:
            try:
                for chunk in decoders.decoded_chunks(raw, log_file.codec):
                    newline = chunk.rfind(b'\n')
                    if newline == -1:
                        tail += chunk
                        continue
                    if out is not None:
                        out.write(tail)
                        out.write(memoryview(chunk)[: newline + 1])
                    tail[:] = memoryview(chunk)[newline + 1 :]
            except decoders.StreamCut:
                stream_cut = True
    except decoders.StreamCorrupt as error:
        raise CorruptLogError(f'{log_file.listed.uri}: {error}', field='location') from error
    line_cut = bool(tail) and not decoders.is_complete_line(bytes(tail))
    if out is not None and tail and not line_cut:
        out.write(tail)
        out.write(b'\n')
    return stream_cut or line_cut


def _whole_lines_end(path: Path, size: int) -> int:
    """Where the whole lines among a plain file's first `size` bytes end, which is `size` when its end is whole.

    A last line with no newline after it is whole when it parses, as in logs Spark finished
    without a final newline.
    """
    position = size
    later: list[bytes] = []
    with path.open('rb') as file:
        while position > 0:
            start = max(0, position - _COPY_BLOCK)
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
    return size if not tail or decoders.is_complete_line(tail) else end


def _plain_copy(local: Path, version: _Version, end: int, *, cut: bool) -> Path:
    """Copy a plain file's first `end` bytes, its whole lines, into its version's copy."""
    if version.copied_cut() is not None:
        return version.copy
    partial = version.partial_copy()
    try:
        with local.open('rb') as source, partial.open('wb') as out:
            remaining = end
            while remaining > 0 and (block := source.read(min(remaining, _COPY_BLOCK))):
                out.write(block)
                remaining -= len(block)
        partial.replace(version.copy)
        version.record_cut(cut)
    finally:
        partial.unlink(missing_ok=True)
    return version.copy


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
