"""Group the files under a location into the applications whose event logs they are, by Spark's names.

Spark names a single-file log `<appId>[.<codec>][.inprogress]`. A rolling log is a directory
`eventlog_v2_<appId>/` holding `events_<N>_<appId>[.<codec>][.compact]` files and an
`appstatus_<appId>[.inprogress]` marker. A log compressed by hand carries its compressor's
suffix instead: `.gz` from gzip and `.zst` from zstd. Names alone never decide that a file is a
log: each candidate is confirmed by its first line afterwards.
"""

from __future__ import annotations

import dataclasses as dc
import re
from collections.abc import Sequence
from typing import Literal

from sparkxray.ingest.decoders import Codec

# `rest_rebuild` is one file sparkxray wrote from a Spark server's REST JSON, which `history_server` reads.
Layout = Literal['single_file', 'rolling', 'rest_rebuild']

_CODEC_SUFFIXES: dict[str, Codec] = {'zstd': 'zstd', 'zst': 'zstd', 'lz4': 'lz4', 'snappy': 'snappy', 'lzf': 'lzf', 'gz': 'gzip'}
_CODEC = r'(?:\.(?P<codec>zstd|zst|lz4|snappy|lzf|gz))?'
# Matched against a name with its `.inprogress` removed.
_SINGLE_FILE = re.compile(rf'(?P<app>[^._][^.]*){_CODEC}')
ROLLING_DIRECTORY = re.compile(r'eventlog_v2_(?P<app>[^.]+)')
_ROLLED_FILE = re.compile(rf'events_(?P<index>\d+)_(?P<app>[^.]+){_CODEC}(?P<compact>\.compact)?')
_APP_STATUS = re.compile(r'appstatus_(?P<app>[^.]+)(?P<in_progress>\.inprogress)?')


@dc.dataclass(frozen=True, slots=True)
class ListedFile:
    """One file under the location. `parts` is its path below the location's parent, by segment."""

    parts: tuple[str, ...]
    uri: str
    size_bytes: int
    etag: str | None
    mtime_ns: int | None

    @property
    def name(self) -> str:
        return self.parts[-1]


@dc.dataclass(frozen=True, slots=True)
class LogFile:
    listed: ListedFile
    codec: Codec


@dc.dataclass(frozen=True, slots=True)
class Candidate:
    """One application's log, as its names describe it. `uri` is the location that names it alone."""

    uri: str
    app_id: str
    layout: Layout
    files: tuple[LogFile, ...]
    in_progress: bool
    compacted: bool


def ignored(name: str) -> bool:
    """Hidden files, Hadoop's `_SUCCESS` and `_temporary`, and checksum files are never logs."""
    return name.startswith(('.', '_')) or name.endswith('.crc')


def without_in_progress(name: str) -> tuple[str, bool]:
    """A file's name with Spark's `.inprogress` removed, and whether it was there.

    Spark puts `.inprogress` last. A person compressing a running log afterwards puts the
    codec's suffix after it, as `gzip local-1.inprogress` writes `local-1.inprogress.gz`.
    """
    if name.endswith('.inprogress'):
        return name.removesuffix('.inprogress'), True
    stem, _, suffix = name.rpartition('.')
    if stem.endswith('.inprogress') and suffix in _CODEC_SUFFIXES:
        return f'{stem.removesuffix(".inprogress")}.{suffix}', True
    return name, False


def codec_of(name: str) -> Codec:
    """The codec a file's name declares, `.inprogress` and `.compact` aside, or none."""
    stem = without_in_progress(name)[0].removesuffix('.compact')
    return _CODEC_SUFFIXES.get(stem.rpartition('.')[2], 'none') if '.' in stem else 'none'


def named_file(listed: ListedFile) -> Candidate:
    """A file the caller named, read as a single-file log whatever its name."""
    stem, in_progress = without_in_progress(listed.name)
    match = _SINGLE_FILE.fullmatch(stem)
    app_id = match['app'] if match else listed.name.partition('.')[0]
    return Candidate(
        uri=listed.uri,
        app_id=app_id,
        layout='single_file',
        files=(LogFile(listed, codec_of(listed.name)),),
        in_progress=in_progress,
        compacted=False,
    )


def group(listing: Sequence[ListedFile]) -> tuple[list[Candidate], list[str]]:
    """The candidate logs in a listing, and the paths of the files that are none of them.

    The listing leaves out every file below a directory `ignored` names, so only a file's own
    name is checked here.
    """
    rolling: dict[tuple[str, ...], list[ListedFile]] = {}
    singles: dict[tuple[tuple[str, ...], str], list[tuple[ListedFile, bool]]] = {}
    unrecognized: list[str] = []
    for listed in listing:
        directory = listed.parts[:-1]
        if ignored(listed.name):
            continue
        if directory and ROLLING_DIRECTORY.fullmatch(directory[-1]):
            rolling.setdefault(directory, []).append(listed)
            continue
        if any(ROLLING_DIRECTORY.fullmatch(part) for part in directory):
            continue
        stem, in_progress = without_in_progress(listed.name)
        if _SINGLE_FILE.fullmatch(stem) is None:
            unrecognized.append('/'.join(listed.parts))
            continue
        singles.setdefault((directory, stem), []).append((listed, in_progress))

    candidates = []
    for versions in singles.values():
        # Glue leaves the `.inprogress` copy beside the finished log, and the finished one is whole.
        listed, _in_progress = min(versions, key=lambda version: version[1])
        candidates.append(named_file(listed))
    for directory, members in rolling.items():
        candidate = _rolling(directory[-1].removeprefix('eventlog_v2_'), members)
        if candidate is None:
            unrecognized.extend('/'.join(member.parts) for member in members)
        else:
            candidates.append(candidate)
    candidates.sort(key=lambda candidate: candidate.uri)
    return candidates, unrecognized


def _rolling(app_id: str, members: list[ListedFile]) -> Candidate | None:
    rolled: list[tuple[int, bool, ListedFile]] = []
    status_in_progress: bool | None = None
    for listed in members:
        if status := _APP_STATUS.fullmatch(listed.name):
            status_in_progress = status['in_progress'] is not None
        elif rolled_file := _ROLLED_FILE.fullmatch(listed.name):
            rolled.append((int(rolled_file['index']), rolled_file['compact'] is not None, listed))
    if not rolled:
        return None
    rolled.sort(key=lambda entry: entry[0])
    # A compacted file replaces every file before it, so reading starts at the last one.
    compact_positions = [position for position, (_index, compact, _listed) in enumerate(rolled) if compact]
    if compact_positions:
        rolled = rolled[compact_positions[-1] :]
    return Candidate(
        uri=rolled[0][2].uri.rpartition('/')[0],
        app_id=app_id,
        layout='rolling',
        files=tuple(LogFile(listed, codec_of(listed.name)) for _index, _compact, listed in rolled),
        # Spark renames the marker when the application ends, so a missing one confirms nothing.
        in_progress=status_in_progress is not False,
        compacted=bool(compact_positions),
    )
