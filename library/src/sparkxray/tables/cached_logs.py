"""The logs whose tables the cache holds: listed newest first, or found by app id."""

from __future__ import annotations

import dataclasses as dc
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import duckdb

from sparkxray.cache_directory import cache_root
from sparkxray.errors import AmbiguousLocationError
from sparkxray.errors import NoEventLogError
from sparkxray.errors import suggest_nearest
from sparkxray.ingest.manifest import IngestManifest
from sparkxray.tables.build import tables_key
from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.relations import Tables


@dc.dataclass(frozen=True, slots=True)
class CachedLog:
    """One log the cache holds tables for, which opens by its app id.

    `location` is what `EventLog.open` takes to read the log from its source again, as it stands
    now: a path, or an S3 URI. `cached_epoch_ms` is when its newest tables were built, and
    `tables_path` their directory. An earlier sparkxray may have built them, and then the next
    open builds them again.
    """

    app_id: str
    app_name: str | None
    spark_version: str | None
    platform: str
    location: str
    log_size_bytes: int
    cached_epoch_ms: int
    tables_path: str

    @classmethod
    def of(cls, tables: Tables) -> CachedLog:
        """The cached log one log's tables are."""
        row = tables.first_row('SELECT app_name FROM application')
        return cls._built(tables.manifest, tables.path, None if row is None else row[0], _openable(tables.manifest.ingest.source))

    @classmethod
    def _built(cls, manifest: TablesManifest, directory: Path, app_name: str | None, location: str) -> CachedLog:
        ingest = manifest.ingest
        return cls(
            app_id=ingest.app_id,
            app_name=app_name,
            spark_version=ingest.spark_version,
            platform=ingest.platform,
            location=location,
            log_size_bytes=sum(file.size_bytes for file in ingest.files),
            cached_epoch_ms=int((directory / 'manifest.json').stat().st_mtime * 1000),
            tables_path=str(directory),
        )

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


def cached_logs(cache_dir: str | os.PathLike[str] | None = None, *, limit: int | None = None) -> list[CachedLog]:
    """Every log the cache holds tables for, newest first, or the first `limit` of them.

    These are the logs an app id resolves among, so an app id listed once opens.
    """
    if limit is not None and limit < 0:
        raise ValueError(f'limit is {limit}, and a list cannot keep fewer than 0 logs')
    builds = _distinct_logs(cache_root(cache_dir))[:limit]
    names = _app_names([build.directory for build in builds])
    return [CachedLog._built(build.manifest, build.directory, names.get(build.directory), build.location) for build in builds]


def cached_ingest(app_id: str, cache: Path) -> IngestManifest | str:
    """What opens the one cached log with this app id.

    Finished tables the installed sparkxray built give their ingest manifest, so the log opens
    without its source. A log Spark was still writing, or tables an earlier sparkxray built, give
    the log's location instead, so the open reads the log as it stands now. No such log raises
    `NoEventLogError`, listing the app ids the cache holds, and two raise
    `AmbiguousLocationError`, listing where each opens from.
    """
    builds = _distinct_logs(cache)
    matches = [build for build in builds if build.manifest.ingest.app_id == app_id]
    if not matches:
        held = sorted({build.manifest.ingest.app_id for build in builds})
        raise NoEventLogError(
            suggest_nearest(f'{app_id!r} names no file or directory, and no log in the cache has that app id', app_id, held),
            field='location',
            valid=held,
            hint='sparkxray logs list',
        )
    if len(matches) > 1:
        raise AmbiguousLocationError(
            f'{len(matches)} logs in the cache have the app id {app_id}',
            field='location',
            valid=[build.location for build in matches],
            hint='pass one of the listed locations',
        )
    (build,) = matches
    ingest = build.manifest.ingest
    if ingest.in_progress or build.manifest.key != tables_key(ingest):
        return build.location
    return ingest


@dc.dataclass(frozen=True, slots=True)
class _Build:
    """The newest tables built for one source, and where that log opens from now."""

    directory: Path
    manifest: TablesManifest
    built: float
    location: str
    # The location exists, or is an S3 URI, which this module never checks.
    reachable: bool


def _distinct_logs(cache: Path) -> list[_Build]:
    """The cached logs that open, newest first, each once.

    Two sources reaching one log's files share a tables key, so they are one log. A log read while
    Spark wrote it is the same log as one read after, whose files carry its names without
    `.inprogress`, so the later read stands. A source that no longer exists loses to one with the
    same app id that does. Two logs left with one app id are two attempts, or two copies that differ.
    """
    by_app: dict[str, list[_Build]] = {}
    for build in _newest_builds(cache):
        by_app.setdefault(build.manifest.ingest.app_id, []).append(build)
    kept = []
    for builds in by_app.values():
        by_key: dict[str, _Build] = {}
        for build in builds:
            held = by_key.get(build.manifest.key)
            if held is None or (build.reachable and not held.reachable):
                by_key[build.manifest.key] = build
        distinct = [build for build in by_key.values() if not any(_finished_as(build, other) for other in by_key.values())]
        if any(build.reachable for build in distinct):
            distinct = [build for build in distinct if build.reachable]
        kept.extend(distinct)
    return sorted(kept, key=lambda build: build.built, reverse=True)


def _finished_as(running: _Build, finished: _Build) -> bool:
    """Whether `finished` read the log `running` read while Spark still wrote it."""
    if not running.manifest.ingest.in_progress or finished.manifest.ingest.in_progress:
        return False
    names = {file.uri.removesuffix('.inprogress') for file in running.manifest.ingest.files}
    return names <= {file.uri for file in finished.manifest.ingest.files}


def _newest_builds(cache: Path) -> list[_Build]:
    """The newest tables built for each source, by any sparkxray, newest first."""
    newest: dict[str, _Build] = {}
    for path in (cache / 'tables').glob('*/*/manifest.json'):
        try:
            manifest = TablesManifest.from_json(json.loads(path.read_text()))
            built = path.stat().st_mtime
        except (OSError, ValueError, KeyError, TypeError):
            continue  # being removed, or written by a sparkxray whose manifest this one cannot read
        # A build in progress writes its manifest before its directory takes the key's name.
        if path.parent.name != manifest.key:
            continue
        held = newest.get(manifest.ingest.source)
        if held is None or built > held.built:
            location, reachable = _located(manifest.ingest)
            newest[manifest.ingest.source] = _Build(path.parent, manifest, built, location, reachable)
    return sorted(newest.values(), key=lambda build: build.built, reverse=True)


def _located(ingest: IngestManifest) -> tuple[str, bool]:
    """Where a cached log opens from now, and whether that location exists.

    Spark drops `.inprogress` from a log's name when it finishes the log, so a log read while
    Spark wrote it is found under its finished name.
    """
    location = _openable(ingest.source)
    if urllib.parse.urlparse(ingest.source).scheme != 'file':
        return location, True
    if os.path.exists(location):
        return location, True
    finished = location.removesuffix('.inprogress')
    if ingest.in_progress and finished != location and os.path.exists(finished):
        return finished, True
    return location, False


def _app_names(directories: list[Path]) -> dict[Path, str | None]:
    if not directories:
        return {}
    files = '[' + ', '.join(_literal(str(directory / 'application.parquet')) for directory in directories) + ']'
    # Tables an earlier sparkxray built may hold other columns, so the files are read by column name.
    rows = (
        duckdb.connect().execute(f'SELECT filename, app_name FROM read_parquet({files}, filename = true, union_by_name = true)').fetchall()
    )
    return {Path(filename).parent: name for filename, name in rows}


def _openable(source: str) -> str:
    """A source URI as `EventLog.open` takes it: a `file:` URI as its path, and an S3 URI as it is."""
    parsed = urllib.parse.urlparse(source)
    return urllib.request.url2pathname(parsed.path) if parsed.scheme == 'file' else source


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
