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
from sparkxray.ingest.manifest import IngestManifest
from sparkxray.tables.build import tables_key
from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.relations import Tables


@dc.dataclass(frozen=True, slots=True)
class CachedLog:
    """One log whose tables the installed sparkxray built, and the cache holds.

    `location` is what `EventLog.open` takes to read the log from its source again: a path, or
    an S3 URI. `cached_ms` is when the tables were built, and `tables_path` their directory.
    """

    app_id: str
    app_name: str | None
    spark_version: str | None
    platform: str
    location: str
    log_size_bytes: int
    cached_ms: int
    tables_path: str

    @classmethod
    def of(cls, tables: Tables) -> CachedLog:
        """The cached log one log's tables are."""
        row = tables.query('SELECT app_name FROM application').fetchone()
        return cls._built(tables.manifest, tables.path, None if row is None else row[0])

    @classmethod
    def _built(cls, manifest: TablesManifest, directory: Path, app_name: str | None) -> CachedLog:
        ingest = manifest.ingest
        return cls(
            app_id=ingest.app_id,
            app_name=app_name,
            spark_version=ingest.spark_version,
            platform=ingest.platform,
            location=_openable(ingest.source),
            log_size_bytes=sum(file.size_bytes for file in ingest.files),
            cached_ms=int((directory / 'manifest.json').stat().st_mtime * 1000),
            tables_path=str(directory),
        )

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


def cached_logs(cache_dir: str | os.PathLike[str] | None = None, *, limit: int | None = None) -> list[CachedLog]:
    """Every log the cache holds tables for, newest first, one per source, or the first `limit` of them.

    Tables an older sparkxray built are left out, because this one would build them again.
    """
    if limit is not None and limit < 0:
        raise ValueError(f'limit is {limit}, and a list cannot keep fewer than 0 logs')
    entries = _current(cache_root(cache_dir))[:limit]
    names = _app_names([directory for directory, _ in entries])
    return [CachedLog._built(manifest, directory, names.get(directory)) for directory, manifest in entries]


def cached_ingest(app_id: str, cache: Path) -> IngestManifest:
    """The ingest manifest of the one cached log with this app id.

    No such log raises `NoEventLogError`, listing the app ids the cache holds, and two raise
    `AmbiguousLocationError`, listing where each came from.
    """
    entries = _current(cache)
    matches = [manifest.ingest for _, manifest in entries if manifest.ingest.app_id == app_id]
    if not matches:
        held = sorted({manifest.ingest.app_id for _, manifest in entries})
        raise NoEventLogError(
            f'{app_id!r} names no file or directory, and no log in the cache has that app id', field='location', valid=held
        )
    if len(matches) > 1:
        raise AmbiguousLocationError(
            f'{len(matches)} logs in the cache have the app id {app_id}',
            field='location',
            valid=[_openable(ingest.source) for ingest in matches],
            hint='pass one of the listed locations',
        )
    return matches[0]


def _current(cache: Path) -> list[tuple[Path, TablesManifest]]:
    """The newest tables the installed sparkxray built for each source, newest first."""
    newest: dict[str, tuple[float, Path, TablesManifest]] = {}
    for path in (cache / 'tables').glob('*/*/manifest.json'):
        try:
            manifest = TablesManifest.from_json(json.loads(path.read_text()))
            built = path.stat().st_mtime
        except (OSError, ValueError, KeyError, TypeError):
            continue  # being removed, or written by a sparkxray whose manifest this one cannot read
        # A build in progress writes its manifest before its directory takes the key's name.
        if path.parent.name != manifest.key or manifest.key != tables_key(manifest.ingest):
            continue
        held = newest.get(manifest.ingest.source)
        if held is None or built > held[0]:
            newest[manifest.ingest.source] = (built, path.parent, manifest)
    return [(directory, manifest) for _, directory, manifest in sorted(newest.values(), key=lambda entry: entry[0], reverse=True)]


def _app_names(directories: list[Path]) -> dict[Path, str | None]:
    if not directories:
        return {}
    files = '[' + ', '.join(_literal(str(directory / 'application.parquet')) for directory in directories) + ']'
    rows = duckdb.connect().execute(f'SELECT filename, app_name FROM read_parquet({files}, filename = true)').fetchall()
    return {Path(filename).parent: name for filename, name in rows}


def _openable(source: str) -> str:
    """A source URI as `EventLog.open` takes it: a `file:` URI as its path, and an S3 URI as it is."""
    parsed = urllib.parse.urlparse(source)
    return urllib.request.url2pathname(parsed.path) if parsed.scheme == 'file' else source


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
