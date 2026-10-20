"""List the files under a local path, an S3 URI or a Spark server's address, and give each one back as a local file.

`history_server` reads a server's. Of the other two:

An S3 object is downloaded into the cache once per ETag and size, under
`logs/s3/<bucket>/<key>/<etag>-<size>`. So an unchanged object is never downloaded twice, and a
log still being appended to is downloaded again. The copy of an older version stays until it has
gone a day unused, because a log opened before the object changed still reads it.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Protocol

from sparkxray.cache_directory import remove_superseded
from sparkxray.cache_directory import used
from sparkxray.errors import NoEventLogError
from sparkxray.ingest.history_server import SCHEMES
from sparkxray.ingest.history_server import HistoryServerLocation
from sparkxray.ingest.layouts import ROLLING_DIRECTORY
from sparkxray.ingest.layouts import ListedFile
from sparkxray.ingest.layouts import ignored

_S3_SCHEMES = ('s3://', 's3a://', 's3n://')
_UNSAFE_SEGMENTS = {'': '%2F', '.': '%2E', '..': '%2E%2E'}


class Location(Protocol):
    uri: str
    # Whether a file `local_path` returns can still grow, as a log Spark is writing does.
    files_may_grow: bool
    # Whether the files are a log sparkxray rebuilt from a server's REST JSON, rather than one Spark wrote.
    rest_rebuild: bool

    def list(self) -> tuple[list[ListedFile], ListedFile | None]:
        """Every file under the location, and the file itself when the location names one."""
        ...

    def leading_bytes(self, listed: ListedFile, size: int) -> bytes: ...

    def local_path(self, listed: ListedFile, cache: Path) -> Path: ...


def location_for(location: str, *, profile: str | None, region: str | None, app: str | None, cache: Path) -> Location:
    """The location a string names: an `s3://` URI, a History Server or driver UI's `http(s)://` address, or else a local path.

    `app` names the application on a server, and raises ValueError beside any other location.
    """
    if location.startswith(SCHEMES):
        return HistoryServerLocation(location, app=app, cache=cache)
    if app is not None:
        raise ValueError(f'{location} is no server address, and only a server address takes an app id beside it')
    if location.startswith(_S3_SCHEMES):
        return S3Location(location, profile=profile, region=region)
    return LocalLocation(Path(location))


class LocalLocation:
    files_may_grow = True
    rest_rebuild = False

    def __init__(self, path: Path) -> None:
        path = path.expanduser().resolve()
        # A file inside a rolling log's directory names the whole log.
        if path.is_file() and ROLLING_DIRECTORY.fullmatch(path.parent.name):
            path = path.parent
        self._path = path
        self.uri = path.as_uri()
        self._paths: dict[str, Path] = {}

    def list(self) -> tuple[list[ListedFile], ListedFile | None]:
        path = self._path
        if not path.exists():
            raise NoEventLogError(f'{path} does not exist', field='location')
        if path.is_file():
            listed = self._listed(path, path.parent)
            return [listed], listed
        listing = []
        for directory, subdirectories, names in os.walk(path):
            subdirectories[:] = sorted(name for name in subdirectories if not ignored(name))
            for name in sorted(names):
                try:
                    listing.append(self._listed(Path(directory, name), path.parent))
                except OSError:
                    continue  # a dangling symlink, or a file removed while listing
        return listing, None

    def leading_bytes(self, listed: ListedFile, size: int) -> bytes:
        with self._paths[listed.uri].open('rb') as file:
            return file.read(size)

    def local_path(self, listed: ListedFile, cache: Path) -> Path:
        return self._paths[listed.uri]

    def _listed(self, path: Path, base: Path) -> ListedFile:
        status = path.stat()
        listed = ListedFile(path.relative_to(base).parts, path.as_uri(), status.st_size, None, status.st_mtime_ns)
        self._paths[listed.uri] = path
        return listed


class S3Location:
    files_may_grow = False  # every file is a downloaded copy
    rest_rebuild = False

    def __init__(self, location: str, *, profile: str | None, region: str | None) -> None:
        from sparkxray.ingest import aws  # boto3 is imported only when a log is on S3

        bucket, _, key = location.partition('://')[2].partition('/')
        # A trailing slash names a directory, so only the keys below it are listed.
        directory = key.endswith('/')
        prefix = key.rstrip('/')
        parent, _, name = prefix.rpartition('/')
        # A file inside a rolling log's directory names the whole log.
        if parent and ROLLING_DIRECTORY.fullmatch(parent.rpartition('/')[2]) and not ROLLING_DIRECTORY.fullmatch(name):
            prefix = parent
            directory = True
        self._bucket = bucket
        self._prefix = prefix
        self._directory = directory and bool(prefix)
        self.uri = f's3://{bucket}/{prefix}/' if self._directory else f's3://{bucket}/{prefix}'
        self._client = aws.S3(profile=profile, region=region)

    def list(self) -> tuple[list[ListedFile], ListedFile | None]:
        prefix = self._prefix
        base = prefix.rpartition('/')[0]
        listing = []
        for item in self._client.list_objects(self._bucket, f'{prefix}/' if self._directory else prefix):
            key = str(item['Key'])
            if key.endswith('/') or not _below(key, prefix):
                continue
            parts = tuple(key[len(base) + 1 :].split('/') if base else key.split('/'))
            if any(ignored(part) for part in parts[1:-1]):
                continue
            etag = str(item['ETag']).strip('"')
            listing.append(ListedFile(parts, f's3://{self._bucket}/{key}', int(str(item['Size'])), etag, None))
        if not listing:
            raise NoEventLogError(f'{self.uri} holds no objects', field='location')
        named = listing[0] if len(listing) == 1 and listing[0].uri == self.uri else None
        return listing, named

    def leading_bytes(self, listed: ListedFile, size: int) -> bytes:
        return self._client.leading_bytes(self._bucket, self._key(listed), size)

    def local_path(self, listed: ListedFile, cache: Path) -> Path:
        key = self._key(listed)
        directory = cache.joinpath('logs', 's3', self._bucket, *(_UNSAFE_SEGMENTS.get(part, part) for part in key.split('/')))
        target = directory / f'{re.sub(r"[^A-Za-z0-9-]", "_", listed.etag or "")}-{listed.size_bytes}'
        remove_superseded(directory, target.name)
        if target.is_file():
            used(target)
            return target
        self._client.download(self._bucket, key, target)
        return target

    def _key(self, listed: ListedFile) -> str:
        return listed.uri.removeprefix(f's3://{self._bucket}/')


def _below(key: str, prefix: str) -> bool:
    """Whether a key is the object `prefix` names, a file beside it named for it, or a key in it.

    `logs/app-1` holds `logs/app-1`, `logs/app-1.inprogress` and `logs/app-1/…`. It never holds
    `logs/app-10`, or `logs/app-1.bak/…`, which is another directory.
    """
    if not prefix or key == prefix:
        return True
    rest = key[len(prefix) :]
    return rest.startswith('/') or (rest.startswith('.') and '/' not in rest)
