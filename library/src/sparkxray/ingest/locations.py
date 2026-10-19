"""List the files under a local path or an S3 URI, and give each one back as a local file.

An S3 object is downloaded into the cache once per ETag and size, under
`logs/s3/<bucket>/<key>/<etag>-<size>`. So an unchanged object is never downloaded twice, and a
log still being appended to is downloaded again.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Protocol

from sparkxray.errors import NoEventLogError
from sparkxray.ingest.layouts import ROLLING_DIRECTORY
from sparkxray.ingest.layouts import ListedFile
from sparkxray.ingest.layouts import ignored

_S3_SCHEMES = ('s3://', 's3a://', 's3n://')
_UNSAFE_SEGMENTS = {'': '%2F', '.': '%2E', '..': '%2E%2E'}


class Location(Protocol):
    uri: str

    def list(self) -> tuple[list[ListedFile], ListedFile | None]:
        """Every file under the location, and the file itself when the location names one."""
        ...

    def leading_bytes(self, listed: ListedFile, size: int) -> bytes: ...

    def local_path(self, listed: ListedFile, cache: Path) -> Path: ...


def location_for(location: str, *, profile: str | None, region: str | None) -> Location:
    """The location a string names: an `s3://` URI, or else a local path."""
    if location.startswith(_S3_SCHEMES):
        return S3Location(location, profile=profile, region=region)
    return LocalLocation(Path(location))


class LocalLocation:
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
    def __init__(self, location: str, *, profile: str | None, region: str | None) -> None:
        from sparkxray.ingest import aws  # boto3 is imported only when a log is on S3

        bucket, _, prefix = location.partition('://')[2].partition('/')
        prefix = prefix.rstrip('/')
        parent, _, name = prefix.rpartition('/')
        # A file inside a rolling log's directory names the whole log.
        if parent and ROLLING_DIRECTORY.fullmatch(parent.rpartition('/')[2]) and not ROLLING_DIRECTORY.fullmatch(name):
            prefix = parent
        self._bucket = bucket
        self._prefix = prefix
        self.uri = f's3://{bucket}/{prefix}'
        self._client = aws.S3(profile=profile, region=region)

    def list(self) -> tuple[list[ListedFile], ListedFile | None]:
        prefix = self._prefix
        base = prefix.rpartition('/')[0]
        listing = []
        for item in self._client.list_objects(self._bucket, prefix):
            key = str(item['Key'])
            # `logs/app-1` holds `logs/app-1`, `logs/app-1.inprogress` and `logs/app-1/…`, never `logs/app-10`.
            if key.endswith('/') or (prefix and key != prefix and key[len(prefix)] not in './'):
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
        if target.is_file():
            return target
        if directory.is_dir():
            for stale in directory.iterdir():
                # A `.part-<pid>` file is another process's download still in flight.
                if stale.is_file() and '.part-' not in stale.name:
                    stale.unlink(missing_ok=True)
        self._client.download(self._bucket, key, target)
        return target

    def _key(self, listed: ListedFile) -> str:
        return listed.uri.removeprefix(f's3://{self._bucket}/')
