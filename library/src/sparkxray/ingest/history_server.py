"""Read one application from a Spark History Server or driver UI: its event log, or its REST JSON rebuilt as one.

A server's address is taken in three forms, each read as the API form
`<root>/api/v1/applications/<app>[/<attempt>]`:

- `http(s)://host:port[/prefix]`, with the app id passed beside it, or naming the server's only
  application;
- a History Server page, `<root>/history/<app>[/<attempt>]/…`;
- an API URL, `<root>/api/v1/applications/<app>[/<attempt>]`.

With no attempt named, the newest attempt is read. The server's zip of that attempt's log files is
downloaded first, and its files read as any log's are. A driver UI answers that request with an
empty zip, and a History Server with 404 or 503 where it has no log. Then the attempt's REST JSON
is rewritten as the log Spark would have written, which `events_from_rest` describes.

Everything lands under `<cache>/logs/rest/<server>/<app>/<attempt>/<version>/`. A finished
attempt's version is its end time and the server's last update, so it is fetched once. A running
attempt gets a fresh version on every read, so its copy is never reused. A version stays until it
has gone a day unused, because a log opened before it was superseded still reads it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import urllib.parse
import zipfile
from collections.abc import Sequence
from pathlib import Path
from pathlib import PurePosixPath
from typing import TYPE_CHECKING
from typing import Any

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.cache_directory import remove_superseded
from sparkxray.cache_directory import used
from sparkxray.errors import AmbiguousLocationError
from sparkxray.errors import NoEventLogError
from sparkxray.errors import UnknownApplicationError
from sparkxray.errors import UnreachableServerError
from sparkxray.ingest.layouts import ListedFile

if TYPE_CHECKING:
    from sparkxray.ingest.rest_client import RestClient

SCHEMES = ('http://', 'https://')
_RECENT = 5
# The Spark UI's tabs, which end the server's root in an address copied from a browser.
_UI_TABS = frozenset({'jobs', 'stages', 'storage', 'environment', 'executors', 'SQL', 'sql', 'connect', 'StreamingQuery', 'streaming'})
_FILES = 'files'
_RECORD = 'source.json'


class HistoryServerLocation:
    """One application attempt a server holds. `uri` is its API URL once `list` has found which application that is."""

    files_may_grow = False  # every file is a copy in the cache

    def __init__(self, url: str, *, app: str | None, cache: Path) -> None:
        root, named_app, attempt = _parsed(url)
        if app is not None and named_app is not None and app != named_app:
            raise AmbiguousLocationError(
                f'{url} names the application {named_app}, and {app} was passed beside it',
                field='app',
                valid=[named_app, app],
                hint='pass one of them',
            )
        self._api_root = f'{root}/api/v1'
        self._app = app or named_app
        self._attempt = attempt
        self._cache = cache
        self._paths: dict[str, Path] = {}
        self.uri = f'{self._api_root}/applications/{_quoted(self._app)}' if self._app else root
        # Set by `list`: whether the files are a log rebuilt from the REST JSON, rather than one Spark wrote.
        self.rest_rebuild = False

    def list(self) -> tuple[list[ListedFile], ListedFile | None]:
        from sparkxray.ingest.rest_client import RestClient  # httpx2 is imported only when a log is read over REST

        with RestClient(self._api_root) as client:
            application = self._application(client)
            attempt = self._chosen_attempt(application)
            app_path = f'applications/{_quoted(str(application["id"]))}'
            if attempt.get('attemptId') is not None:
                app_path += f'/{_quoted(str(attempt["attemptId"]))}'
            self.uri = f'{self._api_root}/{app_path}'
            version = self._fetched(client, app_path, application, attempt)
        self.rest_rebuild = json.loads((version / _RECORD).read_text())['rest_rebuild']
        files = version / _FILES
        listing = []
        for path in sorted(path for path in files.rglob('*') if path.is_file()):
            parts = path.relative_to(files).parts
            uri = self.uri if self.rest_rebuild else f'{self.uri}/logs/{"/".join(parts)}'
            listed = ListedFile(parts, uri, path.stat().st_size, version.name, None)
            self._paths[uri] = path
            listing.append(listed)
        return listing, listing[0] if self.rest_rebuild else None

    def leading_bytes(self, listed: ListedFile, size: int) -> bytes:
        with self._paths[listed.uri].open('rb') as file:
            return file.read(size)

    def local_path(self, listed: ListedFile, cache: Path) -> Path:
        return self._paths[listed.uri]

    def _application(self, client: RestClient) -> dict[str, Any]:
        if self._app is None:
            listed = _recent(client)
            if not listed:
                raise NoEventLogError(f'{self.uri} holds no application', field='location')
            if len(listed) > 1:
                raise AmbiguousLocationError(
                    f'{self.uri} holds more than one application, and ingest reads one. The {len(listed)} most recent are listed',
                    field='location',
                    valid=[f'{self._api_root}/applications/{_quoted(str(each["id"]))}' for each in listed],
                    hint='pass one of the listed addresses as LOG',
                )
            self._app = str(listed[0]['id'])
        application = client.json(f'applications/{_quoted(self._app)}')
        if application is None:
            raise UnknownApplicationError(
                f'{self._api_root} holds no application {self._app}. The most recent it holds are listed',
                field='app',
                valid=[f'{self._api_root}/applications/{_quoted(str(each["id"]))}' for each in _recent(client)],
                hint='pass one of the listed addresses as LOG',
            )
        if not isinstance(application, dict) or 'id' not in application:
            raise _not_rest(client.url(f'applications/{_quoted(self._app)}'))
        return application

    def _chosen_attempt(self, application: dict[str, Any]) -> dict[str, Any]:
        attempts: Sequence[dict[str, Any]] = application.get('attempts') or []
        if self._attempt is None:
            return max(attempts, key=lambda attempt: attempt.get('startTimeEpoch') or 0, default={})
        chosen = next((attempt for attempt in attempts if str(attempt.get('attemptId')) == self._attempt), None)
        if chosen is None:
            app_address = f'{self._api_root}/applications/{_quoted(str(application["id"]))}'
            numbered = [
                f'{app_address}/{_quoted(str(attempt["attemptId"]))}' for attempt in attempts if attempt.get('attemptId') is not None
            ]
            raise UnknownApplicationError(
                f'{application["id"]} has no attempt {self._attempt}',
                field='location',
                valid=numbered or [app_address],
                hint='pass one of the listed addresses as LOG, or the application address for its newest attempt'
                if numbered
                else 'its attempt has no id, so pass the listed address as LOG',
            )
        return chosen

    def _fetched(self, client: RestClient, app_path: str, application: dict[str, Any], attempt: dict[str, Any]) -> Path:
        """The directory holding this attempt's files and the record of how they were got, fetched where this version is not cached."""
        from sparkxray.ingest.events_from_rest import write_rebuilt_log

        server = hashlib.sha256(self._api_root.encode()).hexdigest()[:32]
        directory = self._cache.joinpath('logs', 'rest', server, _safe(str(application['id'])), _safe(str(attempt.get('attemptId', '-'))))
        finished = bool(attempt.get('completed'))
        # A History Server dates an incomplete attempt by its log's last write, so the key moves as the log grows.
        version = (
            f'{attempt.get("endTimeEpoch")}-{attempt.get("lastUpdatedEpoch")}'
            if finished
            else f'incomplete-{attempt.get("lastUpdatedEpoch")}'
        )
        remove_superseded(directory, version)
        target = directory / version
        if (target / _RECORD).is_file() and (finished or not json.loads((target / _RECORD).read_text())['rest_rebuild']):
            used(target)
            return target
        partial = directory / f'{version}{PARTIAL_MARK}{os.getpid()}-{secrets.token_hex(4)}'
        files = partial / _FILES
        files.mkdir(parents=True)
        try:
            archive = partial / 'logs.zip'
            unpacked = client.download(f'{app_path}/logs', archive) and _unpacked(archive, files, client.url(f'{app_path}/logs'))
            archive.unlink(missing_ok=True)
            if not unpacked:
                name = _safe(str(application['id'])) + ('' if finished else '.inprogress')
                write_rebuilt_log(client, app_path, application, attempt, files / name)
                if not finished:
                    # A driver UI dates a running attempt by its start, so its rebuild is never reused.
                    target = directory / f'running-{secrets.token_hex(8)}'
            (partial / _RECORD).write_text(json.dumps({'rest_rebuild': not unpacked}))
            try:
                partial.rename(target)
            except OSError:
                if not (target / _RECORD).is_file():
                    raise
                # another process fetched the same version first, and its directory is kept
        finally:
            shutil.rmtree(partial, ignore_errors=True)
        return target


def _recent(client: RestClient) -> list[dict[str, Any]]:
    """The server's most recent applications, as many as `_RECENT`."""
    listed = client.json('applications', {'limit': _RECENT})
    if not isinstance(listed, list):
        raise _not_rest(client.url('applications'))
    return listed


def _parsed(url: str) -> tuple[str, str | None, str | None]:
    """The server's root, and the app id and attempt id the address names, each None where it names none."""
    split = urllib.parse.urlsplit(url)
    segments = [urllib.parse.unquote(segment) for segment in split.path.split('/') if segment]
    root, app, attempt = segments, None, None
    for position, segment in enumerate(segments):
        rest = segments[position + 1 :]
        if segment == 'api' and rest[:1] == ['v1']:
            root, named = segments[:position], rest[2:] if rest[1:2] == ['applications'] else []
        elif segment == 'history':
            root, named = segments[:position], rest
        elif segment in _UI_TABS:
            root, named = segments[:position], []
        else:
            continue
        app = named[0] if named else None
        # An attempt id is a number, where a UI page or an endpoint follows the app id instead.
        attempt = named[1] if len(named) > 1 and named[1].isdigit() else None
        break
    path = ''.join(f'/{urllib.parse.quote(segment)}' for segment in root)
    return f'{split.scheme}://{split.netloc}{path}', app, attempt


def _quoted(segment: str | None) -> str:
    return urllib.parse.quote(segment or '', safe='')


def _safe(name: str) -> str:
    """A name as one directory's, with every character a path could misread replaced."""
    return re.sub(r'[^A-Za-z0-9_-]', '_', name) or '_'


def _unpacked(archive: Path, files: Path, url: str) -> bool:
    """Unpack the server's zip of an attempt's log files into `files`. False where it holds no file or is no zip."""
    try:
        with zipfile.ZipFile(archive) as zipped:
            members = [member for member in zipped.infolist() if not member.is_dir()]
            for member in members:
                parts = PurePosixPath(member.filename).parts
                if not parts or member.filename.startswith('/') or '\\' in member.filename or any(part in ('.', '..') for part in parts):
                    raise UnreachableServerError(
                        f'{url} answered a zip holding {member.filename!r}, a path outside the log it was asked for', field='location'
                    )
                target = files.joinpath(*parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with zipped.open(member) as source, target.open('wb') as out:
                    shutil.copyfileobj(source, out)
    except zipfile.BadZipFile:
        return False
    return bool(members)


def _not_rest(url: str) -> UnreachableServerError:
    return UnreachableServerError(
        f'{url} answered with what is not the Spark REST API',
        field='location',
        hint='pass the address of a Spark History Server or driver UI',
    )
