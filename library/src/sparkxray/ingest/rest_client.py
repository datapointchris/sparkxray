"""Every request sparkxray sends a Spark History Server or driver UI, through one httpx2 client per ingest.

httpx2 is imported here alone, and only when a log is read over REST. At most `IN_FLIGHT`
requests run at once. A connection error or a 5xx answer is retried 3 times, after 1 s, 2 s and
4 s. The client sends no credentials: a server behind authentication is reached through a tunnel
the caller already runs, such as an SSH port-forward or `kubectl port-forward`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import TypeVar

import httpx2

from sparkxray.errors import UnreachableServerError

IN_FLIGHT = 4
_TIMEOUT = httpx2.Timeout(60.0, connect=10.0)
_RETRIES = 3
_FIRST_BACKOFF_SECONDS = 1.0
_CHUNK_BYTES = 1 << 20
# What the logs endpoint answers when the server has no event log to give.
_NO_LOG_STATUSES = frozenset({404, 503})

_Item = TypeVar('_Item')
_Result = TypeVar('_Result')

Parameters = Mapping[str, str | int]


class RestClient:
    """A client for one server's REST API, rooted at its `/api/v1`."""

    def __init__(self, api_root: str) -> None:
        self.api_root = api_root.rstrip('/')
        self._client = httpx2.Client(timeout=_TIMEOUT, limits=httpx2.Limits(max_connections=IN_FLIGHT), follow_redirects=True)

    def __enter__(self) -> RestClient:
        return self

    def __exit__(self, kind: type[BaseException] | None, error: BaseException | None, traceback: TracebackType | None) -> None:
        self._client.close()

    def url(self, path: str) -> str:
        return f'{self.api_root}/{path.lstrip("/")}'

    def json(self, path: str, parameters: Parameters | None = None) -> Any:
        """The JSON the server answers at `path` below the API root, or None where it answers 404."""
        url = self.url(path)
        response = self._retried(url, lambda: self._client.get(url, params=parameters))
        if response.status_code == 404:
            return None
        _refuse_status(url, response)
        try:
            return response.json()
        except ValueError as error:
            raise UnreachableServerError(
                f'{url} answered with what is not JSON, so it is not the Spark REST API: {response.text[:200]!r}',
                field='location',
                hint='pass the address of a Spark History Server or driver UI',
            ) from error

    def download(self, path: str, target: Path) -> bool:
        """Stream what the server answers at `path` into `target`, and return True.

        Return False, writing nothing, where the server answers 404 or 503, as a server with no
        event log for an application does.
        """
        url = self.url(path)

        def attempt() -> httpx2.Response:
            with self._client.stream('GET', url) as response:
                if response.status_code != 200:
                    response.read()  # so a refusal can quote the body after the stream closes
                    return response
                with target.open('wb') as out:
                    for chunk in response.iter_bytes(_CHUNK_BYTES):
                        out.write(chunk)
                return response

        response = self._retried(url, attempt, retry_statuses=lambda status: status >= 500 and status not in _NO_LOG_STATUSES)
        if response.status_code in _NO_LOG_STATUSES:
            return False
        _refuse_status(url, response)
        return True

    def in_parallel(self, function: Callable[[_Item], _Result], items: Sequence[_Item]) -> list[_Result]:
        """`function` over `items` in order, `IN_FLIGHT` at a time."""
        if len(items) <= 1:
            return [function(item) for item in items]
        with ThreadPoolExecutor(max_workers=IN_FLIGHT) as pool:
            return list(pool.map(function, items))

    def _retried(
        self,
        url: str,
        attempt: Callable[[], httpx2.Response],
        *,
        retry_statuses: Callable[[int], bool] = lambda status: status >= 500,
    ) -> httpx2.Response:
        """The response of the first attempt that neither failed to connect nor answered a status to retry."""
        backoff = _FIRST_BACKOFF_SECONDS
        for retry in range(_RETRIES + 1):
            try:
                response = attempt()
            except httpx2.TransportError as error:
                if retry == _RETRIES:
                    raise UnreachableServerError(
                        f'{url} did not answer after {_RETRIES} retries: {type(error).__name__}: {error}',
                        field='location',
                        hint='check that the server is running and reachable, through a tunnel where it needs one',
                    ) from error
            else:
                if not retry_statuses(response.status_code) or retry == _RETRIES:
                    return response
            time.sleep(backoff)
            backoff *= 2
        raise AssertionError('unreachable: the last retry returns or raises')


def _refuse_status(url: str, response: httpx2.Response) -> None:
    if response.status_code >= 400:
        raise UnreachableServerError(
            f'{url} answered {response.status_code}: {response.text[:200]!r}',
            field='location',
            hint='a server behind authentication is reached through a tunnel that signs in for you',
        )
