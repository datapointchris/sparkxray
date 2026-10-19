"""The record of one ingest: where a log came from, what was read, and how far it could be trusted.

`schemas/ingest-manifest.json` in the package is its JSON Schema. A removed field or a changed
meaning bumps `SCHEMA_VERSION`, and an added field does not.
"""

from __future__ import annotations

import dataclasses as dc
from typing import Any
from typing import Literal

from sparkxray.ingest.decoders import Codec
from sparkxray.ingest.layouts import Layout
from sparkxray.ingest.platforms import Platform

SCHEMA_VERSION = 1

ReadCodec = Literal['none', 'zstd', 'gzip']


@dc.dataclass(frozen=True, slots=True)
class IngestedFile:
    """One file of a log, in read order.

    `uri` is where the file lives: `file://` for a local file, `s3://` for an object. `etag` is
    the object's S3 ETag, and `mtime_ns` a local file's modification time. Together with
    `size_bytes` they identify one version of the file.

    `read_path` is the local file holding its events, which `read_codec` compresses. That is the
    file itself when it is local and readable as it stands. Otherwise it is a copy in the cache:
    the downloaded object, or its whole lines as plain text. A file gets that copy when its codec
    has no standard framing, when its end was cut, or when it is the last file of a local log
    Spark is still writing, which grows after it is opened.
    """

    uri: str
    etag: str | None
    mtime_ns: int | None
    size_bytes: int
    codec: Codec
    read_path: str
    read_codec: ReadCodec


@dc.dataclass(frozen=True, slots=True)
class IngestManifest:
    """One application's log after ingest.

    `in_progress` is set when Spark had not finished the log: an `.inprogress` name, before or
    after a codec's suffix, or a rolling log whose `appstatus` marker is still `.inprogress`.
    `truncated_tail` is set when the log's last file ends partway through a line, a compressed
    block or a frame, and what followed its last whole line was dropped. `compacted` is set when
    a History Server compacted the rolling log, which drops finished tasks.
    """

    source: str
    app_id: str
    layout: Layout
    files: tuple[IngestedFile, ...]
    compacted: bool
    in_progress: bool
    truncated_tail: bool
    spark_version: str | None
    platform: Platform
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> dict[str, Any]:
        """The manifest as the JSON object its schema describes."""
        return {
            'schema_version': self.schema_version,
            'source': self.source,
            'app_id': self.app_id,
            'layout': self.layout,
            'files': [dc.asdict(file) for file in self.files],
            'compacted': self.compacted,
            'in_progress': self.in_progress,
            'truncated_tail': self.truncated_tail,
            'spark_version': self.spark_version,
            'platform': self.platform,
        }
