"""The application a log holds, and the manifest of the files its tables were built from."""

from __future__ import annotations

import dataclasses as dc
from typing import Any

from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.registry import RUN_SPAN_SQL
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import rows_of

_APPLICATION_SQL = f"""
SELECT application.*, run.end_ms - run.start_ms AS duration_ms
FROM application, ({RUN_SPAN_SQL}) AS run
"""


@dc.dataclass(frozen=True, slots=True)
class ApplicationDetail:
    """The `application` row with the run's duration, which runs to the last thing the log saw finish when it never ended."""

    application: dict[str, Any]
    manifest: TablesManifest

    def to_json(self) -> dict[str, Any]:
        return {'application': self.application, 'manifest': self.manifest.to_json()}


def application_detail(tables: Tables) -> ApplicationDetail:
    """The application and the tables manifest of one log."""
    return ApplicationDetail(rows_of(tables.query(_APPLICATION_SQL)).to_json()[0], tables.manifest)
