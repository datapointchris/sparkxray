"""The record beside one log's tables: which build wrote them, from which files, and what the log could not say.

`schemas/tables-manifest.json` in the package is its JSON Schema. A removed field or table, or a
changed meaning, bumps `SCHEMA_VERSION`. An added field, table or column does not.
"""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Mapping
from typing import Any
from typing import Literal

from sparkxray.ingest.manifest import IngestManifest

SCHEMA_VERSION = 1

# What Spark writes only when asked:
#   stage_executor_metrics  spark.eventLog.logStageExecutorMetrics, each stage's executor peaks
#   block_updates           spark.eventLog.logBlockUpdates.enabled, every cached block's changes
#   process_tree_metrics    spark.executor.processTreeMetrics.enabled, the RSS of each executor's
#                           processes, Python workers included
# What a log rebuilt from a server's REST JSON goes without, and an older Spark's log may too:
#   stage_parents           the stages each stage reads, which the simulation then infers
#   task_executor_metrics   each executor's peaks while a task ran
#   task_accumulators       each task's accumulator updates, so no plan node's metrics aggregate
#                           from tasks and node_stages is empty
#   execution_configs       the settings each SQL execution changed from the application's, so a
#                           setting changed at runtime reads as the application's
Lack = Literal[
    'stage_executor_metrics',
    'block_updates',
    'process_tree_metrics',
    'stage_parents',
    'task_executor_metrics',
    'task_accumulators',
    'execution_configs',
]


@dc.dataclass(frozen=True, slots=True)
class TablesManifest:
    """One log's tables.

    `key` is the SHA-256 that names their directory, over the log's files and the versions of the
    decoders and the build. `lacks` names what the log was written without, so a reader knows a
    missing value from a zero. Where the log lacks `stage_executor_metrics`, the per-task peaks in
    `tasks` are its only memory figures.
    """

    key: str
    ingest: IngestManifest
    row_counts: Mapping[str, int]
    lacks: tuple[Lack, ...]
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> dict[str, Any]:
        """The manifest as the JSON object its schema describes."""
        return {
            'schema_version': self.schema_version,
            'key': self.key,
            'row_counts': dict(self.row_counts),
            'lacks': list(self.lacks),
            'ingest': self.ingest.to_json(),
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> TablesManifest:
        """The manifest `to_json` wrote."""
        return cls(
            key=obj['key'],
            ingest=IngestManifest.from_json(obj['ingest']),
            row_counts=dict(obj['row_counts']),
            lacks=tuple(obj['lacks']),
            schema_version=obj['schema_version'],
        )
