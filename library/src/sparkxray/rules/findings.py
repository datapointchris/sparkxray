"""A finding: what one rule found about one part of a run, in the shape `schemas/findings.json` pins."""

from __future__ import annotations

import dataclasses as dc
import importlib.resources
import json
import math
import os
import secrets
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Literal

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.errors import InvalidRuleError
from sparkxray.rules.rule import ScopeLevel
from sparkxray.rules.rule import Severity
from sparkxray.rules.rule import ThresholdUnit

# A field removed or a meaning changed raises this. A field added does not.
SCHEMA_VERSION = 1
Confidence = Literal['high', 'medium', 'low']
CONFIDENCES: tuple[Confidence, ...] = ('high', 'medium', 'low')
ThresholdLayer = Literal['default', 'file', 'sdk', 'flag']
# Every numeric evidence key ends in one of these, so a value always names its unit.
UNIT_SUFFIXES = ('_ms', '_ns', '_bytes', '_records', '_rows', '_count', '_share', '_ratio', '_hours', '_seconds', '_percent', '_id', '_ids')
# The key of `findings.parquet`'s metadata that holds the analysis document, its findings left out.
ANALYSIS_METADATA_KEY = 'sparkxray.analysis'
FINDINGS_TABLE_DESCRIPTION = "The last analysis's findings, one row each, with their evidence and thresholds as JSON text."
# `findings.parquet`'s columns and their DuckDB types: a finding's fields, with its scope's ids as columns of their own.
FINDINGS_PARQUET_COLUMNS: tuple[tuple[str, str], ...] = (
    ('schema_version', 'BIGINT'),
    ('finding_id', 'VARCHAR'),
    ('rule_id', 'VARCHAR'),
    ('rule_version', 'BIGINT'),
    ('severity', 'VARCHAR'),
    ('base_severity', 'VARCHAR'),
    ('title', 'VARCHAR'),
    ('app_id', 'VARCHAR'),
    ('sql_execution_id', 'BIGINT'),
    ('plan_node_id', 'BIGINT'),
    ('plan_node_label', 'VARCHAR'),
    ('stage_id', 'BIGINT'),
    ('stage_attempt', 'BIGINT'),
    ('job_id', 'BIGINT'),
    ('executor_id', 'VARCHAR'),
    ('worst_task_ids', 'BIGINT[]'),
    ('evidence', 'VARCHAR'),
    ('thresholds', 'VARCHAR'),
    ('impact_seconds', 'DOUBLE'),
    ('confidence', 'VARCHAR'),
    ('confidence_reason', 'VARCHAR'),
    ('remediation', 'VARCHAR[]'),
    ('docs', 'VARCHAR[]'),
)

Evidence = bool | int | float | str | None | tuple[bool | int | float | str | None, ...]
_LABEL_CHARACTERS = 100


@dc.dataclass(frozen=True, slots=True)
class FindingScope:
    """The ids of what a finding is about. `worst_task_ids` names the tasks that show it most."""

    app_id: str | None = None
    sql_execution_id: int | None = None
    plan_node_id: int | None = None
    plan_node_label: str | None = None
    stage_id: int | None = None
    stage_attempt: int | None = None
    job_id: int | None = None
    executor_id: str | None = None
    worst_task_ids: tuple[int, ...] = ()

    def key(self, level: ScopeLevel) -> str:
        """The part of a `finding_id` after the rule id: `app`, `e0`, `e0n9`, `s4` or `x3`.

        A plan analyzed alone may have no execution id, so the plan itself is `plan` and its node 9 is `n9`.
        """
        if level == 'application':
            return 'app'
        if level == 'sql_execution':
            return 'plan' if self.sql_execution_id is None else f'e{self.sql_execution_id}'
        if level == 'plan_node':
            node = f'n{_required(self.plan_node_id, "plan_node_id", level)}'
            return node if self.sql_execution_id is None else f'e{self.sql_execution_id}{node}'
        if level == 'stage':
            return f's{_required(self.stage_id, "stage_id", level)}'
        return f'x{_required(self.executor_id, "executor_id", level)}'

    def describe(self) -> str:
        """The scope as a person reads it, widest part first: `SQL 0 › SortMergeJoin [k#2L], [k#10L], Inner › stage 4`.

        A node label past 100 characters is cut to 100, ending in `...`, as Spark cuts a long plan field.
        """
        parts = []
        if self.sql_execution_id is not None:
            parts.append(f'SQL {self.sql_execution_id}')
        if self.plan_node_label is not None:
            label = self.plan_node_label
            parts.append(label if len(label) <= _LABEL_CHARACTERS else f'{label[: _LABEL_CHARACTERS - 3]}...')
        elif self.plan_node_id is not None:
            parts.append(f'node {self.plan_node_id}')
        if self.stage_id is not None:
            parts.append(f'stage {self.stage_id}' if not self.stage_attempt else f'stage {self.stage_id} attempt {self.stage_attempt}')
        if self.executor_id is not None:
            parts.append(f'executor {self.executor_id}')
        return ' › '.join(parts) or 'the application'

    def to_json(self) -> dict[str, Any]:
        return {**dc.asdict(self), 'worst_task_ids': list(self.worst_task_ids)}


@dc.dataclass(frozen=True, slots=True)
class AppliedThreshold:
    """The value a finding was judged against, and the layer that set it.

    `source` is `default` where nothing overrode the rule's own value, and otherwise the layer
    that did: `file`, `sdk` or `flag`. `default` and `default_source` keep the rule's value and
    where it came from, so an override is read beside what it replaced.
    """

    value: float
    unit: ThresholdUnit
    source: ThresholdLayer
    default: float
    default_source: str

    def to_json(self) -> dict[str, Any]:
        return {
            'value': _number(self.value),
            'unit': self.unit,
            'source': self.source,
            'default': _number(self.default),
            'default_source': self.default_source,
        }


@dc.dataclass(frozen=True)
class Finding:
    """One rule's finding about one scope of a run.

    `finding_id` is the rule id and the scope's key, so analyzing the same log again gives the
    same ids. `base_severity` is the rule's grade, and `severity` that grade moved one step by the
    scope's share of the application's wall time, which `evidence` carries. `impact_seconds` is
    the wall time a fix could recover, and None where no one can say.
    """

    finding_id: str
    rule_id: str
    rule_version: int
    severity: Severity
    base_severity: Severity
    title: str
    scope: FindingScope
    evidence: Mapping[str, Evidence]
    thresholds: Mapping[str, AppliedThreshold]
    impact_seconds: float | None
    confidence: Confidence
    confidence_reason: str | None
    remediation: tuple[str, ...]
    docs: tuple[str, ...]
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'finding_id': self.finding_id,
            'rule_id': self.rule_id,
            'rule_version': self.rule_version,
            'severity': self.severity,
            'base_severity': self.base_severity,
            'title': self.title,
            'scope': self.scope.to_json(),
            'evidence': evidence_json(self.evidence),
            'thresholds': {name: applied.to_json() for name, applied in self.thresholds.items()},
            'impact_seconds': None if self.impact_seconds is None else _number(self.impact_seconds),
            'confidence': self.confidence,
            'confidence_reason': self.confidence_reason,
            'remediation': list(self.remediation),
            'docs': list(self.docs),
        }


def evidence_json(evidence: Mapping[str, Evidence]) -> dict[str, Any]:
    return {key: [_number(item) for item in value] if isinstance(value, tuple) else _number(value) for key, value in evidence.items()}


def findings_column_descriptions() -> dict[str, str]:
    """What each `findings.parquet` column holds, read from `schemas/findings.json`: a finding's fields, and its scope's ids."""
    schema = json.loads(importlib.resources.files('sparkxray').joinpath('schemas', 'findings.json').read_text())
    fields = {**schema['$defs']['finding']['properties'], **schema['$defs']['scope']['properties']}
    return {name: field['description'] for name, field in fields.items() if 'description' in field}


def write_findings_parquet(findings: Sequence[Finding], analysis: Mapping[str, Any], path: Path) -> None:
    """Write findings to `path` as one row each, replacing what was there only once the file is whole.

    The scope's ids are columns of their own, so a query joins findings to the tables on
    `stage_id` or `sql_execution_id`. `evidence` and `thresholds` are JSON text. `analysis` is the
    analysis document without its findings, kept as JSON in the file's key-value metadata under
    `ANALYSIS_METADATA_KEY`. So the file says which rules ran, under which thresholds, even when
    it holds no row.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    kinds = {'BIGINT': pa.int64(), 'DOUBLE': pa.float64(), 'VARCHAR': pa.string()}
    rows = [_parquet_row(finding.to_json()) for finding in findings]
    metadata = {ANALYSIS_METADATA_KEY: json.dumps(analysis, sort_keys=True)}
    schema = pa.schema(
        [(name, pa.list_(kinds[kind[:-2]]) if kind.endswith('[]') else kinds[kind]) for name, kind in FINDINGS_PARQUET_COLUMNS],
        metadata=metadata,
    )
    table = pa.table({name: pa.array([row[name] for row in rows], type=schema.field(name).type) for name in schema.names}, schema=schema)
    partial = path.with_name(f'{path.name}{PARTIAL_MARK}{os.getpid()}-{secrets.token_hex(4)}')
    try:
        pq.write_table(table, partial)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _parquet_row(finding: Mapping[str, Any]) -> dict[str, Any]:
    flat = {**finding, **finding['scope']}
    flat['evidence'] = json.dumps(finding['evidence'], sort_keys=True)
    flat['thresholds'] = json.dumps(finding['thresholds'], sort_keys=True)
    return flat


def _number(value: Any) -> Any:
    """A float with no fraction as an int, so `5.0` prints as `5`."""
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    return value


def _required(value: int | str | None, name: str, level: ScopeLevel) -> int | str:
    if value is None:
        raise InvalidRuleError(f'a {level} finding needs {name} in its scope', field='rule')
    return value
