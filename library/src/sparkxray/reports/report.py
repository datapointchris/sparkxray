"""One document per analyzed log: the run, its usage, what the rules found, and the stages, SQL and settings behind it.

`build_report` gathers the `Report`, and `render_json` and `render_markdown` only format it. A
report holds no time it was made, and names a local log by its file name. So the same log file,
thresholds and sparkxray version give the same bytes on any machine. `schemas/report.json` in the
package is the JSON's schema. A removed field or a changed
meaning bumps `SCHEMA_VERSION`, and an added field does not.
"""

from __future__ import annotations

import dataclasses as dc
import json
import os
import posixpath
import re
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Literal

from sparkxray.cache_directory import PARTIAL_MARK
from sparkxray.cache_directory import installed_version
from sparkxray.errors import NotInRunError
from sparkxray.ingest.event_log import EventLog
from sparkxray.plan.aqe_changes import AqeChanges
from sparkxray.rules.analysis import Analysis
from sparkxray.rules.analysis import Gate
from sparkxray.rules.analysis import SkippedRule
from sparkxray.rules.findings import Finding
from sparkxray.rules.rule import Severity
from sparkxray.rules.spark_settings import READ_BY_RULES
from sparkxray.summaries.annotated_plans import AnnotatedPlan
from sparkxray.summaries.annotated_plans import annotated_plan
from sparkxray.summaries.application import application_detail
from sparkxray.summaries.executors import executor_summaries
from sparkxray.summaries.sql_executions import sql_detail
from sparkxray.summaries.sql_executions import sql_summaries
from sparkxray.summaries.stages import stage_summaries
from sparkxray.tables.registry import CORE_TIME_CTES
from sparkxray.tables.registry import GLUE_DPU_CORES
from sparkxray.tables.rows import rows_of

SCHEMA_VERSION = 2
ReportFormat = Literal['markdown', 'json']
LONGEST_STAGE_COUNT = 10

_STAGE_KEYS = (
    'stage_id',
    'name',
    'status',
    'attempt_count',
    'task_count',
    'failed_task_count',
    'wall_ms',
    'wall_share',
    'median_task_duration_ms',
    'max_task_duration_ms',
    'shuffle_read_bytes',
    'memory_spilled_bytes',
    'disk_spilled_bytes',
    'gc_share',
)
_EXECUTOR_KEYS = (
    'executor_id',
    'host',
    'is_driver',
    'core_count',
    'executor_memory_bytes',
    'add_epoch_ms',
    'remove_epoch_ms',
    'lifetime_ms',
    'task_count',
    'failed_task_count',
    'idle_share',
    'peak_jvm_heap_bytes',
    'peak_memory_source',
    'remove_reason',
)
# A setting a finding's fix names, such as `spark.sql.autoBroadcastJoinThreshold`, and not a call such as `spark.conf.set(`.
_SETTING_KEY = re.compile(r'\bspark(?:\.[A-Za-z][A-Za-z0-9]*)+(?![\w(])')
_CONFIG_FAMILIES = ('spark.sql.adaptive.', 'spark.dynamicAllocation.')
_CONFIG_SQL = f"""
SELECT key, value FROM environment
WHERE section = 'spark' AND (list_contains($keys, key) OR {' OR '.join(f"starts_with(key, '{family}')" for family in _CONFIG_FAMILIES)})
"""
_MODIFIED_CONFIGS_SQL = 'SELECT execution_id, modified_configs FROM sql_executions'
_USAGE_SQL = f"""
WITH {CORE_TIME_CTES}
SELECT round(core_totals.core_time_ms / 3600000, 4) AS core_hours,
  round(core_totals.task_core_time_ms / 3600000, 4) AS task_core_hours,
  round((core_totals.core_time_ms - core_totals.task_core_time_ms) / 3600000, 4) AS idle_core_hours,
  CASE WHEN core_totals.core_time_ms > 0 THEN round(1 - core_totals.task_core_time_ms / core_totals.core_time_ms, 4) END
    AS idle_core_share,
  CASE WHEN application.platform = 'glue' THEN round(core_totals.core_time_ms / 3600000 / {GLUE_DPU_CORES}, 4) END AS dpu_hours,
  CASE WHEN application.platform = 'glue'
    THEN round((core_totals.core_time_ms - core_totals.task_core_time_ms) / 3600000 / {GLUE_DPU_CORES}, 4) END AS idle_dpu_hours
FROM core_totals, application
"""


@dc.dataclass(frozen=True, slots=True)
class AppSummary:
    """The application: who ran it, on which Spark and platform, and how long it ran."""

    app_id: str
    app_name: str | None
    spark_version: str | None
    platform: str
    start_epoch_ms: int | None
    end_epoch_ms: int | None
    wall_time_ms: int | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class LogFile:
    """One file of the log, in read order. A local file's `uri` is relative to the directory holding the log."""

    uri: str
    size_bytes: int
    codec: str

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class LogSummary:
    """The log the report read, and what it lacked. A local `source` is the log's name, and an `s3://` one its whole URI."""

    source: str
    layout: str
    files: tuple[LogFile, ...]
    size_bytes: int
    compacted: bool
    in_progress: bool
    truncated_tail: bool
    lacks: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {**dc.asdict(self), 'files': [file.to_json() for file in self.files], 'lacks': list(self.lacks)}


@dc.dataclass(frozen=True, slots=True)
class Usage:
    """The core time the run held and how much ran no task, as the `executor.idle-cores` rule counts it. DPU-hours only on Glue."""

    core_hours: float | None
    task_core_hours: float | None
    idle_core_hours: float | None
    idle_core_share: float | None
    dpu_hours: float | None
    idle_dpu_hours: float | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class SqlSummary:
    """One SQL execution: its `sql list` row, the settings it set, the findings about it, what AQE changed, and its last plan annotated.

    `modified_configs` holds each setting the execution's session changed from the run's launch
    value, which the rules judge it by. `plan` is None where the log holds no plan for it.
    """

    execution: Mapping[str, Any]
    modified_configs: Mapping[str, str]
    finding_ids: tuple[str, ...]
    aqe_changes: AqeChanges | None
    plan: AnnotatedPlan | None

    def to_json(self) -> dict[str, Any]:
        plan = None
        if self.plan is not None:
            plan = {key: value for key, value in self.plan.to_json().items() if key not in ('execution', 'findings')}
        return {
            **self.execution,
            'modified_configs': dict(self.modified_configs),
            'finding_ids': list(self.finding_ids),
            'aqe_changes': None if self.aqe_changes is None else self.aqe_changes.to_json(),
            'plan': plan,
        }


@dc.dataclass(frozen=True, slots=True)
class ConfigEntry:
    """A setting the rules read or a finding's fix names, as the run launched with it. `value` is None where the run left it unset."""

    key: str
    value: str | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class Report:
    """One analyzed log, everything a person or a program reading the run needs, in the order a reader acts on it.

    `gate` is the analysis judged at the report's `fail_on`, as `findings list --fail-on` judges it.
    `findings` and `skipped_rules` are the analysis's, in its order. `stages` holds the
    `LONGEST_STAGE_COUNT` longest by wall time. `sql` holds every execution, `executors` every
    executor and the driver.
    """

    generated_by: str
    app: AppSummary
    log: LogSummary
    usage: Usage
    gate: Gate
    findings: tuple[Finding, ...]
    skipped_rules: tuple[SkippedRule, ...]
    stages: tuple[Mapping[str, Any], ...]
    sql: tuple[SqlSummary, ...]
    executors: tuple[Mapping[str, Any], ...]
    config: tuple[ConfigEntry, ...]
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'generated_by': self.generated_by,
            'app': self.app.to_json(),
            'log': self.log.to_json(),
            'usage': self.usage.to_json(),
            'gate': self.gate.to_json(),
            'findings': [finding.to_json() for finding in self.findings],
            'skipped_rules': [skipped.to_json() for skipped in self.skipped_rules],
            'stages': [dict(stage) for stage in self.stages],
            'sql': [execution.to_json() for execution in self.sql],
            'executors': [dict(executor) for executor in self.executors],
            'config': [entry.to_json() for entry in self.config],
        }


def build_report(log: EventLog, analysis: Analysis | None = None, fail_on: Severity = 'high') -> Report:
    """The report of one log, judged at `fail_on`. Without an `analysis`, it runs `log.analyze()` at the default thresholds.

    A report takes its thresholds from the analysis only, so every finding in it states the ones it was judged by.
    """
    found = log.analyze() if analysis is None else analysis
    tables = log.tables
    application = application_detail(tables).application
    manifest = tables.manifest
    ingest = manifest.ingest
    usage = rows_of(tables.query(_USAGE_SQL)).to_json()
    modified = {row['execution_id']: row['modified_configs'] or {} for row in rows_of(tables.query(_MODIFIED_CONFIGS_SQL)).to_json()}
    executions = []
    for execution in sql_summaries(tables).to_json():
        execution_id = execution['execution_id']
        try:
            plan: AnnotatedPlan | None = annotated_plan(tables, execution_id).with_findings(found.findings)
        except NotInRunError:
            plan = None
        about = (
            tuple(finding.finding_id for finding in plan.findings)
            if plan is not None
            else tuple(finding.finding_id for finding in found.findings if finding.scope.sql_execution_id == execution_id)
        )
        changes = sql_detail(tables, execution_id).aqe_changes if execution['plan_version_count'] > 1 else None
        executions.append(SqlSummary(execution, dict(sorted(modified.get(execution_id, {}).items())), about, changes, plan))
    named = {key for finding in found.findings for step in finding.remediation for key in _SETTING_KEY.findall(step)}
    keys = set(READ_BY_RULES) | named
    recorded = dict(tables.query(_CONFIG_SQL, {'keys': sorted(keys)}).fetchall())
    source = ingest.source
    return Report(
        generated_by=f'sparkxray {installed_version()}',
        app=AppSummary(
            app_id=ingest.app_id,
            app_name=application.get('app_name'),
            spark_version=application.get('spark_version'),
            platform=application.get('platform') or ingest.platform,
            start_epoch_ms=application.get('start_epoch_ms'),
            end_epoch_ms=application.get('end_epoch_ms'),
            wall_time_ms=application.get('duration_ms'),
        ),
        log=LogSummary(
            source=_portable(source, source),
            layout=ingest.layout,
            files=tuple(LogFile(_portable(file.uri, source), file.size_bytes, file.codec) for file in ingest.files),
            size_bytes=sum(file.size_bytes for file in ingest.files),
            compacted=ingest.compacted,
            in_progress=ingest.in_progress,
            truncated_tail=ingest.truncated_tail,
            lacks=tuple(manifest.lacks),
        ),
        usage=Usage(**usage[0]) if usage else Usage(None, None, None, None, None, None),
        gate=found.gate(fail_on),
        findings=found.findings,
        skipped_rules=found.skipped,
        stages=tuple(
            {key: stage[key] for key in _STAGE_KEYS}
            for stage in stage_summaries(tables, sort='wall_ms', limit=LONGEST_STAGE_COUNT).to_json()
        ),
        sql=tuple(executions),
        executors=tuple({key: executor[key] for key in _EXECUTOR_KEYS} for executor in executor_summaries(tables).to_json()),
        config=tuple(ConfigEntry(key, recorded.get(key)) for key in sorted(keys | set(recorded))),
    )


def _portable(uri: str, source: str) -> str:
    """A local file as a path from the directory holding the log, so no path on the writing machine shows. Other URIs whole."""
    if not source.startswith('file:'):
        return uri
    holding = posixpath.dirname(urllib.parse.unquote(urllib.parse.urlparse(source).path).rstrip('/'))
    return posixpath.relpath(urllib.parse.unquote(urllib.parse.urlparse(uri).path), holding)


def render_json(report: Report) -> str:
    """The report as the JSON `schemas/report.json` describes, with raw numbers under names ending in their unit."""
    return json.dumps(report.to_json(), indent=2, ensure_ascii=False) + '\n'


def write_report(text: str, path: str | os.PathLike[str]) -> Path:
    """Write a rendered report to `path`, creating its directory, and return the path. The old file goes once the new one is whole."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f'{target.name}{PARTIAL_MARK}{os.getpid()}')
    partial.write_text(text, encoding='utf-8')
    partial.replace(target)
    return target
