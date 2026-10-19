"""Running rules: over a log's tables and plans with `analyze`, or over one plan with `analyze_plan`."""

from __future__ import annotations

import dataclasses as dc
import decimal
import math
import os
import string
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from sparkxray.errors import InvalidRuleError
from sparkxray.errors import NotInRunError
from sparkxray.errors import UnknownRuleError
from sparkxray.errors import UnknownSeverityError
from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import Plan
from sparkxray.rules.catalog import catalog
from sparkxray.rules.findings import CONFIDENCES
from sparkxray.rules.findings import SCHEMA_VERSION
from sparkxray.rules.findings import UNIT_SUFFIXES
from sparkxray.rules.findings import AppliedThreshold
from sparkxray.rules.findings import Evidence
from sparkxray.rules.findings import Finding
from sparkxray.rules.findings import FindingScope
from sparkxray.rules.findings import evidence_json
from sparkxray.rules.findings import write_findings_parquet
from sparkxray.rules.rule import SEVERITIES
from sparkxray.rules.rule import SIGNALS
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Severity
from sparkxray.rules.rule import Signal
from sparkxray.rules.thresholds import resolve_thresholds
from sparkxray.spark_ui_format import bytes_to_string
from sparkxray.spark_ui_format import format_duration

if TYPE_CHECKING:
    from sparkxray.ingest.event_log import EventLog
    from sparkxray.tables.relations import Tables

Thresholds = Mapping[str, Mapping[str, object]]

_SCOPE_FIELDS = frozenset(field.name for field in dc.fields(FindingScope)) - {'app_id'}
# A scope under this share of the application's wall time drops a grade, and one over the other rises one.
_MINOR_SHARE = 0.05
_MAJOR_SHARE = 0.5

_COUNTED_SIGNALS = ('plan_metrics', 'tasks', 'executors', 'environment', 'timeline')
_SIGNALS_SQL = """
SELECT
  (SELECT count(*) FROM plan_node_metrics WHERE value_count > 0) > 0 AS plan_metrics,
  (SELECT count(*) FROM tasks_effective) > 0 AS tasks,
  (SELECT count(*) FROM executors) > 0 AS executors,
  (SELECT count(*) FROM environment) > 0 AS environment,
  (SELECT count(*) FROM stages WHERE submit_epoch_ms IS NOT NULL) > 0 AS timeline
"""

# Each node's metric totals in its execution's last plan. A name a node lists twice keeps its first accumulator.
_NODE_METRICS_SQL = """
SELECT metrics.execution_id, metrics.node_id, metrics.metric_name, arg_min(metrics.total, metrics.accumulator_id) AS total
FROM plan_node_metrics AS metrics JOIN sql_executions USING (execution_id)
WHERE metrics.plan_version = sql_executions.plan_version_count - 1 AND metrics.total IS NOT NULL
GROUP BY ALL
"""


@dc.dataclass(frozen=True, slots=True)
class RuleRun:
    """A rule the analysis ran, and the thresholds it applied, whether or not it found anything."""

    rule_id: str
    rule_version: int
    thresholds: Mapping[str, AppliedThreshold]

    def to_json(self) -> dict[str, Any]:
        thresholds = {name: applied.to_json() for name, applied in self.thresholds.items()}
        return {'rule_id': self.rule_id, 'rule_version': self.rule_version, 'thresholds': thresholds}


@dc.dataclass(frozen=True, slots=True)
class SkippedRule:
    """A rule the analysis did not run, and the signals its input lacked."""

    rule_id: str
    rule_version: int
    missing_signals: tuple[Signal, ...]

    def to_json(self) -> dict[str, Any]:
        return {'rule_id': self.rule_id, 'rule_version': self.rule_version, 'missing_signals': list(self.missing_signals)}


@dc.dataclass(frozen=True)
class Analysis:
    """What one analysis found.

    `findings` runs from the most wall time a fix could recover to the least, with findings of
    unknown impact last, then by severity. `signals` names what the input held. `rules_run`
    names every rule that ran with the thresholds it applied, and each rule needing a signal the
    input lacked is in `skipped`. So a partial analysis reads as partial, and a rule silenced by
    an override reads as silenced rather than clean.
    """

    app_id: str | None
    signals: frozenset[Signal]
    rules_run: tuple[RuleRun, ...]
    findings: tuple[Finding, ...]
    skipped: tuple[SkippedRule, ...]
    schema_version: int = SCHEMA_VERSION

    def to_json(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'app_id': self.app_id,
            'signals': [signal for signal in SIGNALS if signal in self.signals],
            'rules_run': [run.to_json() for run in self.rules_run],
            'findings': [finding.to_json() for finding in self.findings],
            'skipped': [skipped.to_json() for skipped in self.skipped],
        }

    def gate(self, fail_on: str) -> Gate:
        """Whether no finding is at or above the severity `fail_on`, and which are. Another word raises `UnknownSeverityError`."""
        bar = _rung(fail_on, field='fail_on')
        blocking = tuple(finding.finding_id for finding in self.findings if SEVERITIES.index(finding.severity) >= bar)
        return Gate(SEVERITIES[bar], not blocking, blocking)

    def narrowed(self, *, min_severity: str | None = None, limit: int | None = None) -> Analysis:
        """This analysis with only its findings at or above `min_severity`, and only the first `limit` of those, in its order."""
        findings = self.findings
        if min_severity is not None:
            bar = _rung(min_severity, field='min_severity')
            findings = tuple(finding for finding in findings if SEVERITIES.index(finding.severity) >= bar)
        if limit is not None:
            if limit < 0:
                raise ValueError(f'limit is {limit}, and a list cannot keep fewer than 0 findings')
            findings = findings[:limit]
        return dc.replace(self, findings=findings)

    def finding(self, finding_id: str) -> Finding:
        """The finding with this id. An id the analysis did not find raises `NotInRunError`, listing those it did."""
        for finding in self.findings:
            if finding.finding_id == finding_id:
                return finding
        raise NotInRunError(
            f'this analysis found nothing with the id {finding_id!r}',
            field='finding_id',
            valid=[finding.finding_id for finding in self.findings],
        )


@dc.dataclass(frozen=True, slots=True)
class Gate:
    """Whether an analysis passes a severity bar: it does when no finding is at or above `fail_on`, and `blocking` names those that are."""

    fail_on: Severity
    passed: bool
    blocking: tuple[str, ...]

    def to_json(self) -> dict[str, Any]:
        return {'fail_on': self.fail_on, 'passed': self.passed, 'blocking': list(self.blocking)}


def _rung(severity: str, *, field: str) -> int:
    """Where a severity sits on the ladder, from 0 for `info`."""
    for index, rung in enumerate(SEVERITIES):
        if rung == severity:
            return index
    raise UnknownSeverityError(f'{severity!r} is not a severity', field=field, valid=SEVERITIES)


@dc.dataclass(frozen=True)
class _Input:
    """What the rules of one analysis read, and what the engine weighs their findings by."""

    app_id: str | None
    signals: frozenset[Signal]
    plans: Mapping[int | None, Plan]
    numbered: Mapping[int | None, tuple[NumberedNode, ...]]
    node_metrics: Mapping[int | None, Mapping[int, Mapping[str, float]]]
    tables: Tables | None
    shares: Mapping[tuple[str, int], float]


def analyze(
    log: EventLog,
    *,
    rules: Iterable[str] | None = None,
    thresholds: Thresholds | None = None,
    threshold_file: str | os.PathLike[str] | None = None,
    threshold_flags: Iterable[str] = (),
) -> Analysis:
    """Run the rules over one log, and write its findings beside its tables as `findings.parquet`.

    `rules` names the rule ids to run, and every catalog rule runs by default. `thresholds` sets
    values by rule id and name, as in `{'stage.shuffle-skew': {'ratio': 8}}`. `threshold_file`
    is a TOML file of the same, and `threshold_flags` holds `<rule_id>.<name>=<value>` strings.
    A flag beats the SDK, the SDK beats the file, and the file beats the rule's default. Each
    applied value records the layer that set it.
    """
    selected, every_rule = _selected(rules)
    applied = resolve_thresholds(selected, every_rule, sdk=thresholds, file=threshold_file, flags=threshold_flags)
    tables = log.tables
    plans: dict[int | None, Plan] = {execution_id: plan for execution_id, plan in tables.last_plans().items()}
    held = dict(zip(_COUNTED_SIGNALS, tables.query(_SIGNALS_SQL).fetchall()[0], strict=True))
    signals = frozenset(signal for signal in SIGNALS if held.get(signal) or (signal == 'plan' and plans))
    node_metrics: dict[int | None, dict[int, dict[str, float]]] = {}
    for execution_id, node_id, name, total in tables.query(_NODE_METRICS_SQL).fetchall():
        node_metrics.setdefault(execution_id, {}).setdefault(node_id, {})[name] = total
    application = tables.query('SELECT app_id FROM application').fetchone()
    from sparkxray.tables.registry import WALL_SHARES_SQL

    shares = {(level, scope_id): share for level, scope_id, _, share in tables.query(WALL_SHARES_SQL).fetchall() if share is not None}
    analysis = _run(
        selected,
        applied,
        _Input(
            app_id=None if application is None else application[0],
            signals=signals,
            plans=plans,
            numbered={execution_id: plan.numbered() for execution_id, plan in plans.items()},
            node_metrics=node_metrics,
            tables=tables,
            shares=shares,
        ),
    )
    from sparkxray.tables.relations import FINDINGS_FILE

    document = analysis.to_json()
    del document['findings']
    write_findings_parquet(analysis.findings, document, tables.path / FINDINGS_FILE)
    tables.register_findings()
    return analysis


def analyze_plan(
    plan: Plan,
    *,
    node_metrics: Mapping[int, Mapping[str, float]] | None = None,
    rules: Iterable[str] | None = None,
    thresholds: Thresholds | None = None,
    threshold_file: str | os.PathLike[str] | None = None,
    threshold_flags: Iterable[str] = (),
) -> Analysis:
    """Run the rules over one plan, as one read from `explain()` text, with no log.

    `node_metrics` gives each node's metric totals by name, keyed by its number in
    `plan.numbered()`. Without them only rules reading the plan alone run, and the rest are
    skipped. A plan read from `explain(mode='cost')` text holds the optimizer's estimates too,
    which no log does. The thresholds are set as `analyze` sets them. Nothing is written.
    """
    selected, every_rule = _selected(rules)
    applied = resolve_thresholds(selected, every_rule, sdk=thresholds, file=threshold_file, flags=threshold_flags)
    held: set[Signal] = {'plan'}
    if node_metrics:
        held.add('plan_metrics')
    if plan.estimates:
        held.add('plan_estimates')
    signals = frozenset(held)
    return _run(
        selected,
        applied,
        _Input(
            app_id=None,
            signals=signals,
            plans={plan.execution_id: plan},
            numbered={plan.execution_id: plan.numbered()},
            node_metrics={plan.execution_id: node_metrics or {}},
            tables=None,
            shares={},
        ),
    )


def _selected(rules: Iterable[str] | None) -> tuple[list[Rule], list[Rule]]:
    every_rule = catalog()
    if rules is None:
        return every_rule, every_rule
    known = {rule.rule_id: rule for rule in every_rule}
    wanted = {rules} if isinstance(rules, str) else set(rules)
    for rule_id in sorted(wanted):
        if rule_id not in known:
            raise UnknownRuleError(f'no rule is called {rule_id!r}', field='rules', valid=sorted(known))
    return [known[rule_id] for rule_id in sorted(wanted)], every_rule


def _run(rules: Sequence[Rule], applied: Mapping[str, Mapping[str, AppliedThreshold]], given: _Input) -> Analysis:
    findings: list[Finding] = []
    ran = []
    skipped = []
    for rule in rules:
        missing = tuple(signal for signal in SIGNALS if signal in rule.signals and signal not in given.signals)
        if missing:
            skipped.append(SkippedRule(rule.rule_id, rule.rule_version, missing))
            continue
        ran.append(RuleRun(rule.rule_id, rule.rule_version, applied[rule.rule_id]))
        values = {name: threshold.value for name, threshold in applied[rule.rule_id].items()}
        rows: Iterable[Mapping[str, Any]]
        if rule.check is not None:
            rows = rule.check(RuleContext(given.plans, given.numbered, given.node_metrics, values, given.tables))
        elif rule.sql is not None and given.tables is not None:
            relation = given.tables.query(rule.sql, values or None)
            rows = [dict(zip(relation.columns, row, strict=True)) for row in relation.fetchall()]
        else:
            raise RuntimeError(f'rule {rule.rule_id} reads the tables, and this analysis has none')
        rule_findings = [_finding(rule, row, applied[rule.rule_id], given) for row in rows]
        ids = [finding.finding_id for finding in rule_findings]
        if len(set(ids)) != len(ids):
            raise InvalidRuleError(f'rule {rule.rule_id} gave two findings for one scope: {sorted(ids)}', field='rule')
        findings.extend(rule_findings)
    findings.sort(
        key=lambda finding: (
            finding.impact_seconds is None,
            -(finding.impact_seconds or 0),
            -SEVERITIES.index(finding.severity),
            finding.finding_id,
        )
    )
    return Analysis(given.app_id, given.signals, tuple(ran), tuple(findings), tuple(skipped))


def _finding(rule: Rule, row: Mapping[str, Any], applied: Mapping[str, AppliedThreshold], given: _Input) -> Finding:
    values = dict(row)
    base_severity = values.pop('severity', None)
    if base_severity not in SEVERITIES:
        raise InvalidRuleError(
            f'rule {rule.rule_id} gave a finding with severity {base_severity!r}, not one of {", ".join(SEVERITIES)}', field='rule'
        )
    confidence = values.pop('confidence', 'high')
    if confidence not in CONFIDENCES:
        raise InvalidRuleError(
            f'rule {rule.rule_id} gave a finding with confidence {confidence!r}, not one of {", ".join(CONFIDENCES)}', field='rule'
        )
    impact = _plain(values.pop('impact_seconds', None))
    reason = values.pop('confidence_reason', None)
    scope = _scope({key: values.pop(key) for key in list(values) if key in _SCOPE_FIELDS}, given)
    evidence = {key: _evidence(rule, key, value) for key, value in values.items()}
    share_level, share_id = {
        'stage': ('stage', scope.stage_id),
        'sql_execution': ('sql_execution', scope.sql_execution_id),
        'plan_node': ('sql_execution', scope.sql_execution_id),
    }.get(rule.scope, ('', None))
    share = None if share_id is None else given.shares.get((share_level, share_id))
    if share is not None:
        evidence[f'{share_level}_wall_share'] = share
    fields = {**scope.to_json(), **evidence_json(evidence)}
    try:
        title = _TITLE.format(rule.title, **fields)
    except (KeyError, IndexError, ValueError, TypeError) as error:
        raise InvalidRuleError(
            f'rule {rule.rule_id} gave a finding its title {rule.title!r} cannot be written from: {error!r}', field='rule'
        ) from None
    return Finding(
        finding_id=f'{rule.rule_id}:{scope.key(rule.scope)}',
        rule_id=rule.rule_id,
        rule_version=rule.rule_version,
        severity=_weighed(base_severity, share),
        base_severity=base_severity,
        title=title,
        scope=scope,
        evidence=evidence,
        thresholds=dict(applied),
        impact_seconds=None if impact is None else float(impact),
        confidence=confidence,
        confidence_reason=reason,
        remediation=rule.remediation,
        docs=rule.docs,
    )


def _scope(values: dict[str, Any], given: _Input) -> FindingScope:
    worst = tuple(int(task_id) for task_id in values.pop('worst_task_ids', None) or ())
    if values.get('executor_id') is not None:
        values['executor_id'] = str(values['executor_id'])
    execution_id, node_id = values.get('sql_execution_id'), values.get('plan_node_id')
    nodes = given.numbered.get(execution_id, ())
    if values.get('plan_node_label') is None and node_id is not None and 0 <= node_id < len(nodes):
        values['plan_node_label'] = nodes[node_id].node.label
    return FindingScope(app_id=given.app_id, worst_task_ids=worst, **values)


def _evidence(rule: Rule, key: str, value: Any) -> Evidence:
    value = _plain(value)
    if isinstance(value, list | tuple):
        items = tuple(_plain(item) for item in value)
        numeric = any(_numeric(item) for item in items)
    else:
        items = None
        numeric = _numeric(value)
    if numeric and not key.endswith(UNIT_SUFFIXES):
        raise InvalidRuleError(
            f'rule {rule.rule_id} gave numeric evidence {key!r}, whose name must end in its unit: {", ".join(UNIT_SUFFIXES)}', field='rule'
        )
    for item in items if items is not None else (value,):
        if item is not None and not isinstance(item, bool | int | float | str):
            raise InvalidRuleError(
                f'rule {rule.rule_id} gave evidence {key!r} as {type(item).__name__}, not a number, string, boolean or null', field='rule'
            )
        if isinstance(item, float) and not math.isfinite(item):
            raise InvalidRuleError(f'rule {rule.rule_id} gave evidence {key!r} as {item}, which JSON cannot hold', field='rule')
    return items if items is not None else value


class _TitleFormatter(string.Formatter):
    """`str.format`, with `bytes` and `duration` specs that print a value as the Spark UI does."""

    def format_field(self, value: Any, format_spec: str) -> str:
        if format_spec == 'bytes':
            return bytes_to_string(round(value))
        if format_spec == 'duration':
            return format_duration(round(value))
        return str(super().format_field(value, format_spec))


_TITLE = _TitleFormatter()


def _plain(value: Any) -> Any:
    return float(value) if isinstance(value, decimal.Decimal) else value


def _numeric(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _weighed(severity: Severity, share: float | None) -> Severity:
    """The severity moved one step by the scope's share of wall time. `info` never moves, and nothing drops below `low`."""
    if share is None or severity == 'info':
        return severity
    index = SEVERITIES.index(severity)
    if share < _MINOR_SHARE:
        index = max(index - 1, SEVERITIES.index('low'))
    elif share > _MAJOR_SHARE:
        index = min(index + 1, len(SEVERITIES) - 1)
    return SEVERITIES[index]
