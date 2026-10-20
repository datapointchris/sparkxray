"""What a rule is: the signals it reads, the thresholds it compares against, and the SQL or Python that finds what it reports."""

from __future__ import annotations

import dataclasses as dc
import re
import string
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal

from sparkxray.errors import InvalidRuleError
from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import Plan

if TYPE_CHECKING:
    from sparkxray.rules.findings import Finding
    from sparkxray.tables.relations import Tables

Signal = Literal['plan', 'plan_metrics', 'plan_stages', 'plan_estimates', 'tasks', 'executors', 'environment', 'timeline', 'whole_run']
SIGNALS: tuple[Signal, ...] = (
    'plan',
    'plan_metrics',
    'plan_stages',
    'plan_estimates',
    'tasks',
    'executors',
    'environment',
    'timeline',
    'whole_run',
)
Severity = Literal['info', 'low', 'medium', 'high', 'critical']
SEVERITIES: tuple[Severity, ...] = ('info', 'low', 'medium', 'high', 'critical')
# What one finding of a rule is about, which decides the ids its finding_id is built from.
ScopeLevel = Literal['application', 'sql_execution', 'plan_node', 'stage', 'executor']
SCOPE_LEVELS: tuple[ScopeLevel, ...] = ('application', 'sql_execution', 'plan_node', 'stage', 'executor')
ThresholdUnit = Literal['x', 'bytes', 'ms', 'count', 'rows', 'fraction']
THRESHOLD_UNITS: tuple[ThresholdUnit, ...] = ('x', 'bytes', 'ms', 'count', 'rows', 'fraction')

# Only an event log's tables hold these. A SQL rule reads the tables, so it declares at least one.
_TABLE_SIGNALS = frozenset({'plan_stages', 'tasks', 'executors', 'environment', 'timeline', 'whole_run'})
_RULE_ID = re.compile(r'[a-z]+\.[a-z0-9]+(?:-[a-z0-9]+)*')
_PARAMETER = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)')
_BYTE_SCALES = ((1 << 40, 'TiB'), (1 << 30, 'GiB'), (1 << 20, 'MiB'), (1 << 10, 'KiB'))
_DURATION_SCALES = ((3_600_000, 'h'), (60_000, 'min'), (1000, 's'))


@dc.dataclass(frozen=True, slots=True)
class Threshold:
    """One value a rule compares against: its default, its unit, and where the default came from."""

    name: str
    default: float
    unit: ThresholdUnit
    source: str

    def to_json(self) -> dict[str, Any]:
        default = int(self.default) if float(self.default).is_integer() else self.default
        return {'name': self.name, 'default': default, 'unit': self.unit, 'source': self.source}

    def in_prose(self, value: float | None = None) -> str:
        """`value`, or the default, as a sentence states it: `1 GiB`, `5 min`, `100,000,000`, `8%` or `5x`."""
        return threshold_in_prose(self.unit, self.default if value is None else value)


def threshold_in_prose(unit: ThresholdUnit, value: float) -> str:
    """A threshold's value in its unit, whole where the unit allows: `256 MiB` rather than the Spark UI's `256.0 MiB`."""
    if unit == 'bytes':
        scale, name = next(((scale, name) for scale, name in _BYTE_SCALES if value >= scale), (1, 'B'))
        return f'{_trimmed(value / scale)} {name}'
    if unit == 'ms':
        scale, name = next(((scale, name) for scale, name in _DURATION_SCALES if value >= scale and value % scale == 0), (1, 'ms'))
        return f'{_trimmed(value / scale)} {name}'
    if unit == 'fraction':
        return f'{_trimmed(value * 100)}%'
    if unit == 'x':
        return f'{_trimmed(value)}x'
    return f'{int(value):,}' if float(value).is_integer() else _trimmed(value)


def _trimmed(value: float) -> str:
    return f'{round(value):,}' if float(value).is_integer() else f'{value:.1f}'.removesuffix('.0')


@dc.dataclass(frozen=True)
class RuleContext:
    """What a Python rule's `check` reads.

    `plans` maps each SQL execution id to its last plan, and `numbered` to that plan's
    `Plan.numbered()`. A plan analyzed alone is keyed by its own execution id, which may be None.
    `node_metrics` gives each node's metric totals by name, keyed by execution id and node number,
    and is empty where nothing measured the plan. `thresholds` holds the values this analysis
    applies, by name. `tables` is None where no log was analyzed. `findings` holds, by rule id, the
    findings of each rule named in the rule's `reads_findings_of`.
    """

    plans: Mapping[int | None, Plan]
    numbered: Mapping[int | None, tuple[NumberedNode, ...]]
    node_metrics: Mapping[int | None, Mapping[int, Mapping[str, float]]]
    thresholds: Mapping[str, float]
    tables: Tables | None
    findings: Mapping[str, tuple[Finding, ...]] = dc.field(default_factory=dict)

    def metrics(self, execution_id: int | None, number: int) -> Mapping[str, float]:
        """One node's metric totals by name, empty where none was measured."""
        return self.node_metrics.get(execution_id, {}).get(number, {})


@dc.dataclass(frozen=True)
class Rule:
    """One heuristic, run over a log's tables with `sql` or over its plans with `check`.

    `rule_id` is `<prefix>.<kebab-name>` and never changes meaning; a rename is a breaking change.
    `rule_version` starts at 1 and rises whenever what the rule reports changes. `title` is a
    `str.format` template over a finding's scope ids and evidence, where `{key:bytes}` prints a
    byte count and `{key:duration}` a millisecond duration as the Spark UI prints them, and
    `{key:plural task}` a count with the noun agreeing with it. `scope`
    names what one finding is about, and so which ids its `finding_id` is built from.

    `sql` runs in DuckDB over the tables, and reads each threshold as a `$name` parameter. `check`
    receives a `RuleContext`. Either returns one mapping per finding. The keys `severity`,
    `impact_seconds`, `confidence` and `confidence_reason`, and the scope ids `sql_execution_id`,
    `plan_node_id`, `plan_node_label`, `stage_id`, `stage_attempt`, `job_id`, `executor_id` and
    `worst_task_ids`, fill the finding's own fields. Every other key is evidence, named with its
    unit. `severity` is the rule's grade before the engine weighs the scope's share of the run.

    `lesson` is Markdown, where `{name}` prints a threshold in prose at the value applied, so the
    lesson states the number the rule judges by. It holds no other braces.

    `reads_findings_of` names the rules whose findings a `check` reads from `RuleContext.findings`.
    Selecting the rule runs them first. One not selected itself reports nothing: its findings reach
    the reading rule alone. A rule whose own signals are present still skips when one of theirs is
    missing.
    """

    rule_id: str
    title: str
    signals: frozenset[Signal]
    scope: ScopeLevel
    thresholds: tuple[Threshold, ...]
    severity_ladder: Mapping[Severity, str]
    remediation: tuple[str, ...]
    docs: tuple[str, ...]
    lesson: str
    sql: str | None = None
    check: Callable[[RuleContext], Iterable[Mapping[str, Any]]] | None = None
    rule_version: int = 1
    reads_findings_of: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        problems = []
        if not _RULE_ID.fullmatch(self.rule_id):
            problems.append('its id is not <prefix>.<kebab-name>')
        if (self.sql is None) == (self.check is None):
            problems.append('it needs exactly one of sql and check')
        if not self.signals or not self.signals <= set(SIGNALS):
            problems.append(f'its signals must be some of {", ".join(SIGNALS)}')
        if self.sql is not None and not self.signals & _TABLE_SIGNALS:
            problems.append(f'a SQL rule reads the tables, so it needs one of {", ".join(sorted(_TABLE_SIGNALS))}')
        if self.scope not in SCOPE_LEVELS:
            problems.append(f'its scope must be one of {", ".join(SCOPE_LEVELS)}')
        names = [threshold.name for threshold in self.thresholds]
        if len(set(names)) != len(names):
            problems.append('two thresholds share a name')
        # DuckDB refuses a parameter the query does not read, and one it reads but was not given.
        if self.sql is not None and set(_PARAMETER.findall(self.sql)) != set(names):
            problems.append('its SQL must read each threshold as a $name parameter, and no other parameter')
        if any(threshold.unit not in THRESHOLD_UNITS for threshold in self.thresholds):
            problems.append(f'a threshold unit must be one of {", ".join(THRESHOLD_UNITS)}')
        if not set(self.severity_ladder) <= set(SEVERITIES):
            problems.append(f'its severity ladder keys must be some of {", ".join(SEVERITIES)}')
        if self.rule_version < 1:
            problems.append('its rule_version starts at 1')
        if self.reads_findings_of and self.sql is not None:
            problems.append("only a check reads other rules' findings")
        if self.rule_id in self.reads_findings_of:
            problems.append('it cannot read its own findings')
        problems.extend(_lesson_problems(self.lesson, set(names)))
        if problems:
            raise InvalidRuleError(f'rule {self.rule_id!r} is malformed: {"; ".join(problems)}', field='rule')

    def threshold(self, name: str) -> Threshold | None:
        return next((threshold for threshold in self.thresholds if threshold.name == name), None)

    def lesson_text(self, values: Mapping[str, float] | None = None) -> str:
        """The lesson with each threshold it names at its value in `values`, or at its default."""
        given = values or {}
        return self.lesson.format_map({threshold.name: threshold.in_prose(given.get(threshold.name)) for threshold in self.thresholds})

    def to_json(self) -> dict[str, Any]:
        """What the rule reads, judges by and teaches, without the SQL or Python it runs."""
        return {
            'rule_id': self.rule_id,
            'rule_version': self.rule_version,
            'title': self.title,
            'scope': self.scope,
            'signals': [signal for signal in SIGNALS if signal in self.signals],
            'thresholds': [threshold.to_json() for threshold in self.thresholds],
            'severity_ladder': {severity: self.severity_ladder[severity] for severity in SEVERITIES if severity in self.severity_ladder},
            'remediation': list(self.remediation),
            'docs': list(self.docs),
            'lesson': self.lesson_text(),
            'reads_findings_of': list(self.reads_findings_of),
        }


def _lesson_problems(lesson: str, thresholds: set[str]) -> list[str]:
    """What keeps a lesson from printing: a stray brace, or a `{field}` naming no threshold or carrying a format."""
    try:
        fields = [(name, spec, conversion) for _, name, spec, conversion in string.Formatter().parse(lesson) if name is not None]
    except ValueError:
        return ['its lesson has a brace that opens no {threshold}']
    unknown = sorted({name for name, _, _ in fields} - thresholds)
    problems = [f'its lesson names {", ".join(f"{{{name}}}" for name in unknown)}, which no threshold is'] if unknown else []
    if any(spec or conversion for _, spec, conversion in fields):
        problems.append('a {threshold} in its lesson takes no format')
    return problems
