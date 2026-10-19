"""What a rule is: the signals it reads, the thresholds it compares against, and the SQL or Python that finds what it reports."""

from __future__ import annotations

import dataclasses as dc
import re
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal

from sparkxray.plan.tree import NumberedNode
from sparkxray.plan.tree import Plan

if TYPE_CHECKING:
    from sparkxray.tables.relations import Tables

Signal = Literal['plan', 'plan_metrics', 'tasks', 'executors', 'environment', 'timeline']
SIGNALS: tuple[Signal, ...] = ('plan', 'plan_metrics', 'tasks', 'executors', 'environment', 'timeline')
Severity = Literal['info', 'low', 'medium', 'high', 'critical']
SEVERITIES: tuple[Severity, ...] = ('info', 'low', 'medium', 'high', 'critical')
# What one finding of a rule is about, which decides the ids its finding_id is built from.
ScopeLevel = Literal['application', 'sql_execution', 'plan_node', 'stage', 'executor']
SCOPE_LEVELS: tuple[ScopeLevel, ...] = ('application', 'sql_execution', 'plan_node', 'stage', 'executor')
ThresholdUnit = Literal['x', 'bytes', 'ms', 'count', 'rows', 'fraction']
THRESHOLD_UNITS: tuple[ThresholdUnit, ...] = ('x', 'bytes', 'ms', 'count', 'rows', 'fraction')

# Only an event log's tables hold these. A SQL rule reads the tables, so it declares at least one.
_TABLE_SIGNALS = frozenset({'tasks', 'executors', 'environment', 'timeline'})
_RULE_ID = re.compile(r'[a-z]+\.[a-z0-9]+(?:-[a-z0-9]+)*')
_PARAMETER = re.compile(r'\$([A-Za-z_][A-Za-z0-9_]*)')


@dc.dataclass(frozen=True, slots=True)
class Threshold:
    """One value a rule compares against: its default, its unit, and where the default came from."""

    name: str
    default: float
    unit: ThresholdUnit
    source: str


@dc.dataclass(frozen=True)
class RuleContext:
    """What a Python rule's `check` reads.

    `plans` maps each SQL execution id to its last plan, and `numbered` to that plan's
    `Plan.numbered()`. A plan analyzed alone is keyed by its own execution id, which may be None.
    `node_metrics` gives each node's metric totals by name, keyed by execution id and node number,
    and is empty where nothing measured the plan. `thresholds` holds the values this analysis
    applies, by name. `tables` is None where no log was analyzed.
    """

    plans: Mapping[int | None, Plan]
    numbered: Mapping[int | None, tuple[NumberedNode, ...]]
    node_metrics: Mapping[int | None, Mapping[int, Mapping[str, float]]]
    thresholds: Mapping[str, float]
    tables: Tables | None

    def metrics(self, execution_id: int | None, number: int) -> Mapping[str, float]:
        """One node's metric totals by name, empty where none was measured."""
        return self.node_metrics.get(execution_id, {}).get(number, {})


@dc.dataclass(frozen=True)
class Rule:
    """One heuristic, run over a log's tables with `sql` or over its plans with `check`.

    `rule_id` is `<prefix>.<kebab-name>` and never changes meaning; a rename is a breaking change.
    `rule_version` starts at 1 and rises whenever what the rule reports changes. `title` is a
    `str.format` template over a finding's scope ids and evidence. `scope` names what one finding
    is about, and so which ids its `finding_id` is built from.

    `sql` runs in DuckDB over the tables, and reads each threshold as a `$name` parameter. `check`
    receives a `RuleContext`. Either returns one mapping per finding. The keys `severity`,
    `impact_seconds`, `confidence` and `confidence_reason`, and the scope ids `sql_execution_id`,
    `plan_node_id`, `plan_node_label`, `stage_id`, `stage_attempt`, `job_id`, `executor_id` and
    `worst_task_ids`, fill the finding's own fields. Every other key is evidence, named with its
    unit. `severity` is the rule's grade before the engine weighs the scope's share of the run.
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
        if problems:
            raise ValueError(f'rule {self.rule_id!r} is malformed: {"; ".join(problems)}')

    def threshold(self, name: str) -> Threshold | None:
        return next((threshold for threshold in self.thresholds if threshold.name == name), None)
