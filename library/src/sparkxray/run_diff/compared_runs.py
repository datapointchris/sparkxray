"""Two runs of one job compared: each execution's and stage's cost in both, the settings and findings that changed, and new exceptions."""

from __future__ import annotations

import dataclasses as dc
import os
from collections import Counter
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from sparkxray.cache_directory import installed_version
from sparkxray.rules import Gate
from sparkxray.rules import gate_findings
from sparkxray.run_diff.alignment import ExecutionMatch
from sparkxray.run_diff.alignment import StageMatch
from sparkxray.run_diff.alignment import align_executions
from sparkxray.run_diff.alignment import align_stages
from sparkxray.run_diff.config_changes import ConfigChanges
from sparkxray.run_diff.config_changes import config_changes
from sparkxray.run_diff.exception_triage import TriagedException
from sparkxray.run_diff.exception_triage import triage
from sparkxray.run_diff.finding_changes import FindingChanges
from sparkxray.run_diff.finding_changes import finding_changes
from sparkxray.run_diff.metric_deltas import EXECUTION_METRICS
from sparkxray.run_diff.metric_deltas import STAGE_METRICS
from sparkxray.run_diff.metric_deltas import MetricDelta
from sparkxray.run_diff.metric_deltas import metric_deltas
from sparkxray.run_diff.metric_deltas import summed
from sparkxray.run_diff.run_facts import RunFacts

if TYPE_CHECKING:
    from sparkxray.ingest.event_log import EventLog

# A field removed or a meaning changed raises this. A field added does not.
SCHEMA_VERSION = 1


@dc.dataclass(frozen=True, slots=True)
class DiffedRun:
    """One run of a diff. It `succeeded` when its application ended and every job it ran succeeded."""

    app_id: str | None
    app_name: str | None
    spark_version: str | None
    wall_ms: int | None
    succeeded: bool

    @classmethod
    def of(cls, facts: RunFacts) -> DiffedRun:
        outcome = facts.outcome
        return cls(outcome['app_id'], outcome['app_name'], outcome['spark_version'], outcome['wall_ms'], bool(outcome['succeeded']))

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class OperatorChange:
    """An operator the last plans of a paired execution hold a different number of."""

    operator: str
    a_count: int
    b_count: int

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class MatchedExecution:
    """A SQL execution of run A and the one of run B it pairs with, and what each cost. `description` is run B's.

    Its metrics are its stages' sums, but `wall_ms`, which is the execution's own.
    `final_plan_changed` is whether the two last plans differ in anything but their ids.
    """

    a_execution_id: int
    b_execution_id: int
    description: str | None
    matched_by: ExecutionMatch
    final_plan_changed: bool
    operator_changes: tuple[OperatorChange, ...]
    metrics: tuple[MetricDelta, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'a_execution_id': self.a_execution_id,
            'b_execution_id': self.b_execution_id,
            'description': self.description,
            'matched_by': self.matched_by,
            'final_plan_changed': self.final_plan_changed,
            'operator_changes': [change.to_json() for change in self.operator_changes],
            'metrics': [metric.to_json() for metric in self.metrics],
        }


@dc.dataclass(frozen=True, slots=True)
class UnmatchedExecution:
    """A SQL execution only one run holds."""

    execution_id: int
    description: str | None
    wall_ms: int | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class MatchedStage:
    """A stage of run A and the one of run B it pairs with, each with the SQL execution it ran for, and their costs. `name` is run B's."""

    a_stage_id: int
    b_stage_id: int
    a_execution_id: int | None
    b_execution_id: int | None
    name: str | None
    matched_by: StageMatch
    metrics: tuple[MetricDelta, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'a_stage_id': self.a_stage_id,
            'b_stage_id': self.b_stage_id,
            'a_execution_id': self.a_execution_id,
            'b_execution_id': self.b_execution_id,
            'name': self.name,
            'matched_by': self.matched_by,
            'metrics': [metric.to_json() for metric in self.metrics],
        }


@dc.dataclass(frozen=True, slots=True)
class UnmatchedStage:
    """A stage only one run ran."""

    stage_id: int
    execution_id: int | None
    name: str | None
    wall_ms: int | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class SqlDiff:
    """The SQL executions both runs hold, in run B's order, and those only one does."""

    matched: tuple[MatchedExecution, ...]
    only_a: tuple[UnmatchedExecution, ...]
    only_b: tuple[UnmatchedExecution, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'matched': [execution.to_json() for execution in self.matched],
            'only_a': [execution.to_json() for execution in self.only_a],
            'only_b': [execution.to_json() for execution in self.only_b],
        }


@dc.dataclass(frozen=True)
class StageDiff:
    """The stages both runs ran, in run B's order, and those only one did."""

    matched: tuple[MatchedStage, ...]
    only_a: tuple[UnmatchedStage, ...]
    only_b: tuple[UnmatchedStage, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'matched': [stage.to_json() for stage in self.matched],
            'only_a': [stage.to_json() for stage in self.only_a],
            'only_b': [stage.to_json() for stage in self.only_b],
        }


@dc.dataclass(frozen=True)
class RunDiff:
    """Run B against run A, in the shape `schemas/diff.json` pins.

    Both runs were analyzed at the same thresholds. `baselines` are the further runs the exception
    triage read.
    """

    generated_by: str
    a: DiffedRun
    b: DiffedRun
    baselines: tuple[DiffedRun, ...]
    sql: SqlDiff
    stages: StageDiff
    config: ConfigChanges
    findings: FindingChanges
    exceptions: tuple[TriagedException, ...]
    schema_version: int = SCHEMA_VERSION

    def gate(self, fail_on: str) -> Gate:
        """Whether run B passes the severity `fail_on`, as `Analysis.gate` judges, over the findings it added or raised to the bar.

        A finding both runs report blocks when run B's is at or above `fail_on` and run A's was
        below it. One that was already at the bar in run A does not.
        """
        persisting = self.findings.persisting
        already = set(gate_findings([pair.a for pair in persisting], fail_on).blocking)
        risen = [pair.b for pair in persisting if pair.a.finding_id not in already]
        return gate_findings([*self.findings.new, *risen], fail_on)

    def to_json(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'generated_by': self.generated_by,
            'a': self.a.to_json(),
            'b': self.b.to_json(),
            'baselines': [baseline.to_json() for baseline in self.baselines],
            'sql': self.sql.to_json(),
            'stages': self.stages.to_json(),
            'config': self.config.to_json(),
            'findings': self.findings.to_json(),
            'exceptions': [found.to_json() for found in self.exceptions],
        }


def diff_logs(
    a: EventLog,
    b: EventLog,
    *,
    baselines: Sequence[EventLog] = (),
    thresholds: Mapping[str, Mapping[str, object]] | None = None,
    threshold_file: str | os.PathLike[str] | None = None,
    threshold_flags: Iterable[str] = (),
    ignore_config: Sequence[str] = (),
) -> RunDiff:
    """Run B against run A: each analyzed at the same thresholds, their executions and stages aligned, and every change between them.

    `baselines` are more runs of the job, read for their exceptions alone: one a healthy run also
    threw is routine. `ignore_config` holds glob patterns of setting keys to leave out, beside the
    keys Spark sets anew for every run. The thresholds are set as `analyze` sets them.
    """
    analyses = [log.analyze(thresholds=thresholds, threshold_file=threshold_file, threshold_flags=threshold_flags) for log in (a, b)]
    a_facts, b_facts = RunFacts.read(a.tables), RunFacts.read(b.tables)
    baseline_facts = tuple(RunFacts.read(baseline.tables) for baseline in baselines)
    executions = align_executions(a_facts, b_facts)
    stages = align_stages(a_facts, b_facts, executions)
    findings = finding_changes(analyses[0].findings, analyses[1].findings, (a_facts, b_facts), executions, stages)
    return RunDiff(
        generated_by=f'sparkxray {installed_version()}',
        a=DiffedRun.of(a_facts),
        b=DiffedRun.of(b_facts),
        baselines=tuple(DiffedRun.of(facts) for facts in baseline_facts),
        sql=SqlDiff(
            matched=tuple(_matched_execution(pair.a, pair.b, pair.matched_by, a_facts, b_facts) for pair in executions.paired),
            only_a=tuple(UnmatchedExecution(row['execution_id'], row['description'], row['wall_ms']) for row in executions.only_a),
            only_b=tuple(UnmatchedExecution(row['execution_id'], row['description'], row['wall_ms']) for row in executions.only_b),
        ),
        stages=StageDiff(
            matched=tuple(
                MatchedStage(
                    pair.a['stage_id'],
                    pair.b['stage_id'],
                    pair.a['execution_id'],
                    pair.b['execution_id'],
                    pair.b['name'],
                    pair.matched_by,
                    metric_deltas(pair.a, pair.b, STAGE_METRICS),
                )
                for pair in stages.paired
            ),
            only_a=tuple(UnmatchedStage(row['stage_id'], row['execution_id'], row['name'], row['wall_ms']) for row in stages.only_a),
            only_b=tuple(UnmatchedStage(row['stage_id'], row['execution_id'], row['name'], row['wall_ms']) for row in stages.only_b),
        ),
        config=config_changes(a_facts, b_facts, ignore_config),
        findings=findings,
        exceptions=triage(a_facts, b_facts, baseline_facts),
    )


def _matched_execution(
    a: Mapping[str, Any], b: Mapping[str, Any], matched_by: ExecutionMatch, a_facts: RunFacts, b_facts: RunFacts
) -> MatchedExecution:
    a_id, b_id = a['execution_id'], b['execution_id']
    a_totals = {**summed((stage for stage in a_facts.stages if stage['execution_id'] == a_id), EXECUTION_METRICS), 'wall_ms': a['wall_ms']}
    b_totals = {**summed((stage for stage in b_facts.stages if stage['execution_id'] == b_id), EXECUTION_METRICS), 'wall_ms': b['wall_ms']}
    a_operators = Counter(a_facts.final_operators.get(a_id, {}))
    b_operators = Counter(b_facts.final_operators.get(b_id, {}))
    return MatchedExecution(
        a_execution_id=a_id,
        b_execution_id=b_id,
        description=b['description'],
        matched_by=matched_by,
        final_plan_changed=a['final_plan_hash'] != b['final_plan_hash'],
        operator_changes=tuple(
            OperatorChange(operator, a_operators[operator], b_operators[operator])
            for operator in sorted(a_operators.keys() | b_operators.keys())
            if a_operators[operator] != b_operators[operator]
        ),
        metrics=metric_deltas(a_totals, b_totals, EXECUTION_METRICS),
    )
