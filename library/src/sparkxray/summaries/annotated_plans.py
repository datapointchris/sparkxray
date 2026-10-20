"""One SQL execution's plan with the numbers the Spark UI's SQL tab shows on it, and the findings that name its nodes.

A plan is drawn from the event that logged it, and every other number comes from the tables: a
node's metrics from `plan_node_metrics`, the stages it ran in from `node_stages`, and the
execution's duration and stages from `sql_summaries`. Spark carries a node AQE kept into its next
plan with the accumulators it had, so an earlier version shows the values of the nodes it shares
with the last. `node_stages` and every finding number the nodes of the last plan, so only that
version places them on nodes.
"""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Iterable
from typing import Any

from sparkxray.plan import labels
from sparkxray.plan import node_fields
from sparkxray.plan.tree import Plan
from sparkxray.plan.tree import PlanNode
from sparkxray.plan.tree import PlanSource
from sparkxray.rules.findings import Finding
from sparkxray.rules.rule import Severity
from sparkxray.spark_ui_format import shown_in
from sparkxray.summaries.sql_executions import SQL_SUMMARIES_SQL
from sparkxray.tables.registry import OWN_SPILLS_CTE
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import rows_of

_CONDITION_CHARACTERS = 80
_CUT_MARK = '…'

_EXECUTION_SQL = f'SELECT * FROM ({SQL_SUMMARIES_SQL}) WHERE execution_id = $execution_id'

_EXECUTION_STAGES_SQL = """
SELECT DISTINCT job_stages.stage_id
FROM (SELECT unnest(stage_ids) AS stage_id FROM jobs WHERE sql_execution_id = $execution_id) AS job_stages
JOIN (SELECT DISTINCT stage_id FROM stages) AS ran USING (stage_id)
ORDER BY 1
"""

_NODE_STAGES_SQL = """
SELECT node_id, list(stage_id ORDER BY stage_id) FROM node_stages WHERE execution_id = $execution_id GROUP BY node_id
"""

_NODE_METRICS_SQL = """
SELECT node_id, metric_name, metric_type, unit, coalesce(value_count, 0), total, min, median, max, max_stage_id, max_task_id
FROM plan_node_metrics
WHERE execution_id = $execution_id AND plan_version = $version
QUALIFY row_number() OVER (PARTITION BY node_id, metric_name ORDER BY accumulator_id) = 1
"""

_OWN_SPILLS_SQL = f"""
WITH {OWN_SPILLS_CTE}
SELECT node_id, own_spill_bytes FROM own_spills WHERE execution_id = $execution_id AND plan_version = $version
"""


@dc.dataclass(frozen=True, slots=True)
class NodeMetric:
    """One metric a node declares, aggregated as the SQL tab aggregates it, with `total`, `min`, `median` and `max` in `unit`.

    `total` is None for an average, which has no unit and which Spark shows only as its spread.
    `value_count` is 0 for a metric nothing reported to. `max_stage_id` and `max_task_id` name
    the task that reported the largest value, and are None where the driver did.
    """

    name: str
    metric_type: str | None
    unit: str | None
    value_count: int
    total: float | None
    min: float | None
    median: float | None
    max: float | None
    max_stage_id: int | None
    max_task_id: int | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class NodeDetails:
    """What a node printed that says what it does, as Spark printed it, ids included. A field the node's operator has no use for is None.

    A join has `join_type`, and `left_keys` and `right_keys` where it joins on keys. A `Filter`
    has `condition`. A file scan has `scan_format` and `location`. An exchange has
    `partitioning`, and an `AQEShuffleRead` has `shuffle_read`, such as `coalesced`.
    """

    join_type: str | None = None
    left_keys: tuple[str, ...] | None = None
    right_keys: tuple[str, ...] | None = None
    condition: str | None = None
    scan_format: str | None = None
    location: str | None = None
    partitioning: str | None = None
    shuffle_read: str | None = None

    @classmethod
    def of(cls, node: PlanNode) -> NodeDetails:
        keys = node_fields.join_keys(node) if _is_join(node) else None
        is_file_scan = node.operator in node_fields.FILE_SCAN_OPERATORS
        return cls(
            join_type=node_fields.join_type(node) if _is_join(node) else None,
            left_keys=None if keys is None else keys[0],
            right_keys=None if keys is None else keys[1],
            condition=node_fields.filter_condition(node),
            scan_format=node_fields.field(node, 'Format') if is_file_scan else None,
            location=node_fields.scan_location(node),
            partitioning=node_fields.partitioning(node),
            shuffle_read=node_fields.shuffle_read_kind(node),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            **dc.asdict(self),
            'left_keys': None if self.left_keys is None else list(self.left_keys),
            'right_keys': None if self.right_keys is None else list(self.right_keys),
        }


@dc.dataclass(frozen=True, slots=True)
class HostedSubquery:
    """A subquery plan a node runs: the subquery's id, and its root's number in the same sequence as the node's."""

    subquery_id: int | None
    root: int

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True, slots=True)
class AnnotatedNode:
    """One plan node with the numbers the SQL tab shows beside it.

    `number` is its place in `Plan.numbered()`, as `plan_nodes` and every finding number it.
    `parent` and `children` are numbers too. A subquery's root has the node running it as its
    parent, and that node lists it in `subqueries` rather than `children`. `runtime_size_bytes`
    and `runtime_row_count` are what AQE measured on a finished query stage. `stage_ids` is
    empty in any plan but the last, and `finding_ids` names the findings whose scope is this node.

    `codegen_stage` is the whole-stage codegen pipeline the node runs in. The pipeline's
    `duration` metric sits on its top node and covers every node `AnnotatedPlan.codegen_members`
    lists. `own_spill_bytes` is the part of the node's `spill size` no node below it also counts,
    and None where it declares none.
    """

    number: int
    parent: int | None
    children: tuple[int, ...]
    subqueries: tuple[HostedSubquery, ...]
    operator: str
    label: str
    details: NodeDetails
    codegen_stage: int | None
    runtime_size_bytes: int | None
    runtime_row_count: int | None
    metrics: tuple[NodeMetric, ...]
    own_spill_bytes: float | None
    stage_ids: tuple[int, ...]
    finding_ids: tuple[str, ...] = ()

    def headline(self) -> str:
        """The operator and what it does on one line, without ids, as `SortMergeJoin Inner k = k`.

        A filter's condition is cut to 80 characters.
        """
        details = self.details
        parts = [self.operator]
        if details.join_type is not None:
            parts.append(details.join_type)
        if details.left_keys is not None and details.right_keys is not None:
            parts.append(', '.join(f'{left} = {right}' for left, right in zip(details.left_keys, details.right_keys, strict=False)))
        if details.condition is not None:
            condition = labels.without_ids(details.condition)
            cut = len(condition) > _CONDITION_CHARACTERS
            parts.append(f'{condition[: _CONDITION_CHARACTERS - len(_CUT_MARK)]}{_CUT_MARK}' if cut else condition)
        parts += [value for value in (details.scan_format, details.location, details.partitioning, details.shuffle_read) if value]
        return labels.without_ids(' '.join(parts))

    def to_json(self) -> dict[str, Any]:
        return {
            'number': self.number,
            'parent': self.parent,
            'children': list(self.children),
            'subqueries': [subquery.to_json() for subquery in self.subqueries],
            'operator': self.operator,
            'label': self.label,
            'headline': self.headline(),
            'details': self.details.to_json(),
            'codegen_stage': self.codegen_stage,
            'runtime_size_bytes': self.runtime_size_bytes,
            'runtime_row_count': self.runtime_row_count,
            'stage_ids': list(self.stage_ids),
            'metrics': [metric.to_json() for metric in self.metrics],
            'own_spill_bytes': self.own_spill_bytes,
            'finding_ids': list(self.finding_ids),
        }


@dc.dataclass(frozen=True, slots=True)
class FindingMark:
    """A finding on a node, as a plan drawing marks it: `◀ stage.spill (stage 4, critical)`."""

    text: str
    severity: Severity


@dc.dataclass(frozen=True, slots=True)
class NodeLine:
    """A node as every plan drawing prints it, in the parts a drawer styles.

    Its first line is `number`, `codegen` where it runs in a pipeline, `operator` and `rest`, then
    `runtime` and each mark. `measured` is the line under it: its stages and metrics.
    """

    number: str
    codegen: str | None
    operator: str
    rest: str
    runtime: str | None
    marks: tuple[FindingMark, ...]
    measured: str | None

    def headline(self) -> str:
        """The first line as plain text."""
        line = f'{self.number} {f"{self.codegen} " if self.codegen else ""}{self.operator}{self.rest}'
        if self.runtime:
            line += f'  {self.runtime}'
        return line + ''.join(f'  {mark.text}' for mark in self.marks)


@dc.dataclass(frozen=True, slots=True)
class AnnotatedPlan:
    """One version of a SQL execution's plan, with its nodes' numbers and the findings about the execution.

    `execution` is the execution's row from `sql_summaries`. `version` is 0 for the plan the
    execution started from and `last_version` for its last. `nodes` is in `Plan.numbered()`
    order, so a node's number is its index, and every node with no parent is a root. `findings`
    holds each finding about the execution or a stage it ran, in the analysis's order. Those whose
    scope is a node of this version are listed on that node too.
    """

    execution: dict[str, Any]
    version: int
    last_version: int
    is_final: bool
    truncated: bool
    source: PlanSource
    stage_ids: tuple[int, ...]
    nodes: tuple[AnnotatedNode, ...]
    findings: tuple[Finding, ...] = ()

    @property
    def execution_id(self) -> int:
        execution_id: int = self.execution['execution_id']
        return execution_id

    def with_findings(self, findings: Iterable[Finding]) -> AnnotatedPlan:
        """This plan with the findings about its execution, each one whose scope is a node of the last plan placed on that node.

        A finding is about the execution where its scope names it, or names none and a stage it ran.
        """
        stages = set(self.stage_ids)
        about = tuple(
            finding
            for finding in findings
            if finding.scope.sql_execution_id == self.execution_id
            or (finding.scope.sql_execution_id is None and finding.scope.stage_id in stages)
        )
        placed: dict[int, list[str]] = {}
        if self.version == self.last_version:
            for finding in about:
                number = finding.scope.plan_node_id
                if finding.scope.sql_execution_id == self.execution_id and number is not None and 0 <= number < len(self.nodes):
                    placed.setdefault(number, []).append(finding.finding_id)
        nodes = tuple(dc.replace(node, finding_ids=tuple(placed.get(node.number, ()))) for node in self.nodes)
        return dc.replace(self, nodes=nodes, findings=about)

    def codegen_members(self, node: AnnotatedNode) -> tuple[AnnotatedNode, ...]:
        """The node and those below it in its codegen pipeline, top first: the nodes the pipeline's `duration` on it covers."""
        if node.codegen_stage is None:
            return ()
        members, pending = [], [node.number]
        while pending:
            member = self.nodes[pending.pop()]
            members.append(member)
            pending += [child for child in reversed(member.children) if self.nodes[child].codegen_stage == node.codegen_stage]
        return tuple(members)

    def shown_metric(self, node: AnnotatedNode, metric: NodeMetric) -> str | None:
        """One of a node's metrics as a person reads it: its name and total, or for an average its min, median and max.

        Zero, and an average nothing reported, give None. A pipeline's `duration` names the nodes it
        covers, and a `spill size` the nodes below also count states the node's own part.
        """
        if metric.total is None:
            if not metric.max:
                return None
            return f'{metric.name} ({", ".join(shown_in(metric.unit, value) for value in (metric.min, metric.median, metric.max))})'
        if not metric.total:
            return None
        shown = f'{metric.name} {shown_in(metric.unit, metric.total)}'
        members = self.codegen_members(node) if metric.name == 'duration' else ()
        if len(members) > 1:
            covered = ', '.join(f'n{member.number} {member.operator}' for member in members)
            return f'*({node.codegen_stage}) {shown}: {covered} and their inputs'
        if metric.name == 'spill size' and node.own_spill_bytes is not None and node.own_spill_bytes != metric.total:
            return f'{shown} with the nodes below, {shown_in(metric.unit, node.own_spill_bytes)} its own'
        return shown

    def node_line(self, node: AnnotatedNode) -> NodeLine:
        """The node as `plans show` and a report draw it, in parts: its first line, then its stages and metrics."""
        headline = node.headline()
        runtime = []
        if node.runtime_size_bytes is not None:
            runtime.append(shown_in('bytes', node.runtime_size_bytes))
        if node.runtime_row_count is not None:
            runtime.append(f'{shown_in("rows", node.runtime_row_count)} rows')
        findings = {finding.finding_id: finding for finding in self.findings}
        marks = []
        for finding_id in node.finding_ids:
            finding = findings[finding_id]
            stage = f'stage {finding.scope.stage_id}, ' if finding.scope.stage_id is not None else ''
            marks.append(FindingMark(f'◀ {finding.rule_id} ({stage}{finding.severity})', finding.severity))
        measured = [f'stage{"s" if len(node.stage_ids) > 1 else ""} {shown_in(None, list(node.stage_ids))}'] if node.stage_ids else []
        measured += [shown for shown in (self.shown_metric(node, metric) for metric in node.metrics) if shown]
        return NodeLine(
            number=f'n{node.number}',
            codegen=f'*({node.codegen_stage})' if node.codegen_stage is not None else None,
            operator=node.operator,
            rest=headline[len(node.operator) :],
            runtime=f'{" · ".join(runtime)} (runtime)' if runtime else None,
            marks=tuple(marks),
            measured=' · '.join(measured) or None,
        )

    def unplaced_findings(self) -> tuple[Finding, ...]:
        """The findings no node lists: about the whole execution or one of its stages, or naming a node of another version."""
        placed = {finding_id for node in self.nodes for finding_id in node.finding_ids}
        return tuple(finding for finding in self.findings if finding.finding_id not in placed)

    def to_json(self) -> dict[str, Any]:
        return {
            'execution': self.execution,
            'version': self.version,
            'last_version': self.last_version,
            'is_final': self.is_final,
            'truncated': self.truncated,
            'source': self.source,
            'stage_ids': list(self.stage_ids),
            'nodes': [node.to_json() for node in self.nodes],
            'findings': [finding.to_json() for finding in self.findings],
        }


def annotated_plan(tables: Tables, execution_id: int, *, version: int | None = None) -> AnnotatedPlan:
    """One version of an execution's plan with its numbers, the last where `version` is None. `with_findings` adds the findings.

    An execution, or a version of it, that the run holds no plan for raises `NotInRunError`.
    """
    plan = tables.plan(execution_id, version)
    shown = plan.version if plan.version is not None else 0
    parameters = {'execution_id': execution_id}
    execution = rows_of(tables.query(_EXECUTION_SQL, parameters)).to_json()[0]
    last_version = int(execution['plan_version_count']) - 1
    stage_ids = tuple(row[0] for row in tables.query(_EXECUTION_STAGES_SQL, parameters).fetchall())
    node_stages: dict[int, tuple[int, ...]] = {}
    if shown == last_version:
        node_stages = {node_id: tuple(stages) for node_id, stages in tables.query(_NODE_STAGES_SQL, parameters).fetchall()}
    metrics: dict[int, dict[str, NodeMetric]] = {}
    for node_id, name, *values in tables.query(_NODE_METRICS_SQL, {**parameters, 'version': shown}).fetchall():
        metrics.setdefault(node_id, {})[name] = NodeMetric(name, *values)
    own_spills = dict(tables.query(_OWN_SPILLS_SQL, {**parameters, 'version': shown}).fetchall())
    roots = _subquery_roots(plan)
    hosted: dict[int, list[HostedSubquery]] = {}
    numbered = plan.numbered()
    for each in numbered:
        if each.number in roots and each.parent is not None:
            hosted.setdefault(each.parent, []).append(HostedSubquery(roots[each.number], each.number))
    nodes = []
    for each in numbered:
        node, declared = each.node, metrics.get(each.number, {})
        names = dict.fromkeys(ref.name for ref in node.metrics if ref.name in declared)
        stats = node.runtime_stats
        nodes.append(
            AnnotatedNode(
                number=each.number,
                parent=each.parent,
                children=each.children,
                subqueries=tuple(hosted.get(each.number, ())),
                operator=node.operator,
                label=node.label,
                details=NodeDetails.of(node),
                codegen_stage=node.codegen_stage,
                runtime_size_bytes=None if stats is None else stats.size_bytes,
                runtime_row_count=None if stats is None else stats.row_count,
                metrics=tuple(declared[name] for name in names),
                own_spill_bytes=own_spills.get(each.number),
                stage_ids=node_stages.get(each.number, ()),
            )
        )
    return AnnotatedPlan(execution, shown, last_version, plan.is_final, plan.truncated, plan.source, stage_ids, tuple(nodes))


def _is_join(node: PlanNode) -> bool:
    return node.operator.endswith('Join') or node.operator == 'CartesianProduct'


def _subquery_roots(plan: Plan) -> dict[int, int | None]:
    """Each subquery root's number in `Plan.numbered()`, mapped to the subquery's id. The plan numbers each part in this order."""
    roots: dict[int, int | None] = {}
    count = 0

    def visit(part: Plan) -> None:
        nonlocal count
        if part is not plan and part.nodes:
            roots[count] = part.subquery_id
        count += len(part.nodes)
        for subquery in part.subqueries:
            visit(subquery)

    visit(plan)
    return roots
