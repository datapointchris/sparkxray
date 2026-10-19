"""The plan model every parser returns: one tree of nodes per plan version."""

from __future__ import annotations

import collections
import dataclasses as dc
import hashlib
import types
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Literal

from sparkxray.plan import labels

PlanSource = Literal['plan_info_json', 'formatted_text', 'simple_text', 'rest_graph']


@dc.dataclass(frozen=True, slots=True)
class MetricRef:
    """One SQL metric a node declares.

    An event log's tree carries the accumulator id that task updates report against, and the
    metric's type: `sum`, `size`, `timing`, `nsTiming` or `average`. The History Server's graph
    carries the name alone.
    """

    name: str
    accumulator_id: int | None
    metric_type: str | None


@dc.dataclass(frozen=True, slots=True)
class RuntimeStats:
    """What AQE measured on a finished query stage, as formatted text prints it.

    `size_bytes` is None where Spark printed `8.0 EiB`, which is `Long.MaxValue` standing in for
    a size it does not know. The text rounds each value to one decimal, so a size is approximate.
    """

    size_bytes: int | None
    row_count: int | None


@dc.dataclass(frozen=True, slots=True)
class PlanNode:
    """One physical operator, with every id it printed held as a field.

    `operator` is the same for one node read from any source. `normalized_label` is `label` with
    each id rewritten by `normalize_label`, so two builds of one transform compare equal.
    `subtree_hash` covers the normalized label and details of this node and everything below it.
    """

    node_id: int
    parent_id: int | None
    operator: str
    label: str
    normalized_label: str
    formatted_number: int | None
    codegen_stage: int | None
    details: Mapping[str, str] = dc.field(hash=False)
    metrics: tuple[MetricRef, ...]
    runtime_stats: RuntimeStats | None
    subtree_hash: str
    children: tuple[int, ...]
    expression_ids: tuple[int, ...]
    plan_id: int | None
    query_stage_id: int | None
    subquery_ids: tuple[int, ...]


@dc.dataclass(frozen=True, slots=True)
class Plan:
    """One version of one SQL execution's physical plan.

    `nodes` is in pre-order, and a node's `node_id` is its index. A subquery is its own `Plan`
    in `subqueries`, linked back by `hosting_node_id` and named by `subquery_id`. `version` is
    0 for the plan `SQLExecutionStart` carries and n for the nth AQE update, and None for a
    History Server graph, which serves only the latest.
    """

    source: PlanSource
    execution_id: int | None
    version: int | None
    is_final: bool
    truncated: bool
    nodes: tuple[PlanNode, ...]
    subqueries: tuple[Plan, ...] = ()
    subquery_id: int | None = None
    hosting_node_id: int | None = None
    _initial: Plan | None = dc.field(default=None, repr=False)

    def walk(self) -> Iterator[PlanNode]:
        """This plan's own nodes in pre-order. Subqueries are walked through `subqueries`."""
        return iter(self.nodes)

    def operator_counts(self) -> collections.Counter[str]:
        """How many nodes of each operator the query runs, its subqueries' included."""
        counts = collections.Counter(node.operator for node in self.nodes)
        for subquery in self.subqueries:
            counts.update(subquery.operator_counts())
        return counts

    def initial(self) -> Plan | None:
        """The plan AQE started from, where the text printed an `== Initial Plan ==` block."""
        return self._initial


@dc.dataclass(eq=False)
class RawNode:
    """A node while a parser is still building the tree. `freeze` turns a tree of these into a `Plan`."""

    label: str
    operator: str
    details: dict[str, str] = dc.field(default_factory=dict)
    metrics: list[MetricRef] = dc.field(default_factory=list)
    formatted_number: int | None = None
    codegen_stage: int | None = None
    runtime_stats: RuntimeStats | None = None
    children: list[RawNode] = dc.field(default_factory=list)
    hosted: list[RawSubquery] = dc.field(default_factory=list)


@dc.dataclass(eq=False)
class RawSubquery:
    """A subquery hosted by a node. A reused subquery has no root, because Spark prints its plan once."""

    subquery_id: int | None
    root: RawNode | None
    is_final: bool


@dc.dataclass(eq=False)
class RawPlan:
    """A parsed tree before ids are assigned."""

    root: RawNode | None
    is_final: bool
    truncated: bool
    initial: RawPlan | None = None


def raw_nodes(root: RawNode | None, *, subqueries: bool = True) -> Iterator[RawNode]:
    """Every node of a raw tree in pre-order, and of its subqueries' trees unless told otherwise."""
    stack = [] if root is None else [root]
    while stack:
        node = stack.pop()
        yield node
        if subqueries:
            stack.extend(hosted.root for hosted in reversed(node.hosted) if hosted.root is not None)
        stack.extend(reversed(node.children))


def freeze(
    raw: RawPlan,
    *,
    source: PlanSource,
    execution_id: int | None,
    version: int | None,
    subquery_id: int | None = None,
    hosting_node_id: int | None = None,
) -> Plan:
    """Assign pre-order ids, extract each node's ids, hash every subtree, and freeze the result."""
    order: list[RawNode] = []
    parent_index: dict[int, int | None] = {}

    def visit(node: RawNode, parent: int | None) -> None:
        index = len(order)
        order.append(node)
        parent_index[index] = parent
        for child in node.children:
            visit(child, index)

    if raw.root is not None:
        visit(raw.root, None)
    position = {id(node): index for index, node in enumerate(order)}
    hashes: dict[int, str] = {}
    for index in range(len(order) - 1, -1, -1):
        node = order[index]
        hashes[index] = _subtree_hash(node, [hashes[position[id(child)]] for child in node.children])

    nodes = []
    subqueries = []
    for index, node in enumerate(order):
        texts = [node.label, *node.details.values()]
        ids = labels.extract_ids(node.operator, node.label, node.details)
        hosted_ids = [hosted.subquery_id for hosted in node.hosted if hosted.subquery_id is not None]
        nodes.append(
            PlanNode(
                node_id=index,
                parent_id=parent_index[index],
                operator=node.operator,
                label=node.label,
                normalized_label=labels.normalize_label(node.label),
                formatted_number=node.formatted_number,
                codegen_stage=node.codegen_stage,
                details=types.MappingProxyType(dict(node.details)),
                metrics=tuple(node.metrics),
                runtime_stats=node.runtime_stats,
                subtree_hash=hashes[index],
                children=tuple(position[id(child)] for child in node.children),
                expression_ids=labels.expression_ids(texts),
                plan_id=ids.plan_id,
                query_stage_id=ids.query_stage_id,
                subquery_ids=_unique([*labels.subquery_ids(texts), *hosted_ids]),
            )
        )
        for hosted in node.hosted:
            if hosted.root is None:
                continue
            subqueries.append(
                freeze(
                    RawPlan(hosted.root, hosted.is_final, raw.truncated),
                    source=source,
                    execution_id=execution_id,
                    version=version,
                    subquery_id=hosted.subquery_id,
                    hosting_node_id=index,
                )
            )
    initial = None
    if raw.initial is not None:
        initial = freeze(raw.initial, source=source, execution_id=execution_id, version=version)
    return Plan(
        source=source,
        execution_id=execution_id,
        version=version,
        is_final=raw.is_final,
        truncated=raw.truncated,
        nodes=tuple(nodes),
        subqueries=tuple(subqueries),
        subquery_id=subquery_id,
        hosting_node_id=hosting_node_id,
        _initial=initial,
    )


def _subtree_hash(node: RawNode, child_hashes: list[str]) -> str:
    digest = hashlib.blake2b(digest_size=8)
    digest.update(labels.normalize_label(node.label).encode())
    for key in sorted(node.details):
        digest.update(b'\0' + key.encode() + b'=' + labels.normalize_detail(node.operator, key, node.details[key]).encode())
    for child_hash in child_hashes:
        digest.update(b'\1' + child_hash.encode())
    return digest.hexdigest()


def _unique(values: list[int]) -> tuple[int, ...]:
    return tuple(dict.fromkeys(values))
