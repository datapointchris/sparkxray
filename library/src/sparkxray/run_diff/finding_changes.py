"""Which findings run B adds, which it no longer reports, and which both runs report, matched by rule and the aligned part of the run.

A finding's id carries its run's ids, which shift between runs, so ids never match findings. Two
findings match when one rule reported them about parts the alignment paired: the same execution or
stage, or a plan node of the paired execution. Plan nodes pair by their label in the last plan
first, then in node order, because a setting such as skew-join handling rewrites a join's label.
An application finding matches its rule's other, and executor findings pair in executor id order,
as executors come and go with no identity across runs.
"""

from __future__ import annotations

import dataclasses as dc
from collections import defaultdict
from collections.abc import Hashable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from sparkxray.rules import Finding
from sparkxray.rules import catalog
from sparkxray.run_diff.alignment import Alignment
from sparkxray.run_diff.alignment import ExecutionMatch
from sparkxray.run_diff.alignment import StageMatch
from sparkxray.run_diff.run_facts import RunFacts


@dc.dataclass(frozen=True, slots=True)
class PersistingFinding:
    """One finding both runs report, as each run reported it."""

    a: Finding
    b: Finding

    def to_json(self) -> dict[str, Any]:
        return {'a': self.a.to_json(), 'b': self.b.to_json()}


@dc.dataclass(frozen=True)
class FindingChanges:
    """Run B's findings that run A lacks, run A's that run B lacks, and those both report, each in its own run's order."""

    new: tuple[Finding, ...]
    resolved: tuple[Finding, ...]
    persisting: tuple[PersistingFinding, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'new': [finding.to_json() for finding in self.new],
            'resolved': [finding.to_json() for finding in self.resolved],
            'persisting': [pair.to_json() for pair in self.persisting],
        }


def finding_changes(
    a: Sequence[Finding],
    b: Sequence[Finding],
    facts: tuple[RunFacts, RunFacts],
    executions: Alignment[ExecutionMatch],
    stages: Alignment[StageMatch],
) -> FindingChanges:
    """Run A's findings and run B's, matched as the module describes. Each sequence is in its analysis's order, which the result keeps."""
    scopes = {rule.rule_id: rule.scope for rule in catalog()}
    a_keys = _keys(a, 'a', facts[0], scopes, executions, stages)
    b_keys = _keys(b, 'b', facts[1], scopes, executions, stages)
    partner_of: dict[str, Finding] = {}
    matched_a: set[str] = set()
    for step in range(2):
        waiting: dict[Hashable, list[Finding]] = defaultdict(list)
        for finding in sorted(a, key=_position):
            if finding.finding_id not in matched_a and (key := a_keys[finding.finding_id][step]) is not None:
                waiting[key].append(finding)
        for finding in sorted(b, key=_position):
            key = b_keys[finding.finding_id][step]
            if finding.finding_id not in partner_of and key is not None and waiting.get(key):
                partner = waiting[key].pop(0)
                partner_of[finding.finding_id] = partner
                matched_a.add(partner.finding_id)
    return FindingChanges(
        new=tuple(finding for finding in b if finding.finding_id not in partner_of),
        resolved=tuple(finding for finding in a if finding.finding_id not in matched_a),
        persisting=tuple(PersistingFinding(partner_of[finding.finding_id], finding) for finding in b if finding.finding_id in partner_of),
    )


def _keys(
    findings: Sequence[Finding],
    side: str,
    facts: RunFacts,
    scopes: Mapping[str, str],
    executions: Alignment[ExecutionMatch],
    stages: Alignment[StageMatch],
) -> dict[str, tuple[Hashable | None, Hashable | None]]:
    """Each finding's keys for the two passes, in the pairs' terms, which both runs share.

    The second key is the first but for a plan node, whose label it leaves out. Both are None for
    a finding about a part the alignment left unpaired.
    """
    execution_pair = {getattr(pair, side)['execution_id']: index for index, pair in enumerate(executions.paired)}
    stage_pair = {getattr(pair, side)['stage_id']: index for index, pair in enumerate(stages.paired)}
    keys: dict[str, tuple[Hashable | None, Hashable | None]] = {}
    for finding in findings:
        scope = finding.scope
        level = scopes.get(finding.rule_id)
        part: Hashable | None = ()
        label = None
        if level in ('sql_execution', 'plan_node'):
            part = execution_pair.get(scope.sql_execution_id)
        elif level == 'stage':
            part = stage_pair.get(scope.stage_id)
        if level == 'plan_node' and scope.sql_execution_id is not None and scope.plan_node_id is not None:
            label = facts.final_node_labels.get((scope.sql_execution_id, scope.plan_node_id)) or scope.plan_node_label
        if part is None:
            keys[finding.finding_id] = (None, None)
        else:
            keys[finding.finding_id] = ((finding.rule_id, part, label), (finding.rule_id, part))
    return keys


def _position(finding: Finding) -> tuple[Any, ...]:
    """Where a finding's scope sits in its run, so findings sharing a key pair in their order: by execution, node, stage, then executor."""
    scope = finding.scope
    executor = scope.executor_id or ''
    return (
        scope.sql_execution_id if scope.sql_execution_id is not None else -1,
        scope.plan_node_id if scope.plan_node_id is not None else -1,
        scope.stage_id if scope.stage_id is not None else -1,
        not executor.isdigit(),
        int(executor) if executor.isdigit() else 0,
        executor,
    )
