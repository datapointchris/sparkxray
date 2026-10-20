"""A rule as `rules show` teaches it: its lesson at the thresholds applied, and what it found on a committed fixture log.

`lesson_examples.json` beside this module holds one finding per rule, written by the repository's
`fixtures/write_lesson_examples.py` from the fixture logs committed with the source.
"""

from __future__ import annotations

import dataclasses as dc
import functools
import importlib.resources
import json
import os
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from sparkxray.rules.catalog import catalog
from sparkxray.rules.catalog import rule_by_id
from sparkxray.rules.findings import AppliedThreshold
from sparkxray.rules.findings import Finding
from sparkxray.rules.findings import evidence_json
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Severity
from sparkxray.rules.thresholds import resolve_thresholds


@dc.dataclass(frozen=True, slots=True)
class LessonExample:
    """One finding a rule reported on a fixture log, at the default thresholds.

    `source` is the fixture's path under the repository's `fixtures/` directory. `description` is
    what the job, SQL query or run the finding is about was written to show, from its Spark
    description, and None where the fixture gave it none.
    """

    source: str
    description: str | None
    finding_id: str
    severity: Severity
    title: str
    scope: str
    evidence: Mapping[str, Any]

    @classmethod
    def of_finding(cls, finding: Finding, source: str, description: str | None) -> LessonExample:
        return cls(
            source,
            description,
            finding.finding_id,
            finding.severity,
            finding.title,
            finding.scope.describe(),
            evidence_json(finding.evidence),
        )

    def to_json(self) -> dict[str, Any]:
        return {**dc.asdict(self), 'evidence': dict(self.evidence)}


@dc.dataclass(frozen=True)
class RuleLesson:
    """A rule's lesson rendered at the thresholds an analysis would apply, with each threshold's value and who set it."""

    rule: Rule
    thresholds: Mapping[str, AppliedThreshold]
    text: str
    example: LessonExample | None

    def to_json(self) -> dict[str, Any]:
        return {
            **self.rule.to_json(),
            'lesson': self.text,
            'applied_thresholds': {name: applied.to_json() for name, applied in self.thresholds.items()},
            'example': None if self.example is None else self.example.to_json(),
        }


def rule_lesson(
    rule_id: str,
    *,
    thresholds: Mapping[str, Mapping[str, object]] | None = None,
    threshold_file: str | os.PathLike[str] | None = None,
    threshold_flags: Iterable[str] = (),
) -> RuleLesson:
    """One rule's lesson, stating each threshold at the value `analyze` would apply given the same overrides."""
    rule = rule_by_id(rule_id)
    applied = resolve_thresholds([rule], catalog(), sdk=thresholds, file=threshold_file, flags=threshold_flags)[rule.rule_id]
    text = rule.lesson_text({name: threshold.value for name, threshold in applied.items()})
    return RuleLesson(rule, applied, text, lesson_examples().get(rule.rule_id))


@functools.cache
def lesson_examples() -> Mapping[str, LessonExample]:
    """The fixture finding each of sparkxray's own rules shows at the end of its lesson, by rule id."""
    stored = json.loads(importlib.resources.files('sparkxray.rules').joinpath('lesson_examples.json').read_text())
    return {rule_id: LessonExample(**example) for rule_id, example in stored.items()}
