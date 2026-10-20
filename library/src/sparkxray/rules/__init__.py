"""Heuristic rules over a Spark run, and the findings they report in a versioned shape."""

from sparkxray.rules.analysis import Analysis
from sparkxray.rules.analysis import Gate
from sparkxray.rules.analysis import RuleRun
from sparkxray.rules.analysis import SkippedRule
from sparkxray.rules.analysis import analyze
from sparkxray.rules.analysis import analyze_plan
from sparkxray.rules.analysis import gate_findings
from sparkxray.rules.catalog import catalog
from sparkxray.rules.catalog import register
from sparkxray.rules.catalog import rule_by_id
from sparkxray.rules.findings import AppliedThreshold
from sparkxray.rules.findings import Finding
from sparkxray.rules.findings import FindingScope
from sparkxray.rules.lessons import LessonExample
from sparkxray.rules.lessons import RuleLesson
from sparkxray.rules.lessons import rule_lesson
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Severity
from sparkxray.rules.rule import Signal
from sparkxray.rules.rule import Threshold

__all__ = [
    'Analysis',
    'AppliedThreshold',
    'Finding',
    'FindingScope',
    'Gate',
    'LessonExample',
    'Rule',
    'RuleContext',
    'RuleLesson',
    'RuleRun',
    'Severity',
    'Signal',
    'SkippedRule',
    'Threshold',
    'analyze',
    'analyze_plan',
    'catalog',
    'gate_findings',
    'register',
    'rule_by_id',
    'rule_lesson',
]
