"""The rules an analysis can run: sparkxray's own, and any a caller registers."""

from __future__ import annotations

from sparkxray.errors import InvalidRuleError
from sparkxray.errors import UnknownRuleError
from sparkxray.rules import plan
from sparkxray.rules import stage
from sparkxray.rules.rule import Rule

_RULES: dict[str, Rule] = {rule.rule_id: rule for rule in (*plan.RULES, *stage.RULES)}


def catalog() -> list[Rule]:
    """Every rule, sorted by id."""
    return [_RULES[rule_id] for rule_id in sorted(_RULES)]


def rule_by_id(rule_id: str) -> Rule:
    """The rule with this id. An id the catalog does not hold raises `UnknownRuleError`, listing those it does."""
    if rule_id not in _RULES:
        raise UnknownRuleError(f'no rule is called {rule_id!r}', field='rule_id', valid=sorted(_RULES))
    return _RULES[rule_id]


def register(rule: Rule) -> None:
    """Add a rule to the catalog, so every later analysis can run it. An id already taken is refused."""
    if rule.rule_id in _RULES:
        raise InvalidRuleError(f'a rule called {rule.rule_id!r} is already registered', field='rule', valid=sorted(_RULES))
    _RULES[rule.rule_id] = rule
