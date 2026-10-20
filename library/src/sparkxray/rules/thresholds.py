"""The threshold values an analysis applies: each rule's defaults, overridden by a file, the SDK and flags, in that order."""

from __future__ import annotations

import math
import os
import re
import tomllib
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path

from sparkxray.errors import ThresholdValueError
from sparkxray.errors import UnknownRuleError
from sparkxray.errors import UnknownThresholdError
from sparkxray.errors import suggest_nearest
from sparkxray.rules.findings import AppliedThreshold
from sparkxray.rules.findings import ThresholdLayer
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import Threshold

# Sizes read as Spark reads its own: `m`, `mb` and `MiB` are all 1024². Case does not matter.
_BYTE_UNITS = {
    '': 1,
    'b': 1,
    **{suffix: 1024**power for power, letter in enumerate('kmgtp', start=1) for suffix in (letter, f'{letter}b', f'{letter}ib')},
}
_TIME_UNITS = {'': 1, 'ms': 1, 's': 1000, 'm': 60_000, 'min': 60_000, 'h': 3_600_000, 'd': 86_400_000}
_UNIT_SUFFIXES = {'bytes': _BYTE_UNITS, 'ms': _TIME_UNITS, 'x': {'': 1, 'x': 1}}
_EXAMPLES = {
    'bytes': ('268435456', '256MiB', '512m', '1g'),
    'ms': ('500', '500ms', '30s', '5min', '1h'),
    'x': ('5', '5x', '2.5'),
    'count': ('10',),
    'rows': ('10000000000',),
    'fraction': ('0.05', '0.5'),
}
_TAKES = {
    'bytes': 'a size in bytes',
    'ms': 'a duration in milliseconds',
    'x': 'a multiple',
    'count': 'a whole count',
    'rows': 'a whole number of rows',
    'fraction': 'a fraction from 0 to 1',
}
_VALUE = re.compile(r'(?P<number>[0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?)\s*(?P<unit>[a-z]*)')


def resolve_thresholds(
    rules: Sequence[Rule],
    every_rule: Sequence[Rule],
    *,
    sdk: Mapping[str, Mapping[str, object]] | None = None,
    file: str | os.PathLike[str] | None = None,
    flags: Iterable[str] = (),
) -> dict[str, dict[str, AppliedThreshold]]:
    """The applied thresholds of each rule in `rules`, by rule id and threshold name.

    Overrides are checked against `every_rule`, the whole catalog. So a file naming a rule this
    analysis does not run still reads, and a misspelt rule or threshold is refused either way.
    """
    known = {rule.rule_id: rule for rule in every_rule}
    overrides: dict[tuple[str, str], tuple[float, ThresholdLayer]] = {}
    if file is not None:
        for rule_id, name, raw in _file_overrides(Path(file), known):
            overrides[rule_id, name] = (_value(raw, known[rule_id], name), 'file')
    for rule_id, values in (sdk or {}).items():
        _rule(rule_id, known, field=f'threshold.{rule_id}')
        if not isinstance(values, Mapping):
            raise ThresholdValueError(
                f'the thresholds given for {rule_id} are {values!r}, not a mapping of threshold names to values',
                field=f'threshold.{rule_id}',
                valid=("{'stage.shuffle-skew': {'ratio': 8}}",),
            )
        for name, raw in values.items():
            overrides[rule_id, name] = (_value(raw, known[rule_id], name), 'sdk')
    for flag in flags:
        rule_id, name, raw = _flag(flag, known)
        overrides[rule_id, name] = (_value(raw, known[rule_id], name), 'flag')
    applied: dict[str, dict[str, AppliedThreshold]] = {}
    for rule in rules:
        rule_values = applied[rule.rule_id] = {}
        for threshold in rule.thresholds:
            value, layer = overrides.get((rule.rule_id, threshold.name), (threshold.default, 'default'))
            rule_values[threshold.name] = AppliedThreshold(value, threshold.unit, layer, threshold.default, threshold.source)
    return applied


def parse_threshold_value(raw: object, threshold: Threshold, *, field: str) -> float:
    """A threshold value in its unit, from a number or from a string such as `"512MiB"` or `"30s"`."""
    if isinstance(raw, bool) or not isinstance(raw, int | float | str):
        raise _bad_value(raw, threshold, field)
    if isinstance(raw, str):
        match = _VALUE.fullmatch(raw.strip().lower())
        multipliers = _UNIT_SUFFIXES.get(threshold.unit, {'': 1})
        if match is None or match['unit'] not in multipliers:
            raise _bad_value(raw, threshold, field)
        value = float(match['number']) * multipliers[match['unit']]
    else:
        value = float(raw)
    if not math.isfinite(value) or value < 0:
        raise _bad_value(raw, threshold, field)
    if threshold.unit == 'fraction' and value > 1:
        raise _bad_value(raw, threshold, field)
    if threshold.unit in ('count', 'rows') and not value.is_integer():
        raise _bad_value(raw, threshold, field)
    if threshold.unit == 'bytes':
        value = float(round(value))
    return value


def _file_overrides(path: Path, known: Mapping[str, Rule]) -> Iterable[tuple[str, str, object]]:
    hint = 'give one ["<rule_id>"] table per rule, as in ["stage.shuffle-skew"] ratio = 8'
    try:
        document = tomllib.loads(path.read_text())
    except FileNotFoundError:
        raise ThresholdValueError(f'the thresholds file {path} does not exist', field='thresholds_file', hint=hint) from None
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ThresholdValueError(f'the thresholds file {path} is not TOML: {error}', field='thresholds_file', hint=hint) from None
    for rule_id, values in document.items():
        _rule(rule_id, known, field=f'threshold.{rule_id}')
        if not isinstance(values, dict):
            raise ThresholdValueError(f'{rule_id} in {path} is a value, not a table of thresholds', field=f'threshold.{rule_id}', hint=hint)
        for name, raw in values.items():
            yield rule_id, name, raw


def _flag(flag: str, known: Mapping[str, Rule]) -> tuple[str, str, str]:
    key, equals, raw = flag.partition('=')
    rule_id, dot, name = key.strip().rpartition('.')
    if not equals or not dot:
        raise ThresholdValueError(
            f'the threshold override {flag!r} is not <rule_id>.<threshold>=<value>',
            field=f'threshold.{key.strip()}',
            valid=('stage.shuffle-skew.ratio=8',),
        )
    _rule(rule_id, known, field=f'threshold.{rule_id}')
    return rule_id, name, raw


def _rule(rule_id: str, known: Mapping[str, Rule], *, field: str) -> Rule:
    if rule_id not in known:
        raise unknown_rule(rule_id, sorted(known), field=field)
    return known[rule_id]


def unknown_rule(rule_id: str, known: list[str], *, field: str) -> UnknownRuleError:
    """The refusal of a rule id the catalog does not hold, led by the nearest id it does."""
    return UnknownRuleError(
        suggest_nearest(f'no rule is called {rule_id!r}', rule_id, known), field=field, valid=known, hint='sparkxray rules list'
    )


def _value(raw: object, rule: Rule, name: str) -> float:
    threshold = rule.threshold(name)
    if threshold is None:
        names = [known.name for known in rule.thresholds]
        raise UnknownThresholdError(
            suggest_nearest(f'{rule.rule_id} has no threshold called {name!r}', name, names),
            field=f'threshold.{rule.rule_id}.{name}',
            valid=names,
            hint=f'sparkxray rules show {rule.rule_id}',
        )
    return parse_threshold_value(raw, threshold, field=f'threshold.{rule.rule_id}.{name}')


def _bad_value(raw: object, threshold: Threshold, field: str) -> ThresholdValueError:
    named = field.removeprefix('threshold.')
    return ThresholdValueError(
        f'{named} takes {_TAKES[threshold.unit]}, and {raw!r} is not one', field=field, valid=_EXAMPLES[threshold.unit]
    )
