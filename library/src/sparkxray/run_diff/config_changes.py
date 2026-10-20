"""How two runs' settings differ: the Spark, Hadoop and system properties added, removed and changed, compared as strings.

Some keys differ on every run, such as the app id and the driver's port, so they are ignored, with
any pattern the caller adds. Spark writes a redacted value as the same mask in both runs, so a key
either run redacted is unknown rather than unchanged.
"""

from __future__ import annotations

import dataclasses as dc
import fnmatch
from collections.abc import Sequence
from typing import Any

from sparkxray.run_diff.run_facts import CONFIG_SECTIONS
from sparkxray.run_diff.run_facts import RunFacts

# The keys Spark sets anew for every run, as glob patterns. A YARN cluster names its proxy in `*.amfilter.*` keys.
ROUTINELY_CHANGED_CONFIG = (
    'spark.app.id',
    'spark.app.startTime',
    'spark.app.submitTime',
    'spark.driver.host',
    'spark.driver.port',
    'spark.driver.appUIAddress',
    '*.amfilter.*',
)
# `Utils.REDACTION_REPLACEMENT_TEXT`, which Spark writes in place of a value its redaction pattern matches.
REDACTED = '*********(redacted)'


@dc.dataclass(frozen=True, slots=True)
class SettingChange:
    """One setting in run A and run B. `a` is None for a key only run B set, and `b` for one only run A set."""

    section: str
    key: str
    a: str | None
    b: str | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


@dc.dataclass(frozen=True)
class ConfigChanges:
    """The settings that differ, each list in section and key order. `ignored` holds every pattern a key was dropped by."""

    ignored: tuple[str, ...]
    added: tuple[SettingChange, ...]
    removed: tuple[SettingChange, ...]
    changed: tuple[SettingChange, ...]
    unknown: tuple[SettingChange, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'ignored': list(self.ignored),
            'added': [setting.to_json() for setting in self.added],
            'removed': [setting.to_json() for setting in self.removed],
            'changed': [setting.to_json() for setting in self.changed],
            'unknown': [setting.to_json() for setting in self.unknown],
        }


def config_changes(a: RunFacts, b: RunFacts, ignore: Sequence[str] = ()) -> ConfigChanges:
    """The settings run B differs from run A in, less the keys every run changes and those matching an `ignore` pattern."""
    ignored = (*ROUTINELY_CHANGED_CONFIG, *(pattern for pattern in ignore if pattern not in ROUTINELY_CHANGED_CONFIG))
    added, removed, changed, unknown = [], [], [], []
    for section, key in sorted(a.config.keys() | b.config.keys(), key=lambda entry: (CONFIG_SECTIONS.index(entry[0]), entry[1])):
        if any(fnmatch.fnmatchcase(key, pattern) for pattern in ignored):
            continue
        in_a, in_b = (section, key) in a.config, (section, key) in b.config
        setting = SettingChange(section, key, a.config.get((section, key)), b.config.get((section, key)))
        if not in_a:
            added.append(setting)
        elif not in_b:
            removed.append(setting)
        elif REDACTED in (setting.a, setting.b):
            unknown.append(setting)
        elif setting.a != setting.b:
            changed.append(setting)
    return ConfigChanges(ignored, tuple(added), tuple(removed), tuple(changed), tuple(unknown))
