"""Which of run B's exceptions are new, and which healthy runs also throw, so a reader looking for a cause starts at the new ones.

A failing run's log holds exceptions a healthy run throws too: a fetch failure a retry recovered,
or a speculative copy killed because its twin finished. So each exception becomes a key, the one
line naming it with its run's hosts, paths, UUIDs, hex ids and numbers replaced. A key run B shares
with a run that succeeded is `routine`. One it shares only with a failed run A is `recurring`, and
one no other run holds is `new`. A key only run A holds is `resolved`. A run succeeded when its
application ended and every job it ran succeeded, so a failed run marks nothing routine.
"""

from __future__ import annotations

import dataclasses as dc
import re
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Literal

from sparkxray.run_diff.run_facts import LoggedException
from sparkxray.run_diff.run_facts import RunFacts

ExceptionStatus = Literal['new', 'recurring', 'routine', 'resolved']
STATUS_ORDER: tuple[ExceptionStatus, ...] = ('new', 'recurring', 'routine', 'resolved')

# Each rewrite runs in order over the line, after the run's own hosts are replaced. A URI or path
# goes before a number inside it, and a UUID before the hex run inside it.
_REWRITES = (
    (re.compile(r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b'), '<uuid>'),
    (re.compile(r'\b[A-Za-z][A-Za-z0-9+.-]*:/[^\s,;\'")\]]*'), '<path>'),
    (re.compile(r'(?<![^\s(\[\'"=])/[^\s,;\'")\]]+'), '<path>'),
    (re.compile(r'\b\d{1,3}(?:\.\d{1,3}){3}\b'), '<host>'),
    (re.compile(r'\b0x[0-9a-fA-F]+\b'), '<hex>'),
    (re.compile(r'\b(?=[0-9a-fA-F]*[0-9])(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{8,}\b'), '<hex>'),
    (re.compile(r'(?<![A-Za-z_])\d+'), '<n>'),
)


@dc.dataclass(frozen=True, slots=True)
class TriagedException:
    """One exception key, how many times each run logged it, and where it first appeared.

    `first_*` is where run B first logged it, or run A for a `resolved` key. `example` is that
    first line as logged. `baseline_count` counts the baselines that succeeded.
    """

    status: ExceptionStatus
    source: str
    key: str
    example: str
    a_count: int
    b_count: int
    baseline_count: int
    first_task_id: int | None
    first_stage_id: int | None
    first_epoch_ms: int | None

    def to_json(self) -> dict[str, Any]:
        return dc.asdict(self)


def exception_key(logged: LoggedException, hosts: Sequence[str]) -> str:
    """The key an exception counts under: its line with the parts that change from run to run replaced.

    A task's line leads with its end reason, except an `ExceptionFailure`, whose line leads with the exception's class.
    """
    line = logged.line or ''
    for host in hosts:
        line = line.replace(host, '<host>')
    for pattern, placeholder in _REWRITES:
        line = pattern.sub(placeholder, line)
    if logged.reason is not None and logged.reason != 'ExceptionFailure':
        line = f'{logged.reason}: {line}' if line else logged.reason
    return line


def triage(a: RunFacts, b: RunFacts, baselines: Sequence[RunFacts]) -> tuple[TriagedException, ...]:
    """Every key run A or run B logged, new ones first, then recurring, routine and resolved, the most logged first in each."""
    a_keys = _keyed(a)
    b_keys = _keyed(b)
    baseline_counts: dict[tuple[str, str], int] = {}
    for baseline in baselines:
        if baseline.outcome['succeeded']:
            for key, sightings in _keyed(baseline).items():
                baseline_counts[key] = baseline_counts.get(key, 0) + len(sightings)
    a_succeeded = bool(a.outcome['succeeded'])
    triaged = []
    for key in b_keys.keys() | a_keys.keys():
        in_a, in_b = a_keys.get(key, []), b_keys.get(key, [])
        if not in_b:
            status: ExceptionStatus = 'resolved'
        elif (in_a and a_succeeded) or baseline_counts.get(key):
            status = 'routine'
        elif in_a:
            status = 'recurring'
        else:
            status = 'new'
        first = (in_b or in_a)[0]
        triaged.append(
            TriagedException(
                status=status,
                source=key[0],
                key=key[1],
                example=first.line or first.reason or '',
                a_count=len(in_a),
                b_count=len(in_b),
                baseline_count=baseline_counts.get(key, 0),
                first_task_id=first.task_id,
                first_stage_id=first.stage_id,
                first_epoch_ms=first.epoch_ms,
            )
        )
    return tuple(
        sorted(triaged, key=lambda found: (STATUS_ORDER.index(found.status), -max(found.b_count, found.a_count), found.source, found.key))
    )


def _keyed(facts: RunFacts) -> Mapping[tuple[str, str], list[LoggedException]]:
    keyed: dict[tuple[str, str], list[LoggedException]] = {}
    for logged in _in_order(facts.exceptions):
        keyed.setdefault((logged.source, exception_key(logged, facts.hosts)), []).append(logged)
    return keyed


def _in_order(exceptions: Iterable[LoggedException]) -> list[LoggedException]:
    return sorted(exceptions, key=lambda logged: (logged.epoch_ms is None, logged.epoch_ms or 0))
