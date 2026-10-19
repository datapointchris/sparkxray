"""Read the metric strings the History Server displays back into numbers."""

from __future__ import annotations

import dataclasses as dc
import re
from typing import Literal

# The units Spark's Utils.bytesToString and Utils.msDurationToString write.
_BYTES = {'B': 1, 'KiB': 2**10, 'MiB': 2**20, 'GiB': 2**30, 'TiB': 2**40, 'PiB': 2**50, 'EiB': 2**60}
_MILLISECONDS = {'ms': 1, 's': 1000, 'm': 60_000, 'h': 3_600_000}
_VALUE = r'-?[\d,]+(?:\.\d+)?(?: (?:B|KiB|MiB|GiB|TiB|PiB|EiB|ms|s|m|h))?'
_DISPLAY = re.compile(
    rf'(?P<total>{_VALUE})?\s*'
    rf'(?:\((?P<min>{_VALUE}), (?P<median>{_VALUE}), (?P<max>{_VALUE}) '
    r'\((?:stage (?P<stage>\d+)\.(?P<attempt>\d+): task (?P<task>\d+)|driver)\)\))?'
)
_HEADER = 'total (min, med, max (stageId: taskId))'


@dc.dataclass(frozen=True, slots=True)
class DisplayValue:
    """One metric's display string as numbers, in bytes, milliseconds or a plain count.

    An average metric prints no total. A metric with one value prints no min, median or max, and
    neither does a driver-side one, so those fields are None there. `max_stage_id`,
    `max_stage_attempt` and `max_task_id` name the task that held the maximum.
    """

    unit: Literal['bytes', 'ms', 'count']
    total: float | None
    minimum: float | None
    median: float | None
    maximum: float | None
    max_stage_id: int | None
    max_stage_attempt: int | None
    max_task_id: int | None


def parse_display_value(text: str) -> DisplayValue:
    """Parse a metric value as the History Server's SQL endpoint returns it.

    Handles `1,000`, `8.2 MiB`, `1.5 s`, the multi-line
    `total (min, med, max (stageId: taskId))\\n12 ms (6 ms, 6 ms, 6 ms (stage 28.0: task 37))`
    form, an average's `(1.0, 1.5, 2.0 (stage 3.0: task 4))`, and a `name=value` prefix.
    Raises ValueError on any other string.
    """
    body = text.strip()
    if body.startswith(_HEADER):
        body = body[len(_HEADER) :].strip()
    if '=' in body:
        body = body.rsplit('=', 1)[1].strip()
    match = _DISPLAY.fullmatch(body)
    if match is None or not (match['total'] or match['max']):
        raise ValueError(f'not a Spark metric display value: {text!r}')
    values = [match[group] for group in ('total', 'min', 'median', 'max') if match[group]]
    unit = _unit(values)
    return DisplayValue(
        unit=unit,
        total=_number(match['total'], unit),
        minimum=_number(match['min'], unit),
        median=_number(match['median'], unit),
        maximum=_number(match['max'], unit),
        max_stage_id=_integer(match['stage']),
        max_stage_attempt=_integer(match['attempt']),
        max_task_id=_integer(match['task']),
    )


def _unit(values: list[str]) -> Literal['bytes', 'ms', 'count']:
    suffixes = {value.rpartition(' ')[2] for value in values if ' ' in value}
    if suffixes & _BYTES.keys():
        return 'bytes'
    if suffixes & _MILLISECONDS.keys():
        return 'ms'
    return 'count'


def _number(value: str | None, unit: str) -> float | None:
    if value is None:
        return None
    number, _, suffix = value.partition(' ')
    amount = float(number.replace(',', ''))
    if unit == 'bytes':
        return round(amount * _BYTES.get(suffix, 1))
    if unit == 'ms':
        return round(amount * _MILLISECONDS.get(suffix, 1))
    return int(amount) if amount.is_integer() else amount


def _integer(value: str | None) -> int | None:
    return None if value is None else int(value)
