"""Byte counts and durations printed as the Spark UI prints them, so a number sparkxray shows reads as the UI's does.

`bytes_to_string` follows Spark's `Utils.bytesToString`, and `format_duration` its
`UIUtils.formatDuration`. Both round half up, as Java's `String.format` does.
"""

from __future__ import annotations

import decimal

_BYTE_UNITS = ((1 << 60, 'EiB'), (1 << 50, 'PiB'), (1 << 40, 'TiB'), (1 << 30, 'GiB'), (1 << 20, 'MiB'), (1 << 10, 'KiB'), (1, 'B'))


def bytes_to_string(size: int) -> str:
    """`702.5 MiB`: the largest unit the size is at least twice of, to one decimal."""
    scale, unit = next(((scale, unit) for scale, unit in _BYTE_UNITS if size >= 2 * scale), _BYTE_UNITS[-1])
    return f'{_half_up(decimal.Decimal(size) / scale, 1)} {unit}'


def format_duration(milliseconds: int) -> str:
    """`25 ms`, `0.7 s`, `26 s`, `2.3 min`, `45 min` or `1.5 h`, by the steps the Stages tab takes."""
    if milliseconds < 100:
        return f'{milliseconds} ms'
    seconds = decimal.Decimal(milliseconds) / 1000
    if seconds < 1:
        return f'{_half_up(seconds, 1)} s'
    if seconds < 60:
        return f'{_half_up(seconds, 0)} s'
    minutes = seconds / 60
    if minutes < 10:
        return f'{_half_up(minutes, 1)} min'
    if minutes < 60:
        return f'{_half_up(minutes, 0)} min'
    return f'{_half_up(minutes / 60, 1)} h'


def _half_up(value: decimal.Decimal, places: int) -> decimal.Decimal:
    return value.quantize(decimal.Decimal(1).scaleb(-places), rounding=decimal.ROUND_HALF_UP)
