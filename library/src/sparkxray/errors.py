"""Every error sparkxray raises, one class per failure, each with a stable code a caller branches on.

A code is never reused or renumbered. A retired code keeps its class with `retired` set, so the
registry still lists it. `registered()` is the registry, so a code is declared in one place.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

_CODES: dict[str, type[SparkxrayError]] = {}
_VALID_SHOWN = 10


class SparkxrayError(Exception):
    """Base of every error sparkxray raises.

    `field` names what the caller passed that failed, `valid` lists what would have been
    accepted there, and `hint` is the next thing to try. `exit_code` is the status a command
    exits with on this error: 2 when the input was wrong, 3 when a person must act.
    """

    code: ClassVar[str]
    raised_when: ClassVar[str]
    exit_code: ClassVar[int]
    retired: ClassVar[bool] = False

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if cls.code in _CODES:
            raise TypeError(f'{cls.__name__} reuses {cls.code}, which {_CODES[cls.code].__name__} holds')
        _CODES[cls.code] = cls

    def __init__(self, message: str, *, field: str | None = None, valid: Sequence[str] = (), hint: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.field = field
        self.valid = tuple(valid)
        self.hint = hint

    def __str__(self) -> str:
        text = f'{self.code}: {self.message}'
        if self.valid:
            text += '\n  valid:' + ''.join(f'\n    {value}' for value in self.valid[:_VALID_SHOWN])
            if len(self.valid) > _VALID_SHOWN:
                text += f'\n    and {len(self.valid) - _VALID_SHOWN} more'
        if self.hint:
            text += f'\n  hint: {self.hint}'
        return text


def registered() -> tuple[type[SparkxrayError], ...]:
    """Every error class, retired ones included, in code order."""
    return tuple(_CODES[code] for code in sorted(_CODES))


class NoEventLogError(SparkxrayError):
    code = 'E-INGEST-001'
    raised_when = 'the path or prefix holds no event log; lists up to ten of the files it holds'
    exit_code = 3


class AwsDeniedError(SparkxrayError):
    code = 'E-INGEST-002'
    raised_when = 'AWS denied an S3 call, or found no credentials; names the action and the key'
    exit_code = 3


class NotAnEventLogError(SparkxrayError):
    code = 'E-INGEST-003'
    raised_when = 'the location names one log, and its decoded first line is not SparkListenerLogStart'
    exit_code = 3


class UnsupportedCodecError(SparkxrayError):
    code = 'E-INGEST-004'
    raised_when = 'the log is lzf-compressed, which sparkxray cannot decode'
    exit_code = 3


class AmbiguousLocationError(SparkxrayError):
    code = 'E-INGEST-005'
    raised_when = 'the location holds more than one log, or a log beside files of Spark events that start no log; lists them'
    exit_code = 2


class CorruptLogError(SparkxrayError):
    code = 'E-INGEST-006'
    raised_when = (
        "a compressed block does not decode, a file other than the log's last ends partway through a block, a frame or a line, "
        'or a line is not JSON; names the file and line'
    )
    exit_code = 3
