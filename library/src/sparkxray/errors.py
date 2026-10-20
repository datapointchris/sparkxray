"""Every error sparkxray raises, one class per failure, each with a stable code a caller branches on.

A code is never reused or renumbered. A retired code keeps its class with `retired` set, so the
registry still lists it. `registered()` is the registry, so a code is declared in one place.
"""

from __future__ import annotations

import difflib
from collections.abc import Sequence
from typing import Any
from typing import ClassVar

_CODES: dict[str, type[SparkxrayError]] = {}
_VALID_SHOWN = 10


class SparkxrayError(Exception):
    """Base of every error sparkxray raises.

    `field` names what the caller passed that failed, `valid` lists what would have been
    accepted there, and `hint` is the next thing to try. A hint starting `sparkxray` is a command
    that runs as written. `exit_code` is the status a command exits with on this error: 2 when
    the input was wrong, 3 when a person must act.
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

    def to_json(self) -> dict[str, Any]:
        """The error as the `errors` schema's envelope holds it, under `error`, with every valid value."""
        return {'code': self.code, 'message': self.message, 'field': self.field, 'valid': list(self.valid), 'hint': self.hint}


def registered() -> tuple[type[SparkxrayError], ...]:
    """Every error class, retired ones included, in code order."""
    return tuple(_CODES[code] for code in sorted(_CODES))


def suggest_nearest(message: str, typed: str, valid: Sequence[str]) -> str:
    """`message`, asking after the valid value nearest `typed` where one is close enough to be a typo of it."""
    nearest = difflib.get_close_matches(typed, valid, n=1)
    return f'{message}; did you mean {nearest[0]}?' if nearest else message


class NoEventLogError(SparkxrayError):
    code = 'E-INGEST-001'
    raised_when = (
        'the path or prefix holds no event log, listing up to ten of the files it holds; '
        "or it names neither a path nor a cached log's app id, listing the cached app ids"
    )
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


class AppBesideNonServerError(SparkxrayError):
    code = 'E-INGEST-007'
    raised_when = 'an app id is given beside a location that is no server address, where only a server holds several applications'
    exit_code = 2


class UnreachableServerError(SparkxrayError):
    code = 'E-REST-001'
    raised_when = (
        'a History Server or driver UI did not answer, answered 5xx after its retries, answered another error status, '
        'or answered with what is not its REST API; names the URL and the last error'
    )
    exit_code = 3


class UnknownApplicationError(SparkxrayError):
    code = 'E-REST-002'
    raised_when = (
        'a History Server or driver UI holds no application with the app id, or no attempt with the attempt id; '
        'lists the most recent app ids it holds, or the attempts'
    )
    exit_code = 2


class EnrichmentDeniedError(SparkxrayError):
    code = 'E-ENRICH-001'
    raised_when = 'AWS denied a CloudWatch or CloudWatch Logs read, or found no credentials or region; names the action and the resource'
    exit_code = 3


class UnstructuredDriverLogError(SparkxrayError):
    code = 'E-ENRICH-002'
    raised_when = (
        "a driver log's first 64 KiB hold no record of Spark's structured logging, as a plain-text log4j layout writes; "
        'names the file and its first line'
    )
    exit_code = 3


class NoGlueMetricsError(SparkxrayError):
    code = 'E-ENRICH-003'
    raised_when = (
        'returned as a warning beside the enrichment, never raised: CloudWatch lists no Glue observability metric for the job run, '
        'as where it ran without --enable-observability-metrics, or over two weeks ago'
    )
    exit_code = 3


class NoDriverLogError(SparkxrayError):
    code = 'E-ENRICH-004'
    raised_when = 'a driver log names no file, S3 object or CloudWatch Logs stream that exists'
    exit_code = 3


class UnknownRuleError(SparkxrayError):
    code = 'E-RULE-001'
    raised_when = 'a rule id is not in the catalog; lists the rule ids it holds'
    exit_code = 2


class UnknownThresholdError(SparkxrayError):
    code = 'E-RULE-002'
    raised_when = "a threshold name is not one of its rule's; lists the rule's thresholds"
    exit_code = 2


class ThresholdValueError(SparkxrayError):
    code = 'E-RULE-003'
    raised_when = (
        "a threshold value does not parse or carries another unit than the threshold's, a rule's thresholds are not a mapping, "
        'or the thresholds file is missing or not TOML'
    )
    exit_code = 2


class InvalidRuleError(SparkxrayError):
    code = 'E-RULE-004'
    raised_when = (
        'a rule is malformed or its id is taken, or a finding it returned breaks the finding contract; names the rule and what is wrong'
    )
    exit_code = 2


class NotAnalyzedError(SparkxrayError):
    code = 'E-RULE-005'
    raised_when = "a log's findings were read before any analysis of it wrote them"
    exit_code = 2


class UnknownSeverityError(SparkxrayError):
    code = 'E-RULE-006'
    raised_when = 'a severity is not one of info, low, medium, high or critical'
    exit_code = 2


class QueryError(SparkxrayError):
    code = 'E-QUERY-001'
    raised_when = "SQL over a log's tables failed; carries DuckDB's message"
    exit_code = 2


class NotInRunError(SparkxrayError):
    code = 'E-QUERY-002'
    raised_when = 'a stage, SQL execution, plan version or finding named is not in the run; lists those it holds'
    exit_code = 2


class UnknownColumnError(SparkxrayError):
    code = 'E-QUERY-003'
    raised_when = 'a list is sorted by a column it does not have; lists its columns'
    exit_code = 2


class UnknownMetricError(SparkxrayError):
    code = 'E-METRIC-001'
    raised_when = 'a metric name is no column, Spark field or plan metric the glossary describes; lists the names it holds'
    exit_code = 2


class SimulationStalledError(SparkxrayError):
    code = 'E-SIM-001'
    raised_when = 'the executor simulation could not finish a job group, because its stages read each other in a cycle; names the stages'
    exit_code = 3


class UnknownSchemaError(SparkxrayError):
    code = 'E-SCHEMA-001'
    raised_when = 'a schema name is not one sparkxray ships; lists the names it does'
    exit_code = 2


class CommandLineError(SparkxrayError):
    code = 'E-USAGE-001'
    raised_when = (
        'the command names no such command or option, lacks a required one, or gives a value its parameter refuses; '
        'names the parameter and, for a choice, its values. Only the command raises it'
    )
    exit_code = 2
