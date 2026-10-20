"""What a read of the tables returns: its column names, their DuckDB types, and every row as plain Python values."""

from __future__ import annotations

import dataclasses as dc
import datetime as dt
import decimal
import math
import uuid
from typing import Any

import duckdb


@dc.dataclass(frozen=True, slots=True)
class Rows:
    """The rows of one read, fetched.

    Each value is a number, a string, a boolean, None, or a list or dict of those. A decimal
    becomes an int or a float, an instant its ISO 8601 text, and a NaN or infinity None, which
    JSON cannot hold.
    """

    columns: tuple[str, ...]
    types: tuple[str, ...]
    values: tuple[tuple[Any, ...], ...]

    def __len__(self) -> int:
        return len(self.values)

    def to_json(self) -> list[dict[str, Any]]:
        """Each row as a mapping of column name to value."""
        return [dict(zip(self.columns, row, strict=True)) for row in self.values]


def rows_of(relation: duckdb.DuckDBPyRelation | None) -> Rows:
    """Fetch a relation. A statement that returns no relation, such as `CREATE`, gives no rows."""
    if relation is None:
        return Rows((), (), ())
    columns = tuple(relation.columns)
    types = tuple(str(kind) for kind in relation.types)
    return Rows(columns, types, tuple(tuple(_plain(value) for value in row) for row in relation.fetchall()))


def _plain(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dt.date | dt.time):
        return value.isoformat()
    if isinstance(value, dt.timedelta | uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    return value
