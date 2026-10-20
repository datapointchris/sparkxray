"""Sort a list by one of its columns and keep its first rows, as every list a summary returns is sorted and cut."""

from __future__ import annotations

from collections.abc import Mapping

from sparkxray.errors import UnknownColumnError
from sparkxray.tables.relations import Tables
from sparkxray.tables.rows import Rows
from sparkxray.tables.rows import rows_of


def ordered_rows(
    tables: Tables,
    sql: str,
    *,
    order_by: str,
    sort: str | None = None,
    limit: int | None = None,
    parameters: Mapping[str, object] | None = None,
) -> Rows:
    """The rows of `sql`, largest `sort` first with nulls last and then in `order_by`, the first `limit` of them.

    A `sort` that is not one of the rows' columns raises `UnknownColumnError`, listing them.
    """
    columns = tables.query(sql, parameters).columns
    if sort is not None and sort not in columns:
        raise UnknownColumnError(f'this list has no column called {sort!r}', field='sort', valid=columns)
    if limit is not None and limit < 0:
        raise ValueError(f'limit is {limit}, and a list cannot keep fewer than 0 rows')
    order = f'{_identifier(sort)} DESC NULLS LAST, {order_by}' if sort is not None else order_by
    cut = '' if limit is None else f' LIMIT {int(limit)}'
    return rows_of(tables.query(f'SELECT * FROM ({sql}) AS listed ORDER BY {order}{cut}', parameters))


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
