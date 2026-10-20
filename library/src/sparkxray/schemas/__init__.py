"""The JSON Schemas sparkxray ships, each read by the name `schemas show` takes, and each naming what prints it.

A schema covers a document a program keeps or checks, the error envelope, or a row of a table. A
command's listing, such as `stages list --json`, has none.

Every schema but `errors` and `tables` is a file in this package. Those two are built from the error
registry and the table registry when they are read, so neither can drift from the code. A `$ref` to another document names its file, as
`findings.json` does for the document named `findings`.
"""

from __future__ import annotations

import dataclasses as dc
import importlib.resources
import json
from collections.abc import Callable
from collections.abc import Mapping
from typing import Any

from sparkxray.errors import UnknownSchemaError
from sparkxray.errors import registered
from sparkxray.rules.findings import FINDINGS_PARQUET_COLUMNS
from sparkxray.rules.findings import FINDINGS_TABLE_DESCRIPTION
from sparkxray.rules.findings import findings_column_descriptions
from sparkxray.tables.registry import TABLES
from sparkxray.tables.registry import TASKS_EFFECTIVE_DESCRIPTION
from sparkxray.tables.registry import column_unit

_DRAFT = 'https://json-schema.org/draft/2020-12/schema'
_JSON_TYPES = {'BIGINT': 'integer', 'DOUBLE': 'number', 'BOOLEAN': 'boolean', 'VARCHAR': 'string'}


@dc.dataclass(frozen=True, slots=True)
class QueryableColumn:
    """One column a query can read: its name, its DuckDB type, and what it holds."""

    name: str
    type: str
    description: str | None


@dc.dataclass(frozen=True, slots=True)
class QueryableTable:
    """One table or view `tables query` reads, as every log's tables hold it."""

    name: str
    description: str
    columns: tuple[QueryableColumn, ...]


@dc.dataclass(frozen=True, slots=True)
class SchemaSummary:
    """One schema sparkxray ships: the name `schemas show` takes, and its title."""

    name: str
    title: str

    def to_json(self) -> dict[str, Any]:
        return {'name': self.name, 'title': self.title}


def queryable_tables() -> tuple[QueryableTable, ...]:
    """Every table and view a query reads, in the order the build writes them: `tasks_effective` after `tasks`, `findings` last.

    `findings` exists in a log's tables once the log is analyzed.
    """
    tables = [
        QueryableTable(table.name, table.description, tuple(QueryableColumn(c.name, c.type, c.description) for c in table.columns))
        for table in TABLES
    ]
    tasks = next(table for table in tables if table.name == 'tasks')
    tables.insert(tables.index(tasks) + 1, QueryableTable('tasks_effective', TASKS_EFFECTIVE_DESCRIPTION, tasks.columns))
    described = findings_column_descriptions()
    findings = tuple(QueryableColumn(name, kind, described.get(name)) for name, kind in FINDINGS_PARQUET_COLUMNS)
    tables.append(QueryableTable('findings', FINDINGS_TABLE_DESCRIPTION, findings))
    return tuple(tables)


def schema_names() -> tuple[str, ...]:
    """Every name `schema_document` takes, sorted."""
    shipped = (entry.name.removesuffix('.json') for entry in importlib.resources.files(__name__).iterdir() if entry.name.endswith('.json'))
    return tuple(sorted({*shipped, *_BUILT}))


def schema_document(name: str) -> dict[str, Any]:
    """One JSON Schema, draft 2020-12. A name sparkxray ships no schema for raises `UnknownSchemaError`, listing those it does."""
    names = schema_names()
    if name not in names:
        raise UnknownSchemaError(f'sparkxray ships no schema called {name!r}', field='name', valid=names, hint='sparkxray schemas list')
    if name in _BUILT:
        return _BUILT[name]()
    document: dict[str, Any] = json.loads(importlib.resources.files(__name__).joinpath(f'{name}.json').read_text())
    return document


def schema_summaries() -> tuple[SchemaSummary, ...]:
    """Every schema sparkxray ships, by name, with its title."""
    return tuple(SchemaSummary(name, schema_document(name)['title']) for name in schema_names())


def _tables_document() -> dict[str, Any]:
    return {
        '$schema': _DRAFT,
        'title': 'sparkxray tables',
        'description': (
            "One row of each table and view a log's tables hold, as `tables query --json` prints it, under $defs by name. "
            'Each column gives its DuckDB type under x-duckdb-type, and the unit its name ends in under x-unit. A number is raw, '
            "in that unit. `tables list LOG` gives the same columns, with each table's row count and Parquet file."
        ),
        '$defs': {table.name: _row_schema(table) for table in queryable_tables()},
    }


def _row_schema(table: QueryableTable) -> dict[str, Any]:
    return {
        'type': 'object',
        'description': table.description,
        'additionalProperties': False,
        'required': [column.name for column in table.columns],
        'properties': {column.name: _column_schema(column) for column in table.columns},
    }


def _column_schema(column: QueryableColumn) -> dict[str, Any]:
    described: dict[str, Any] = {**_value_schema(column.type), 'x-duckdb-type': column.type}
    unit = column_unit(column.name)
    if unit is not None:
        described['x-unit'] = unit
    if column.description is not None:
        described['description'] = column.description
    return described


def _value_schema(kind: str) -> dict[str, Any]:
    """A DuckDB value as JSON, or null, which any column can hold."""
    if kind.endswith('[]'):
        return {'type': ['array', 'null'], 'items': _value_schema(kind[:-2])}
    if kind.startswith('MAP('):
        _, value = kind.removeprefix('MAP(').removesuffix(')').split(', ')
        return {'type': ['object', 'null'], 'additionalProperties': _value_schema(value)}
    return {'type': [_JSON_TYPES[kind], 'null']}


def _errors_document() -> dict[str, Any]:
    codes = []
    for error in registered():
        code: dict[str, Any] = {
            'const': error.code,
            'title': error.__name__,
            'description': error.raised_when,
            'x-exit-code': error.exit_code,
        }
        if error.retired:
            code['deprecated'] = True
        codes.append(code)
    return {
        '$schema': _DRAFT,
        'title': 'sparkxray error',
        'description': (
            'What a command prints on stderr when it refuses, asked for JSON by --json or --format json. Otherwise a library '
            'refusal prints the same content as a sentence, and a command line the parser refuses prints its own text. The command '
            "exits with the code's x-exit-code: 2 when the input was wrong, 3 when a person must act. A code is never reused or "
            'renumbered, and a retired one stays, marked deprecated.'
        ),
        'type': 'object',
        'additionalProperties': False,
        'required': ['error'],
        'properties': {
            'error': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['code', 'message', 'field', 'valid', 'hint'],
                'properties': {
                    'code': {'oneOf': codes},
                    'message': {'type': 'string'},
                    'field': {
                        'type': ['string', 'null'],
                        'description': 'What the caller passed that failed, as a path, such as threshold.stage.shuffle-skew.ratio.',
                    },
                    'valid': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Every value field would have taken.'},
                    'hint': {'type': ['string', 'null'], 'description': 'The next thing to try, often a command.'},
                },
            }
        },
    }


_BUILT: Mapping[str, Callable[[], dict[str, Any]]] = {'errors': _errors_document, 'tables': _tables_document}
