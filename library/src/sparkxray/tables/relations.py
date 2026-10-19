"""One log's Parquet tables, read through a DuckDB connection that has a view over each."""

from __future__ import annotations

import dataclasses as dc
import json
from pathlib import Path
from typing import Any

import duckdb

from sparkxray.errors import NotAnalyzedError
from sparkxray.errors import QueryError
from sparkxray.plan.last_plans import plan_for_event
from sparkxray.plan.tree import Plan
from sparkxray.rules.findings import ANALYSIS_METADATA_KEY
from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.registry import TABLES
from sparkxray.tables.registry import TASKS_EFFECTIVE
from sparkxray.tables.registry import column_unit
from sparkxray.tables.rows import Rows
from sparkxray.tables.rows import rows_of

FINDINGS_FILE = 'findings.parquet'
_TASKS_EFFECTIVE_DESCRIPTION = (
    'A view of `tasks` where `effective` is true: each partition of each stage once, its first success across the stage attempts.'
)
_FINDINGS_DESCRIPTION = "The last analysis's findings, one row each, with their evidence and thresholds as JSON text."


@dc.dataclass(frozen=True, slots=True)
class ListedColumn:
    """One column of a table: its name, its DuckDB type, and the unit its name ends in, where it names one."""

    name: str
    type: str
    unit: str | None

    def to_json(self) -> dict[str, Any]:
        return {'name': self.name, 'type': self.type, 'unit': self.unit}


@dc.dataclass(frozen=True, slots=True)
class ListedTable:
    """One table or view a query can read, how many rows it holds, and the Parquet file behind it."""

    name: str
    description: str
    row_count: int
    path: str
    columns: tuple[ListedColumn, ...]

    def to_json(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'description': self.description,
            'row_count': self.row_count,
            'path': self.path,
            'columns': [column.to_json() for column in self.columns],
        }


class Tables:
    """The tables built from one log, each a lazy DuckDB relation.

    `path` is the directory holding `<table>.parquet` and `manifest.json`, which DuckDB or any
    Parquet reader can open without sparkxray. `tasks_effective` is `tasks` where `effective` is
    true, and every rule reads it rather than `tasks`. `findings` exists once the log has been
    analyzed, and holds the last analysis's findings.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.manifest = TablesManifest.from_json(json.loads((path / 'manifest.json').read_text()))
        self.connection = duckdb.connect()
        for table in TABLES:
            self.connection.execute(f'CREATE VIEW {table.name} AS SELECT * FROM read_parquet({_literal(path / f"{table.name}.parquet")})')
        self.connection.execute(TASKS_EFFECTIVE)
        if (path / FINDINGS_FILE).is_file():
            self.register_findings()

    def register_findings(self) -> None:
        """Point the `findings` view at `findings.parquet`, which `analyze` calls after writing it."""
        self.connection.execute(f'CREATE OR REPLACE VIEW findings AS SELECT * FROM read_parquet({_literal(self.path / FINDINGS_FILE)})')

    def query(self, sql: str, parameters: object = None) -> duckdb.DuckDBPyRelation:
        """A relation over any SQL that reads the tables, by their names, with `$name` or `?` parameters."""
        return self.connection.sql(sql, params=parameters)

    def rows(self, sql: str, parameters: object = None) -> Rows:
        """The rows of a caller's SQL over the tables. SQL DuckDB refuses raises `QueryError` with DuckDB's message."""
        try:
            return rows_of(self.connection.sql(sql, params=parameters))
        except duckdb.Error as error:
            names = [table.name for table in TABLES] + ['tasks_effective'] + (['findings'] if (self.path / FINDINGS_FILE).is_file() else [])
            raise QueryError(str(error), field='sql', valid=names) from None

    def listing(self) -> tuple[ListedTable, ...]:
        """Every table and view a query can read, in the order the build writes them, with `findings` last once it exists."""
        listed = [
            ListedTable(
                table.name,
                table.description,
                self.manifest.row_counts[table.name],
                str(self.path / f'{table.name}.parquet'),
                tuple(ListedColumn(column.name, column.type, column_unit(column.name)) for column in table.columns),
            )
            for table in TABLES
        ]
        tasks = next(table for table in listed if table.name == 'tasks')
        listed.insert(listed.index(tasks) + 1, self._view_listing('tasks_effective', _TASKS_EFFECTIVE_DESCRIPTION, tasks.path))
        if (self.path / FINDINGS_FILE).is_file():
            listed.append(self._view_listing('findings', _FINDINGS_DESCRIPTION, str(self.path / FINDINGS_FILE)))
        return tuple(listed)

    def _view_listing(self, name: str, description: str, path: str) -> ListedTable:
        described = self.connection.execute(f'DESCRIBE {name}').fetchall()
        count = self.connection.execute(f'SELECT count(*) FROM {name}').fetchone()
        columns = tuple(ListedColumn(column, kind, column_unit(column)) for column, kind, *_ in described)
        return ListedTable(name, description, int(count[0]) if count else 0, path, columns)

    def last_plans(self) -> dict[int, Plan]:
        """Each SQL execution's last plan, read from the event that logged it as `sparkxray.plan.last_plans` reads it."""
        rows = self.connection.execute(
            'SELECT execution_id, plan_version_count - 1, final_plan_json, final_plan_text FROM sql_executions '
            'WHERE plan_version_count > 0 ORDER BY execution_id'
        ).fetchall()
        plans = {}
        for execution_id, version, info, text in rows:
            event = {'sparkPlanInfo': json.loads(info) if info is not None else None, 'physicalPlanDescription': text}
            plan = plan_for_event(event, execution_id, version)
            if plan is not None:
                plans[execution_id] = plan
        return plans

    @property
    def application(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('application')

    @property
    def environment(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('environment')

    @property
    def executors(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('executors')

    @property
    def jobs(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('jobs')

    @property
    def stages(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('stages')

    @property
    def tasks(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('tasks')

    @property
    def tasks_effective(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('tasks_effective')

    @property
    def sql_executions(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('sql_executions')

    @property
    def plan_nodes(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('plan_nodes')

    @property
    def plan_node_metrics(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('plan_node_metrics')

    @property
    def node_stages(self) -> duckdb.DuckDBPyRelation:
        return self.connection.view('node_stages')

    @property
    def findings(self) -> duckdb.DuckDBPyRelation:
        """The last analysis's findings, one row each. `stored_analysis` says which rules ran to find them."""
        self._analyzed()
        return self.connection.view('findings')

    def stored_analysis(self) -> dict[str, Any]:
        """The last analysis's document without its findings: the signals it read, the rules it ran and skipped, and their thresholds."""
        path = self._analyzed()
        rows = self.connection.execute(
            'SELECT value FROM parquet_kv_metadata(?) WHERE decode(key) = ?', [str(path), ANALYSIS_METADATA_KEY]
        ).fetchall()
        document: dict[str, Any] = json.loads(rows[0][0])
        return document

    def _analyzed(self) -> Path:
        path = self.path / FINDINGS_FILE
        if not path.is_file():
            raise NotAnalyzedError(
                f'the log {self.manifest.ingest.app_id} has not been analyzed, so it has no findings yet',
                hint='call analyze() on its EventLog',
            )
        return path


def _literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
