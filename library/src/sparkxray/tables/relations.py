"""One log's Parquet tables, read through a DuckDB connection that has a view over each."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.registry import TABLES
from sparkxray.tables.registry import TASKS_EFFECTIVE


class Tables:
    """The tables built from one log, each a lazy DuckDB relation.

    `path` is the directory holding `<table>.parquet` and `manifest.json`, which DuckDB or any
    Parquet reader can open without sparkxray. `tasks_effective` is a view over `tasks` and
    `stages`, and every rule reads it rather than `tasks`.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.manifest = TablesManifest.from_json(json.loads((path / 'manifest.json').read_text()))
        self.connection = duckdb.connect()
        for table in TABLES:
            self.connection.execute(f'CREATE VIEW {table.name} AS SELECT * FROM read_parquet({_literal(path / f"{table.name}.parquet")})')
        self.connection.execute(TASKS_EFFECTIVE)

    def query(self, sql: str, parameters: object = None) -> duckdb.DuckDBPyRelation:
        """A relation over any SQL that reads the tables, by their names, with `$name` or `?` parameters."""
        return self.connection.sql(sql, params=parameters)

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


def _literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
