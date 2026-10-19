"""One log's Parquet tables, read through a DuckDB connection that has a view over each."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from sparkxray.plan.last_plans import plan_for_event
from sparkxray.plan.tree import Plan
from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.registry import TABLES
from sparkxray.tables.registry import TASKS_EFFECTIVE

FINDINGS_FILE = 'findings.parquet'


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

    def last_plans(self) -> dict[int, Plan]:
        """Each SQL execution's last plan, read from the event that logged it as `sparkxray.plan.last_plans` reads it."""
        rows = self.connection.execute(
            'SELECT execution_id, plan_versions - 1, final_plan_json, final_plan_text FROM sql_executions '
            'WHERE plan_versions > 0 ORDER BY execution_id'
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
        """The last analysis's findings. DuckDB raises a CatalogException before the log is analyzed."""
        return self.connection.view('findings')


def _literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"
