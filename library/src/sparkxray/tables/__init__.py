"""One Spark application's events as Parquet tables, built with DuckDB and kept in the cache."""

from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.registry import TABLES
from sparkxray.tables.relations import Tables

__all__ = ['TABLES', 'Tables', 'TablesManifest']
