"""One Spark application's events as Parquet tables, built with DuckDB and kept in the cache.

`Tables`, which opens them, lives in `sparkxray.tables.relations`, so a module reading the registry
alone, as every rule does, loads no DuckDB.
"""

from sparkxray.tables.manifest import TablesManifest
from sparkxray.tables.registry import ENRICHMENT_TABLES
from sparkxray.tables.registry import TABLES

__all__ = ['ENRICHMENT_TABLES', 'TABLES', 'TablesManifest']
