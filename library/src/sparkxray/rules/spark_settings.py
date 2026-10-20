"""The Spark settings a rule judges by: as SQL, a SQL execution's own, then the application's, then the release's default.

A query using these opens its `WITH` with `spark_settings_cte(...)` and `SPARK_RELEASE_CTE`, and
joins `sql_executions` where it reads an execution's `modified_configs`. A memory setting is read
in Python, as Spark reads it, by `memory_setting_bytes`.
"""

from __future__ import annotations

import re

from sparkxray.tables.registry import TASK_CPUS

ADAPTIVE = 'spark.sql.adaptive.enabled'
SKEW_JOIN = 'spark.sql.adaptive.skewJoin.enabled'
SHUFFLE_PARTITIONS = 'spark.sql.shuffle.partitions'
EXCHANGE_REUSE = 'spark.sql.exchange.reuse'
LOG_STAGE_EXECUTOR_METRICS = 'spark.eventLog.logStageExecutorMetrics'
EXECUTOR_MEMORY = 'spark.executor.memory'
DRIVER_MEMORY = 'spark.driver.memory'
DYNAMIC_ALLOCATION = 'spark.dynamicAllocation.enabled'
# Every setting a rule's SQL reads, `spark.task.cpus` through the core time the registry counts.
READ_BY_RULES = (
    ADAPTIVE,
    SKEW_JOIN,
    SHUFFLE_PARTITIONS,
    EXCHANGE_REUSE,
    LOG_STAGE_EXECUTOR_METRICS,
    EXECUTOR_MEMORY,
    DRIVER_MEMORY,
    DYNAMIC_ALLOCATION,
    TASK_CPUS,
)

# Both memory settings default to 1g.
DEFAULT_MEMORY_BYTES = 1073741824
# spark.sql.adaptive.advisoryPartitionSizeInBytes defaults to 64 MB, the size a suggested partition count aims for.
ADVISORY_PARTITION_BYTES = 67108864

# JavaUtils.byteStringAs: a whole number and a binary unit, where a bare number takes the setting's own unit.
_SIZE = re.compile(r'([0-9]+)([a-z]+)?')
_SIZE_UNITS = {
    'b': 1,
    'k': 1 << 10,
    'kb': 1 << 10,
    'm': 1 << 20,
    'mb': 1 << 20,
    'g': 1 << 30,
    'gb': 1 << 30,
    't': 1 << 40,
    'tb': 1 << 40,
    'p': 1 << 50,
    'pb': 1 << 50,
}


def memory_setting_bytes(value: str | None) -> int | None:
    """A memory setting such as `spark.executor.memory` in bytes, where a bare number is MiB. None where Spark would refuse it."""
    if value is None:
        return None
    match = _SIZE.fullmatch(value.strip().lower())
    if match is None or (match[2] is not None and match[2] not in _SIZE_UNITS):
        return None
    return int(match[1]) * _SIZE_UNITS[match[2] or 'm']


# Spark 3.2 turned AQE on by default, so the release reads as major × 100 + minor.
ADAPTIVE_DEFAULT_RELEASE = 302

SPARK_RELEASE_CTE = """spark_release AS (
  SELECT coalesce(try_cast(split_part(spark_version, '.', 1) AS INTEGER), 0) * 100
    + coalesce(try_cast(split_part(spark_version, '.', 2) AS INTEGER), 0) AS number
  FROM application
)"""


def spark_settings_cte(*keys: str) -> str:
    """A one-row `spark_settings` CTE: each key's value among the application's Spark properties, in a column named by the key."""
    columns = ', '.join(f'max(value) FILTER (WHERE key = \'{key}\') AS "{key}"' for key in keys)
    return f"spark_settings AS (SELECT {columns} FROM environment WHERE section = 'spark')"


def setting(key: str, configs: str | None = None) -> str:
    """SQL for a setting's value: `configs[key]` where `configs` is an execution's `modified_configs`, else the application's."""
    application = f'spark_settings."{key}"'
    return application if configs is None else f"coalesce({configs}['{key}'], {application})"


def adaptive_enabled(configs: str | None = None) -> str:
    """SQL that is true where AQE was on, which it is by default from Spark 3.2."""
    default = f"CASE WHEN spark_release.number >= {ADAPTIVE_DEFAULT_RELEASE} THEN 'true' ELSE 'false' END"
    return f"lower(coalesce({setting(ADAPTIVE, configs)}, {default})) = 'true'"


def skew_join_enabled(configs: str | None = None) -> str:
    """SQL that is true where AQE and its skew join were both on. The skew join is on wherever AQE is, unless turned off."""
    return f"{adaptive_enabled(configs)} AND lower(coalesce({setting(SKEW_JOIN, configs)}, 'true')) = 'true'"
