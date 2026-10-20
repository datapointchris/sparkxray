"""Rules over Python code a plan runs: a plain UDF called row by row, and Arrow UDFs called batch by batch."""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from typing import Any

from sparkxray.rules.plan.node_reading import OUTPUT_ROWS
from sparkxray.rules.plan.node_reading import counted_nodes
from sparkxray.rules.plan.node_reading import metric
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext

ROW_AT_A_TIME_OPERATORS = frozenset({'BatchEvalPython', 'BatchEvalPythonUDTF'})
# Spark 3.5 and 4.x name some of these differently, and both spellings are here.
ARROW_OPERATORS = frozenset(
    {
        'ArrowEvalPython',
        'ArrowEvalPythonUDTF',
        'MapInPandas',
        'MapInArrow',
        'PythonMapInArrow',
        'FlatMapGroupsInPandas',
        'FlatMapGroupsInArrow',
        'FlatMapCoGroupsInPandas',
        'FlatMapCoGroupsInArrow',
        'FlatMapGroupsInPandasWithState',
        'TransformWithStateInPySpark',
        'AggregateInPandas',
        'ArrowAggregatePython',
        'WindowInPandas',
        'ArrowWindowPython',
    }
)

# The stages that ran the node, their share of every effective task's run time, and the Python workers' peak memory in them.
_STAGES_SQL = """
WITH running AS (SELECT DISTINCT stage_id FROM node_stages WHERE execution_id = $execution_id AND node_id = $node_id)
SELECT
  (SELECT list(stage_id ORDER BY stage_id) FROM running) AS stage_ids,
  sum(executor_run_time_ms) FILTER (WHERE stage_id IN (SELECT stage_id FROM running)) / nullif(sum(executor_run_time_ms), 0) AS share,
  max(peak_python_rss_bytes) FILTER (WHERE stage_id IN (SELECT stage_id FROM running)) AS peak_python_rss_bytes
FROM tasks_effective
"""


def _python_nodes(operators: frozenset[str]) -> Callable[[RuleContext], Iterator[dict[str, Any]]]:
    def check(context: RuleContext) -> Iterator[dict[str, Any]]:
        for execution_id, _, numbered in counted_nodes(context):
            if numbered.node.operator not in operators:
                continue
            metrics = context.metrics(execution_id, numbered.number)
            finding: dict[str, Any] = {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'info',
                'impact_seconds': None,
                'python_operator': numbered.node.operator,
                'output_rows': metric(metrics, OUTPUT_ROWS),
                'sent_to_python_bytes': metric(metrics, 'data sent to Python workers'),
                'returned_from_python_bytes': metric(metrics, 'data returned from Python workers'),
            }
            if context.tables is not None and execution_id is not None:
                parameters = {'execution_id': execution_id, 'node_id': numbered.number}
                stage_ids, share, peak = context.tables.query(_STAGES_SQL, parameters).fetchall()[0]
                finding |= {
                    'stage_ids': tuple(stage_ids or ()),
                    'stage_run_time_share': share,
                    'peak_python_rss_bytes': peak,
                }
            yield finding

    return check


_ROW_AT_A_TIME_LESSON = """\
## What it measures

A `BatchEvalPython` node runs a plain Python UDF, one declared with `udf()` or `@udf`. Spark
pickles rows to a Python worker, which calls the function once for each row.

## Why it costs time

Each row crosses from the JVM to Python and back, pickled both ways, and the function runs in the
interpreter one call at a time. That work happens outside the JVM, so no SQL metric times it, and
it shows only in the task time of the stages running it. The optimizer sees nothing inside the
function, so it pushes no filter through it.

## Where the Spark UI shows it

SQL tab › the query › the `BatchEvalPython` node, with `data sent to Python workers` and `data
returned from Python workers`. On the Stages tab, the stages running it carry its time in their
task durations. With a log, the finding names those stages and their share of all task run time.

## How to fix it

- Replace the function with a built-in from `pyspark.sql.functions`, which runs in the JVM.
- Otherwise declare it as a `pandas_udf`, which runs over Arrow batches. From Spark 3.5,
  `udf(..., useArrow=True)` or `spark.sql.execution.pythonUDF.arrow.enabled` runs a plain UDF over
  Arrow too.
"""

PYTHON_UDF_ROW_AT_A_TIME = Rule(
    rule_id='plan.python-udf-row-at-a-time',
    title='{python_operator} calls a Python function once for every row',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(),
    severity_ladder={'info': 'the plan runs a plain Python UDF'},
    remediation=(
        'replace the function with a built-in from pyspark.sql.functions',
        'declare it as a pandas_udf, or pass useArrow=True on Spark 3.5 and later',
    ),
    docs=('https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html',),
    lesson=_ROW_AT_A_TIME_LESSON,
    check=_python_nodes(ROW_AT_A_TIME_OPERATORS),
)

_ARROW_LESSON = """\
## What it measures

An Arrow Python node: `ArrowEvalPython` for a `pandas_udf`, `MapInPandas` or `MapInArrow` for
`mapInPandas` and `mapInArrow`, `FlatMapGroupsInPandas` for `applyInPandas`, and the aggregate and
window forms. Spark sends columnar batches to a Python worker, which calls the function once for
each batch, or once for each group.

## Why it costs time

Arrow moves a batch at a time with no pickling per row, so it costs far less than a plain UDF. The
function still runs in a separate Python process, so its time and its memory are outside every JVM
metric. A batch, or a whole group for `applyInPandas`, must fit in that process's memory, which is
where these nodes usually fail.

## Where the Spark UI shows it

SQL tab › the query › the Arrow node, with `data sent to Python workers` and `data returned from
Python workers`. The event log carries the workers' memory as `ProcessTreePythonRSSMemory` once
`spark.executor.processTreeMetrics.enabled` is on, and the finding reports it as
`peak_python_rss_bytes`.

## How to fix it

- Replace the function with a built-in from `pyspark.sql.functions` where one exists.
- Keep each group small for `applyInPandas`: group on a finer key, or salt a hot one.
- Size batches with `spark.sql.execution.arrow.maxRecordsPerBatch`, and give the worker its own
  memory with `spark.executor.pyspark.memory`.
"""

PYTHON_UDF_ARROW = Rule(
    rule_id='plan.python-udf-arrow',
    title='{python_operator} runs Python over Arrow batches, outside every JVM metric',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(),
    severity_ladder={'info': 'the plan runs Python over Arrow batches'},
    remediation=(
        'replace the function with a built-in from pyspark.sql.functions where one exists',
        'keep each applyInPandas group small, on a finer or salted key',
        'size batches with spark.sql.execution.arrow.maxRecordsPerBatch, and the worker with spark.executor.pyspark.memory',
    ),
    docs=('https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html',),
    lesson=_ARROW_LESSON,
    check=_python_nodes(ARROW_OPERATORS),
)

RULES = (PYTHON_UDF_ROW_AT_A_TIME, PYTHON_UDF_ARROW)
