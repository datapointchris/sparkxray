"""Rules over how a plan filters what it reads: a condition the source left, a partitioned table read whole, and a long condition."""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from sparkxray.plan import labels
from sparkxray.rules.plan.node_reading import OUTPUT_ROWS
from sparkxray.rules.plan.node_reading import field
from sparkxray.rules.plan.node_reading import filter_condition
from sparkxray.rules.plan.node_reading import is_cut
from sparkxray.rules.plan.node_reading import list_entries
from sparkxray.rules.plan.node_reading import metric
from sparkxray.rules.plan.node_reading import scan_location
from sparkxray.rules.plan.node_reading import split_top_level
from sparkxray.rules.rule import Rule
from sparkxray.rules.rule import RuleContext
from sparkxray.rules.rule import Threshold

_ATTRIBUTE = re.compile(r'(?:`(?P<quoted>[^`]+)`|(?P<name>[A-Za-z_]\w*))#\d+L?(?!\w)')
_SOURCE_FILTER = re.compile(r'(?P<name>\w+)\((?P<arguments>.*)\)', re.DOTALL)
_SOURCE_COMBINATORS = frozenset({'And', 'Or', 'Not'})
_NO_PRUNING = re.compile(r'isnotnull\([^()]*\)|dynamicpruningexpression\(true\)')
# Between a filter and the scan it reads, only a conversion from columnar batches to rows.
_ROW_CONVERSIONS = frozenset({'ColumnarToRow'})


def conjuncts(condition: str) -> list[str]:
    """The condition split on its top-level `AND`s, each part as printed."""
    parts = split_top_level(_unwrapped(condition), ' AND ')
    if len(parts) == 1:
        return [condition.strip()]
    return [conjunct for part in parts for conjunct in conjuncts(part)]


def _unwrapped(text: str) -> str:
    """The text without parentheses that wrap all of it."""
    text = text.strip()
    while text.startswith('(') and text.endswith(')') and _closing(text) == len(text) - 1:
        text = text[1:-1].strip()
    return text


def _is_not_null_check(conjunct: str) -> bool:
    """Whether the conjunct only checks a value is not null, as Spark infers beside every comparison."""
    text = _unwrapped(conjunct)
    return text.startswith('isnotnull(') and _closing(text) == len(text) - 1


def _closing(text: str) -> int:
    """The index of the parenthesis closing the text's first one, or -1."""
    depth = 0
    for index, char in enumerate(text):
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
            if depth == 0:
                return index
    return -1


def _columns(expression: str) -> frozenset[str]:
    """The columns a Catalyst expression reads, lower-cased, by their top-level name."""
    found = set()
    for match in _ATTRIBUTE.finditer(expression):
        found.add((match['quoted'] or match['name']).lower())
    return frozenset(found)


def _source_columns(entry: str) -> frozenset[str]:
    """The columns one pushed source filter, such as `Or(EqualTo(id,0),GreaterThan(id,9))`, reads."""
    match = _SOURCE_FILTER.fullmatch(entry.strip())
    if match is None:
        return frozenset()
    arguments = split_top_level(match['arguments'], ',')
    if match['name'] in _SOURCE_COMBINATORS:
        return frozenset().union(*(_source_columns(argument) for argument in arguments))
    column = arguments[0].strip().strip('`') if arguments else ''
    return frozenset({column.split('.')[0].lower()}) if column else frozenset()


def unpushed_conjuncts(condition: str, pushed_filters: str, partition_filters: str) -> list[str]:
    """The conjuncts of a filter over a scan that no entry of the scan's `PushedFilters` carries.

    Spark translates each conjunct it can into one pushed filter, in the condition's order. A
    conjunct checking only that a column is not null, one running a subquery, and one over a
    partition column are left out, because Spark pushes those elsewhere or always. Each remaining
    conjunct is matched to the next pushed entry reading the same columns, and one with no match
    was not pushed.
    """
    partition_columns = _columns(partition_filters)
    pushed = [
        _source_columns(entry) for entry in list_entries(pushed_filters) if not entry.startswith('IsNotNull(') and _source_columns(entry)
    ]
    unpushed = []
    position = 0
    for conjunct in conjuncts(condition):
        columns = _columns(conjunct)
        if (
            not columns
            or _is_not_null_check(conjunct)
            or labels.subquery_ids([conjunct])
            or 'dynamicpruning' in conjunct
            or columns & partition_columns
        ):
            continue
        match = next((index for index in range(position, len(pushed)) if pushed[index] == columns), None)
        if match is None:
            unpushed.append(conjunct)
        else:
            position = match + 1
    return unpushed


def _filter_not_pushed(context: RuleContext) -> Iterator[dict[str, Any]]:
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            condition = filter_condition(numbered.node)
            if condition is None or len(numbered.children) != 1:
                continue
            below = nodes[numbered.children[0]]
            while below.node.operator in _ROW_CONVERSIONS and len(below.children) == 1:
                below = nodes[below.children[0]]
            location = scan_location(below.node)
            pushed = field(below.node, 'PushedFilters')
            partition_filters = field(below.node, 'PartitionFilters') or '[]'
            if location is None or pushed is None or is_cut(pushed) or is_cut(partition_filters):
                continue
            unpushed = unpushed_conjuncts(condition, pushed, partition_filters)
            if not unpushed:
                continue
            filter_metrics = context.metrics(execution_id, numbered.number)
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'medium',
                'impact_seconds': None,
                'unpushed_conditions': tuple(unpushed),
                'pushed_filters': pushed,
                'scan_location': location,
                'scan_output_rows': metric(context.metrics(execution_id, below.number), OUTPUT_ROWS),
                'filter_output_rows': metric(filter_metrics, OUTPUT_ROWS),
            }


_FILTER_NOT_PUSHED_LESSON = """\
## What it measures

A `Filter` directly above a file scan, holding a condition the scan did not take. Spark translates
each conjunct of a condition it can into one entry of the scan's `PushedFilters`, which the source
uses to skip files and row groups. A conjunct with no entry, such as one over `cast(id as string)`,
is checked on every row read.

## Why it costs time

A pushed filter lets Parquet or ORC skip a whole row group by its minimum and maximum, before
decoding it. An unpushed one decodes every row first, and drops most of them after.

## Where the Spark UI shows it

SQL tab › the query › the scan node's details: `PushedFilters` lists what the source took, beside
the `Filter` node's condition. The scan's `number of output rows` against the filter's shows how
much was read only to be dropped.

## How to fix it

- Compare the column as it is stored: `event_date = date'2026-10-19'`, not
  `cast(event_date as string) = '2026-10-19'`.
- Keep functions off the filtered column, such as `upper(name) = 'X'`. Store a normalized copy of
  the column where the query needs one.
- Check `spark.sql.parquet.filterPushdown` or `spark.sql.orc.filterPushdown` is on, as it is by
  default.
"""

FILTER_NOT_PUSHED = Rule(
    rule_id='plan.filter-not-pushed',
    title='A filter over {scan_location} checks every row read, because the scan could not take it',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(),
    severity_ladder={'medium': "a conjunct of the filter has no entry in the scan's PushedFilters"},
    remediation=(
        'compare the column as it is stored, without a cast or function on it',
        'store a normalized copy of the column where the query needs one',
        'check spark.sql.parquet.filterPushdown or spark.sql.orc.filterPushdown is on',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#configuration',),
    lesson=_FILTER_NOT_PUSHED_LESSON,
    check=_filter_not_pushed,
)


def _partition_not_pruned(context: RuleContext) -> Iterator[dict[str, Any]]:
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            location = scan_location(numbered.node)
            metrics = context.metrics(execution_id, numbered.number)
            partitions = metric(metrics, 'number of partitions read')
            partition_filters = field(numbered.node, 'PartitionFilters')
            if location is None or not partitions or partition_filters is None or is_cut(partition_filters):
                continue
            if any(not _NO_PRUNING.fullmatch(entry) for entry in list_entries(partition_filters)):
                continue
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'medium',
                'impact_seconds': None,
                'scan_location': location,
                'partition_filters': partition_filters,
                'partitions_read_count': partitions,
                'files_read_count': metric(metrics, 'number of files read'),
                'read_bytes': metric(metrics, 'size of files read'),
            }


_PARTITION_NOT_PRUNED_LESSON = """\
## What it measures

A scan of a partitioned table whose `PartitionFilters` hold nothing beyond `isnotnull`, so it read
every partition. An event log carries no catalog, so the scan's `number of partitions read`
metric, which only a partitioned table reports, is the sign the table is partitioned.

## Why it costs time

Partition pruning is the cheapest filter Spark has, because it skips whole directories before
listing their files. A scan that prunes nothing lists and reads them all. A filter on the
partition column that Spark could not use, such as one through a function, costs the full read
without saying so.

## Where the Spark UI shows it

SQL tab › the query › the scan node: `number of partitions read` and `number of files read`, with
`PartitionFilters` in its details.

## How to fix it

- Filter on the partition column itself, against a value Spark can fold: `part = 1`, not
  `cast(part as string) = '1'`.
- For a join on the partition column, keep dynamic partition pruning on, with
  `spark.sql.optimizer.dynamicPartitionPruning.enabled`.
- Where the query means to read every partition, the finding confirms it, and nothing needs
  fixing.
"""

PARTITION_NOT_PRUNED = Rule(
    rule_id='plan.partition-not-pruned',
    title='A scan of {scan_location} read all {partitions_read_count:,} partitions, pruning none',
    signals=frozenset({'plan', 'plan_metrics'}),
    scope='plan_node',
    thresholds=(),
    severity_ladder={'medium': 'a partitioned table was read with no partition filter'},
    remediation=(
        'filter on the partition column itself, against a value Spark can fold',
        'keep spark.sql.optimizer.dynamicPartitionPruning.enabled on for a join on the partition column',
    ),
    docs=('https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery',),
    lesson=_PARTITION_NOT_PRUNED_LESSON,
    check=_partition_not_pruned,
)


def _long_filter(context: RuleContext) -> Iterator[dict[str, Any]]:
    max_characters = context.thresholds['max_characters']
    for execution_id, nodes in context.numbered.items():
        for numbered in nodes:
            condition = filter_condition(numbered.node)
            if condition is None or len(condition) <= max_characters:
                continue
            yield {
                'sql_execution_id': execution_id,
                'plan_node_id': numbered.number,
                'severity': 'low',
                'impact_seconds': None,
                'condition_character_count': len(condition),
            }


_LONG_FILTER_LESSON = """\
## What it measures

A `Filter` condition over 1,000 characters, DataFlint's threshold. A condition that long is
usually a list of values written out as comparisons joined by `OR`.

## Why it costs time

Spark checks the whole condition on every row. Code generation for a very long expression can pass
`spark.sql.codegen.hugeMethodLimit`, and Spark then runs that part of the plan without whole-stage
code generation.

## Where the Spark UI shows it

SQL tab › the query › the `Filter` node, whose condition its details print.

## How to fix it

- Put the values in a DataFrame and inner join on it, broadcast where it is small, so Spark hashes
  them once.
- Write a chain of `OR` comparisons on one column as one `isin(...)`.
- Rewrite the condition shorter.
"""

LONG_FILTER = Rule(
    rule_id='plan.long-filter',
    title='A filter condition runs {condition_character_count:,} characters',
    signals=frozenset({'plan'}),
    scope='plan_node',
    thresholds=(Threshold('max_characters', 1000, 'count', 'DataFlint spark-ui/src/reducers/Alerts/LongFilterConditions.ts'),),
    severity_ladder={'low': 'the condition is longer than max_characters'},
    remediation=(
        'turn the list of values into a DataFrame and inner join on it',
        'write a chain of OR comparisons on one column as one isin',
        'rewrite the condition shorter',
    ),
    docs=('https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration',),
    lesson=_LONG_FILTER_LESSON,
    check=_long_filter,
)

RULES = (FILTER_NOT_PUSHED, PARTITION_NOT_PRUNED, LONG_FILTER)
