"""A Spark application whose event logs seed the plan rules: one query each rule must fire on, and one it must not.

Each query's job description names the plan rules that must fire on it and those that must stay
silent, as `fires=<ids>; silent=<ids>; <what it does>`. A test reads its expectation from the log.
The estimate queries never run, because an event log carries no optimizer estimate. Their
`explain(mode='cost')` text is written to ESTIMATES as JSON, beside the same descriptions.

    spark-submit plan_rules_job.py DATA_DIR ESTIMATES
"""

import argparse
import contextlib
import io
import json
from collections.abc import Callable

import pandas as pd
import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# A build side of this many rows of 1,000 characters each hashes to more than 1 GiB.
BROADCAST_ROWS = 1_200_000
PADDING_CHARACTERS = 1_000


def describe(spark: SparkSession, fires: tuple[str, ...], silent: tuple[str, ...], what: str) -> str:
    description = f'fires={",".join(fires)}; silent={",".join(silent)}; {what}'
    spark.sparkContext.setJobDescription(description)
    return description


def noop(frame: DataFrame) -> None:
    frame.write.format('noop').mode('overwrite').save()


def write_tables(spark: SparkSession, data: str) -> None:
    describe(spark, (), (), 'writes the tables the queries read')
    spark.range(0, 100_000).select(sf.col('id'), (sf.col('id') % 7).alias('g')).write.mode('overwrite').parquet(f'{data}/t')
    for year in (2025, 2026):
        spark.range(0, 1_000).select(sf.col('id'), (sf.col('id') % 7).alias('g')).write.mode('overwrite').parquet(f'{data}/t{year}')
    fact = spark.range(0, 10_000).select(sf.col('id'), (sf.col('id') % 5).alias('part'))
    fact.write.mode('overwrite').partitionBy('part').parquet(f'{data}/fact')


def large_sides_join(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.smj-small-side', 'plan.exploding-join'), 'sort-merge join of two sides too large to broadcast')
    left = spark.range(0, 2_000_000).select(sf.col('id').alias('k'), (sf.col('id') * 3).alias('v'))
    right = spark.range(0, 1_000_000).select(sf.col('id').alias('k'), (sf.col('id') * 5).alias('w'))
    noop(left.join(right, 'k'))


def exploding_join(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.exploding-join', 'plan.smj-small-side'), (), 'join on two keys, each matching thousands of rows on both sides')
    left = spark.range(0, 20_000).select((sf.col('id') % 2).alias('k'), sf.col('id').alias('v'))
    right = spark.range(0, 11_000).select((sf.col('id') % 2).alias('k'), sf.col('id').alias('w'))
    noop(left.join(right, 'k'))


def cross_join(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.cartesian',), (), 'cross join of two tables')
    right = spark.read.parquet(f'{data}/t2026').select(sf.col('id').alias('other_id'))
    noop(spark.read.parquet(f'{data}/t2025').crossJoin(right))


def nested_loop_join(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.cartesian',), ('plan.broadcast-too-large',), 'join on a range condition, broadcast as a nested loop')
    right = spark.read.parquet(f'{data}/t2026').select(sf.col('id').alias('other_id'))
    noop(spark.read.parquet(f'{data}/t2025').join(sf.broadcast(right), sf.col('id') < sf.col('other_id')))


def large_broadcast(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.broadcast-too-large',), (), 'broadcast join whose build side is over 1 GiB')
    # The padding repeats a digit of the row's id, so the optimizer cannot fold it into a constant
    # and carry it past the join, and the broadcast compresses it to almost nothing.
    padding = sf.repeat(sf.substring(sf.col('id').cast('string'), 1, 1), PADDING_CHARACTERS)
    build = spark.range(0, BROADCAST_ROWS).select(sf.col('id').alias('k'), padding.alias('pad'))
    probe = spark.range(0, 1_000).select(sf.col('id').alias('k'))
    noop(probe.join(sf.broadcast(build), 'k'))


def arrow_udf(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.python-udf-arrow',), ('plan.python-udf-row-at-a-time',), 'pandas UDF, run over Arrow batches')

    @sf.pandas_udf('long')
    def doubled(values: pd.Series) -> pd.Series:
        return values * 2

    noop(spark.read.parquet(f'{data}/t').select(doubled('id').alias('doubled')))


def window_without_partition(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.window-single-partition',), (), 'window with no partitionBy, so one task ranks every row')
    ranked = spark.read.parquet(f'{data}/t').select('id', sf.row_number().over(Window.orderBy('id')).alias('r'))
    noop(ranked)


def window_by_group(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.window-single-partition',), 'window partitioned by group')
    ranked = spark.read.parquet(f'{data}/t').select('id', sf.row_number().over(Window.partitionBy('g').orderBy('id')).alias('r'))
    noop(ranked)


def self_union(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.repeated-scan', 'plan.repeated-subtree'), (), 'union of one table with itself, filtered the same way')
    table = spark.read.parquet(f'{data}/t')
    noop(table.filter('id > 10').union(table.filter('id > 10')))


def union_of_two_tables(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.repeated-scan', 'plan.repeated-subtree'), 'union of two tables written with one schema')
    first = spark.read.parquet(f'{data}/t2025')
    second = spark.read.parquet(f'{data}/t2026')
    noop(first.filter('id > 10').union(second.filter('id > 10')))


def aggregate_union_without_reuse(spark: SparkSession, data: str) -> None:
    describe(
        spark,
        ('plan.missed-exchange-reuse', 'plan.repeated-scan'),
        ('plan.repeated-subtree',),
        'union of one aggregate with itself, with exchange reuse off',
    )
    counts = spark.read.parquet(f'{data}/t').groupBy('g').count()
    spark.conf.set('spark.sql.exchange.reuse', 'false')
    try:
        noop(counts.union(counts))
    finally:
        spark.conf.unset('spark.sql.exchange.reuse')


def aggregate_union_with_reuse(spark: SparkSession, data: str) -> None:
    describe(
        spark,
        (),
        ('plan.missed-exchange-reuse', 'plan.repeated-scan', 'plan.repeated-subtree'),
        'union of one aggregate with itself, its exchange reused',
    )
    counts = spark.read.parquet(f'{data}/t').groupBy('g').count()
    noop(counts.union(counts))


def four_exchanges(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.many-exchanges',), (), 'repartition, two aggregations on different keys, then a sort: four exchanges')
    rows = spark.range(0, 100_000).repartition(4).select(sf.col('id'), (sf.col('id') % 7).alias('g'), (sf.col('id') % 11).alias('h'))
    noop(rows.groupBy('g', 'h').count().groupBy('g').agg(sf.sum('count').alias('total')).orderBy('g'))


def filter_not_pushed(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.filter-not-pushed',), (), 'filter on a column cast to a string, which Parquet cannot take')
    noop(spark.read.parquet(f'{data}/t').filter(sf.col('id').cast('string').like('9%')))


def filter_pushed(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.filter-not-pushed',), 'filter Parquet takes as a pushed filter')
    noop(spark.read.parquet(f'{data}/t').filter('id > 99000'))


def full_partitioned_scan(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.partition-not-pruned',), (), 'aggregate over every partition of a partitioned table')
    noop(spark.read.parquet(f'{data}/fact').groupBy('part').count())


def pruned_partitioned_scan(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.partition-not-pruned',), 'read of one partition of a partitioned table')
    noop(spark.read.parquet(f'{data}/fact').filter('part = 1'))


def long_filter(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.long-filter',), ('plan.filter-not-pushed',), 'filter whose condition compares one column with 200 values')
    condition = ' OR '.join(f'id = {value}' for value in range(0, 2_800, 14))
    noop(spark.read.parquet(f'{data}/t').filter(condition))


def small_files_written(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.small-files-written',), (), 'write of 200 files of about 1 KiB each')
    spark.range(0, 20_000).repartition(200).write.mode('overwrite').parquet(f'{data}/small-files')


def small_files_read(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.small-files-read',), (), 'read of 200 files of about 1 KiB each')
    noop(spark.read.parquet(f'{data}/small-files'))


def outer_join_small_left(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.smj-small-side',), 'left outer join whose small side is the left one, which Spark cannot broadcast')
    small = spark.read.parquet(f'{data}/t2025').select(sf.col('id').alias('k'))
    large = spark.range(0, 2_000_000).select(sf.col('id').alias('k'), (sf.col('id') * 3).alias('v'))
    noop(small.join(large, 'k', 'left'))


def cached_unpushed_filter(spark: SparkSession, data: str) -> DataFrame:
    return spark.read.parquet(f'{data}/t').filter(sf.col('id').cast('string').like('8%')).cache()


def cached_filter_first_read(spark: SparkSession, data: str) -> None:
    describe(spark, ('plan.filter-not-pushed',), (), 'first read of a cached filter Parquet cannot take, which fills the cache')
    noop(cached_unpushed_filter(spark, data))


def cached_filter_read_again(spark: SparkSession, data: str) -> None:
    describe(spark, (), ('plan.filter-not-pushed',), 'the same cached filter read again, from the cache')
    cached = cached_unpushed_filter(spark, data)
    noop(cached)
    cached.unpersist()


QUERIES: tuple[Callable[[SparkSession, str], None], ...] = (
    large_sides_join,
    exploding_join,
    cross_join,
    nested_loop_join,
    large_broadcast,
    arrow_udf,
    window_without_partition,
    window_by_group,
    self_union,
    union_of_two_tables,
    aggregate_union_without_reuse,
    aggregate_union_with_reuse,
    four_exchanges,
    filter_not_pushed,
    filter_pushed,
    full_partitioned_scan,
    pruned_partitioned_scan,
    long_filter,
    small_files_written,
    small_files_read,
    outer_join_small_left,
    cached_filter_first_read,
    cached_filter_read_again,
)


def write_estimates(spark: SparkSession, data: str, path: str) -> None:
    """Write each estimate query's description and `explain(mode='cost')` text, without running it."""
    table = spark.read.parquet(f'{data}/t')
    estimates: tuple[tuple[tuple[str, ...], tuple[str, ...], str, DataFrame], ...] = (
        (('plan.estimate-unreliable',), (), 'join of two tables with no statistics', table.join(spark.read.parquet(f'{data}/t2025'), 'id')),
        ((), ('plan.estimate-unreliable',), 'aggregate of one table', table.groupBy('g').count()),
    )
    explained = []
    for fires, silent, what, frame in estimates:
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            frame.explain(mode='cost')
        explained.append({'description': describe(spark, fires, silent, what), 'text': printed.getvalue()})
    with open(path, 'w') as handle:
        handle.write(json.dumps(explained, indent=2) + '\n')


def main() -> None:
    parser = argparse.ArgumentParser(description='Run the plan-rule fixture queries, and explain the estimate queries.')
    parser.add_argument('data', help='where the queries write the tables they read')
    parser.add_argument('estimates', help="where the estimate queries' cost-mode explain text is written, as JSON")
    args = parser.parse_args()

    spark = SparkSession.builder.appName('sparkxray-plan-rules-fixture').getOrCreate()
    write_tables(spark, args.data)
    for query in QUERIES:
        query(spark, args.data)
    write_estimates(spark, args.data, args.estimates)
    spark.stop()


if __name__ == '__main__':
    main()
