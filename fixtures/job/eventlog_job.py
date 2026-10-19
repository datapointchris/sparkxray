"""The Spark application whose event logs are sparkxray's fixtures, one seeded problem per job.

Each job's description names the rules that must fire on it and the rules that must stay silent,
as `fires=<ids>; silent=<ids>; <what it does>`. A test reads its expectation from the log itself.

    spark-submit eventlog_job.py OUTPUT_DIR [--warmup] [--only JOB ...] [--rolling-padding JOBS]
"""

import argparse
from collections.abc import Callable

import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

PADDING_PROPERTY = 'sparkxray.fixture.padding'
PADDING_BYTES = 1024 * 1024


def describe(spark: SparkSession, fires: tuple[str, ...], silent: tuple[str, ...], what: str) -> None:
    spark.sparkContext.setJobDescription(f'fires={",".join(fires)}; silent={",".join(silent)}; {what}')


def fact(spark: SparkSession) -> DataFrame:
    """40M rows, 80% of them on key 0, so one shuffle partition holds most of the data.

    The padding is hashed, so shuffle compression cannot shrink key 0's partition back under
    the skew floor of 256 MiB."""
    return spark.range(0, 40_000_000).select(
        sf.when(sf.col('id') % 10 < 8, sf.lit(0)).otherwise(sf.col('id') % 100_000).alias('k'),
        sf.col('id').alias('v'),
        sf.substring(sf.sha2(sf.col('id').cast('string'), 256), 1, 16).alias('pad'),
    )


def dim(spark: SparkSession) -> DataFrame:
    return spark.range(0, 100_000).select(sf.col('id').alias('k'), sf.sha2(sf.col('id').cast('string'), 256).alias('name'))


def skewed_join(spark: SparkSession, output: str) -> None:
    describe(
        spark,
        ('stage.shuffle-skew', 'plan.smj-small-side'),
        (),
        'skewed sort-merge join: 80% of fact rows share key 0',
    )
    joined = fact(spark).join(dim(spark), 'k')
    # Reading the padding keeps it in the shuffle. Spark prunes a column nothing downstream reads.
    totals = joined.groupBy('name').agg(sf.sum('v').alias('total'), sf.max('pad').alias('pad'))
    totals.write.format('noop').mode('overwrite').save()


def wide_sort(spark: SparkSession, output: str) -> None:
    describe(spark, ('stage.spill',), ('stage.shuffle-skew',), 'wide sort forced to spill')
    wide = spark.range(0, 3_000_000).select(
        sf.col('id'),
        sf.sha2(sf.col('id').cast('string'), 512).alias('a'),
        sf.sha2((sf.col('id') * 7).cast('string'), 512).alias('b'),
    )
    wide.repartition(4).sortWithinPartitions('a').write.format('noop').mode('overwrite').save()


def python_udf(spark: SparkSession, output: str) -> None:
    describe(spark, ('plan.python-udf-row-at-a-time',), (), 'row-at-a-time Python UDF')
    upper = sf.udf(lambda s: s.upper() if s else None, st.StringType())
    dim(spark).select(upper('name').alias('u')).write.format('noop').mode('overwrite').save()


def small_side_join(spark: SparkSession, output: str) -> None:
    describe(spark, ('plan.smj-small-side',), (), 'sort-merge join where the small side would fit a broadcast')
    small = spark.range(0, 1_000).select(sf.col('id').alias('k'), sf.lit('x').alias('tag'))
    keys = spark.range(0, 2_000_000).select((sf.col('id') % 1_000).alias('k'))
    keys.join(small, 'k').write.format('noop').mode('overwrite').save()


def small_files(spark: SparkSession, output: str) -> None:
    describe(spark, ('plan.small-files-written', 'stage.tiny-tasks'), ('stage.task-skew',), 'many small output files')
    spark.range(0, 20_000).repartition(200).write.mode('overwrite').parquet(f'{output}/small-files')


def cached_reuse(spark: SparkSession, output: str) -> None:
    describe(spark, (), ('plan.repeated-subtree',), 'cached reuse')
    cached = fact(spark).groupBy('k').count().cache()
    cached.count()
    cached.filter('count > 10').count()


JOBS: dict[str, Callable[[SparkSession, str], None]] = {
    'skewed-join': skewed_join,
    'wide-sort': wide_sort,
    'python-udf': python_udf,
    'small-side-join': small_side_join,
    'small-files': small_files,
    'cached-reuse': cached_reuse,
}


def warmup(spark: SparkSession) -> None:
    describe(spark, (), (), 'warmup action that shifts every later stage id')
    spark.range(0, 1_000).repartition(8).count()


def pad_past_rolling_size(spark: SparkSession, jobs: int) -> None:
    """Grow the log with jobs whose start events each carry a 1 MiB property.

    Spark refuses a rolling size under 10 MiB. The padding reaches it in a few jobs, and it is
    one repeated byte, so each rolled file compresses to almost nothing."""
    sc = spark.sparkContext
    describe(spark, (), (), 'pads the log past the rolling file size')
    sc.setLocalProperty(PADDING_PROPERTY, 'x' * PADDING_BYTES)
    for _ in range(jobs):
        sc.parallelize(range(10), 1).count()


def main() -> None:
    parser = argparse.ArgumentParser(description='Run the fixture jobs, each seeding one problem.')
    parser.add_argument('output', help='where the small-files job writes its Parquet')
    parser.add_argument('--warmup', action='store_true', help='run one extra job first, shifting every stage id')
    parser.add_argument('--only', nargs='+', choices=list(JOBS), default=list(JOBS), metavar='JOB', help='run these jobs only')
    parser.add_argument('--rolling-padding', type=int, default=0, metavar='JOBS', help='add this many padding jobs at the end')
    args = parser.parse_args()

    spark = SparkSession.builder.appName('sparkxray-fixture').getOrCreate()
    if args.warmup:
        warmup(spark)
    for name in JOBS:
        if name in args.only:
            JOBS[name](spark, args.output)
    if args.rolling_padding:
        pad_past_rolling_size(spark, args.rolling_padding)
    spark.stop()


if __name__ == '__main__':
    main()
