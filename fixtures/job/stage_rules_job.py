"""A Spark application whose event log seeds the stage, executor, application and config rules.

Each job's description names the rules that must fire on it and those that must stay silent, as
`fires=<ids>; silent=<ids>; <what it does>`. A rule whose finding belongs to the whole run rather
than to one job is named the same way in the `spark.sparkxray.fixture.application` property. A
test reads both from the log.

The job expects two executors of two cores and a 1 GiB heap each, as `local-cluster[2,2,1024]`
starts them, so one executor can run out of memory and be replaced while the driver goes on.

    spark-submit stage_rules_job.py
"""

import contextlib
import time
from collections.abc import Callable
from collections.abc import Iterable

import pyspark.sql.functions as sf
from py4j.protocol import Py4JJavaError
from pyspark import TaskContext
from pyspark.errors import PythonException
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

APPLICATION_PROPERTY = 'spark.sparkxray.fixture.application'
EXECUTORS = 2

# Each padded row is a long and a 256-character string. Unsafe rows of that shape write about
# 284 bytes each to an uncompressed shuffle.
PADDING_CHARACTERS = 256
# 1.5M rows on one key put about 400 MiB into one shuffle partition, past the 256 MiB skew floor.
HOT_ROWS = 1_500_000
SPREAD_ROWS = 160_000
# 4.6M padded rows shuffle about 1.2 GiB: four partitions of over 256 MiB each, or six of under.
WIDE_ROWS = 4_600_000
# A string this long cannot be copied into a 1 GiB heap.
OOM_STRING_CHARACTERS = 1_100_000_000
DRIVER_GAP_SECONDS = 75
SMALL_JOBS = 60


def expectation(fires: tuple[str, ...], silent: tuple[str, ...], what: str) -> str:
    return f'fires={",".join(fires)}; silent={",".join(silent)}; {what}'


def describe(spark: SparkSession, fires: tuple[str, ...], silent: tuple[str, ...], what: str) -> None:
    spark.sparkContext.setJobDescription(expectation(fires, silent, what))


def noop(frame: DataFrame) -> None:
    frame.write.format('noop').mode('overwrite').save()


def padded(column: str) -> Column:
    return sf.lpad(sf.col(column).cast('string'), PADDING_CHARACTERS, 'x').alias('pad')


def wait_for_executors(spark: SparkSession, count: int) -> None:
    """Block until `count` executors have registered, so each seed starts on the same four slots."""
    deadline = time.monotonic() + 120
    # The block manager status lists the driver beside each executor.
    while spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() < count + 1:
        if time.monotonic() > deadline:
            raise RuntimeError(f'fewer than {count} executors registered within two minutes')
        time.sleep(1)


def task_skew(spark: SparkSession) -> None:
    describe(spark, ('stage.task-skew',), ('stage.executor-imbalance',), 'one task of twelve sleeps 45 s, the others 4 s')

    def sleep(index: int, rows: Iterable[int]) -> Iterable[int]:
        time.sleep(45 if index == 0 else 4)
        return rows

    spark.sparkContext.parallelize(range(12), 12).mapPartitionsWithIndex(sleep).count()


def retry_waste(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.retry-waste',),
        ('stage.failed', 'stage.executor-imbalance'),
        'four tasks of twelve run 12 s and throw on their first attempt',
    )

    def fail_first_attempts(index: int, rows: Iterable[int]) -> Iterable[int]:
        if index < 4 and TaskContext.get().attemptNumber() == 0:
            time.sleep(12)
            raise RuntimeError('seeded failure on the first attempt')
        time.sleep(3)
        return rows

    spark.sparkContext.parallelize(range(12), 12).mapPartitionsWithIndex(fail_first_attempts).count()


def failed_stage(spark: SparkSession) -> None:
    describe(spark, ('stage.failed',), ('stage.retry-waste',), 'one task that throws on every attempt, failing its job')

    def fail(rows: Iterable[int]) -> Iterable[int]:
        raise RuntimeError('seeded failure on every attempt')

    # Spark 3.5 raises the job's failure as py4j's error, and Spark 4 as its own PythonException.
    with contextlib.suppress(Py4JJavaError, PythonException):
        spark.sparkContext.parallelize([0], 1).mapPartitions(fail).count()


def task_oom(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.task-oom',),
        ('stage.retry-waste',),
        "a Python UDF returns a 1.1 GB string on its first attempt, which the executor's 1 GiB heap cannot hold",
    )

    @sf.udf('string')
    def oversized(value: int) -> str:
        if value == 0 and TaskContext.get().attemptNumber() == 0:
            return 'x' * OOM_STRING_CHARACTERS
        time.sleep(3)
        return str(value)

    noop(spark.range(0, 4, 1, 4).select(oversized('id').alias('value')))
    # The executor that ran out of memory exits, and its replacement registers before the next seed.
    wait_for_executors(spark, EXECUTORS)


def skewed_hash_join(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.shuffle-skew', 'config.skew-join-off-with-skew'),
        ('stage.spill', 'stage.task-skew', 'stage.oversized-partitions'),
        'shuffled hash join with skew joins off for the session: 1.5M of 1.66M left rows share key 0',
    )
    hot = spark.range(0, HOT_ROWS).select(sf.lit(0).cast('long').alias('k'), padded('id'))
    spread = spark.range(0, SPREAD_ROWS).select((sf.col('id') % 1000 + 1).alias('k'), padded('id'))
    right = spark.range(0, 1001).select(sf.col('id').alias('k'), (sf.col('id') * 3).alias('w'))
    spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'false')
    try:
        noop(hot.union(spread).join(right.hint('shuffle_hash'), 'k'))
    finally:
        spark.conf.unset('spark.sql.adaptive.skewJoin.enabled')


def wide_rows(spark: SparkSession) -> DataFrame:
    return spark.range(0, WIDE_ROWS).select(sf.col('id'), padded('id'))


def oversized_partitions(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.oversized-partitions',),
        ('stage.low-shuffle-parallelism', 'stage.spill'),
        'repartition of about 1.2 GiB into four partitions of over 256 MiB each',
    )
    noop(wide_rows(spark).repartition(4, 'id'))


def low_shuffle_parallelism(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.low-shuffle-parallelism',),
        ('stage.oversized-partitions',),
        'repartition of about 1.2 GiB into six partitions, on a shuffle of over 1 GiB',
    )
    noop(wide_rows(spark).repartition(6, 'id'))


def aqe_off(spark: SparkSession) -> None:
    describe(spark, (), (), 'aggregate run with adaptive execution off for the session')
    spark.conf.set('spark.sql.adaptive.enabled', 'false')
    try:
        noop(spark.range(0, 100_000).groupBy((sf.col('id') % 7).alias('g')).count())
    finally:
        spark.conf.unset('spark.sql.adaptive.enabled')


def driver_gap(spark: SparkSession) -> None:
    describe(spark, (), (), f'two one-task jobs with {DRIVER_GAP_SECONDS} s of driver-only sleep between them')
    spark.sparkContext.parallelize([1], 1).count()
    time.sleep(DRIVER_GAP_SECONDS)
    spark.sparkContext.parallelize([1], 1).count()


def many_small_jobs(spark: SparkSession) -> None:
    describe(spark, (), ('stage.tiny-tasks',), f'{SMALL_JOBS} one-task jobs of under a second each')
    for _ in range(SMALL_JOBS):
        spark.sparkContext.parallelize([1], 1).count()


SEEDS: tuple[Callable[[SparkSession], None], ...] = (
    task_skew,
    retry_waste,
    failed_stage,
    task_oom,
    skewed_hash_join,
    oversized_partitions,
    low_shuffle_parallelism,
    aqe_off,
    driver_gap,
    many_small_jobs,
)

APPLICATION = expectation(
    ('executor.lost', 'executor.idle-cores', 'app.driver-gaps', 'app.many-small-jobs', 'app.failed-jobs', 'config.aqe-off'),
    ('config.event-log-incomplete', 'config.shuffle-partitions-too-few'),
    'the whole run',
)


def main() -> None:
    spark = SparkSession.builder.appName('sparkxray-stage-rules-fixture').config(APPLICATION_PROPERTY, APPLICATION).getOrCreate()
    wait_for_executors(spark, EXECUTORS)
    for seed in SEEDS:
        seed(spark)
    spark.stop()


if __name__ == '__main__':
    main()
