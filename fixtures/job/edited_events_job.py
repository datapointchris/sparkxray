"""A Spark application whose stages `edit_events.py` edits, to seed the rules a run on one host cannot.

Each job's description names the rules the edited log must fire on it and those that must stay
silent, as `fires=<ids>; silent=<ids>; <what it does>`, and says what the edit changes. The rules
about the whole run are named in the `spark.sparkxray.fixture.application` property. The log
Spark writes is never committed: `logs/spark-3.5.9-edited-events` holds it after the edit.

Like the stage-rules job, it expects two executors of two cores and a 1 GiB heap each, as
`local-cluster[2,2,1024]` starts them. It leaves `spark.sql.shuffle.partitions` unset.

    spark-submit edited_events_job.py
"""

import time
from collections.abc import Callable
from collections.abc import Iterable

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from stage_rules_job import APPLICATION_PROPERTY
from stage_rules_job import EXECUTORS
from stage_rules_job import describe
from stage_rules_job import expectation
from stage_rules_job import noop
from stage_rules_job import wait_for_executors


def sleeping_tasks(spark: SparkSession, tasks: int, seconds: Callable[[int], float]) -> None:
    """Run one stage of `tasks` tasks, each sleeping `seconds(index)`."""

    def sleep(index: int, rows: Iterable[int]) -> Iterable[int]:
        time.sleep(seconds(index))
        return rows

    spark.sparkContext.parallelize(range(tasks), tasks).mapPartitionsWithIndex(sleep).count()


def gc_pressure(spark: SparkSession) -> None:
    describe(spark, ('stage.gc-pressure',), (), "200 tasks of 1 s; the edit sets each one's JVM GC time to a quarter of its run time")
    sleeping_tasks(spark, 200, lambda index: 1.0)


def scheduler_delay(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.scheduler-delay',),
        ('stage.deserialization', 'stage.tiny-tasks'),
        "200 tasks of 100 ms; the edit adds 700 ms to each one's duration, outside its run time",
    )
    sleeping_tasks(spark, 200, lambda index: 0.1)


def deserialization(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.deserialization',),
        ('stage.scheduler-delay', 'stage.tiny-tasks'),
        '200 tasks of 100 ms; the edit adds 700 ms of deserialization to each',
    )
    sleeping_tasks(spark, 200, lambda index: 0.1)


def fetch_wait(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.fetch-wait',),
        ('stage.tiny-tasks',),
        "a shuffle read by 200 tasks of 600 ms; the edit sets each one's fetch wait to 40% of its run time",
    )

    def sleep(rows: Iterable[tuple[int, int]]) -> Iterable[tuple[int, int]]:
        time.sleep(0.6)
        return rows

    spark.sparkContext.parallelize(range(200), 4).map(lambda value: (value, value)).partitionBy(200).mapPartitions(sleep).count()


def speculation(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.speculation',),
        ('stage.retry-waste', 'stage.task-skew'),
        'ten of 40 tasks run 8 s and the rest 1 s; the edit gives each slow task a speculative copy that wins',
    )
    sleeping_tasks(spark, 40, lambda index: 8.0 if index < 10 else 1.0)


def imbalance(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.executor-imbalance', 'stage.host-imbalance'),
        (),
        '40 tasks of 1 s; the edit runs 32 of them on executor 0, and puts executor 1 on a host of its own',
    )
    sleeping_tasks(spark, 40, lambda index: 1.0)


def oversized_shuffle_reads(spark: SparkSession) -> None:
    describe(
        spark,
        ('stage.oversized-partitions',),
        ('stage.low-shuffle-parallelism',),
        "an aggregate shuffled into the default 200 partitions with coalescing off; the edit raises each reduce task's read to 1.1 GiB",
    )
    spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'false')
    try:
        noop(spark.range(0, 1_000_000).groupBy((sf.col('id') % 1000).alias('g')).count())
    finally:
        spark.conf.unset('spark.sql.adaptive.coalescePartitions.enabled')


SEEDS: tuple[Callable[[SparkSession], None], ...] = (
    gc_pressure,
    scheduler_delay,
    deserialization,
    fetch_wait,
    speculation,
    imbalance,
    oversized_shuffle_reads,
)

APPLICATION = expectation(
    ('executor.memory-under', 'executor.driver-memory-under', 'config.shuffle-partitions-too-few'),
    ('executor.memory-over', 'config.event-log-incomplete'),
    "the whole run; the edit raises executor 0's heap peak, and the driver's, to 97% of 1 GiB",
)


def main() -> None:
    spark = SparkSession.builder.appName('sparkxray-edited-events-fixture').config(APPLICATION_PROPERTY, APPLICATION).getOrCreate()
    wait_for_executors(spark, EXECUTORS)
    for seed in SEEDS:
        seed(spark)
    spark.stop()


if __name__ == '__main__':
    main()
