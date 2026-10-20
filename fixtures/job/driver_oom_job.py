"""A Spark application that collects more rows than its driver's heap holds, so the driver runs out of memory.

Run with `spark.log.structuredLogging.enabled=true`, its stderr is the driver's structured log,
which records the OutOfMemoryError the event log never names. The run's expected findings are in
the `spark.sparkxray.fixture.application` property, as `fires=<ids>; silent=<ids>; <what it does>`.

    spark-submit --driver-memory 512m --conf spark.driver.maxResultSize=0 driver_oom_job.py
"""

import pyspark.sql.functions as sf
from pyspark.sql import SparkSession

APPLICATION_PROPERTY = 'spark.sparkxray.fixture.application'
APPLICATION = 'fires=app.driver-oom; silent=; a collect larger than the driver heap, which the event log records as a success'
# 20M rows of a long and a 64-character string are about 1.6 GiB as Java objects, past a 512 MiB heap.
ROWS = 20_000_000
PADDING_CHARACTERS = 64


def main() -> None:
    spark = SparkSession.builder.appName('sparkxray-fixture-driver-oom').config(APPLICATION_PROPERTY, APPLICATION).getOrCreate()
    spark.sparkContext.setJobDescription('collect rows past the driver heap')
    rows = spark.range(0, ROWS, numPartitions=8).select('id', sf.repeat(sf.lit('x'), PADDING_CHARACTERS).alias('padding'))
    rows.collect()


if __name__ == '__main__':
    main()
