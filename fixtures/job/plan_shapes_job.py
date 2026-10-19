"""A Spark application running one small query per physical plan shape the plan parsers read.

Each query's job description is its shape's name, which Spark copies into the SQL execution. The
last two run under a `spark.sql.maxPlanStringLength` of 400 and 0, so their plan text is cut while
their JSON plan stays whole.

    spark-submit plan_shapes_job.py DATA_DIR
"""

import argparse
from collections.abc import Callable

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('data')
    data = parser.parse_args().data
    spark = SparkSession.builder.getOrCreate()

    spark.range(0, 1000).select(sf.col('id'), (sf.col('id') % 7).alias('g')).write.mode('overwrite').parquet(f'{data}/t')
    spark.range(0, 30).write.mode('overwrite').parquet(f'{data}/s')
    fact = spark.range(0, 1000).select(sf.col('id'), (sf.col('id') % 5).alias('part'))
    fact.write.mode('overwrite').partitionBy('part').parquet(f'{data}/fact')
    dim = spark.range(0, 5).select(sf.col('id').alias('part'), (sf.col('id') * 10).alias('w'))
    dim.write.mode('overwrite').parquet(f'{data}/dim')
    for name in ('t', 's', 'fact', 'dim'):
        spark.read.parquet(f'{data}/{name}').createOrReplaceTempView(name)
    t = spark.table('t')
    s = spark.table('s')
    agg = t.groupBy('g').count()

    shapes: dict[str, Callable[[], DataFrame]] = {
        'scan-filter-agg': lambda: t.filter('id > 10').groupBy('g').count(),
        'smj': lambda: t.join(s, 'id'),
        'broadcast': lambda: t.join(sf.broadcast(s), 'id'),
        'scalar-subquery': lambda: spark.sql('select * from t where id > (select avg(id) from s)'),
        'two-subqueries': lambda: spark.sql('select * from t where id > (select avg(id) from s) and g < (select max(id) from s)'),
        'reused-exchange': lambda: agg.join(agg.withColumnRenamed('count', 'c2'), 'g'),
        'dpp': lambda: spark.sql('select f.id, d.w from fact f join dim d on f.part = d.part where d.w > 20'),
        'cartesian': lambda: t.crossJoin(s),
        'bnlj': lambda: t.join(s, t.id < s.id),
        'window': lambda: spark.sql('select id, g, row_number() over (partition by g order by id) r from t'),
        'cache': lambda: t.union(t).cache().groupBy('g').count(),
        'udf': lambda: t.select(sf.udf(lambda v: v * 2, 'long')('id')),
        'wide': lambda: spark.range(0, 10).select(*[sf.col('id').alias(f'c{i}') for i in range(40)]),
    }
    for name, build in shapes.items():
        spark.sparkContext.setJobDescription(name)
        build().collect()

    for name, length in (('max-plan-length', 400), ('max-plan-length-zero', 0)):
        spark.conf.set('spark.sql.maxPlanStringLength', length)
        spark.sparkContext.setJobDescription(name)
        t.join(s, 'id').collect()
    spark.stop()


if __name__ == '__main__':
    main()
