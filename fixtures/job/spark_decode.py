"""Decode a compressed event log with Spark's own codec, the way the History Server reads it.

    spark-submit spark_decode.py CODEC COMPRESSED_LOG PLAIN_OUTPUT

The plain output is what sparkxray's decoder for that codec must reproduce byte for byte.
"""

import sys

from pyspark import SparkConf
from pyspark import SparkContext


def main() -> None:
    codec_name, source, target = sys.argv[1:4]
    conf = SparkConf().setMaster('local[1]').setAppName('sparkxray-decode').set('spark.eventLog.enabled', 'false')
    sc = SparkContext(conf=conf)
    jvm = sc._jvm
    codec = jvm.org.apache.spark.io.CompressionCodec.createCodec(sc._jsc.sc().conf(), codec_name)
    plain = codec.compressedInputStream(jvm.java.io.FileInputStream(source))
    out = jvm.java.io.FileOutputStream(target)
    plain.transferTo(out)
    out.close()
    plain.close()
    sc.stop()


if __name__ == '__main__':
    main()
