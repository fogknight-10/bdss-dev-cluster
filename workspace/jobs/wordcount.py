"""Word count on YARN - reads a text file visible on every worker, writes the result to HDFS.

Run inside the dev container:
    spark-submit --master yarn --deploy-mode client /workspace/jobs/wordcount.py
    spark-submit --master yarn /workspace/jobs/wordcount.py file:///data/my_book.txt     # your own file

Built by Mohamed (github.com/fogknight-10/bdss-dev-cluster) using the original cluster by usf-etman.
"""
import sys

from pyspark.sql import SparkSession, functions as F

# /workspace is bind-mounted on the dev container and on every worker, so this README is readable everywhere.
path = sys.argv[1] if len(sys.argv) > 1 else "file:///workspace/README.md"

spark = SparkSession.builder.appName("bdss-wordcount").getOrCreate()

lines = spark.read.text(path)
words = (
    lines.select(F.explode(F.split(F.lower(F.col("value")), r"\W+")).alias("word"))
    .where(F.col("word") != "")
)
counts = words.groupBy("word").count().orderBy(F.desc("count"))

counts.show(10)
counts.write.mode("overwrite").parquet("hdfs:///user/wordcount_result")   # stored inside HDFS
spark.stop()
