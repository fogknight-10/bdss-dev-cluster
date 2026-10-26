"""BDSS Dev Cluster - end-to-end smoke test.

Run inside the dev container:

    python /opt/bdss-scripts/smoke_test.py              # all checks
    python /opt/bdss-scripts/smoke_test.py --mode local # Spark local mode only (no YARN)

What it proves
  1. HDFS      write + read back through the active NameNode
  2. Spark     local mode reads file:///data (the bind-mounted folder)
  3. YARN      a Spark job on the cluster reads the SAME file:///data from the workers,
               runs Python code on the executors, and writes Parquet into HDFS
  4. Kafka     the broker answers a metadata request

Built by Mohamed (github.com/fogknight-10/bdss-dev-cluster) on top of usf-etman/bdss-cluster.
"""
import argparse
import csv
import os
import subprocess
import sys
import time

SAMPLE_DIR = "/data/_bdss_smoke"                  # created by this script; appears on your computer in data/_bdss_smoke/
SAMPLE = "file:///data/_bdss_smoke/orders.csv"    # 3 columns, no header (customer, item, amount)
SAMPLE_ROWS = 10000
HDFS_OUT = "hdfs:///tmp/bdss_smoke/orders_parquet"

results = []


def record(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'OK' if ok else 'FAIL'}]  {name}" + (f" - {detail}" if detail else ""))


def make_sample():
    """Create the sample file inside the bind-mounted /data folder (the workers must be able to read it)."""
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    with open(f"{SAMPLE_DIR}/orders.csv", "w", newline="") as fh:
        writer = csv.writer(fh)
        for i in range(SAMPLE_ROWS):
            writer.writerow([i % 100, 1000 + i % 37, round(1 + (i * 7919) % 5000 / 100, 2)])
    os.chmod(SAMPLE_DIR, 0o777)
    os.chmod(f"{SAMPLE_DIR}/orders.csv", 0o666)


def check_hdfs():
    path = f"/tmp/bdss_smoke_{int(time.time())}.txt"
    try:
        subprocess.run(["hdfs", "dfs", "-put", "-f", "-", path], input=b"bdss\n", check=True, capture_output=True)
        out = subprocess.run(["hdfs", "dfs", "-cat", path], check=True, capture_output=True).stdout.decode().strip()
        subprocess.run(["hdfs", "dfs", "-rm", "-f", "-skipTrash", path], capture_output=True)
        record("HDFS write/read", out == "bdss", f"read back '{out}'")
    except Exception as exc:  # noqa: BLE001
        record("HDFS write/read", False, str(exc)[:200])


def _session(master):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(f"bdss-smoke-{master}").master(master)
    if master == "yarn":
        builder = builder.config("spark.submit.deployMode", "client")
    return builder.getOrCreate()


def check_spark_local():
    try:
        spark = _session("local[2]")
        rows = spark.read.csv(SAMPLE).count()
        record("Spark local reads file:///data", rows == SAMPLE_ROWS, f"{rows} rows")
        spark.stop()
    except Exception as exc:  # noqa: BLE001
        record("Spark local reads file:///data", False, str(exc)[:200])


def explain(exc):
    """Short, readable reason for a long Spark/Py4J error."""
    text = str(exc)
    for marker, hint in (
        ("No module named 'pyspark'", "executors cannot import pyspark - check spark.executorEnv.PYTHONPATH in /opt/spark/conf/spark-defaults.conf"),
        ("different version", "Python on the workers has a different minor version than the driver (3.12)"),
        ("Permission denied", "HDFS permission problem - check HADOOP_USER_NAME"),
        ("Connection refused", "a service is not reachable yet"),
    ):
        if marker in text:
            return f"{hint} [{marker}]"
    return " ".join(text.split())[:240]


def check_spark_yarn():
    try:
        spark = _session("yarn")
    except Exception as exc:  # noqa: BLE001
        record("Spark on YARN starts a session", False, explain(exc))
        return

    rows = None
    try:
        rows = spark.read.csv(SAMPLE).count()
        record("Spark on YARN reads file:///data on the workers", rows == SAMPLE_ROWS, f"{rows} rows")
    except Exception as exc:  # noqa: BLE001
        record("Spark on YARN reads file:///data on the workers", False, explain(exc))

    try:
        doubled = spark.sparkContext.parallelize(range(100), 4).map(lambda x: x * 2).sum()
        record("Python code runs on YARN executors", doubled == 9900, f"sum={doubled}")
    except Exception as exc:  # noqa: BLE001
        record("Python code runs on YARN executors", False, explain(exc))

    try:
        df = spark.read.csv(SAMPLE).toDF("customer", "item", "amount")
        df.write.mode("overwrite").parquet(HDFS_OUT)
        back = spark.read.parquet(HDFS_OUT).count()
        record("Spark writes Parquet to HDFS and reads it back", rows is not None and back == rows, f"{back} rows")
    except Exception as exc:  # noqa: BLE001
        record("Spark writes Parquet to HDFS and reads it back", False, explain(exc))

    spark.stop()


def check_kafka():
    try:
        from confluent_kafka.admin import AdminClient

        servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        meta = AdminClient({"bootstrap.servers": servers}).list_topics(timeout=10)
        record("Kafka metadata", len(meta.brokers) >= 1, f"{len(meta.brokers)} broker(s) at {servers}")
    except Exception as exc:  # noqa: BLE001
        record("Kafka metadata", False, str(exc)[:200])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=["all", "local"], default="all")
    args = parser.parse_args()

    print("BDSS Dev Cluster smoke test")
    make_sample()
    check_hdfs()
    check_spark_local()
    if args.mode == "all":
        check_spark_yarn()
        check_kafka()

    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
