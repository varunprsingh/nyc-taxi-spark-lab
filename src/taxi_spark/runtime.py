"""Small, explicit local Spark runtime; no implicit remote cluster."""
import os
from pathlib import Path

from pyspark.sql import SparkSession


def session(name="taxi-spark-lab", optimized=True, event_dir=None):
    builder = (
        SparkSession.builder.appName(name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8" if optimized else "32")
        .config("spark.sql.adaptive.enabled", str(optimized).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760" if optimized else "-1")
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760" if optimized else "-1")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    if event_dir:
        Path(event_dir).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", Path(event_dir).absolute().as_uri()
        ).config("spark.eventLog.compress", "false")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
