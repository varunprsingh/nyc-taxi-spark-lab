"""Measure published artifacts; never substitute historical numbers for executed results."""
import json
import csv
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from taxi_spark.runtime import session
from taxi_spark.storage import atomic_json, catalog, published_trips, read_json


def main():
    root = Path("/workspace/data")
    spark = session("taxi-final-evidence")
    try:
        trips, partitions = published_trips(spark, root)
        stats = trips.agg(F.count("*").alias("accepted_rows"), F.sum("total_amount").alias("total_collected_amount"),
                          F.sum("fare_amount").alias("fare_amount"),
                          F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("negative_charge_rows"),
                          F.sum(F.when(F.date_format("pickup_at", "yyyy-MM") != F.col("source_month"), 1).otherwise(0)).alias("outside_owning_month_rows")).first().asDict()
        reasons = {}
        quarantine_count = 0
        for item in partitions.values():
            rejected = spark.read.parquet(str(root / item["quarantine_path"]))
            quarantine_count += rejected.count()
            for record in rejected.select(F.explode("reject_reasons").alias("reason")).groupBy("reason").count().collect():
                reasons[record.reason] = reasons.get(record.reason, 0) + record["count"]
        assert stats["accepted_rows"] == sum(p["accepted_rows"] for p in partitions.values())
        assert quarantine_count == sum(p["rejected_rows"] for p in partitions.values())
        reporting = read_json(root / "catalog/reporting.json")
        report_dir = root / reporting["report_path"]
        tables = {}
        table_stats = {}
        for name in ("hourly", "routes", "demand"):
            table = spark.read.parquet(str(report_dir / name))
            written = table.agg(F.count("*").alias("rows"), F.sum("trip_count").alias("trips"),
                                F.sum("total_collected_amount").alias("total_collected_amount")).first().asDict()
            assert written["trips"] == reporting["trip_count"], (name, written)
            assert written["total_collected_amount"] == Decimal(reporting["total_collected_amount"]), (name, written)
            tables[name] = written["rows"]
            table_stats[name] = written
        assert reporting["partition_versions"] == {k:v["version"] for k,v in partitions.items()}
        accepted_files = [p for item in partitions.values() for p in (root / item["accepted_path"]).rglob("*.parquet")]
        with (root / "input/taxi_zone_lookup.csv").open(encoding="utf-8-sig", newline="") as handle:
            reference_zones = sum(1 for _ in csv.DictReader(handle))
        evidence = {"spark_version": spark.version, "python_version": __import__("sys").version,
                    "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                    "master": spark.sparkContext.master, "source_rows": sum(p["source_rows"] for p in partitions.values()),
                    "quarantined_rows": quarantine_count, "quarantine_reasons": reasons,
                    "stats": stats, "partitions": catalog(root), "reporting": reporting,
                    "table_rows": tables, "written_table_reconciliation": table_stats,
                    "published_accepted_files": len(accepted_files),
                    "published_accepted_bytes": sum(p.stat().st_size for p in accepted_files),
                    "reference_zone_rows": reference_zones,
                    "input_manifest": read_json(root / "input/manifest.json")}
        atomic_json(root / "final-evidence.json", evidence)
        print(json.dumps(evidence, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
