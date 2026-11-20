"""Distributed multiset equality checks; collects only a compact summary."""
import argparse
from decimal import Decimal
from pathlib import Path
from statistics import median

from pyspark.sql import functions as F

from taxi_spark.runtime import session
from taxi_spark.storage import atomic_json, catalog, read_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/workspace/data")
    parser.add_argument("--suite", required=True)
    args = parser.parse_args()
    root = Path(args.root)
    suite_path = root / "benchmarks" / f"{args.suite}.json"
    suite = read_json(suite_path)
    spark = session("benchmark-equality")
    try:
        measured = [read_json(root / "benchmarks" / run / "metrics.json") for run in suite["measured_runs"]]
        baseline = spark.read.parquet(str(root / "benchmarks" / suite["measured_runs"][0] / "result"))
        versions = measured[0]["partition_versions"]
        for item in measured[1:]:
            if item["partition_versions"] != versions:
                raise AssertionError("Published partitions changed during benchmark suite")
            other = spark.read.parquet(str(root / "benchmarks" / item["run_id"] / "result"))
            if baseline.exceptAll(other).limit(1).count() or other.exceptAll(baseline).limit(1).count():
                raise AssertionError("Benchmark output differs: " + item["run_id"])
        current = catalog(root)
        if {k:v["version"] for k,v in current.items()} != versions:
            raise AssertionError("Source catalog changed before final benchmark verification")
        totals = baseline.agg(F.count("*").alias("result_rows"), F.sum("trip_count").alias("trips"),
                              F.sum("total_collected_amount").alias("total_collected_amount")).first()
        source = current[suite["month"]]
        assert totals.trips == source["accepted_rows"] == measured[0]["input_trip_rows"]
        assert totals.total_collected_amount == Decimal(source["total_collected_amount"])
        suite["result_rows"] = totals.result_rows
        suite["aggregate_reconciliation"] = {"trips": totals.trips,
                                             "total_collected_amount": str(totals.total_collected_amount)}
        suite["exact_multiset_equality"] = True
        suite["metrics"] = measured
        suite["median_seconds"] = {name: median(item["seconds"] for item in measured if item["config"] == name)
                                   for name in sorted({item["config"] for item in measured})}
        atomic_json(root / "benchmarks" / f"{args.suite}-verified.json", suite)
        print(suite["median_seconds"], "All results exactly equal")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
