"""One fresh Spark application per measured run; orchestrator alternates configurations."""
import argparse
import json
from pathlib import Path
from time import perf_counter

from pyspark.sql import functions as F, Window

from taxi_spark.runtime import session
from taxi_spark.storage import atomic_json, published_trips, month
from taxi_spark.transforms import enrich, zones

CONFIGS = {
    "baseline": {"broadcast": False, "aqe": False, "shuffle": 32},
    "broadcast_only": {"broadcast": True, "aqe": False, "shuffle": 32},
    "tuned": {"broadcast": True, "aqe": True, "shuffle": 8},
}


def task_metrics(directory):
    totals = {"shuffle_read_bytes": 0, "shuffle_write_bytes": 0, "disk_spilled_bytes": 0,
              "memory_spilled_bytes": 0, "executor_run_ms": 0, "successful_tasks": 0}
    seen = set()
    final_plans = []
    for path in Path(directory).glob("*"):
        if not path.is_file() or path.name.startswith("."):
            continue
        with path.open() as handle:
            for line in handle:
                event = json.loads(line)
                if event["Event"].endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
                    final_plans.append(event.get("physicalPlanDescription", ""))
                if event["Event"] != "SparkListenerTaskEnd" or event.get("Task End Reason", {}).get("Reason") != "Success":
                    continue
                key = (event["Stage ID"], event["Task Info"]["Index"])
                if key in seen:
                    continue
                seen.add(key)
                metric = event["Task Metrics"]
                read = metric.get("Shuffle Read Metrics", {})
                totals["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
                totals["shuffle_write_bytes"] += metric.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                totals["disk_spilled_bytes"] += metric.get("Disk Bytes Spilled", 0)
                totals["memory_spilled_bytes"] += metric.get("Memory Bytes Spilled", 0)
                totals["executor_run_ms"] += metric.get("Executor Run Time", 0)
                totals["successful_tasks"] += 1
    return totals, final_plans


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", choices=CONFIGS, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--month", type=month, default="2024-01")
    parser.add_argument("--root", default="/workspace/data")
    args = parser.parse_args()
    output = Path(args.root) / "benchmarks" / args.run_id
    output.mkdir(parents=True, exist_ok=False)
    config = CONFIGS[args.config]
    spark = session("taxi-benchmark-" + args.run_id, config["aqe"], output / "events")
    spark.conf.set("spark.sql.shuffle.partitions", str(config["shuffle"]))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760" if config["broadcast"] else "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760" if config["broadcast"] else "-1")
    try:
        # Startup/control-plane lookup validation is deliberately outside the query timer.
        trips, snapshot = published_trips(spark, args.root)
        if args.month not in snapshot:
            raise ValueError("Benchmark month is not published")
        zone_df = zones(spark, Path(args.root) / "input" / "taxi_zone_lookup.csv")
        # Common initialization action for all cases; not a timed workload warmup.
        spark.range(1000).count()
        start = perf_counter()
        chosen = trips.filter(F.col("source_month") == args.month)
        joined = enrich(chosen, zone_df, config["broadcast"])
        result = joined.groupBy("pickup_location_id", "dropoff_location_id", "pickup_zone", "dropoff_zone",
                                F.dayofweek("pickup_at").alias("weekday"),
                                F.hour("pickup_at").alias("hour")).agg(
            F.count("*").alias("trip_count"), F.sum("total_amount").alias("total_collected_amount")
        ).withColumn("route_rank", F.dense_rank().over(Window.partitionBy("weekday", "hour").orderBy(F.desc("trip_count"))))
        (output / "plan.txt").write_text(result._jdf.queryExecution().toString(), encoding="utf-8")
        result.write.mode("errorifexists").parquet(str(output / "result"))
        seconds = perf_counter() - start
        files = list((output / "result").glob("*.parquet"))
        metrics = {"run_id": args.run_id, "config": args.config, "settings": config,
                   "seconds": round(seconds, 3), "source_month": args.month,
                   "input_trip_rows": snapshot[args.month]["accepted_rows"],
                   "partition_versions": {k: v["version"] for k,v in snapshot.items()},
                   "output_files": len(files), "output_bytes": sum(p.stat().st_size for p in files),
                   "spark_version": spark.version, "master": spark.sparkContext.master,
                   "application_id": spark.sparkContext.applicationId,
                   "java_version": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                   "python_version": __import__("sys").version,
                   "metrics_scope": "whole application including lookup validation and initialization; timer is query+write only"}
    finally:
        spark.stop()
    counters, plans = task_metrics(output / "events")
    metrics.update(counters)
    (output / "adaptive-plans.txt").write_text("\n\n".join(plans), encoding="utf-8")
    atomic_json(output / "metrics.json", metrics)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
