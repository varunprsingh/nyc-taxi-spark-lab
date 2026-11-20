"""Real-data acceptance: repeat, failure recovery, exact multiset and aggregate checks."""
import json
from pathlib import Path

from pyspark.sql import functions as F

from taxi_spark.pipeline import ingest
from taxi_spark.runtime import session
from taxi_spark.storage import atomic_json, catalog


def measure(spark, root, item):
    df = spark.read.parquet(str(root / item["accepted_path"]))
    return df, df.agg(F.count("*").alias("rows"), F.sum("total_amount").alias("total"),
                      F.sum("fare_amount").alias("fare")).first().asDict()


def main():
    root = Path("/workspace/data")
    spark = session("taxi-real-reprocessing-verification")
    evidence = {"source_month": "2024-01", "spark_version": spark.version}
    try:
        before = catalog(root)["2024-01"]
        old, baseline = measure(spark, root, before)
        evidence["baseline"] = baseline
        ingest(spark, root, root / "input", ["2024-01"], reprocess=True)
        after_repeat = catalog(root)["2024-01"]
        repeated, repeated_stats = measure(spark, root, after_repeat)
        assert baseline == repeated_stats, (baseline, repeated_stats)
        assert not old.exceptAll(repeated).limit(1).count()
        assert not repeated.exceptAll(old).limit(1).count()
        evidence["repeat_aggregates_equal"] = True
        evidence["repeat_multiset_equal"] = True
        evidence["versions"] = {"initial": before["version"], "repeat": after_repeat["version"]}
        try:
            ingest(spark, root, root / "input", ["2024-01"], reprocess=True, fail_before_publish=True)
        except RuntimeError as exc:
            assert "Controlled failure" in str(exc)
            evidence["controlled_error"] = str(exc)
        else:
            raise AssertionError("Controlled replacement unexpectedly succeeded")
        assert catalog(root)["2024-01"] == after_repeat
        preserved, preserved_stats = measure(spark, root, catalog(root)["2024-01"])
        assert preserved_stats == baseline
        assert not repeated.exceptAll(preserved).limit(1).count()
        evidence["failed_replacement_preserved_catalog_and_data"] = True
        evidence["partitions"] = catalog(root)
        atomic_json(root / "real-verification.json", evidence)
        print(json.dumps(evidence, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
