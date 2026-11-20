"""Publish monthly clean data and consistent analytical snapshots."""
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.sql import functions as F

from . import transforms as tx
from .storage import atomic_json, catalog, month, published_trips, sha256, writer_lock

POLICY_VERSION = "2024-yellow-v1"


def now():
    return datetime.now(timezone.utc).isoformat()


def ingest(spark, root, input_dir, months, reprocess=False, max_reject_fraction=0.01, fail_before_publish=False):
    root, input_dir = Path(root), Path(input_dir)
    if not 0 <= max_reject_fraction <= 1:
        raise ValueError("Rejection threshold must be between zero and one")
    for value in months:
        month(value)
    lookup = input_dir / "taxi_zone_lookup.csv"
    zone_hash = sha256(lookup)
    zone_df = tx.zones(spark, lookup)
    outcomes = []
    with writer_lock(root):
        for value in months:
            source = input_dir / f"yellow_tripdata_{value}.parquet"
            source_hash = sha256(source)
            partitions = catalog(root)
            previous = partitions.get(value)
            if previous and previous["source_sha256"] == source_hash and previous["zones_sha256"] == zone_hash \
                    and previous["policy_version"] == POLICY_VERSION and not reprocess:
                if previous["rejected_rows"] / previous["source_rows"] > max_reject_fraction:
                    raise ValueError("Previously published data exceeds the requested rejection threshold")
                outcomes.append({"source_month": value, "status": "skipped", "version": previous["version"]})
                print(f"Skipped unchanged partition {value}", flush=True)
                continue
            started = perf_counter()
            version = f"{value}-{uuid4().hex}"
            relative = Path("versions") / version
            destination = root / relative
            destination.mkdir(parents=True)
            audit = {"source_month": value, "version": version, "status": "running", "started_at": now(),
                     "source_sha256": source_hash, "zones_sha256": zone_hash, "source_bytes": source.stat().st_size,
                     "policy_version": POLICY_VERSION}
            audit_path = root / "audit" / f"{version}.json"
            atomic_json(audit_path, audit)
            print(f"Started partition {value}: {source.stat().st_size:,} input bytes", flush=True)
            cleaned = None
            try:
                raw = spark.read.parquet(str(source))
                audit["source_schema"] = json.loads(raw.schema.json())
                # Do not cache the wide raw JSON projection. Catalyst can prune it from
                # counts/accepted writes and compute it only for quarantined records.
                cleaned = tx.clean(raw, value, zone_df)
                stats = cleaned.agg(
                    F.count("*").alias("source_rows"),
                    F.sum(F.when(F.size("reject_reasons") > 0, 1).otherwise(0)).alias("rejected_rows"),
                ).first().asDict()
                stats["rejected_rows"] = stats["rejected_rows"] or 0
                stats["accepted_rows"] = stats["source_rows"] - stats["rejected_rows"]
                audit.update(stats)
                if not stats["accepted_rows"] or stats["rejected_rows"] / stats["source_rows"] > max_reject_fraction:
                    raise ValueError(f"Quality gate failed: {stats}")
                accepted = tx.accepted_columns(cleaned.filter(F.size("reject_reasons") == 0))
                rejected = cleaned.filter(F.size("reject_reasons") > 0).select(
                    "source_month", "raw_record", "reject_reasons"
                )
                accepted.repartition(2).write.mode("errorifexists").partitionBy("source_month").parquet(
                    str(destination / "accepted")
                )
                rejected.coalesce(1).write.mode("errorifexists").parquet(str(destination / "quarantine"))
                # Read written files back before publishing the pointer.
                written = spark.read.parquet(str(destination / "accepted"))
                measured = written.agg(F.count("*").alias("n"), F.sum("total_amount").alias("total"),
                                       F.sum("fare_amount").alias("fare")).first()
                if measured.n != stats["accepted_rows"] or spark.read.parquet(
                    str(destination / "quarantine")
                ).count() != stats["rejected_rows"]:
                    raise ValueError("Written output does not reconcile with source counts")
                # Verify the input did not change while Spark was reading it.
                if sha256(source) != source_hash or sha256(lookup) != zone_hash:
                    raise ValueError("Input changed during ingestion")
                if fail_before_publish:
                    raise RuntimeError("Controlled failure after output validation, before catalog publication")
                item = {**audit, "status": "success", "published_at": now(),
                        "accepted_path": str(relative / "accepted"), "quarantine_path": str(relative / "quarantine"),
                        "total_collected_amount": str(measured.total), "fare_amount": str(measured.fare),
                        "elapsed_seconds": round(perf_counter() - started, 3)}
                atomic_json(destination / "manifest.json", item)
                partitions[value] = item
                atomic_json(root / "catalog" / "partitions.json", partitions)
                audit.update(item)
                outcomes.append(item)
                print(f"Published {value}: {stats['accepted_rows']:,} accepted, "
                      f"{stats['rejected_rows']:,} quarantined", flush=True)
            except Exception as exc:
                audit.update(status="failed", error=str(exc), finished_at=now())
                raise
            finally:
                if cleaned is not None:
                    cleaned.unpersist()
                audit["elapsed_seconds"] = round(perf_counter() - started, 3)
                atomic_json(audit_path, audit)
    return outcomes


def report(spark, root, input_dir):
    root = Path(root)
    with writer_lock(root):
        trips, snapshot = published_trips(spark, root)
        keys = sorted(snapshot)
        first = date.fromisoformat(keys[0] + "-01")
        end_year, end_month = map(int, keys[-1].split("-"))
        end = date(end_year + (end_month == 12), end_month % 12 + 1, 1)
        expected_months = (end.year - first.year) * 12 + end.month - first.month
        if expected_months != len(keys):
            raise ValueError("Reporting requires contiguous published source months; fill missing months first")
        if expected_months > 36:
            raise ValueError("Limit each reporting calendar to 36 months")
        lookup = Path(input_dir) / "taxi_zone_lookup.csv"
        zone_hash = sha256(lookup)
        if any(item["zones_sha256"] != zone_hash for item in snapshot.values()):
            raise ValueError("Zone lookup changed; reprocess source partitions before reporting")
        within = trips.filter((F.col("pickup_date") >= F.lit(first)) & (F.col("pickup_date") < F.lit(end)))
        # Preserve source outliers in clean data, but bound demand calendars to the source-month range.
        excluded = trips.filter((F.col("pickup_date") < F.lit(first)) | (F.col("pickup_date") >= F.lit(end))).count()
        enriched = tx.enrich(within, tx.zones(spark, Path(input_dir) / "taxi_zone_lookup.csv")).persist(
            StorageLevel.DISK_ONLY
        )
        version = uuid4().hex
        relative = Path("reports") / version
        destination = root / relative
        destination.mkdir(parents=True)
        started = perf_counter()
        try:
            hourly = tx.hourly(enriched).persist(StorageLevel.DISK_ONLY)
            # Observed hourly and route facts retain all observed hours; dense grid fills zeros only for demand.
            hourly.coalesce(2).write.parquet(str(destination / "hourly"))
            route_df = tx.routes(enriched)
            route_df.coalesce(2).write.parquet(str(destination / "routes"))
            demand = tx.demand_windows(hourly, first, end - timedelta(days=1))
            demand.coalesce(2).write.parquet(str(destination / "demand"))
            facts = enriched.agg(F.count("*").alias("trips"), F.sum("total_amount").alias("total")).first()
            if facts.trips != within.count():
                raise ValueError("Zone enrichment changed the accepted trip count")
            hstats = hourly.agg(F.sum("trip_count").alias("trips"), F.sum("total_collected_amount").alias("total")).first()
            rstats = route_df.agg(F.sum("trip_count").alias("trips"), F.sum("total_collected_amount").alias("total")).first()
            if facts != hstats or facts != rstats:
                raise ValueError("Reporting counts or charges do not reconcile")
            dashboard = {
                "trips": facts.trips, "total_collected_amount": str(facts.total),
                "first_date": str(first), "end_date_exclusive": str(end), "outside_calendar_rows": excluded,
                "observed_zones": hourly.select("pickup_location_id").distinct().count(),
                "top_zones": [r.asDict() for r in hourly.groupBy("pickup_location_id", "pickup_zone").agg(
                    F.sum("trip_count").alias("trips")
                ).orderBy(F.desc("trips"), "pickup_zone").limit(10).collect()],
                "daily": [r.asDict() for r in hourly.groupBy("pickup_date").agg(
                    F.sum("trip_count").alias("trips"), F.sum("total_collected_amount").alias("collected")
                ).orderBy("pickup_date").collect()],
                "hourly": [r.asDict() for r in hourly.groupBy("pickup_hour").agg(
                    F.sum("trip_count").alias("trips")
                ).orderBy("pickup_hour").collect()],
                "routes": [r.asDict() for r in route_df.filter(F.col("trip_count") >= 100).orderBy(
                    F.desc("p95_duration_minutes"), "pickup_location_id", "dropoff_location_id"
                ).limit(20).collect()],
                "spikes": [r.asDict() for r in demand.filter(
                    (F.col("trip_count") >= 100) & (F.col("previous_four_week_average") >= 20)
                ).orderBy(F.desc("demand_ratio"), "pickup_date", "pickup_hour", "pickup_location_id").limit(20).collect()],
            }
            atomic_json(destination / "dashboard.json", dashboard)
            manifest = {"version": version, "published_at": now(), "report_path": str(relative),
                        "partition_versions": {k: v["version"] for k, v in snapshot.items()},
                        "trip_count": facts.trips, "total_collected_amount": str(facts.total),
                        "outside_calendar_rows": excluded, "elapsed_seconds": round(perf_counter()-started, 3)}
            atomic_json(destination / "manifest.json", manifest)
            atomic_json(root / "catalog" / "reporting.json", manifest)
            return manifest
        finally:
            enriched.unpersist()
            if "hourly" in locals():
                hourly.unpersist()
