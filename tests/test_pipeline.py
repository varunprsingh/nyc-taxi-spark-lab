import json
import tempfile
import unittest
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path

from pyspark.sql import types as T, functions as F

from taxi_spark import transforms as tx
from taxi_spark.pipeline import ingest, report
from taxi_spark.runtime import session
from taxi_spark.storage import catalog, month, published_trips, writer_lock


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session("taxi-fixture-tests")
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.input = cls.root / "input"
        cls.input.mkdir()
        (cls.input / "taxi_zone_lookup.csv").write_text(
            'LocationID,Borough,Zone,service_zone\n1,Manhattan,One,Yellow\n2,Queens,Two,Yellow\n'
        )
        cls.zones = tx.zones(cls.spark, cls.input / "taxi_zone_lookup.csv")
        cls.schema = T.StructType([
            T.StructField("tpep_pickup_datetime", T.TimestampNTZType()),
            T.StructField("tpep_dropoff_datetime", T.TimestampNTZType()),
            T.StructField("PULocationID", T.IntegerType()), T.StructField("DOLocationID", T.IntegerType()),
            T.StructField("trip_distance", T.DoubleType()), T.StructField("fare_amount", T.DoubleType()),
            T.StructField("total_amount", T.DoubleType()),
        ])
        pickup, dropoff = datetime(2024, 1, 1, 12), datetime(2024, 1, 1, 12, 10)
        valid = (pickup, dropoff, 1, 2, 2.0, 10.0, 12.0)
        cls.raw = cls.spark.createDataFrame([
            valid, valid, (pickup, dropoff, 1, 2, 0.0, -5.0, -5.0),
            (dropoff, pickup, 999, 2, -1.0, 10.0, 12.0),
            (pickup, dropoff, 1, 2, float("inf"), 10.0, 12.0),
            (None, dropoff, 1, 2, 1.0, 10.0, 12.0),
        ], cls.schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_month_validation(self):
        self.assertEqual(month("2024-01"), "2024-01")
        for value in ["2024-1", "2024-13", "../../etc", "2024-00"]:
            with self.assertRaises(ValueError):
                month(value)

    def test_validation_preserves_duplicates_refunds_and_multiple_reasons(self):
        result = tx.clean(self.raw, "2024-01", self.zones)
        self.assertEqual(result.count(), 6)
        accepted = tx.accepted_columns(result.filter(F.size("reject_reasons") == 0))
        self.assertEqual(accepted.count(), 3)
        self.assertEqual(accepted.agg(F.sum("total_amount")).first()[0], Decimal("19.00"))
        invalid = result.filter(F.array_contains("reject_reasons", "negative_duration")).first()
        self.assertEqual(set(invalid.reject_reasons), {"negative_duration", "invalid_pickup_zone", "invalid_distance"})
        self.assertIn('"PULocationID":999', invalid.raw_record)
        self.assertEqual(accepted.first().duration_minutes, 10.0)

    def test_schema_and_lookup_rejected(self):
        with self.assertRaises(ValueError):
            tx.clean(self.raw.drop("fare_amount"), "2024-01", self.zones)
        with self.assertRaises(ValueError):
            tx.clean(self.raw.withColumn("fare_amount", F.lit("bad")), "2024-01", self.zones)
        path = self.input / "duplicate.csv"
        path.write_text('LocationID,Borough,Zone,service_zone\n1,A,B,C\n1,A,B,C\n')
        with self.assertRaises(ValueError):
            tx.zones(self.spark, path)

    def test_dense_calendar_and_complete_four_week_baseline(self):
        schema = "pickup_date date, pickup_hour int, pickup_location_id int, pickup_zone string, pickup_borough string, trip_count long, total_collected_amount decimal(18,2), average_distance double"
        data = self.spark.createDataFrame([
            (date(2024,1,day), 12, 1, "One", "Manhattan", trips, Decimal("10.00"), 1.0)
            for day,trips in [(1,10),(15,30),(22,40),(29,80)]
        ], schema)
        result = tx.demand_windows(data)
        self.assertEqual(result.filter((F.col("pickup_date") == F.lit(date(2024,1,8))) &
                                       (F.col("pickup_hour") == 12)).first().trip_count, 0)
        last = result.filter((F.col("pickup_date") == F.lit(date(2024,1,29))) & (F.col("pickup_hour") == 12)).first()
        self.assertEqual(last.history_weeks, 4)
        self.assertEqual(last.previous_four_week_average, 20.0)
        self.assertEqual(last.previous_week_trips, 40)
        self.assertEqual(last.demand_ratio, 4.0)
        self.assertEqual(last.zone_rank, 1)
        self.assertIsNone(result.filter(F.col("pickup_date") == F.lit(date(2024,1,22))).first().previous_four_week_average)
        bounded = tx.demand_windows(data, date(2024,1,1), date(2024,1,31))
        self.assertEqual(bounded.count(), 31 * 24)
        final_empty = bounded.filter((F.col("pickup_date") == F.lit(date(2024,1,31))) &
                                     (F.col("pickup_hour") == 12)).first()
        self.assertEqual(final_empty.trip_count, 0)

    def test_atomic_reprocess_failure_and_reporting_reconciliation(self):
        source = self.input / "yellow_tripdata_2024-01.parquet"
        self.raw.coalesce(1).write.parquet(str(self.root / "fixture"))
        import shutil
        shutil.copyfile(next((self.root / "fixture").glob("*.parquet")), source)
        first = ingest(self.spark, self.root, self.input, ["2024-01"], max_reject_fraction=0.6)[0]
        self.assertEqual((first["source_rows"], first["accepted_rows"], first["rejected_rows"]), (6,3,3))
        self.assertEqual(ingest(self.spark, self.root, self.input, ["2024-01"], max_reject_fraction=0.6)[0]["status"], "skipped")
        with self.assertRaises(ValueError):
            ingest(self.spark, self.root, self.input, ["2024-01"], max_reject_fraction=0.01)
        repeat = ingest(self.spark, self.root, self.input, ["2024-01"], reprocess=True, max_reject_fraction=0.6)[0]
        self.assertEqual(first["total_collected_amount"], repeat["total_collected_amount"])
        self.assertNotEqual(first["version"], repeat["version"])
        before = catalog(self.root)
        with self.assertRaisesRegex(RuntimeError, "Controlled failure"):
            ingest(self.spark, self.root, self.input, ["2024-01"], reprocess=True,
                   max_reject_fraction=0.6, fail_before_publish=True)
        self.assertEqual(before, catalog(self.root))
        failed = [json.loads(p.read_text()) for p in (self.root / "audit").glob("*.json")
                  if json.loads(p.read_text())["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(published_trips(self.spark, self.root)[0].count(), 3)
        manifest = report(self.spark, self.root, self.input)
        self.assertEqual(manifest["trip_count"], 3)
        self.assertEqual(manifest["total_collected_amount"], "19.00")

    def test_writer_lock_excludes_other_publisher(self):
        with writer_lock(self.root):
            with self.assertRaises(RuntimeError):
                with writer_lock(self.root):
                    pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
