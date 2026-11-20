"""All row transformations use Spark SQL expressions, never Python UDFs."""
from pyspark.sql import functions as F, types as T, Window

REQUIRED = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "total_amount",
}


def zones(spark, path):
    schema = T.StructType([
        T.StructField("LocationID", T.IntegerType()), T.StructField("Borough", T.StringType()),
        T.StructField("Zone", T.StringType()), T.StructField("service_zone", T.StringType()),
    ])
    result = spark.read.option("header", True).schema(schema).csv(str(path)).select(
        F.col("LocationID").alias("zone_id"), F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
    )
    summary = result.agg(F.count("*").alias("n"), F.countDistinct("zone_id").alias("unique")).first()
    if summary.n == 0 or summary.n != summary.unique or result.filter(
        F.col("zone_name").isNull() | F.col("borough").isNull()
    ).limit(1).count():
        raise ValueError("Zone lookup must be nonempty with unique, nonnull IDs/names/boroughs")
    return result


def clean(raw, source_month, zone_df):
    missing = REQUIRED - set(raw.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    for name in REQUIRED:
        dtype = raw.schema[name].dataType
        expected = (T.TimestampType, T.TimestampNTZType) if "datetime" in name else (T.NumericType,)
        if not isinstance(dtype, expected):
            raise ValueError(f"Unsupported source type for {name}: {dtype}")
    df = raw.select(
        F.col("tpep_pickup_datetime").cast("timestamp_ntz").alias("pickup_at"),
        F.col("tpep_dropoff_datetime").cast("timestamp_ntz").alias("dropoff_at"),
        F.col("PULocationID").alias("pickup_location_raw"),
        F.col("DOLocationID").alias("dropoff_location_raw"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_raw"),
        F.col("total_amount").cast("double").alias("total_raw"),
        F.to_json(F.struct(*[F.col(c) for c in raw.columns]), {"ignoreNullFields": "false"}).alias("raw_record"),
    ).withColumn("source_month", F.lit(source_month)).withColumn(
        "pickup_location_id", F.col("pickup_location_raw").cast("int")
    ).withColumn("dropoff_location_id", F.col("dropoff_location_raw").cast("int"))
    for prefix in ("pickup", "dropoff"):
        dim = F.broadcast(zone_df.select(F.col("zone_id").alias(f"_{prefix}_zone")))
        df = df.join(dim, F.col(f"{prefix}_location_id") == F.col(f"_{prefix}_zone"), "left")
    df = df.withColumn("duration_minutes", F.expr(
        "timestampdiff(MICROSECOND, pickup_at, dropoff_at) / 60000000.0"
    ))
    checks = [
        ("missing_timestamp", F.col("pickup_at").isNull() | F.col("dropoff_at").isNull()),
        ("negative_duration", F.col("dropoff_at") < F.col("pickup_at")),
        ("invalid_pickup_zone", F.col("_pickup_zone").isNull() |
         (F.col("pickup_location_raw") != F.col("pickup_location_id"))),
        ("invalid_dropoff_zone", F.col("_dropoff_zone").isNull() |
         (F.col("dropoff_location_raw") != F.col("dropoff_location_id"))),
        ("invalid_distance", F.col("trip_distance").isNull() | F.isnan("trip_distance") |
         (F.abs(F.col("trip_distance")) == float("inf")) | (F.col("trip_distance") < 0)),
    ]
    for name in ("fare", "total"):
        value = F.col(f"{name}_raw")
        checks.append((f"invalid_{name}", value.isNull() | F.isnan(value) |
                       (F.abs(value) == float("inf")) | value.cast("decimal(18,2)").isNull()))
    reasons = F.filter(F.array(*[F.when(condition, F.lit(label)) for label, condition in checks]),
                       lambda value: value.isNotNull())
    return df.withColumn("reject_reasons", reasons).withColumn(
        "fare_amount", F.col("fare_raw").cast("decimal(18,2)")
    ).withColumn("total_amount", F.col("total_raw").cast("decimal(18,2)"))


def accepted_columns(df):
    return df.select("source_month", "pickup_at", "dropoff_at", "pickup_location_id", "dropoff_location_id",
                     "trip_distance", "fare_amount", "total_amount", "duration_minutes").withColumn(
        "pickup_date", F.to_date("pickup_at")
    )


def enrich(trips, zone_df, broadcast=True):
    result = trips
    for prefix in ("pickup", "dropoff"):
        dimension = zone_df.select(
            F.col("zone_id").alias(f"{prefix}_location_id"),
            F.col("zone_name").alias(f"{prefix}_zone"),
            F.col("borough").alias(f"{prefix}_borough"),
        )
        result = result.join(F.broadcast(dimension) if broadcast else dimension,
                             f"{prefix}_location_id", "inner")
    return result


def hourly(trips):
    return trips.groupBy("pickup_date", F.hour("pickup_at").alias("pickup_hour"),
                        "pickup_location_id", "pickup_zone", "pickup_borough").agg(
        F.count("*").alias("trip_count"), F.sum("total_amount").alias("total_collected_amount"),
        F.avg("trip_distance").alias("average_distance"),
    )


def routes(trips):
    return trips.groupBy("pickup_location_id", "dropoff_location_id", "pickup_zone", "dropoff_zone").agg(
        F.count("*").alias("trip_count"), F.avg("duration_minutes").alias("average_duration_minutes"),
        F.percentile_approx("duration_minutes", [0.5, 0.95], 10000).alias("duration_percentiles"),
        F.sum("total_amount").alias("total_collected_amount"),
    ).withColumn("median_duration_minutes", F.col("duration_percentiles")[0]).withColumn(
        "p95_duration_minutes", F.col("duration_percentiles")[1]
    ).drop("duration_percentiles")


def demand_windows(hourly_df, first_date=None, last_date=None):
    """Dense calendar: absent observed hours count as zero; incomplete history remains null."""
    bounds = hourly_df.agg(F.min("pickup_date").alias("first"), F.max("pickup_date").alias("last"))
    if first_date is not None and last_date is not None:
        bounds = bounds.select(F.lit(first_date).cast("date").alias("first"),
                               F.lit(last_date).cast("date").alias("last"))
    dates = bounds.select(F.explode(F.sequence("first", "last", F.expr("interval 1 day"))).alias("pickup_date"))
    hours = hourly_df.sparkSession.range(24).select(F.col("id").cast("int").alias("pickup_hour"))
    locations = hourly_df.select("pickup_location_id", "pickup_zone", "pickup_borough").distinct()
    grid = dates.crossJoin(hours).crossJoin(F.broadcast(locations))
    dense = grid.join(hourly_df, ["pickup_date", "pickup_hour", "pickup_location_id", "pickup_zone",
                                 "pickup_borough"], "left").fillna({"trip_count": 0, "total_collected_amount": 0})
    dense = dense.withColumn("weekday", F.dayofweek("pickup_date")).withColumn(
        "day_number", F.datediff("pickup_date", F.lit("1970-01-01"))
    )
    previous = Window.partitionBy("pickup_location_id", "pickup_hour", "weekday").orderBy(
        "day_number"
    ).rangeBetween(-28, -7)
    ranked = Window.partitionBy("pickup_date", "pickup_hour").orderBy(F.desc("trip_count"))
    return dense.withColumn("history_weeks", F.count("*").over(previous)).withColumn(
        "previous_four_week_average", F.when(F.col("history_weeks") == 4, F.avg("trip_count").over(previous))
    ).withColumn("previous_week_trips", F.lag("trip_count").over(
        Window.partitionBy("pickup_location_id", "pickup_hour", "weekday").orderBy("day_number")
    )).withColumn("demand_ratio", F.when(F.col("previous_four_week_average") > 0,
                                       F.col("trip_count") / F.col("previous_four_week_average"))).withColumn(
        "zone_rank", F.dense_rank().over(ranked)
    ).drop("day_number")
