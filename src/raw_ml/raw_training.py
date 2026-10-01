from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SUPPORTED_SUFFIXES = {".csv", ".parquet"}


def _files(raw_root: Path, keywords: tuple[str, ...]) -> list[Path]:
    files = [
        path for path in raw_root.rglob("*")
        if path.is_file()
        and path.suffix.lower() in SUPPORTED_SUFFIXES
        and any(keyword.lower() in str(path).lower() for keyword in keywords)
    ]

    if not files:
        raise FileNotFoundError(
            f"No raw dataset found for keywords {keywords} in {raw_root}"
        )

    return sorted(files)


def _read(spark: SparkSession, paths: list[Path]) -> DataFrame:
    def read_one(path: Path) -> DataFrame:
        if path.suffix.lower() == ".csv":
            return (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("mode", "PERMISSIVE")
                .csv(str(path))
            )

        return spark.read.parquet(str(path))

    return reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        [read_one(path) for path in paths],
    )


def _rename(df: DataFrame, target: str, aliases: tuple[str, ...]) -> DataFrame:
    columns = {column.lower(): column for column in df.columns}

    for alias in aliases:
        if alias.lower() in columns:
            source = columns[alias.lower()]
            if source != target:
                return df.withColumnRenamed(source, target)
            return df

    raise ValueError(
        f"Missing required column '{target}'. Available columns: {df.columns}"
    )


def _timestamp(df: DataFrame, source: str, target: str) -> DataFrame:
    return df.withColumn(
        target,
        F.coalesce(
            F.to_timestamp(F.col(source)),
            F.to_timestamp(F.col(source), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col(source), "MM/dd/yyyy HH:mm:ss"),
        ),
    )


def _load_taxi(spark: SparkSession, raw_root: Path) -> DataFrame:
    taxi = _read(spark, _files(raw_root, ("trip", "yellow", "green")))

    taxi = _rename(
        taxi,
        "pickup_datetime",
        ("tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"),
    )
    taxi = _rename(
        taxi,
        "dropoff_datetime",
        ("tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"),
    )
    taxi = _rename(taxi, "pickup_location_id", ("PULocationID", "pickup_location_id"))
    taxi = _rename(taxi, "dropoff_location_id", ("DOLocationID", "dropoff_location_id"))
    taxi = _rename(taxi, "fare_amount", ("fare_amount", "fare", "total_amount"))
    taxi = _rename(taxi, "trip_distance", ("trip_distance", "distance"))

    taxi = _timestamp(taxi, "pickup_datetime", "pickup_datetime")
    taxi = _timestamp(taxi, "dropoff_datetime", "dropoff_datetime")

    return (
        taxi
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("fare_amount").isNotNull()
            & F.col("trip_distance").isNotNull()
            & (F.col("fare_amount") > 0)
            & (F.col("trip_distance") > 0.05)
        )
        .withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
    )


def _load_zones(spark: SparkSession, raw_root: Path) -> DataFrame:
    zones = _read(spark, _files(raw_root, ("zone", "lookup")))

    zones = _rename(zones, "location_id", ("LocationID", "location_id"))
    zones = _rename(zones, "borough", ("Borough", "borough"))

    return (
        zones
        .select(
            F.col("location_id").cast("int").alias("location_id"),
            F.col("borough").alias("borough"),
        )
        .dropDuplicates(["location_id"])
    )


def _load_weather(spark: SparkSession, raw_root: Path) -> DataFrame:
    weather = _read(spark, _files(raw_root, ("weather",)))

    weather = _rename(
        weather,
        "temperature_c",
        ("temperature_c", "temperature", "temp", "temp_c"),
    )
    weather = _rename(
        weather,
        "wind_speed_ms",
        ("wind_speed_ms", "wind_speed", "windspeed", "wind", "wspd"),
    )

    column_names = {column.lower() for column in weather.columns}

    if {"year", "month", "day", "hour"}.issubset(column_names):
        weather = weather.withColumn(
            "weather_datetime",
            F.make_timestamp(
                F.col("year").cast("int"),
                F.col("month").cast("int"),
                F.col("day").cast("int"),
                F.col("hour").cast("int"),
                F.lit(0),
                F.lit(0),
            ),
        )
    else:
        weather = _rename(
            weather,
            "weather_datetime",
            ("timestamp", "datetime", "weather_datetime", "date"),
        )
        weather = _timestamp(
            weather,
            "weather_datetime",
            "weather_datetime",
        )

    return (
        weather
        .withColumn("weather_hour", F.date_trunc("hour", "weather_datetime"))
        .withColumn("temperature_c", F.col("temperature_c").cast("double"))
        .withColumn(
            "wind_speed_ms",
            F.col("wind_speed_ms").cast("double") / F.lit(3.6),
        )
        .groupBy("weather_hour")
        .agg(
            F.avg("temperature_c").alias("temperature_c"),
            F.avg("wind_speed_ms").alias("wind_speed_ms"),
        )
    )


def _load_air_quality(spark: SparkSession, raw_root: Path) -> DataFrame:
    air = _read(
        spark,
        _files(raw_root, ("air_quality", "hourly_88101")),
    )

    air = _rename(
        air,
        "air_date",
        ("Date Local", "Date (Local)", "date_local", "date"),
    )
    air = _rename(
        air,
        "air_time",
        ("Time Local", "Time (Local)", "time_local", "time"),
    )
    air = _rename(
        air,
        "pm25",
        ("Sample Measurement", "sample_measurement", "pm25", "PM2.5"),
    )

    air = air.withColumn(
        "air_datetime",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.col("air_date").cast("string"),
                F.col("air_time").cast("string"),
            ),
            "yyyy-MM-dd HH:mm",
        ),
    )

    return (
        air
        .withColumn("air_hour", F.date_trunc("hour", "air_datetime"))
        .withColumn("pm25", F.col("pm25").cast("double"))
        .filter(F.col("air_datetime").isNotNull())
        .groupBy("air_hour")
        .agg(F.avg("pm25").alias("pm25"))
    )

def build_raw_training_splits(
    spark: SparkSession,
    raw_root: Path,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    taxi = _load_taxi(spark, raw_root)
    zones = _load_zones(spark, raw_root)
    weather = _load_weather(spark, raw_root)
    air_quality = _load_air_quality(spark, raw_root)

    pickup_zones = (
        zones
        .withColumnRenamed("location_id", "pickup_location_id")
        .withColumnRenamed("borough", "pickup_borough")
    )

    dropoff_zones = (
        zones
        .withColumnRenamed("location_id", "dropoff_location_id")
        .withColumnRenamed("borough", "dropoff_borough")
    )

    integrated = (
        taxi
        .join(pickup_zones, "pickup_location_id", "left")
        .join(dropoff_zones, "dropoff_location_id", "left")
        .join(
            weather,
            F.col("pickup_hour") == F.col("weather_hour"),
            "left",
        )
        .join(
            air_quality,
            F.col("pickup_hour") == F.col("air_hour"),
            "left",
        )
        .drop("weather_hour", "air_hour")
        .withColumn(
            "pickup_borough",
            F.coalesce(F.col("pickup_borough"), F.lit("Unknown")),
        )
        .withColumn(
            "dropoff_borough",
            F.coalesce(F.col("dropoff_borough"), F.lit("Unknown")),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .filter(~F.col("pickup_borough").isin("Unknown", "EWR"))
    )
    
    integrated = (
        integrated
        .withColumn("temperature_c", F.coalesce(F.col("temperature_c"), F.lit(0.0)))
        .withColumn("wind_speed_ms", F.coalesce(F.col("wind_speed_ms"), F.lit(0.0)))
        .withColumn("pm25", F.coalesce(F.col("pm25"), F.lit(0.0)))
    )

    train = integrated.filter(F.col("pickup_date") < F.lit("2024-02-21"))
    validation = integrated.filter(
        (F.col("pickup_date") >= F.lit("2024-02-21"))
        & (F.col("pickup_date") < F.lit("2024-03-11"))
    )
    test = integrated.filter(F.col("pickup_date") >= F.lit("2024-03-11"))

    return train, validation, test
