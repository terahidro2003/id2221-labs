"""Gold integration: enrich silver trips → integrated_taxi_trips."""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.lake import GOLD, SILVER, read_delta, show_delta, write_gold
from src.monitoring import record_run

NYC_COUNTIES = [5, 47, 61, 81, 85]
SCHEMA_VERSION = "1.0"


def build_hourly_weather(weather: DataFrame) -> DataFrame:
    aggs = [
        F.avg("temperature_c").alias("temperature_c"),
        F.avg("wind_speed_ms").alias("wind_speed_ms"),
    ]
    if "humidity" in weather.columns:
        aggs.append(F.avg("humidity").alias("humidity"))
    return weather.groupBy(
        F.col("observation_date").alias("pickup_date"),
        F.col("observation_hour").alias("pickup_hour"),
    ).agg(*aggs)


def build_hourly_air_quality(air_quality: DataFrame) -> DataFrame:
    aggs = [
        F.avg("value").alias("pm25"),
        F.first("unit").alias("pm25_unit"),
    ]
    if "aqi" in air_quality.columns:
        aggs.append(F.avg("aqi").alias("aqi"))
    return (
        air_quality.filter(F.col("county_code").isin(NYC_COUNTIES))
        .groupBy(
            F.col("measurement_date").alias("pickup_date"),
            F.col("measurement_hour").alias("pickup_hour"),
        )
        .agg(*aggs)
    )


def build_integrated(
    trips: DataFrame,
    weather: DataFrame,
    air_quality: DataFrame,
    zones: DataFrame,
) -> DataFrame:
    hourly_weather = build_hourly_weather(weather)
    hourly_aq = build_hourly_air_quality(air_quality)

    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )

    select_cols = [
        "taxi_type",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pickup_location_id",
        "pickup_zone",
        "pickup_borough",
        "dropoff_location_id",
        "dropoff_zone",
        "dropoff_borough",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "temperature_c",
        "wind_speed_ms",
        "pm25",
        "pm25_unit",
        "pickup_date",
        "pickup_hour",
    ]
    if "humidity" in hourly_weather.columns:
        select_cols.insert(select_cols.index("pm25"), "humidity")
    if "aqi" in hourly_aq.columns:
        select_cols.insert(select_cols.index("pickup_date"), "aqi")

    return (
        trips.join(F.broadcast(pickup_zones), "pickup_location_id", "left")
        .join(F.broadcast(dropoff_zones), "dropoff_location_id", "left")
        .join(F.broadcast(hourly_weather), ["pickup_date", "pickup_hour"], "left")
        .join(F.broadcast(hourly_aq), ["pickup_date", "pickup_hour"], "left")
        .fillna(
            {
                "pickup_zone": "UNKNOWN",
                "pickup_borough": "UNKNOWN",
                "dropoff_zone": "UNKNOWN",
                "dropoff_borough": "UNKNOWN",
            }
        )
        .select(*select_cols)
    )


def integrate(
    spark: SparkSession,
    *,
    write_borough_layout: bool = True,
) -> DataFrame:
    with record_run(
        spark,
        layer="gold",
        dataset="integrated_taxi_trips",
        schema_version=SCHEMA_VERSION,
    ) as run:
        trips = read_delta(spark, SILVER / "taxi_trips")
        weather = read_delta(spark, SILVER / "weather")
        air_quality = read_delta(spark, SILVER / "air_quality")
        zones = read_delta(spark, SILVER / "taxi_zones")

        integrated = build_integrated(trips, weather, air_quality, zones).cache()
        n_rows = integrated.count()

        t0 = time.perf_counter()
        write_gold(integrated, "integrated_taxi_trips", partition_by=["pickup_date"])
        print(f"write by_date: {time.perf_counter() - t0:.1f}s")
        show_delta(spark, GOLD / "integrated_taxi_trips")

        if write_borough_layout:
            t0 = time.perf_counter()
            write_gold(
                integrated,
                "integrated_taxi_trips_by_borough",
                partition_by=["pickup_borough"],
            )
            print(f"write by_borough: {time.perf_counter() - t0:.1f}s")

        run.set_counts(
            processed=n_rows,
            inserted=n_rows,
            rejected=0,
            validation_failures=0,
            validation_ok=True,
        )
        return integrated
