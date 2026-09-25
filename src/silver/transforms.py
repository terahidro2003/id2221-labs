"""Silver transforms ported from ingestion.ipynb."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from src.common.naming import rename_columns, utc_to_local
from src.lake import BRONZE, read_delta


def transform_taxi_zones(df: DataFrame) -> DataFrame:
    zones = rename_columns(
        df,
        {
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        },
    )
    return (
        zones.select("location_id", "borough", "zone", "service_zone")
        .withColumn("location_id", F.col("location_id").cast("int"))
        .fillna({"borough": "UNKNOWN", "zone": "UNKNOWN", "service_zone": "UNKNOWN"})
    )


def transform_weather(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn(
            "obs_timestamp",
            F.make_timestamp(
                F.col("year"),
                F.col("month"),
                F.col("day"),
                F.col("hour"),
                F.lit(0),
                F.lit(0),
            ),
        )
        .withColumn("observation_date", F.to_date("obs_timestamp"))
        .withColumn("observation_hour", F.hour("obs_timestamp"))
    )
    cols = [
        F.lit("72505394728").alias("station_id"),
        F.col("obs_timestamp"),
        F.col("temp").cast("double").alias("temperature_c"),
        (F.col("wspd").cast("double") / F.lit(3.6)).alias("wind_speed_ms"),
        F.col("observation_date"),
        F.col("observation_hour"),
    ]
    # Schema evolution: optional humidity from incremental updates
    if "humidity" in out.columns:
        cols.append(F.col("humidity").cast("double").alias("humidity"))
    return out.select(*cols)


def _gmt_timestamp_local(date_col: str, time_col: str):
    """Combine GMT date + time into America/New_York local wall time."""
    combined = F.concat_ws(
        " ",
        F.date_format(F.col(date_col).cast("date"), "yyyy-MM-dd"),
        F.date_format(F.col(time_col).cast("timestamp"), "HH:mm:ss"),
    )
    return utc_to_local(F.to_timestamp(combined, "yyyy-MM-dd HH:mm:ss"))


def transform_air_quality(df: DataFrame) -> DataFrame:
    aq = rename_columns(
        df,
        {
            "County_Code": "county_code",
            "Site_Num": "site_num",
            "Parameter_Name": "parameter",
            "Date_GMT": "date_gmt",
            "Time_GMT": "time_gmt",
            "Sample_Measurement": "value",
            "Units_of_Measure": "unit",
        },
    )
    out = (
        aq.withColumn(
            "measurement_timestamp",
            _gmt_timestamp_local("date_gmt", "time_gmt"),
        )
        .withColumn("measurement_date", F.to_date("measurement_timestamp"))
        .withColumn("measurement_hour", F.hour("measurement_timestamp"))
    )
    cols = [
        F.col("state_code").cast("int").alias("state_code"),
        F.col("county_code").cast("int").alias("county_code"),
        F.col("site_num").cast("int").alias("site_num"),
        F.col("parameter").cast("string").alias("parameter"),
        F.col("value").cast("double").alias("value"),
        F.col("unit").cast("string").alias("unit"),
        F.col("measurement_timestamp"),
        F.col("measurement_date"),
        F.col("measurement_hour"),
    ]
    # Schema evolution: optional aqi from incremental updates
    if "aqi" in out.columns:
        cols.append(F.col("aqi").cast("double").alias("aqi"))
    return out.select(*cols)


def transform_taxi_trips(df: DataFrame) -> DataFrame:
    trips = rename_columns(
        df,
        {
            "VendorID": "vendor_id",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "fare_amount": "fare_amount",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "total_amount": "total_amount",
        },
    )
    return (
        trips.withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .select(
            F.col("taxi_type").cast("string").alias("taxi_type"),
            F.col("vendor_id").cast("int").alias("vendor_id"),
            "pickup_datetime",
            "dropoff_datetime",
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("pickup_location_id").cast("int").alias("pickup_location_id"),
            F.col("dropoff_location_id").cast("int").alias("dropoff_location_id"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("tolls_amount").cast("double").alias("tolls_amount"),
            F.col("total_amount").cast("double").alias("total_amount"),
            "pickup_date",
            "pickup_hour",
        )
    )


TRANSFORMS = {
    "taxi_zones": transform_taxi_zones,
    "weather": transform_weather,
    "air_quality": transform_air_quality,
    "taxi_trips": transform_taxi_trips,
}


def transform_dataset(spark, name: str) -> DataFrame:
    fn = TRANSFORMS.get(name)
    if fn is None:
        raise KeyError(f"No silver transform for dataset: {name}")
    bronze_df = read_delta(spark, BRONZE / name)
    return fn(bronze_df)
