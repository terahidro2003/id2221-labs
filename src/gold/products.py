"""Gold data products under data/lake/gold/data_products/."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession

from src.common.metadata import safe_write_delta, should_refresh
from src.lake import GOLD, read_delta
from src.queries.analytical import (
    QUERY_1_MONTHLY_ZONE_DEMAND,
    QUERY_4_ZONE_WEATHER_SENSITIVITY,
    QUERY_DAILY_BOROUGH_MOBILITY,
    query_2_with_humidity,
    query_3_with_aqi,
)

PRODUCTS_ROOT = GOLD / "data_products"


@dataclass(frozen=True)
class ProductSpec:
    name: str
    sql: str
    partition_by: Optional[list[str]] = None


def build_products(spark: SparkSession, force: bool = False) -> None:
    integrated_path = GOLD / "integrated_taxi_trips"
    integrated = read_delta(spark, integrated_path)
    integrated.createOrReplaceTempView("integrated_taxi_trips")

    has_humidity = "humidity" in integrated.columns
    has_aqi = "aqi" in integrated.columns
    schema_version = "1.1" if (has_humidity or has_aqi) else "1.0"
    print(f"--- Pipeline Execution Plan (Schema Version: {schema_version}) ---")

    products = [
        ProductSpec("daily_borough_mobility", QUERY_DAILY_BOROUGH_MOBILITY, ["pickup_date"]),
        ProductSpec("taxi_zone_monthly_demand", QUERY_1_MONTHLY_ZONE_DEMAND, ["trip_month"]),
        ProductSpec("weather_impact_summary", query_2_with_humidity(has_humidity)),
        ProductSpec("air_quality_demand_summary", query_3_with_aqi(has_aqi)),
        ProductSpec("zone_weather_sensitivity", QUERY_4_ZONE_WEATHER_SENSITIVITY),
    ]

    for product in products:
        target = PRODUCTS_ROOT / product.name
        if not force and not should_refresh(spark, target, [integrated_path]):
            print(f"[SKIPPED] Product: {product.name} (Up to date)")
            continue
        print(f"[REFRESHING] Product: {product.name}...")
        safe_write_delta(
            spark.sql(product.sql),
            target,
            partition_by=product.partition_by,
            schema_ver=schema_version,
        )
        print(f"Product {product.name} refreshed successfully.")
