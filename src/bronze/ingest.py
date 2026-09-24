"""Bronze ingest: read raw → schema validate → write_bronze."""

from __future__ import annotations

from typing import Any, Callable, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common.config import list_dataset_configs, load_dataset_config
from src.lake import RAW, write_bronze
from src.validation import run_schema_checks


def _read_raw(spark: SparkSession, cfg: dict[str, Any]) -> DataFrame:
    path = RAW / cfg["path"]
    if cfg.get("format", "csv") == "parquet":
        return spark.read.parquet(str(path))

    reader = spark.read
    opts = cfg.get("read") or {}
    if opts.get("header", True):
        reader = reader.option("header", True)
    if opts.get("inferSchema", True):
        reader = reader.option("inferSchema", True)
    return reader.csv(str(path))


def _apply_filter(df: DataFrame, filt: dict[str, Any] | None) -> DataFrame:
    if not filt:
        return df
    column = filt["column"]
    if "equals" in filt:
        return df.filter(F.col(column) == filt["equals"])
    return df


def _prep_observation_date_from_ymd(df: DataFrame, _: dict[str, Any]) -> DataFrame:
    return df.withColumn(
        "observation_date",
        F.make_date(F.col("year"), F.col("month"), F.col("day")),
    )


def _prep_air_quality_bronze(df: DataFrame, _: dict[str, Any]) -> DataFrame:
    return (
        df.withColumn("state_code", F.col("State Code"))
        .withColumn("measurement_date", F.to_date(F.col("Date GMT"), "yyyy-MM-dd"))
        .drop("State Code")
    )


def _prep_normalize_yellow_trips(df: DataFrame, bronze: dict[str, Any]) -> DataFrame:
    taxi_type = bronze.get("taxi_type", "yellow")
    return (
        df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("taxi_type", F.lit(taxi_type))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )


_PREP_FNS: dict[str, Callable[[DataFrame, dict[str, Any]], DataFrame]] = {
    "observation_date_from_ymd": _prep_observation_date_from_ymd,
    "air_quality_bronze": _prep_air_quality_bronze,
    "normalize_yellow_trips": _prep_normalize_yellow_trips,
}


def _prep_bronze(df: DataFrame, cfg: dict[str, Any]) -> DataFrame:
    bronze = cfg.get("bronze") or {}
    df = _apply_filter(df, bronze.get("filter"))

    prep_name = bronze.get("prep")
    if not prep_name:
        return df

    prep_fn = _PREP_FNS.get(prep_name)
    if prep_fn is None:
        return df
    return prep_fn(df, bronze)


def ingest_dataset(spark: SparkSession, name: str) -> None:
    cfg = load_dataset_config(name)
    bronze = cfg.get("bronze") or {}
    table = bronze.get("table", name)
    dataset_label = "taxi_trips/yellow" if name == "taxi_trips" else name

    raw_df = _read_raw(spark, cfg)
    run_schema_checks(raw_df, bronze, dataset=dataset_label)
    framed = _prep_bronze(raw_df, cfg)
    write_bronze(framed, table, partition_by=bronze.get("partition_by") or [])
    print(f"[bronze/{table}] written")


def ingest_all(spark: SparkSession, datasets: Optional[list[str]] = None) -> None:
    for name in datasets or list_dataset_configs():
        ingest_dataset(spark, name)
