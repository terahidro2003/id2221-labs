"""Bronze ingest: read raw → schema validate → write_bronze."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common.config import list_dataset_configs, load_dataset_config
from src.lake import RAW, write_bronze
from src.monitoring import record_run
from src.validation import run_schema_checks

_UPDATE_FILES = {
    "taxi_trips": RAW / "taxi_trips" / "yellow_tripdata_update.parquet",
    "weather": RAW / "weather" / "weather_update.csv",
    "air_quality": RAW / "air_quality" / "hourly_88101_update.csv",
}

_SPARK_CAST = {
    "integer": "int",
    "long": "long",
    "double": "double",
    "float": "float",
    "string": "string",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "date": "date",
}


def _read_csv(spark: SparkSession, path: Path, opts: dict[str, Any]) -> DataFrame:
    reader = spark.read
    if opts.get("header", True):
        reader = reader.option("header", True)
    if opts.get("inferSchema", True):
        reader = reader.option("inferSchema", True)
    return reader.csv(str(path))


def _expected_from_cfg(cfg: dict[str, Any]) -> dict[str, str]:
    for item in (cfg.get("bronze") or {}).get("validation") or []:
        if item.get("rule") == "schema":
            return dict(item.get("expected") or {})
    return {}


def _cast_expected(df: DataFrame, expected: dict[str, str]) -> DataFrame:
    """Cast columns to configured logical types (CSV often lands as string)."""
    out = df
    for col, logical in expected.items():
        if col not in out.columns:
            continue
        spark_type = _SPARK_CAST.get(logical)
        if spark_type is None:
            continue
        out = out.withColumn(col, F.col(col).cast(spark_type))
    return out


def _union_update(
    spark: SparkSession,
    base: DataFrame,
    update_path: Path,
    *,
    fmt: str,
    opts: dict[str, Any],
) -> DataFrame:
    if not update_path.is_file():
        return base
    print(f"  + merging update file {update_path.name}")
    if fmt == "parquet":
        update = spark.read.parquet(str(update_path))
    else:
        update = _read_csv(spark, update_path, opts)
    return base.unionByName(update, allowMissingColumns=True)


def _read_raw(spark: SparkSession, cfg: dict[str, Any], name: str) -> DataFrame:
    path = RAW / cfg["path"]
    fmt = cfg.get("format", "csv")
    opts = cfg.get("read") or {}

    if fmt == "parquet":
        df = spark.read.parquet(str(path))
    else:
        df = _read_csv(spark, path, opts)

    update_path = _UPDATE_FILES.get(name)
    if update_path is not None:
        df = _union_update(spark, df, update_path, fmt=fmt, opts=opts)

    # After multi-file CSV merges, types can widen to string — coerce from config.
    if fmt == "csv":
        df = _cast_expected(df, _expected_from_cfg(cfg))
    return df


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
    out = df
    if "tpep_pickup_datetime" in out.columns:
        out = out.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    if "tpep_dropoff_datetime" in out.columns:
        out = out.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    return (
        out.withColumn("taxi_type", F.lit(taxi_type))
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
    schema_version = str(cfg.get("schema_version", "1.0"))

    with record_run(
        spark, layer="bronze", dataset=name, schema_version=schema_version
    ) as run:
        raw_df = _read_raw(spark, cfg, name)
        processed = raw_df.count()
        run.set_counts(processed=processed, inserted=0, rejected=0)

        checked = run_schema_checks(raw_df, bronze, dataset=dataset_label)
        if not checked.ok:
            run.set_counts(
                validation_failures=len(checked.errors),
                validation_ok=False,
            )
            run.error_message = "; ".join(checked.errors)[:2000]
            raise ValueError(
                f"Schema validation failed for {dataset_label}: {checked.errors}"
            )

        framed = _prep_bronze(raw_df, cfg)
        write_bronze(framed, table, partition_by=bronze.get("partition_by") or [])
        inserted = framed.count()
        run.set_counts(
            processed=processed,
            inserted=inserted,
            rejected=0,
            validation_failures=0,
            validation_ok=True,
        )
        print(f"[bronze/{table}] written ({inserted:,} rows)")


def ingest_all(spark: SparkSession, datasets: Optional[list[str]] = None) -> None:
    for name in datasets or list_dataset_configs():
        ingest_dataset(spark, name)
