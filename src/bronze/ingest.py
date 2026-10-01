"""Bronze ingest: read raw → schema validate → write_bronze."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Literal, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common.config import list_dataset_configs, load_dataset_config
from src.lake import BRONZE, RAW, write_bronze
from src.monitoring import record_run
from src.validation import run_schema_checks

_UPDATE_FILES = {
    "taxi_trips": RAW / "taxi_trips" / "yellow_tripdata_update.parquet",
    "weather": RAW / "weather" / "weather_update.csv",
    "air_quality": RAW / "air_quality" / "hourly_88101_update.csv",
}

IngestMode = Literal["auto", "full", "incremental"]

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


def _read_csv(
    spark: SparkSession,
    path: Path,
    opts: dict[str, Any],
    *,
    infer_schema: bool,
) -> DataFrame:
    reader = spark.read
    if opts.get("header", True):
        reader = reader.option("header", True)
    # inferSchema forces an extra full/sample scan — prefer explicit casts from YAML.
    reader = reader.option("inferSchema", bool(infer_schema))
    return reader.csv(str(path))


def _expected_from_cfg(cfg: dict[str, Any]) -> dict[str, str]:
    for item in (cfg.get("bronze") or {}).get("validation") or []:
        if item.get("rule") == "schema":
            return dict(item.get("expected") or {})
    return {}


def _cast_expected(df: DataFrame, expected: dict[str, str]) -> DataFrame:
    """Cast configured columns to logical types (CSV often lands as string)."""
    out = df
    for col, logical in expected.items():
        if col not in out.columns:
            continue
        spark_type = _SPARK_CAST.get(logical)
        if spark_type is None:
            continue
        out = out.withColumn(col, F.col(col).cast(spark_type))
    return out


def _csv_infer_schema(cfg: dict[str, Any], expected: dict[str, str]) -> bool:
    opts = dict(cfg.get("read") or {})
    # When YAML declares expected types, skip inferSchema (avoids a full CSV probe scan).
    return bool(opts.get("inferSchema", True)) and not (
        cfg.get("format", "csv") == "csv" and bool(expected)
    )


def _read_file(
    spark: SparkSession,
    path: Path,
    cfg: dict[str, Any],
    *,
    expected: dict[str, str],
    infer_schema: bool,
) -> DataFrame:
    fmt = cfg.get("format", "csv")
    opts = dict(cfg.get("read") or {})
    if fmt == "parquet":
        return spark.read.parquet(str(path))
    df = _read_csv(spark, path, opts, infer_schema=infer_schema)
    if expected and not infer_schema:
        df = _cast_expected(df, expected)
    return df


def update_path_for(name: str) -> Optional[Path]:
    path = _UPDATE_FILES.get(name)
    return path if path is not None and path.is_file() else None


def bronze_table_exists(name: str, cfg: Optional[dict[str, Any]] = None) -> bool:
    cfg = cfg or load_dataset_config(name)
    table = (cfg.get("bronze") or {}).get("table", name)
    delta_log = BRONZE / table / "_delta_log"
    return delta_log.is_dir()


def _resolve_mode(name: str, cfg: dict[str, Any], mode: IngestMode) -> IngestMode:
    if mode != "auto":
        return mode
    if update_path_for(name) is not None and bronze_table_exists(name, cfg):
        return "incremental"
    return "full"


def _union_update(
    spark: SparkSession,
    base: DataFrame,
    update_path: Path,
    cfg: dict[str, Any],
    *,
    expected: dict[str, str],
    infer_schema: bool,
) -> DataFrame:
    print(f"  + merging update file {update_path.name}")
    update = _read_file(
        spark, update_path, cfg, expected=expected, infer_schema=infer_schema
    )
    out = base.unionByName(update, allowMissingColumns=True)
    if cfg.get("format", "csv") == "csv" and expected and not infer_schema:
        out = _cast_expected(out, expected)
    return out


def _read_raw(spark: SparkSession, cfg: dict[str, Any], name: str) -> DataFrame:
    path = RAW / cfg["path"]
    expected = _expected_from_cfg(cfg)
    infer_schema = _csv_infer_schema(cfg, expected)
    df = _read_file(spark, path, cfg, expected=expected, infer_schema=infer_schema)

    update_path = update_path_for(name)
    if update_path is not None:
        df = _union_update(
            spark, df, update_path, cfg, expected=expected, infer_schema=infer_schema
        )
    return df


def _read_update_only(spark: SparkSession, cfg: dict[str, Any], name: str) -> DataFrame:
    """Read only the incremental update file (skip multi-GB base raw)."""
    update_path = update_path_for(name)
    if update_path is None:
        raise FileNotFoundError(f"No update file registered/present for dataset: {name}")
    expected = _expected_from_cfg(cfg)
    infer_schema = _csv_infer_schema(cfg, expected)
    print(f"  + incremental-only read {update_path.name}")
    return _read_file(
        spark, update_path, cfg, expected=expected, infer_schema=infer_schema
    )


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


def _prep_bronze(df: DataFrame, cfg: dict[str, Any], *, apply_filter: bool = True) -> DataFrame:
    bronze = cfg.get("bronze") or {}
    if apply_filter:
        df = _apply_filter(df, bronze.get("filter"))

    prep_name = bronze.get("prep")
    if not prep_name:
        return df

    prep_fn = _PREP_FNS.get(prep_name)
    if prep_fn is None:
        return df
    return prep_fn(df, bronze)


def ingest_dataset(
    spark: SparkSession,
    name: str,
    *,
    mode: IngestMode = "auto",
) -> None:
    """
    Ingest one dataset into bronze.

    mode:
      - ``full``: read base (+ optional update union), overwrite bronze
      - ``incremental``: read only the update file, append to existing bronze
      - ``auto``: incremental when update file + bronze table both exist, else full
    """
    cfg = load_dataset_config(name)
    bronze = cfg.get("bronze") or {}
    table = bronze.get("table", name)
    dataset_label = "taxi_trips/yellow" if name == "taxi_trips" else name
    schema_version = str(cfg.get("schema_version", "1.0"))
    resolved = _resolve_mode(name, cfg, mode)
    write_mode = "append" if resolved == "incremental" else "overwrite"

    with record_run(
        spark, layer="bronze", dataset=name, schema_version=schema_version
    ) as run:
        if resolved == "incremental":
            raw_df = _read_update_only(spark, cfg, name)
        else:
            raw_df = _read_raw(spark, cfg, name)

        # Push dataset filters (e.g. NY air quality) before any action/count.
        filtered = _apply_filter(raw_df, bronze.get("filter"))

        checked = run_schema_checks(filtered, bronze, dataset=dataset_label)
        if not checked.ok:
            run.set_counts(
                processed=0,
                inserted=0,
                rejected=0,
                validation_failures=len(checked.errors),
                validation_ok=False,
            )
            run.error_message = "; ".join(checked.errors)[:2000]
            raise ValueError(
                f"Schema validation failed for {dataset_label}: {checked.errors}"
            )

        framed = _prep_bronze(filtered, cfg, apply_filter=False)
        # One materialization: cache → count → write (avoids a second full scan).
        framed = framed.cache()
        try:
            n_rows = framed.count()
            write_bronze(
                framed,
                table,
                partition_by=bronze.get("partition_by") or [],
                mode=write_mode,
            )
            run.set_counts(
                processed=n_rows,
                inserted=n_rows,
                rejected=0,
                validation_failures=0,
                validation_ok=True,
            )
            print(
                f"[bronze/{table}] {write_mode} ({n_rows:,} rows) "
                f"[mode={resolved}]"
            )
        finally:
            framed.unpersist()


def ingest_all(
    spark: SparkSession,
    datasets: Optional[list[str]] = None,
    *,
    mode: IngestMode = "auto",
) -> None:
    for name in datasets or list_dataset_configs():
        ingest_dataset(spark, name, mode=mode)
