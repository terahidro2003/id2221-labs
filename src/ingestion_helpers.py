import yaml
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T

from src.lake import BRONZE, RAW, ROOT, write_bronze, show_delta
from src.spark import create_spark

spark = None


def get_spark():
    global spark
    if spark is None:
        spark = create_spark("ingestion-helper")
    return spark


def show_table(table_name: str, n: int = 3) -> None:
    show_delta(get_spark(), BRONZE / table_name, n)


with open(ROOT / "config" / "compatible_types.yaml") as f:
    _type_names = yaml.safe_load(f)

COMPATIBLE = {
    logical: tuple(getattr(T, name) for name in names)
    for logical, names in _type_names.items()
}


def validate_schema(df: DataFrame, expected: dict, dataset: str) -> dict:
    """Require expected columns; allow extra columns; check types."""
    actual = {f.name: f.dataType for f in df.schema.fields}
    missing = [c for c in expected if c not in actual]
    mismatches = []

    for col, expected_type in expected.items():
        if col not in actual:
            continue
        allowed = COMPATIBLE.get(expected_type)
        if allowed and not isinstance(actual[col], allowed):
            mismatches.append(
                f"{col}: got {actual[col].simpleString()}, expected {expected_type}"
            )

    check = {
        "ok": not missing and not mismatches,
        "missing": missing,
        "type_mismatches": mismatches,
    }
    print(f"[{dataset}] schema ok={check['ok']}")
    if check["missing"]:
        print("  missing:", check["missing"])
    if check["type_mismatches"]:
        print("  mismatches:", check["type_mismatches"])
    if not check["ok"]:
        raise ValueError(f"Schema validation failed for {dataset}")
    return check


from pyspark.sql.window import Window

from src.lake import SILVER, read_delta, write_silver

LOCAL_TZ = "America/New_York"


def utc_to_local(col):
    """UTC instant → America/New_York wall time (Spark session is UTC)."""
    return F.from_utc_timestamp(col, LOCAL_TZ)


def read_bronze(table_name: str) -> DataFrame:
    return read_delta(get_spark(), BRONZE / table_name)


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """Rename only columns that exist."""
    out = df
    for src, dst in mapping.items():
        if src in out.columns and src != dst:
            out = out.withColumnRenamed(src, dst)
    return out


def write_rejects(df: DataFrame, table_name: str) -> None:
    path = BRONZE / f"{table_name}_rejects"
    (
        df.withColumn("_rejected_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(str(path))
    )


def quality_filter(df: DataFrame, not_null=None, timestamp_order=None, numeric_range=None, timestamp_range=None):
    reasons = []

    for col in not_null or []:
        if col in df.columns:
            reasons.append(F.when(F.col(col).isNull(), F.lit(f"null:{col}")))

    if timestamp_order:
        before, after = timestamp_order
        if before in df.columns and after in df.columns:
            bad = (
                F.col(before).isNotNull()
                & F.col(after).isNotNull()
                & (F.col(before) > F.col(after))
            )
            reasons.append(F.when(bad, F.lit(f"ts_order:{before}>{after}")))

    for col, bounds in (numeric_range or {}).items():
        if col not in df.columns:
            continue
        bad = F.lit(False)
        if bounds.get("min") is not None:
            bad = bad | (F.col(col).isNotNull() & (F.col(col) < F.lit(bounds["min"])))
        if bounds.get("max") is not None:
            bad = bad | (F.col(col).isNotNull() & (F.col(col) > F.lit(bounds["max"])))
        reasons.append(F.when(bad, F.lit(f"range:{col}")))

    for col, bounds in (timestamp_range or {}).items():
        if col not in df.columns:
            continue
        bad = F.lit(False)
        if bounds.get("min") is not None:
            bad = bad | (F.col(col).isNotNull() & (F.col(col) < F.to_timestamp(F.lit(bounds["min"]))))
        if bounds.get("max") is not None:
            bad = bad | (F.col(col).isNotNull() & (F.col(col) >= F.to_timestamp(F.lit(bounds["max"]))))
        reasons.append(F.when(bad, F.lit(f"ts_range:{col}")))

    if not reasons:
        return df, df.limit(0).withColumn("_reject_reason", F.lit(""))

    flagged = df.withColumn("_reject_reason", F.concat_ws(",", *reasons))
    good = flagged.filter(F.col("_reject_reason") == "").drop("_reject_reason")
    rejects = flagged.filter(F.col("_reject_reason") != "")
    return good, rejects


def promote(table_name, df, primary_key, partition_by=None, **dq):
    """DQ filter → keep first row per PK → write silver + rejects."""
    good, rejects = quality_filter(df, **dq)

    pk = [c for c in primary_key if c in good.columns]
    if pk:
        ranked = good.withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy(*pk).orderBy(F.lit(1))),
        )
        dup_rejects = (
            ranked.filter(F.col("_rn") > 1)
            .drop("_rn")
            .withColumn("_reject_reason", F.lit("duplicate_pk"))
        )
        good = ranked.filter(F.col("_rn") == 1).drop("_rn")
        rejects = rejects.unionByName(dup_rejects, allowMissingColumns=True)

    write_rejects(rejects, table_name)
    write_silver(good, table_name, partition_by=partition_by)

    n_good = get_spark().read.format("delta").load(str(SILVER / table_name)).count()
    n_bad = get_spark().read.format("delta").load(str(BRONZE / f"{table_name}_rejects")).count()
    print(f"[silver/{table_name}] kept={n_good:,}  rejected={n_bad:,}")

from delta.tables import DeltaTable

def merge_to_silver(
    spark,
    update_df: DataFrame,
    table_name: str,
    primary_key: list,
    partition_by: list = None,
    **dq
) -> None:
    """Quality check -> pre-deduplicate stream -> MERGE INTO Silver Delta table."""
    good, rejects = quality_filter(update_df, **dq)

    # 1. Append DQ rejects without wiping initial rejects
    if rejects.count() > 0:
        write_rejects(rejects, table_name)

    # 2. Pre-deduplicate update stream to prevent MERGE key collisions
    pk = [c for c in primary_key if c in good.columns]
    if pk:
        window_spec = Window.partitionBy(*pk).orderBy(F.lit(1))
        good = (
            good.withColumn("_rn", F.row_number().over(window_spec))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    # 3. Enable Delta Schema Auto-Merge on the Spark session
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # 4. Execute Delta MERGE (Note: .option() removed from DeltaMergeBuilder)
    silver_target = DeltaTable.forPath(spark, str(SILVER / table_name))
    merge_condition = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk])

    (
        silver_target.alias("target")
        .merge(good.alias("source"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )

    n_good = spark.read.format("delta").load(str(SILVER / table_name)).count()
    n_bad = spark.read.format("delta").load(str(BRONZE / f"{table_name}_rejects")).count()
    print(f"[silver/{table_name}] Successfully merged update stream. Total rows={n_good:,} (Total Rejects={n_bad:,})")
    
