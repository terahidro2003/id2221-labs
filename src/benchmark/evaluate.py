"""Query evaluation helpers (baseline vs data product / AQE)."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from src.benchmark.storage import get_storage_info


def _timed_count(df: DataFrame) -> tuple[int, float]:
    started = time.time()
    rows = df.count()
    return rows, time.time() - started


def _assert_same_results(baseline: DataFrame, optimized: DataFrame, base_rows: int, opt_rows: int) -> None:
    assert base_rows == opt_rows, f"Row count mismatch: {base_rows} vs {opt_rows}"
    columns = baseline.columns
    assert (
        baseline.orderBy(*columns).collect() == optimized.orderBy(*columns).collect()
    ), "Result data mismatch!"
    print("\n[VERIFIED] Optimized query produces identical results to baseline.")


def _print_summary(
    base_rows: int,
    base_time: float,
    opt_time: float,
    storage_str: str,
) -> float:
    speedup = ((base_time - opt_time) / base_time) * 100 if base_time > 0 else 0
    print("\n--- Performance Summary ---")
    print(f"Result Rows:       {base_rows:,}")
    print(f"Baseline Time:     {base_time:.2f}s")
    print(f"Optimized Time:    {opt_time:.2f}s")
    print(f"Speedup:           {speedup:.1f}%")
    print(f"Storage Overhead:  {storage_str}\n")
    return speedup


def evaluate_query(
    spark: SparkSession,
    query_name: str,
    df_baseline: Optional[DataFrame] = None,
    df_optimized: Optional[DataFrame] = None,
    data_product_path: Optional[Path] = None,
    aqe_query: Optional[str] = None,
) -> dict:
    print(f"\n{'=' * 60}")
    print(f"EVALUATING: {query_name}")
    print(f"{'=' * 60}")

    if aqe_query is not None:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        df_baseline = spark.sql(aqe_query)
        print("\n--- Baseline Physical Plan (AQE Disabled) ---")
        df_baseline.explain("formatted")
        base_rows, base_time = _timed_count(df_baseline)

        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        df_optimized = spark.sql(aqe_query)
        opt_rows, opt_time = _timed_count(df_optimized)
        print("\n--- Optimized Physical Plan (AQE Enabled) ---")
        df_optimized.explain("formatted")
        storage_str = "N/A (AQE Engine Optimization on Integrated Table)"
    else:
        print("\n--- Baseline Physical Plan (On-Demand) ---")
        df_baseline.explain("formatted")
        print("\n--- Optimized Physical Plan (Data Product) ---")
        df_optimized.explain("formatted")

        base_rows, base_time = _timed_count(df_baseline)
        opt_rows, opt_time = _timed_count(df_optimized)
        storage_str = (
            get_storage_info(data_product_path) if data_product_path else "N/A"
        )

    _assert_same_results(df_baseline, df_optimized, base_rows, opt_rows)
    speedup = _print_summary(base_rows, base_time, opt_time, storage_str)

    return {
        "query": query_name,
        "rows": base_rows,
        "baseline_s": base_time,
        "optimized_s": opt_time,
        "speedup_pct": speedup,
        "storage": storage_str,
    }
