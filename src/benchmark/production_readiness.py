"""Production-readiness metrics: timing and storage instrumentation."""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import SparkSession

from src.bronze.ingest import _read_raw, ingest_all
from src.common.config import load_dataset_config
from src.gold.integrate import integrate
from src.gold.products import build_products
from src.lake import BRONZE, GOLD, OPS, RAW, SILVER
from src.monitoring.recorder import PIPELINE_RUNS, append_pipeline_run
from src.silver.promote import promote_all
from src.silver.transforms import transform_dataset
from src.validation import run_row_checks, run_schema_checks


INCREMENTAL_DATASETS = ("taxi_trips", "weather", "air_quality")


@dataclass
class StorageSnapshot:
    raw_bytes: int = 0
    bronze_bytes: int = 0
    silver_bytes: int = 0
    gold_bytes: int = 0
    ops_bytes: int = 0
    products_bytes: int = 0
    rejects_bytes: int = 0
    dual_layout_bytes: int = 0
    file_count: int = 0

    @property
    def lake_bytes(self) -> int:
        return (
            self.bronze_bytes
            + self.silver_bytes
            + self.gold_bytes
            + self.ops_bytes
        )

    @property
    def platform_overhead_bytes(self) -> int:
        """Lake storage beyond raw landing zone (medallion + ops)."""
        return self.lake_bytes

    def as_mb(self) -> dict[str, float]:
        values = {
            "raw_mb": self.raw_bytes,
            "bronze_mb": self.bronze_bytes,
            "silver_mb": self.silver_bytes,
            "gold_mb": self.gold_bytes,
            "ops_mb": self.ops_bytes,
            "products_mb": self.products_bytes,
            "rejects_mb": self.rejects_bytes,
            "dual_layout_mb": self.dual_layout_bytes,
            "lake_mb": self.lake_bytes,
            "platform_overhead_mb": self.platform_overhead_bytes,
        }
        return {k: round(v / (1024 * 1024), 2) for k, v in values.items()} | {
            "file_count": float(self.file_count)
        }


@dataclass
class PhaseTiming:
    name: str
    seconds: float


@dataclass
class ProductionReadinessReport:
    incremental_update_sec: float = 0.0
    incremental_phases: list[PhaseTiming] = field(default_factory=list)
    analytical_refresh_sec: float = 0.0
    storage_before: Optional[StorageSnapshot] = None
    storage_after: Optional[StorageSnapshot] = None
    storage_growth_bytes: int = 0
    validation_overhead_sec: float = 0.0
    validation_by_dataset: dict[str, float] = field(default_factory=dict)
    monitoring_overhead_sec: float = 0.0
    monitoring_per_append_sec: float = 0.0
    monitoring_estimated_pipeline_sec: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "incremental_update_sec": round(self.incremental_update_sec, 3),
            "incremental_phases": [
                {"name": p.name, "seconds": round(p.seconds, 3)}
                for p in self.incremental_phases
            ],
            "analytical_refresh_sec": round(self.analytical_refresh_sec, 3),
            "storage_before_mb": self.storage_before.as_mb() if self.storage_before else None,
            "storage_after_mb": self.storage_after.as_mb() if self.storage_after else None,
            "storage_growth_mb": round(self.storage_growth_bytes / (1024 * 1024), 2),
            "validation_overhead_sec": round(self.validation_overhead_sec, 3),
            "validation_by_dataset_sec": {
                k: round(v, 3) for k, v in self.validation_by_dataset.items()
            },
            "monitoring_overhead_sec": round(self.monitoring_overhead_sec, 3),
            "monitoring_per_append_sec": round(self.monitoring_per_append_sec, 4),
            "monitoring_estimated_pipeline_sec": round(
                self.monitoring_estimated_pipeline_sec, 3
            ),
        }


def _dir_bytes(path: Path, pattern: str = "**/*") -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    total = 0
    files = 0
    for f in path.glob(pattern):
        if f.is_file() and not f.name.startswith("."):
            try:
                total += f.stat().st_size
                files += 1
            except OSError:
                continue
    return total, files


def snapshot_storage() -> StorageSnapshot:
    """Measure on-disk footprint of raw + lake layers."""
    snap = StorageSnapshot()
    snap.raw_bytes, n_raw = _dir_bytes(RAW)
    snap.bronze_bytes, n_bronze = _dir_bytes(BRONZE)
    snap.silver_bytes, n_silver = _dir_bytes(SILVER)
    snap.gold_bytes, n_gold = _dir_bytes(GOLD)
    snap.ops_bytes, n_ops = _dir_bytes(OPS)

    products = GOLD / "data_products"
    snap.products_bytes, _ = _dir_bytes(products)

    rejects = 0
    for p in BRONZE.glob("*_rejects"):
        b, _ = _dir_bytes(p)
        rejects += b
    snap.rejects_bytes = rejects

    dual = GOLD / "integrated_taxi_trips_by_borough"
    snap.dual_layout_bytes, _ = _dir_bytes(dual)

    snap.file_count = n_raw + n_bronze + n_silver + n_gold + n_ops
    return snap


def measure_incremental_update(
    spark: SparkSession,
    *,
    datasets: tuple[str, ...] = INCREMENTAL_DATASETS,
    generate: bool = True,
    refresh_products: bool = False,
) -> tuple[float, list[PhaseTiming]]:
    """
    Wall-clock time for an incremental refresh cycle.

    Phases: generate (optional) → bronze ingest → silver promote → gold integrate
    → products (optional; usually measured separately as analytical refresh).
    """
    from src.generators.incremental import run_all_generators

    phases: list[PhaseTiming] = []
    t_all = time.perf_counter()

    if generate:
        t0 = time.perf_counter()
        run_all_generators(spark=spark)
        phases.append(PhaseTiming("generate", time.perf_counter() - t0))

    t0 = time.perf_counter()
    ingest_all(spark, list(datasets))
    phases.append(PhaseTiming("bronze_ingest", time.perf_counter() - t0))

    t0 = time.perf_counter()
    promote_all(spark, list(datasets))
    phases.append(PhaseTiming("silver_promote", time.perf_counter() - t0))

    t0 = time.perf_counter()
    integrate(spark)
    phases.append(PhaseTiming("gold_integrate", time.perf_counter() - t0))

    if refresh_products:
        t0 = time.perf_counter()
        build_products(spark, force=True)
        phases.append(PhaseTiming("analytical_products", time.perf_counter() - t0))

    total = time.perf_counter() - t_all
    return total, phases


def measure_analytical_refresh(spark: SparkSession, *, force: bool = True) -> float:
    """Wall-clock time to rebuild gold analytical data products."""
    t0 = time.perf_counter()
    build_products(spark, force=force)
    return time.perf_counter() - t0


def measure_validation_overhead(
    spark: SparkSession,
    *,
    datasets: tuple[str, ...] = INCREMENTAL_DATASETS,
) -> tuple[float, dict[str, float]]:
    """
    Additional wall time spent in DQ checks (schema + row), without writing.

    Forces Spark actions so timing reflects real validation work.
    """
    by_dataset: dict[str, float] = {}
    total = 0.0

    for name in datasets:
        cfg = load_dataset_config(name)
        bronze_cfg = cfg.get("bronze") or {}
        silver_cfg = cfg.get("silver") or {}
        dataset_sec = 0.0

        # Bronze schema validation on raw (same input path as ingest).
        raw_df = _read_raw(spark, cfg, name)
        t0 = time.perf_counter()
        schema_result = run_schema_checks(raw_df, bronze_cfg, dataset=name)
        # Touch plan so timing is stable even if checks are metadata-only.
        _ = raw_df.schema
        _ = schema_result.ok
        dataset_sec += time.perf_counter() - t0

        # Silver row validation on transformed bronze.
        transformed = transform_dataset(spark, name)
        t0 = time.perf_counter()
        row_result = run_row_checks(transformed, silver_cfg)
        n_good = row_result.good_df.count() if row_result.good_df is not None else 0
        n_bad = row_result.rejects_df.count() if row_result.rejects_df is not None else 0
        _ = n_good + n_bad
        dataset_sec += time.perf_counter() - t0

        by_dataset[name] = dataset_sec
        total += dataset_sec
        print(
            f"[validation overhead] {name}: {dataset_sec:.2f}s "
            f"(good={n_good:,} rejects={n_bad:,})"
        )

    return total, by_dataset


def measure_monitoring_overhead(
    spark: SparkSession,
    *,
    samples: int = 3,
    pipeline_steps_estimate: int = 12,
) -> tuple[float, float, float]:
    """
    Measure Delta append cost of `pipeline_runs` (monitoring write path).

    Returns (total_sample_sec, per_append_sec, estimated_full_pipeline_sec).
    """
    OPS.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    timings: list[float] = []
    for i in range(samples):
        t0 = time.perf_counter()
        append_pipeline_run(
            spark,
            run_id=f"prod-readiness-{uuid.uuid4()}",
            layer="ops",
            dataset="_monitoring_probe",
            started_at=now,
            finished_at=now,
            duration_seconds=0.0,
            processed_records=0,
            inserted_records=0,
            rejected_records=0,
            schema_version="probe",
            validation_failures=0,
            validation_ok=True,
            status="probe",
            error_message=None,
        )
        timings.append(time.perf_counter() - t0)

    total = sum(timings)
    per = total / max(len(timings), 1)
    estimated = per * pipeline_steps_estimate
    print(
        f"[monitoring overhead] {samples} appends → "
        f"{total:.3f}s total, {per:.3f}s/append, "
        f"~{estimated:.2f}s for {pipeline_steps_estimate} pipeline steps"
    )
    print(f"  ops table: {PIPELINE_RUNS}")
    return total, per, estimated


def print_production_readiness_report(report: ProductionReadinessReport) -> None:
    """Pretty-print the five production-readiness metrics."""
    d = report.to_dict()
    print("\n" + "=" * 72)
    print("PRODUCTION READINESS METRICS")
    print("=" * 72)

    print("\n1) Incremental update time")
    print(f"   total: {d['incremental_update_sec']:.2f}s")
    for phase in d["incremental_phases"]:
        print(f"   - {phase['name']:<22} {phase['seconds']:.2f}s")

    print("\n2) Analytical refresh time")
    print(f"   gold data products (force refresh): {d['analytical_refresh_sec']:.2f}s")

    print("\n3) Storage overhead (updated platform)")
    after = d["storage_after_mb"] or {}
    print(f"   raw landing zone:     {after.get('raw_mb', 0):>10.2f} MB")
    print(f"   bronze:               {after.get('bronze_mb', 0):>10.2f} MB")
    print(f"   silver:               {after.get('silver_mb', 0):>10.2f} MB")
    print(f"   gold (all):           {after.get('gold_mb', 0):>10.2f} MB")
    print(f"     data products:      {after.get('products_mb', 0):>10.2f} MB")
    print(f"     dual borough layout:{after.get('dual_layout_mb', 0):>10.2f} MB")
    print(f"   rejects:              {after.get('rejects_mb', 0):>10.2f} MB")
    print(f"   ops (monitoring):     {after.get('ops_mb', 0):>10.2f} MB")
    print(f"   lake total:           {after.get('lake_mb', 0):>10.2f} MB")
    raw_mb = after.get("raw_mb", 0) or 0
    lake_mb = after.get("lake_mb", 0) or 0
    ratio = (lake_mb / raw_mb) if raw_mb else float("nan")
    print(f"   platform / raw ratio: {ratio:>10.2f}x")
    print(f"   growth this eval:     {d['storage_growth_mb']:>10.2f} MB")

    print("\n4) Validation overhead (DQ execution time)")
    print(f"   total: {d['validation_overhead_sec']:.2f}s")
    for name, sec in d["validation_by_dataset_sec"].items():
        print(f"   - {name:<22} {sec:.2f}s")

    print("\n5) Monitoring overhead (pipeline_runs append)")
    print(f"   sample total:           {d['monitoring_overhead_sec']:.3f}s")
    print(f"   per append:             {d['monitoring_per_append_sec']:.3f}s")
    print(
        f"   est. full pipeline:     {d['monitoring_estimated_pipeline_sec']:.2f}s "
        f"(~12 steps)"
    )
    print("=" * 72)


def evaluate_production_readiness(
    spark: SparkSession,
    *,
    datasets: tuple[str, ...] = INCREMENTAL_DATASETS,
    run_incremental: bool = True,
    generate: bool = True,
    run_analytical_refresh: bool = True,
    measure_validation: bool = True,
    measure_monitoring: bool = True,
    monitoring_samples: int = 3,
) -> ProductionReadinessReport:
    """
    Run instrumentation for the five production-readiness metrics and return a report.

    Designed for notebooks: prints a summary table and returns structured results.
    """
    report = ProductionReadinessReport()
    report.storage_before = snapshot_storage()

    if run_incremental:
        print("\n=== Measuring incremental update time ===")
        total, phases = measure_incremental_update(
            spark,
            datasets=datasets,
            generate=generate,
            refresh_products=False,
        )
        report.incremental_update_sec = total
        report.incremental_phases = phases
        print(f"Incremental update total: {total:.2f}s")

    if run_analytical_refresh:
        print("\n=== Measuring analytical refresh time ===")
        report.analytical_refresh_sec = measure_analytical_refresh(spark, force=True)
        print(f"Analytical refresh: {report.analytical_refresh_sec:.2f}s")

    if measure_validation:
        print("\n=== Measuring validation overhead ===")
        total, by_ds = measure_validation_overhead(spark, datasets=datasets)
        report.validation_overhead_sec = total
        report.validation_by_dataset = by_ds

    if measure_monitoring:
        print("\n=== Measuring monitoring overhead ===")
        total, per, estimated = measure_monitoring_overhead(
            spark, samples=monitoring_samples
        )
        report.monitoring_overhead_sec = total
        report.monitoring_per_append_sec = per
        report.monitoring_estimated_pipeline_sec = estimated

    report.storage_after = snapshot_storage()
    report.storage_growth_bytes = max(
        0, report.storage_after.lake_bytes - report.storage_before.lake_bytes
    )

    print_production_readiness_report(report)
    return report
