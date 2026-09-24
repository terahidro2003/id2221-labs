"""Storage layout helpers for gold integrated tables."""

from __future__ import annotations

from pathlib import Path

from src.lake import GOLD


def storage_report(table_name: str, base: Path | None = None) -> dict:
    path = (base or GOLD) / table_name
    files = [f for f in path.rglob("*.parquet") if f.is_file()]
    size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
    n_parts = len({f.parent for f in files})
    print(
        f"{table_name:40} files={len(files):>5}  partitions={n_parts:>4}  size={size_mb:>8.1f} MB"
    )
    return {
        "table": table_name,
        "files": len(files),
        "partitions": n_parts,
        "size_mb": size_mb,
    }


def get_storage_info(delta_path: Path) -> str:
    if not delta_path.exists():
        return "N/A"
    parquet_files = list(delta_path.glob("**/*.parquet"))
    total_bytes = sum(f.stat().st_size for f in parquet_files)
    return f"{total_bytes / 1024:.1f} KB ({len(parquet_files)} files)"


def compare_integrated_layouts() -> None:
    print("Storage")
    storage_report("integrated_taxi_trips")
    storage_report("integrated_taxi_trips_by_borough")
