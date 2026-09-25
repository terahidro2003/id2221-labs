"""CLI: print operational Spark SQL answers from pipeline_runs."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.monitoring.queries import print_monitoring_report
from src.monitoring.recorder import PIPELINE_RUNS
from src.spark import create_spark


def main(argv: list[str] | None = None) -> None:
    _ = argv
    if not PIPELINE_RUNS.exists():
        print(
            f"No ops table at {PIPELINE_RUNS}. "
            "Run bronze/silver/gold jobs first to record metrics."
        )
        return

    spark = create_spark("run-monitoring-report")
    try:
        print_monitoring_report(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
