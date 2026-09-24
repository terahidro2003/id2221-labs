"""CLI: bronze ingest (schema validate + write bronze)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.bronze.ingest import ingest_all, ingest_dataset
from src.spark import create_spark


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ingest raw → bronze Delta tables")
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        help="Dataset name (repeatable). Default: all configs.",
    )
    args = parser.parse_args(argv)

    spark = create_spark("run-bronze")
    try:
        if args.datasets:
            for name in args.datasets:
                ingest_dataset(spark, name)
        else:
            ingest_all(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
