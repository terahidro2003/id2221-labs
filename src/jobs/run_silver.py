"""CLI: silver promote (transform + row DQ + write silver/rejects)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.silver.promote import promote_all, promote_dataset
from src.spark import create_spark


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Promote bronze → silver")
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        help="Dataset name (repeatable). Default: all configs.",
    )
    args = parser.parse_args(argv)

    spark = create_spark("run-silver")
    try:
        if args.datasets:
            for name in args.datasets:
                promote_dataset(spark, name)
        else:
            promote_all(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
