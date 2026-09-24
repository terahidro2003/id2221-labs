"""CLI: gold integrate and/or data products."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.gold.integrate import integrate
from src.gold.products import build_products
from src.spark import create_spark


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Gold integrate / data products")
    parser.add_argument(
        "--stage",
        choices=["integrate", "products", "all"],
        default="all",
        help="Which gold stage to run",
    )
    parser.add_argument(
        "--force-products",
        action="store_true",
        help="Rebuild data products even if up to date",
    )
    parser.add_argument(
        "--skip-borough-layout",
        action="store_true",
        help="Skip integrated_taxi_trips_by_borough write",
    )
    args = parser.parse_args(argv)

    spark = create_spark("run-gold")
    try:
        if args.stage in ("integrate", "all"):
            integrate(spark, write_borough_layout=not args.skip_borough_layout)
        if args.stage in ("products", "all"):
            build_products(spark, force=args.force_products)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
