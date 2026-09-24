"""CLI: generate incremental raw update files."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.generators.incremental import run_all_generators


def main() -> None:
    run_all_generators()


if __name__ == "__main__":
    main()
