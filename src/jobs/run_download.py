"""CLI: download raw datasets from Google Drive."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.bronze.download import DRIVE_FOLDER_URL, download_raw


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download raw urban datasets")
    parser.add_argument("--url", default=DRIVE_FOLDER_URL, help="Google Drive folder URL")
    args = parser.parse_args(argv)
    download_raw(args.url)


if __name__ == "__main__":
    main()
