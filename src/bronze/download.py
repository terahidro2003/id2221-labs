"""Download raw datasets from the shared Google Drive folder."""

from __future__ import annotations

from pathlib import Path
from shutil import copy2
from zipfile import ZipFile

from src.spark import project_root

DRIVE_FOLDER_URL = (
    "https://drive.google.com/drive/folders/"
    "1qjBtPVDepDE22j0axqrLVR0A2a969Qyy?usp=sharing"
)


def _copy_file(source: Path, dest_dir: Path, root: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    copy2(source, dest_dir / source.name)
    print(f"copy  {source.name} -> {dest_dir.relative_to(root)}")


def _unzip(source: Path, dest_dir: Path, root: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(source) as archive:
        archive.extractall(dest_dir)
    print(f"unzip {source.relative_to(root)} -> {dest_dir.relative_to(root)}")


def _place_downloaded_file(source: Path, raw: Path, root: Path) -> None:
    name = source.name
    if name == "air_quality.zip":
        _unzip(source, raw / "air_quality", root)
    elif name == "taxi_zone_lookup.csv":
        _copy_file(source, raw / "taxi_zones", root)
    elif name == "weather.csv":
        _copy_file(source, raw / "weather", root)
    elif name.startswith("yellow_tripdata_") and name.endswith(".parquet"):
        _copy_file(source, raw / "taxi_trips" / "yellow", root)


def download_raw(drive_url: str = DRIVE_FOLDER_URL) -> Path:
    """Download Drive folder and place files under data/raw/."""
    import gdown

    root = project_root()
    raw = root / "data" / "raw"
    download_dir = root / "data" / "drive_download"
    download_dir.mkdir(parents=True, exist_ok=True)
    raw.mkdir(parents=True, exist_ok=True)

    gdown.download_folder(url=drive_url, output=str(download_dir), quiet=False)

    for source in download_dir.rglob("*"):
        if source.is_file():
            _place_downloaded_file(source, raw, root)

    return raw
