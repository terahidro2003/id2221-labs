"""Load dataset YAML configs from config/datasets/."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.spark import project_root


def config_dir() -> Path:
    return project_root() / "config"


def datasets_dir() -> Path:
    return config_dir() / "datasets"


def load_compatible_types() -> dict[str, list[str]]:
    path = config_dir() / "compatible_types.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def load_dataset_config(name: str) -> dict[str, Any]:
    path = datasets_dir() / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Dataset config not found: {path}")
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    cfg.setdefault("name", name)
    return cfg


def list_dataset_configs() -> list[str]:
    return sorted(p.stem for p in datasets_dir().glob("*.yaml"))
