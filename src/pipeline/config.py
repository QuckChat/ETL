"""Configuration loader for ETL pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class PipelineConfig:
    """Typed configuration object for ETL jobs."""

    app_name: str
    batch_date: str
    paths: dict[str, str]
    jdbc: dict[str, Any]
    quality: dict[str, Any]


def load_config(config_path: str | os.PathLike[str]) -> PipelineConfig:
    """Load YAML configuration and map it into a typed structure."""

    with Path(config_path).open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return PipelineConfig(
        app_name=raw["app_name"],
        batch_date=raw["batch_date"],
        paths=raw["paths"],
        jdbc=raw["jdbc"],
        quality=raw["quality"],
    )
