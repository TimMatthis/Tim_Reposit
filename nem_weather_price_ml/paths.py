"""Resolve repository paths from nem_weather_price_ml package root."""

from pathlib import Path

_PKG_ROOT = Path(__file__).resolve().parent
REPO_ROOT = _PKG_ROOT.parent


def prices_csv_dir() -> Path:
    return REPO_ROOT / "Weather and prices"


def data_raw_bom() -> Path:
    p = REPO_ROOT / "data" / "raw" / "weather" / "bom"
    p.mkdir(parents=True, exist_ok=True)
    return p


def data_processed() -> Path:
    p = REPO_ROOT / "data" / "processed"
    p.mkdir(parents=True, exist_ok=True)
    return p


def bom_config_path() -> Path:
    return _PKG_ROOT / "config" / "bom_stations.yaml"
