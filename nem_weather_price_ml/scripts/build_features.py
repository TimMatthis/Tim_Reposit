#!/usr/bin/env python3
"""Join prices_daily + weather_daily; add calendar fields, HDD/CDD, and lags."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parents[1]
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

import pandas as pd

from paths import data_processed

HDD_BASE_C = 18.0


def add_calendar(df: pd.DataFrame) -> pd.DataFrame:
    d = df["date"]
    out = df.copy()
    out["dow"] = d.dt.dayofweek
    out["month"] = d.dt.month
    out["year"] = d.dt.year
    out["is_weekend"] = out["dow"] >= 5
    return out


def add_degree_days(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "tmax_c" in out.columns and "tmin_c" in out.columns:
        tmean = (out["tmax_c"] + out["tmin_c"]) / 2.0
        out["cdd"] = (tmean - HDD_BASE_C).clip(lower=0.0)
        out["hdd"] = (HDD_BASE_C - tmean).clip(lower=0.0)
    return out


def add_group_lags(
    df: pd.DataFrame,
    group_col: str,
    value_cols: list[str],
    lags: range,
) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values([group_col, "date"])
    for col in value_cols:
        if col not in out.columns:
            continue
        for k in lags:
            out[f"{col}_lag{k}"] = out.groupby(group_col, sort=False)[col].shift(k)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prices", type=Path, default=None)
    ap.add_argument("--weather", type=Path, default=None)
    ap.add_argument("--output", type=Path, default=None)
    args = ap.parse_args()

    proc = data_processed()
    p_path = args.prices or (proc / "prices_daily.parquet")
    w_path = args.weather or (proc / "weather_daily.parquet")
    out_path = args.output or (proc / "features_daily.parquet")

    prices = pd.read_parquet(p_path)
    weather = pd.read_parquet(w_path)

    prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()
    weather["date"] = pd.to_datetime(weather["date"]).dt.normalize()

    merged = prices.merge(
        weather,
        on=["date", "region"],
        how="left",
        suffixes=("", "_wx"),
        validate="one_to_one",
    )

    merged = add_calendar(merged)
    merged = add_degree_days(merged)

    lag_cols = [c for c in ("tmax_c", "tmin_c", "rain_mm", "rrp_avg", "cdd", "hdd") if c in merged.columns]
    merged = add_group_lags(merged, "region", lag_cols, range(1, 8))

    merged["weather_missing"] = merged["tmax_c"].isna()
    merged.to_parquet(out_path, index=False)
    miss = int(merged["weather_missing"].sum())
    print(f"Wrote {len(merged)} rows to {out_path} (weather_missing={miss})")


if __name__ == "__main__":
    main()
