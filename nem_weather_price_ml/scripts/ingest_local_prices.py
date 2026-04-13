#!/usr/bin/env python3
"""Load WEB_AVERAGE_PRICE_DAY_*.csv exports into a canonical long-form table."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parents[1]
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

import pandas as pd

from paths import data_processed, prices_csv_dir

REGIONS = ("NSW", "QLD", "SA", "TAS", "VIC")


def _parse_money(s: str) -> float | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).strip()
    if t.upper() in ("N/A", "NA", ""):
        return None
    t = t.strip('"').replace(",", "").replace("$", "")
    try:
        return float(t)
    except ValueError:
        return None


def load_price_folder(folder: Path) -> pd.DataFrame:
    paths = sorted(folder.glob("WEB_AVERAGE_PRICE_DAY_*.csv"))
    if not paths:
        raise FileNotFoundError(f"No WEB_AVERAGE_PRICE_DAY_*.csv in {folder}")

    frames: list[pd.DataFrame] = []
    for p in paths:
        raw = pd.read_csv(p)
        if raw.empty:
            continue
        # Source files repeat the header label "PEAK RRP" for each region; rename explicitly.
        cols = ["date"]
        for r in REGIONS:
            cols.append(f"{r}_rrp_avg")
            cols.append(f"{r}_rrp_peak")
        if len(raw.columns) != len(cols):
            raise ValueError(f"{p.name}: expected {len(cols)} columns, got {list(raw.columns)}")
        raw.columns = cols
        raw["source_file"] = p.name
        frames.append(raw)

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], format="%Y/%m/%d", errors="coerce")
    for r in REGIONS:
        for suffix in ("rrp_avg", "rrp_peak"):
            col = f"{r}_{suffix}"
            out[col] = out[col].map(_parse_money)
    out = out.dropna(subset=["date"])
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    long_frames: list[pd.DataFrame] = []
    for r in REGIONS:
        sub = out[["date", f"{r}_rrp_avg", f"{r}_rrp_peak", "source_file"]].copy()
        sub["region"] = r
        sub = sub.rename(columns={f"{r}_rrp_avg": "rrp_avg", f"{r}_rrp_peak": "rrp_peak"})
        long_frames.append(sub)
    long_df = pd.concat(long_frames, ignore_index=True)
    long_df = long_df.sort_values(["region", "date"]).reset_index(drop=True)
    return long_df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--prices-dir",
        type=Path,
        default=None,
        help="Override folder containing WEB_AVERAGE_PRICE_DAY_*.csv",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output Parquet path (default: data/processed/prices_daily.parquet)",
    )
    args = ap.parse_args()
    folder = args.prices_dir or prices_csv_dir()
    out_path = args.output or (data_processed() / "prices_daily.parquet")

    df = load_price_folder(folder)
    df.to_parquet(out_path, index=False)
    print(f"Wrote {len(df)} rows ({df['date'].min().date()} .. {df['date'].max().date()}) to {out_path}")


if __name__ == "__main__":
    main()
