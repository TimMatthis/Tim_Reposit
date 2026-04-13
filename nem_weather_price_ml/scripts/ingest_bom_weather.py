#!/usr/bin/env python3
"""Download BOM daily climate tables from anonymous FTP and build weather_daily.parquet."""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from datetime import date
from pathlib import Path

_PKG = Path(__file__).resolve().parents[1]
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

import pandas as pd
import yaml

from paths import bom_config_path, data_processed, data_raw_bom

COL_NAMES = [
    "station",
    "date",
    "et_mm",
    "rain_mm",
    "pan_evap_mm",
    "tmax_c",
    "tmin_c",
    "rh_max_pct",
    "rh_min_pct",
    "wind_mps",
    "solar_mj_m2",
]


def _iter_months(d0: date, d1: date) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    y, m = d0.year, d0.month
    while (y, m) <= (d1.year, d1.month):
        out.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _parse_bom_station_csv(text: str) -> pd.DataFrame:
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith("Station Name") or line.startswith('"Station Name'):
            start = i
            break
    if start is None:
        raise ValueError("Could not find 'Station Name' header in BOM CSV")

    block = "\n".join(lines[start:])
    reader = csv.reader(io.StringIO(block))
    header = next(reader)
    _units = next(reader)
    rows = list(reader)
    if not rows:
        return pd.DataFrame()

    width = len(header)
    if width != len(COL_NAMES):
        # Some sites may omit trailing columns; pad/truncate names safely.
        names = [f"c{i}" for i in range(width)]
        for i, name in enumerate(COL_NAMES):
            if i < width:
                names[i] = name
    else:
        names = COL_NAMES

    df = pd.DataFrame(rows, columns=names)
    if "date" not in df.columns:
        raise ValueError("Missing date column after parse")

    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")
    for col in ("tmax_c", "tmin_c", "rain_mm", "et_mm", "wind_mps", "solar_mj_m2"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _ftp_fetch(host: str, remote_dir: str, filename: str) -> bytes:
    import ftplib

    bio = io.BytesIO()
    with ftplib.FTP(host, timeout=120) as ftp:
        ftp.login()
        ftp.cwd(remote_dir)
        ftp.retrbinary(f"RETR {filename}", bio.write)
    return bio.getvalue()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--prices-parquet",
        type=Path,
        default=None,
        help="prices_daily.parquet (used to infer date range)",
    )
    ap.add_argument(
        "--date-from",
        type=str,
        default=None,
        help="YYYY-MM-DD (override range start)",
    )
    ap.add_argument(
        "--date-to",
        type=str,
        default=None,
        help="YYYY-MM-DD (override range end)",
    )
    ap.add_argument("--sleep", type=float, default=0.35, help="Seconds between FTP RETRs")
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output Parquet (default: data/processed/weather_daily.parquet)",
    )
    args = ap.parse_args()

    cfg = yaml.safe_load(bom_config_path().read_text())
    host = cfg["ftp"]["host"]
    root = cfg["ftp"]["remote_root"].strip("/")
    stations: dict = cfg["stations"]

    prices_path = args.prices_parquet or (data_processed() / "prices_daily.parquet")
    if args.date_from and args.date_to:
        d0 = pd.to_datetime(args.date_from).date()
        d1 = pd.to_datetime(args.date_to).date()
    elif prices_path.exists():
        px = pd.read_parquet(prices_path, columns=["date"])
        d0 = px["date"].min().date()
        d1 = px["date"].max().date()
    else:
        raise SystemExit(f"Need --date-from/--date-to or existing {prices_path}")

    cache_dir = data_raw_bom()
    months = _iter_months(d0, d1)
    all_parts: list[pd.DataFrame] = []

    for region, meta in stations.items():
        state = meta["ftp_state"]
        site = meta["ftp_site"]
        prefix = meta["file_prefix"]
        for y, m in months:
            yyyymm = f"{y:04d}{m:02d}"
            fname = f"{prefix}-{yyyymm}.csv"
            remote_dir = f"/{root}/{state}/{site}"
            local = cache_dir / region / fname
            local.parent.mkdir(parents=True, exist_ok=True)
            if not local.exists():
                raw = _ftp_fetch(host, remote_dir, fname)
                local.write_bytes(raw)
                time.sleep(args.sleep)
            else:
                raw = local.read_bytes()

            text = raw.decode("utf-8", errors="replace")
            try:
                df = _parse_bom_station_csv(text)
            except Exception as e:
                raise RuntimeError(f"Parse failed for {region} {fname}: {e}") from e
            if df.empty:
                continue
            df = df.dropna(subset=["date"])
            df["region"] = region
            df["bom_file"] = fname
            all_parts.append(df)

    if not all_parts:
        raise SystemExit("No BOM rows parsed; check FTP paths and date range.")

    out = pd.concat(all_parts, ignore_index=True)
    keep = [
        "date",
        "region",
        "tmax_c",
        "tmin_c",
        "rain_mm",
        "et_mm",
        "wind_mps",
        "solar_mj_m2",
        "station",
        "bom_file",
    ]
    keep = [c for c in keep if c in out.columns]
    out = out[keep].sort_values(["region", "date"]).reset_index(drop=True)

    out_path = args.output or (data_processed() / "weather_daily.parquet")
    out.to_parquet(out_path, index=False)
    print(
        f"Wrote {len(out)} rows to {out_path} "
        f"(cached under {cache_dir})"
    )


if __name__ == "__main__":
    main()
