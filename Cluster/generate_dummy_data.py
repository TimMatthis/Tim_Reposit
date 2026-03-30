"""
Generate a realistic dummy energy dataset for testing the cluster pipeline.

Writes one row at a time to avoid loading the full matrix into memory.

Output: dummy_energy_data.csv
  Rows  : N_HOMES  (default 50 — increase with --homes)
  Cols  : N_DAYS × 288  5-min intervals
  Shape : 5 archetypal behavioural patterns + realistic noise
"""

import argparse
import csv
import time
from pathlib import Path

import numpy as np

RANDOM_SEED       = 42
INTERVALS_PER_DAY = 288
OUTPUT_FILE       = Path("dummy_energy_data.csv")


# ---------------------------------------------------------------------------
# Archetype base curves  (single 24-hour profile, 288 points)
# ---------------------------------------------------------------------------

def build_archetypes() -> list[tuple[str, np.ndarray]]:
    t = np.linspace(0, 24, INTERVALS_PER_DAY, endpoint=False)

    def gauss(center, width, height=1.0):
        return height * np.exp(-0.5 * ((t - center) / width) ** 2)

    evening_peaker = gauss(19.5, 1.2, 1.0) + gauss(7.5, 0.8, 0.35) + 0.08
    morning_peaker = gauss(7.5,  1.0, 1.0) + gauss(12.0, 1.5, 0.2) + 0.07
    solar_exporter = np.clip(
        gauss(8.0, 0.9, 0.7) + gauss(18.5, 1.0, 0.8) - gauss(13.0, 2.0, 0.6) + 0.3,
        0, None,
    )
    flatliner      = np.ones(INTERVALS_PER_DAY) * 0.5 + gauss(13.0, 3.0, 0.15)
    night_owl      = gauss(23.0, 1.0, 1.0) + gauss(1.0, 0.8, 0.6) + gauss(0.0, 0.5, 0.4) + 0.05

    return [
        ("EveningPeaker", evening_peaker),
        ("MorningPeaker", morning_peaker),
        ("SolarExporter", solar_exporter),
        ("Flatliner",     flatliner),
        ("NightOwl",      night_owl),
    ]


# ---------------------------------------------------------------------------
# Row generator  (yields one home at a time — constant memory)
# ---------------------------------------------------------------------------

def iter_homes(n_homes: int, n_days: int, rng: np.random.Generator):
    archetypes   = build_archetypes()
    n_archetypes = len(archetypes)
    n_cols       = INTERVALS_PER_DAY * n_days

    for home_i in range(n_homes):
        arch_idx   = home_i % n_archetypes
        arch_name, base_curve = archetypes[arch_idx]
        home_id    = f"{arch_name}_{home_i:03d}"

        scale       = rng.uniform(0.3, 3.0)
        daily_tiles = np.tile(base_curve, n_days)
        day_factors = np.repeat(rng.uniform(0.8, 1.2, n_days), INTERVALS_PER_DAY)
        noise       = rng.normal(1.0, 0.08, n_cols)

        row = np.clip(daily_tiles * day_factors * noise * scale, 0, None).round(3)
        yield home_id, row


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate dummy energy CSV for testing")
    p.add_argument("--homes", type=int, default=50,  help="Number of homes (default: 50)")
    p.add_argument("--days",  type=int, default=30,  help="Number of days  (default: 30)")
    p.add_argument("--out",   default=str(OUTPUT_FILE), help="Output CSV path")
    return p.parse_args()


def main():
    args   = parse_args()
    n_cols = INTERVALS_PER_DAY * args.days
    out    = Path(args.out)
    rng    = np.random.default_rng(RANDOM_SEED)

    col_headers = [
        f"d{day:03d}_t{slot:03d}"
        for day in range(args.days)
        for slot in range(INTERVALS_PER_DAY)
    ]

    approx_mb = args.homes * n_cols * 6 / 1e6
    print(f"Generating {args.homes} homes × {n_cols:,} columns (~{approx_mb:.0f} MB on disk) …")
    print(f"Writing row-by-row to '{out}' — memory stays flat.\n")

    t0 = time.time()

    with out.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["home_id"] + col_headers)

        for i, (home_id, row) in enumerate(iter_homes(args.homes, args.days, rng)):
            writer.writerow([home_id] + row.tolist())
            if (i + 1) % 10 == 0 or (i + 1) == args.homes:
                elapsed = time.time() - t0
                print(f"  {i + 1:>4}/{args.homes} homes written  ({elapsed:.1f}s elapsed)")

    size_mb = out.stat().st_size / 1e6
    print(f"\nDone. '{out}'  —  {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
