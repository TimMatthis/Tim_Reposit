#!/usr/bin/env python3
"""Run price ingest → BOM ingest → feature join."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent


def run(name: str) -> None:
    script = SCRIPTS / name
    print("==>", script.name, flush=True)
    subprocess.run([sys.executable, str(script)], check=True)


def main() -> None:
    run("ingest_local_prices.py")
    run("ingest_bom_weather.py")
    run("build_features.py")
    run("train_baseline.py")
    print("Pipeline OK.", flush=True)


if __name__ == "__main__":
    main()
