#!/usr/bin/env python3
"""Time-based split: HistGradientBoosting vs seasonal persistence (lag-7 same dow)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parents[1]
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from paths import data_processed


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--features", type=Path, default=None)
    ap.add_argument("--cutoff", type=str, default="2025-10-01", help="Train strictly before this date")
    ap.add_argument("--metrics-json", type=Path, default=None)
    args = ap.parse_args()

    path = args.features or (data_processed() / "features_daily.parquet")
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    df = df[~df["weather_missing"]].copy()

    cutoff = pd.Timestamp(args.cutoff)
    train = df[df["date"] < cutoff]
    test = df[df["date"] >= cutoff]

    feat_cols = [
        "tmax_c",
        "tmin_c",
        "rain_mm",
        "cdd",
        "hdd",
        "dow",
        "month",
        "tmax_c_lag1",
        "tmin_c_lag1",
        "rain_mm_lag1",
        "cdd_lag1",
        "hdd_lag1",
    ]
    X_tr = train[feat_cols + ["region"]]
    X_te = test[feat_cols + ["region"]]
    y_tr = train["rrp_avg"].to_numpy()
    y_te = test["rrp_avg"].to_numpy()

    pre = ColumnTransformer(
        [
            ("num", "passthrough", feat_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), ["region"]),
        ]
    )
    model = HistGradientBoostingRegressor(
        max_depth=6,
        learning_rate=0.06,
        max_iter=200,
        random_state=42,
    )
    pipe = Pipeline([("pre", pre), ("model", model)])
    pipe.fit(X_tr, y_tr)
    pred = pipe.predict(X_te)

    # Same region, price seven days earlier (simple short-horizon baseline)
    lag7 = test["rrp_avg_lag7"].to_numpy()
    mae_model = mean_absolute_error(y_te, pred)
    mask = np.isfinite(lag7)
    mae_lag7 = mean_absolute_error(y_te[mask], lag7[mask])

    out = {
        "n_train": int(len(train)),
        "n_test": int(len(test)),
        "cutoff": args.cutoff,
        "mae_hgbr": float(mae_model),
        "mae_same_dow_lag7": float(mae_lag7),
    }
    print(json.dumps(out, indent=2))

    if args.metrics_json:
        args.metrics_json.parent.mkdir(parents=True, exist_ok=True)
        args.metrics_json.write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
