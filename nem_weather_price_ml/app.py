#!/usr/bin/env python3
"""Flask app: NEM daily prices + BOM weather explorer (hub-aligned UI)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from flask import Flask, Response, render_template, send_from_directory, url_for

_PKG = Path(__file__).resolve().parent
_REPO = _PKG.parent
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

import pandas as pd

from paths import data_processed

DEFAULT_PORT = 5020

app = Flask(
    __name__,
    template_folder=str(_PKG / "templates"),
    static_folder=str(_PKG / "static"),
    static_url_path="/static",
)


def _features_path() -> Path:
    override = os.environ.get("NEM_FEATURES_PARQUET")
    if override:
        return Path(override).expanduser()
    return data_processed() / "features_daily.parquet"


@app.get("/hub-style.css")
def hub_style() -> Response:
    """Serve the project hub stylesheet so this app matches index.html."""
    return send_from_directory(_REPO / "css", "style.css", mimetype="text/css")


def _hub_index_url() -> str:
    return os.environ.get("PROJECT_HUB_URL", "http://127.0.0.1:3847/index.html")


@app.get("/")
def explorer() -> str:
    return render_template(
        "nem_explorer.html",
        hub_index_url=_hub_index_url(),
        chart_js_version="4.4.1",
    )


@app.get("/api/features")
def api_features() -> Response:
    path = _features_path()
    if not path.exists():
        return Response(
            json.dumps(
                {
                    "ok": False,
                    "error": f"Missing {path}. Run: python nem_weather_price_ml/scripts/run_pipeline.py",
                }
            ),
            status=404,
            mimetype="application/json",
        )
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    records = df.replace({float("nan"): None}).to_dict(orient="records")
    return Response(
        json.dumps({"ok": True, "rows": records}),
        mimetype="application/json",
    )


@app.get("/healthz")
def healthz() -> tuple[str, int]:
    return "ok", 200


def main() -> None:
    port = int(os.environ.get("PORT", DEFAULT_PORT))
    print(
        f"\n  NEM + BOM explorer → http://127.0.0.1:{port}/\n"
        f"  (This page only loads while this process is running — Ctrl+C to stop.)\n",
        flush=True,
    )
    app.run(host="127.0.0.1", port=port, debug=os.environ.get("FLASK_DEBUG") == "1")


if __name__ == "__main__":
    main()
