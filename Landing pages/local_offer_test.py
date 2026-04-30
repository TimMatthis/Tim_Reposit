#!/usr/bin/env python3
"""
Local test server for Landing pages/Retrofit_CVP_GET_Offer.html.

Serves the HTML with API_BASE rewritten to this server and a stub
GET /offer/data?token=... so you can preview the offer UI without the live backend.
Also supports the offer page when opened from a local static server such as
http://localhost:3847/Landing%20pages/Retrofit_CVP_GET_Offer.html.

Usage (from repo root or this folder):
    python "Landing pages/local_offer_test.py"

Then open either the printed URL or the static preview URL on localhost:3847.
"""

# pylint: disable=missing-function-docstring,invalid-name

import re
import sys
from pathlib import Path

try:
    from flask import Flask, Response, jsonify, request
except ImportError:
    sys.exit("Flask not found — pip install flask")

HERE = Path(__file__).resolve().parent
OFFER_HTML = HERE / "Retrofit_CVP_GET_Offer.html"

PORT = 5050
STUB_TOKEN = "test-dev-token"

STUB_OFFER = {
    "uid": "dev-uid-123",
    "offer": {
        "annual_saving": 1800,
        "monthly_fee": 89,
        "contract_term_years": 5,
        "install_cost": 0,
    },
    "expires_at": "2027-12-31T00:00:00+00:00",
}

app = Flask(__name__)


@app.after_request
def allow_local_preview(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


@app.route("/")
def index():
    if not OFFER_HTML.exists():
        return f"HTML not found at {OFFER_HTML}", 404
    html = OFFER_HTML.read_text(encoding="utf-8")
    html = html.replace(
        "var API_BASE = 'https://dev-customer.repositpower.com';",
        f"var API_BASE = 'http://localhost:{PORT}';",
    )
    html = re.sub(r"\{\{[^}]*\}\}", "", html)
    return Response(html, mimetype="text/html")


@app.route("/offer/data")
def offer_data():
    token = request.args.get("token", "")
    if token != STUB_TOKEN:
        return jsonify({"error": "Offer not found or expired"}), 404
    return jsonify(STUB_OFFER)


if __name__ == "__main__":
    url = f"http://localhost:{PORT}/?token={STUB_TOKEN}"
    print(f"\n  Open this URL (Flask serves the page + stub API):\n  {url}")
    print(f"  Stub token:  {STUB_TOKEN}")
    print("  Edit STUB_OFFER in this file to change displayed values.\n")
    app.run(port=PORT, debug=False)
