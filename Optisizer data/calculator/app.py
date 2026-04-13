"""
Optisizer fleet summary: sum net_cost positives/negatives per home, stream progress via SSE.

Optional ABS census (demographic augmentation, same POA join as Cluster/enrichment.py):
  See ``demographic_augmentation_documentation()`` for the full bridge + DataFrame notes.
  Bridge: ``house_postcodes.csv`` with ``house_id``, ``postcode`` (4-digit POA).
"""

from __future__ import annotations

import csv
import importlib.util
import json
import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from flask import Flask, Response, render_template, stream_with_context

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent
CATAN_RESULTS = BASE_DIR.parent / "catan_results"
HOUSE_POSTCODES_CSV = BASE_DIR / "house_postcodes.csv"
CSV_NAME = "optimisation_intervals.csv"
CACHE_DIR = BASE_DIR / ".summary_cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_SCHEMA_VERSION = 2
INTERVAL_HOURS = 5.0 / 60.0


def discover_csv_paths() -> list[Path]:
    if not CATAN_RESULTS.is_dir():
        return []
    paths = sorted(CATAN_RESULTS.glob(f"*/{CSV_NAME}"))
    return paths


def normalize_postcode(raw: str) -> str | None:
    """Match Cluster enrichment: 4-digit POA string for joining ABS tables."""
    s = str(raw).strip()
    if not s:
        return None
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) < 4:
        return None
    if len(digits) > 4:
        digits = digits[-4:]
    return digits.zfill(4)


def load_house_postcode_bridge() -> dict[str, str]:
    """house_id (folder UUID) -> 4-digit postcode from optional sidecar CSV."""
    if not HOUSE_POSTCODES_CSV.is_file():
        return {}
    out: dict[str, str] = {}
    try:
        with HOUSE_POSTCODES_CSV.open(newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "house_id" not in reader.fieldnames:
                return {}
            if "postcode" not in reader.fieldnames:
                return {}
            for row in reader:
                hid = (row.get("house_id") or "").strip()
                if not hid:
                    continue
                pc = normalize_postcode(row.get("postcode") or "")
                if pc:
                    out[hid] = pc
    except OSError:
        return {}
    return out


_abs_census_df: pd.DataFrame | None = None
_abs_census_load_attempted = False


def load_abs_census_dataframe() -> pd.DataFrame | None:
    """Reuse Cluster ``load_abs_census_data()`` (POA_CODE_2021 index, same as enrich_sites)."""
    global _abs_census_df, _abs_census_load_attempted
    if _abs_census_load_attempted:
        return _abs_census_df
    _abs_census_load_attempted = True
    path = REPO_ROOT / "Cluster" / "enrichment.py"
    if not path.is_file():
        return None
    try:
        spec = importlib.util.spec_from_file_location("cluster_enrichment", path)
        if spec is None or spec.loader is None:
            return None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        df = mod.load_abs_census_data()
        if df is None or df.empty:
            _abs_census_df = None
        else:
            _abs_census_df = df
    except Exception:
        _abs_census_df = None
    return _abs_census_df


def _abs_index_for_postcode(abs_df: pd.DataFrame, pc: str) -> str | None:
    """Map 4-digit POA string to row index (ABS files use ``POA2000``-style keys)."""
    if pc in abs_df.index:
        return pc
    try:
        nodash = f"POA{int(pc)}"
    except ValueError:
        return None
    if nodash in abs_df.index:
        return nodash
    return None


def merge_census_into_houses(houses: list[dict]) -> dict:
    """Attach ABS columns to each ok home when bridge postcode matches POA index."""
    bridge = load_house_postcode_bridge()
    abs_df = load_abs_census_dataframe()
    try:
        rel_bridge = str(HOUSE_POSTCODES_CSV.relative_to(REPO_ROOT))
    except ValueError:
        rel_bridge = str(HOUSE_POSTCODES_CSV)
    meta = {
        "bridge_file": rel_bridge if HOUSE_POSTCODES_CSV.is_file() else None,
        "bridge_mapping_count": len(bridge),
        "abs_loaded": abs_df is not None and not abs_df.empty,
        "abs_columns": list(abs_df.columns) if abs_df is not None and not abs_df.empty else [],
        "homes_with_bridge": 0,
        "homes_abs_matched": 0,
        "documentation": demographic_augmentation_documentation(),
    }
    if not bridge or abs_df is None or abs_df.empty:
        return meta

    for h in houses:
        if not h.get("ok", True):
            continue
        hid = h.get("house_id")
        if not isinstance(hid, str):
            continue
        pc = bridge.get(hid)
        if not pc:
            continue
        meta["homes_with_bridge"] += 1
        h["postcode"] = pc
        idx = _abs_index_for_postcode(abs_df, pc)
        if idx is None:
            continue
        meta["homes_abs_matched"] += 1
        ser = abs_df.loc[idx]
        for col in abs_df.columns:
            val = ser[col]
            try:
                if isinstance(val, pd.Series):
                    val = val.iloc[0] if len(val) else float("nan")
                if pd.isna(val):
                    h[col] = None
                else:
                    fv = float(val)
                    h[col] = round(fv, 6)
            except (TypeError, ValueError):
                h[col] = None
    return meta


ABS_CENSUS_KEYS: tuple[str, ...] = (
    "median_household_income",
    "avg_household_size",
    "avg_persons_per_bedroom",
    "median_weekly_rent",
    "pct_65_plus",
    "pct_not_in_labour_force",
    "pct_part_time_employed",
    "pct_university_educated",
)

ABS_CENSUS_LABELS: dict[str, str] = {
    "median_household_income": "Census: median household income (weekly $)",
    "avg_household_size": "Census: avg household size",
    "avg_persons_per_bedroom": "Census: avg persons per bedroom",
    "median_weekly_rent": "Census: median weekly rent ($)",
    "pct_65_plus": "Census: % population 65+",
    "pct_not_in_labour_force": "Census: % not in labour force",
    "pct_part_time_employed": "Census: % employed part-time",
    "pct_university_educated": "Census: % university educated",
}


def demographic_augmentation_documentation() -> dict:
    """
    Static copy for the UI header and for API/CSV consumers building a fleet DataFrame.

    Each SSE ``complete`` payload includes this under ``census["documentation"]``.
    """
    return {
        "title": "Demographic augmentation (optional)",
        "summary": (
            "Optimisation interval files do not contain a postcode, so ABS census features cannot be joined "
            "directly. Add a small bridge CSV that maps each home folder id to a four-digit Australian "
            "postal area (POA); the server then merges the same postcode-level columns used in "
            "Cluster/enrichment.py (load_abs_census_data)."
        ),
        "steps": [
            "Create or edit house_postcodes.csv in this calculator folder with header: house_id,postcode.",
            "house_id must equal the catan_results subfolder name (UUID). postcode is the POA join key "
            "(four digits, e.g. 2600); it is normalised and matched to ABS rows.",
            "Keep ABS GCP POA extracts under Cluster/abs_data/ (see repo). No change to interval CSVs is required.",
            "Run the fleet analysis again. Successful homes gain postcode when bridged; census columns appear "
            "on each house object when the postcode matches ABS data (see census.abs_columns in the API).",
        ],
        "join_key": "postcode (4-digit POA, same variable as Cluster census join)",
        "bridge_path": "Optisizer data/calculator/house_postcodes.csv",
        "abs_source": "Cluster/enrichment.py — load_abs_census_data() reading Cluster/abs_data/",
        "census_column_names": list(ABS_CENSUS_KEYS),
        "pandas_example": (
            "import pandas as pd\n"
            "# msg = SSE 'complete' JSON\n"
            "df = pd.DataFrame([h for h in msg['houses'] if h.get('ok', True)])\n"
            "# Demographic columns exist only after a successful bridge + POA match; use df.get('postcode')."
        ),
    }


FLEET_DF_COLUMN_PRIORITY: tuple[str, ...] = (
    "house_id",
    "postcode",
    "ok",
    "error",
    "path",
    "n_rows",
    "gross_cost",
    "gross_revenue",
    "net_margin",
    "margin_pct_revenue",
    "margin_pct_activity",
    "max_solar_kw",
    "max_battery_soc_kwh",
    "max_battery_charge_kw",
    "max_battery_discharge_kw",
    "avg_import_price",
    "std_import_price",
    "avg_export_price",
    "std_export_price",
    "avg_price_spread",
    "spread_positive_pct",
    "avg_load_kw",
    "std_load_kw",
    "evening_load_share_pct",
    "total_import_kwh",
    "total_export_kwh",
    "total_load_kwh",
    "total_solar_kwh",
    "battery_charge_kwh",
    "battery_discharge_kwh",
    "battery_throughput_kwh",
    "import_active_pct",
    "export_active_pct",
    "battery_active_pct",
    "export_to_import_ratio",
    "solar_self_consumption_pct",
    "start_date",
    "end_date",
    "segment_id",
    "segment_label",
)


def houses_to_dataframe(houses: list[dict]) -> pd.DataFrame:
    """One row per home dict (ok and failed); nested interval summaries are omitted for a flat table."""
    skip_keys = frozenset({"monthly_rows", "monthly_net_margin"})
    rows: list[dict] = []
    for h in houses:
        if not isinstance(h, dict):
            continue
        flat = {k: v for k, v in h.items() if k not in skip_keys}
        rows.append(flat)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    priority = list(FLEET_DF_COLUMN_PRIORITY) + [c for c in ABS_CENSUS_KEYS if c not in FLEET_DF_COLUMN_PRIORITY]
    ordered = [c for c in priority if c in df.columns]
    ordered.extend(sorted(c for c in df.columns if c not in ordered))
    return df[ordered]


def summarize_csv(csv_path: Path) -> dict:
    stat = csv_path.stat()
    cache_path = CACHE_DIR / f"{csv_path.parent.name}.json"
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            meta = cached.get("meta", {})
            if (
                meta.get("mtime_ns") == stat.st_mtime_ns
                and meta.get("size") == stat.st_size
                and meta.get("schema") == CACHE_SCHEMA_VERSION
            ):
                summary = cached.get("summary")
                if isinstance(summary, dict):
                    return summary
        except (json.JSONDecodeError, OSError, ValueError):
            pass

    def _f(row: list[str], idx: int) -> float:
        if idx < 0 or idx >= len(row):
            return 0.0
        s = row[idx]
        return float(s) if s else 0.0

    def _s(row: list[str], idx: int) -> str:
        if idx < 0 or idx >= len(row):
            return ""
        return row[idx]

    gross_cost = 0.0
    gross_revenue = 0.0
    max_solar_kw = 0.0
    max_battery_soc_kwh = 0.0
    max_battery_charge_kw = 0.0
    max_battery_discharge_kw = 0.0
    start_date = ""
    end_date = ""
    import_price_sum = 0.0
    import_price_sumsq = 0.0
    export_price_sum = 0.0
    export_price_sumsq = 0.0
    spread_sum = 0.0
    spread_pos_count = 0
    load_sum = 0.0
    load_sumsq = 0.0
    evening_load_sum = 0.0
    total_import_kwh = 0.0
    total_export_kwh = 0.0
    total_load_kwh = 0.0
    total_solar_kwh = 0.0
    battery_charge_kwh = 0.0
    battery_discharge_kwh = 0.0
    import_active_count = 0
    export_active_count = 0
    battery_active_count = 0
    monthly_rows: dict[str, int] = {}
    monthly_net_margin: dict[str, float] = {}
    n_rows = 0
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header or "net_cost" not in header:
            raise ValueError("Missing net_cost column")
        col = {name: i for i, name in enumerate(header)}
        idx_net = col["net_cost"]
        idx_ts = col.get("timestamp", -1)
        idx_solar = col.get("solar_output_kw", -1)
        idx_import_price = col.get("import_price", -1)
        idx_export_price = col.get("export_price", -1)
        idx_import_kw = col.get("grid_import_kw", -1)
        idx_export_kw = col.get("grid_export_kw", -1)
        idx_load = col.get("house_load_kw", -1)
        idx_soc = col.get("battery_soc_kwh", -1)
        idx_ch = col.get("battery_charge_kw", -1)
        idx_dis = col.get("battery_discharge_kw", -1)

        for row in reader:
            n_rows += 1
            v = _f(row, idx_net)
            ts_raw = _s(row, idx_ts)
            if len(ts_raw) >= 10:
                date_key = ts_raw[:10]  # ISO date prefix: YYYY-MM-DD (lexicographically sortable)
                month_key = ts_raw[:7]  # ISO month prefix: YYYY-MM
                if not start_date or date_key < start_date:
                    start_date = date_key
                if not end_date or date_key > end_date:
                    end_date = date_key
                monthly_rows[month_key] = monthly_rows.get(month_key, 0) + 1
                monthly_net_margin[month_key] = monthly_net_margin.get(month_key, 0.0) + (-v)
            hour = int(ts_raw[11:13]) if len(ts_raw) >= 13 and ts_raw[11:13].isdigit() else -1
            import_price = _f(row, idx_import_price)
            export_price = _f(row, idx_export_price)
            import_kw = _f(row, idx_import_kw)
            export_kw = _f(row, idx_export_kw)
            load_kw = _f(row, idx_load)
            solar_kw = _f(row, idx_solar)
            charge_kw = _f(row, idx_ch)
            discharge_kw = _f(row, idx_dis)

            import_price_sum += import_price
            import_price_sumsq += import_price * import_price
            export_price_sum += export_price
            export_price_sumsq += export_price * export_price
            spread = export_price - import_price
            spread_sum += spread
            if spread > 0:
                spread_pos_count += 1
            load_sum += load_kw
            load_sumsq += load_kw * load_kw
            if 17 <= hour < 22:
                evening_load_sum += load_kw

            total_import_kwh += import_kw * INTERVAL_HOURS
            total_export_kwh += export_kw * INTERVAL_HOURS
            total_load_kwh += load_kw * INTERVAL_HOURS
            total_solar_kwh += solar_kw * INTERVAL_HOURS
            battery_charge_kwh += charge_kw * INTERVAL_HOURS
            battery_discharge_kwh += discharge_kw * INTERVAL_HOURS
            if import_kw > 0:
                import_active_count += 1
            if export_kw > 0:
                export_active_count += 1
            if charge_kw > 0 or discharge_kw > 0:
                battery_active_count += 1
            if v > 0:
                gross_cost += v
            elif v < 0:
                gross_revenue += -v
            max_solar_kw = max(max_solar_kw, solar_kw)
            max_battery_soc_kwh = max(max_battery_soc_kwh, _f(row, idx_soc))
            max_battery_charge_kw = max(max_battery_charge_kw, charge_kw)
            max_battery_discharge_kw = max(max_battery_discharge_kw, discharge_kw)

    net_margin = gross_revenue - gross_cost
    activity = gross_revenue + gross_cost
    margin_pct_revenue = (net_margin / gross_revenue * 100.0) if gross_revenue > 0 else None
    margin_pct_activity = (net_margin / activity * 100.0) if activity > 0 else None
    n = max(n_rows, 1)
    avg_import_price = import_price_sum / n
    avg_export_price = export_price_sum / n
    avg_load_kw = load_sum / n
    std_import_price = math.sqrt(max((import_price_sumsq / n) - (avg_import_price * avg_import_price), 0.0))
    std_export_price = math.sqrt(max((export_price_sumsq / n) - (avg_export_price * avg_export_price), 0.0))
    std_load_kw = math.sqrt(max((load_sumsq / n) - (avg_load_kw * avg_load_kw), 0.0))
    spread_mean = spread_sum / n
    spread_positive_pct = spread_pos_count / n * 100.0
    evening_load_share_pct = (evening_load_sum / load_sum * 100.0) if load_sum > 0 else None
    export_to_import_ratio = (total_export_kwh / total_import_kwh) if total_import_kwh > 0 else None
    solar_self_consumption_pct = (
        max(total_solar_kwh - total_export_kwh, 0.0) / total_solar_kwh * 100.0
    ) if total_solar_kwh > 0 else None

    house_id = csv_path.parent.name
    result = {
        "house_id": house_id,
        "path": str(csv_path.relative_to(BASE_DIR.parent)),
        "n_rows": n_rows,
        "gross_cost": round(gross_cost, 6),
        "gross_revenue": round(gross_revenue, 6),
        "net_margin": round(net_margin, 6),
        "margin_pct_revenue": None if margin_pct_revenue is None else round(margin_pct_revenue, 4),
        "margin_pct_activity": None if margin_pct_activity is None else round(margin_pct_activity, 4),
        "max_solar_kw": round(max_solar_kw, 6),
        "max_battery_soc_kwh": round(max_battery_soc_kwh, 6),
        "max_battery_charge_kw": round(max_battery_charge_kw, 6),
        "max_battery_discharge_kw": round(max_battery_discharge_kw, 6),
        "avg_import_price": round(avg_import_price, 6),
        "std_import_price": round(std_import_price, 6),
        "avg_export_price": round(avg_export_price, 6),
        "std_export_price": round(std_export_price, 6),
        "avg_price_spread": round(spread_mean, 6),
        "spread_positive_pct": round(spread_positive_pct, 4),
        "avg_load_kw": round(avg_load_kw, 6),
        "std_load_kw": round(std_load_kw, 6),
        "evening_load_share_pct": None if evening_load_share_pct is None else round(evening_load_share_pct, 4),
        "total_import_kwh": round(total_import_kwh, 6),
        "total_export_kwh": round(total_export_kwh, 6),
        "total_load_kwh": round(total_load_kwh, 6),
        "total_solar_kwh": round(total_solar_kwh, 6),
        "battery_charge_kwh": round(battery_charge_kwh, 6),
        "battery_discharge_kwh": round(battery_discharge_kwh, 6),
        "battery_throughput_kwh": round(battery_charge_kwh + battery_discharge_kwh, 6),
        "import_active_pct": round(import_active_count / n * 100.0, 4),
        "export_active_pct": round(export_active_count / n * 100.0, 4),
        "battery_active_pct": round(battery_active_count / n * 100.0, 4),
        "export_to_import_ratio": None if export_to_import_ratio is None else round(export_to_import_ratio, 6),
        "solar_self_consumption_pct": None
        if solar_self_consumption_pct is None
        else round(solar_self_consumption_pct, 4),
        "start_date": start_date or None,
        "end_date": end_date or None,
        "monthly_rows": monthly_rows,
        "monthly_net_margin": {k: round(vv, 6) for k, vv in monthly_net_margin.items()},
    }
    try:
        cache_path.write_text(
            json.dumps(
                {
                    "meta": {
                        "mtime_ns": stat.st_mtime_ns,
                        "size": stat.st_size,
                        "schema": CACHE_SCHEMA_VERSION,
                    },
                    "summary": result,
                }
            ),
            encoding="utf-8",
        )
    except OSError:
        pass
    return result


def fleet_means(houses: list[dict]) -> dict:
    if not houses:
        return {}
    n = len(houses)

    def mean(key: str) -> float:
        return sum(h[key] for h in houses) / n

    rev_vals = [h["margin_pct_revenue"] for h in houses if h["margin_pct_revenue"] is not None]
    act_vals = [h["margin_pct_activity"] for h in houses if h["margin_pct_activity"] is not None]

    return {
        "n_homes": n,
        "avg_gross_cost": round(mean("gross_cost"), 6),
        "avg_gross_revenue": round(mean("gross_revenue"), 6),
        "avg_net_margin": round(mean("net_margin"), 6),
        "avg_margin_pct_revenue": None if not rev_vals else round(sum(rev_vals) / len(rev_vals), 4),
        "avg_margin_pct_activity": None if not act_vals else round(sum(act_vals) / len(act_vals), 4),
    }


def decile_profiles(houses: list[dict]) -> list[dict]:
    """Build D1..D10 profiles by profitability (net_margin), lowest to highest."""
    if not houses:
        return []

    sorted_houses = sorted(houses, key=lambda h: h["net_margin"])
    n = len(sorted_houses)
    buckets: list[list[dict]] = [[] for _ in range(10)]

    for i, house in enumerate(sorted_houses):
        idx = (i * 10) // n  # rank-based deciles with near-equal counts
        buckets[idx].append(house)

    out: list[dict] = []
    for i, bucket in enumerate(buckets, start=1):
        if not bucket:
            out.append(
                {
                    "decile": i,
                    "label": f"D{i}",
                    "n_homes": 0,
                    "avg_gross_cost": None,
                    "avg_gross_revenue": None,
                    "avg_net_margin": None,
                    "min_net_margin": None,
                    "max_net_margin": None,
                    "avg_margin_pct_activity": None,
                }
            )
            continue

        m = len(bucket)
        net_vals = [h["net_margin"] for h in bucket]
        act_vals = [h["margin_pct_activity"] for h in bucket if h["margin_pct_activity"] is not None]
        out.append(
            {
                "decile": i,
                "label": f"D{i}",
                "n_homes": m,
                "avg_gross_cost": round(sum(h["gross_cost"] for h in bucket) / m, 6),
                "avg_gross_revenue": round(sum(h["gross_revenue"] for h in bucket) / m, 6),
                "avg_net_margin": round(sum(net_vals) / m, 6),
                "min_net_margin": round(min(net_vals), 6),
                "max_net_margin": round(max(net_vals), 6),
                "avg_margin_pct_activity": None if not act_vals else round(sum(act_vals) / len(act_vals), 4),
                "avg_max_solar_kw": round(sum(h["max_solar_kw"] for h in bucket) / m, 6),
                "avg_max_battery_soc_kwh": round(sum(h["max_battery_soc_kwh"] for h in bucket) / m, 6),
            }
        )
    return out


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / math.sqrt(sxx * syy)


def size_profitability_profile(houses: list[dict]) -> dict:
    if not houses:
        return {}
    net = [h["net_margin"] for h in houses]
    solar = [h["max_solar_kw"] for h in houses]
    soc = [h["max_battery_soc_kwh"] for h in houses]
    ch = [h["max_battery_charge_kw"] for h in houses]
    dis = [h["max_battery_discharge_kw"] for h in houses]

    corr_solar = _pearson(net, solar)
    corr_soc = _pearson(net, soc)

    return {
        "avg_max_solar_kw": round(sum(solar) / len(solar), 4),
        "avg_max_battery_soc_kwh": round(sum(soc) / len(soc), 4),
        "avg_max_battery_charge_kw": round(sum(ch) / len(ch), 4),
        "avg_max_battery_discharge_kw": round(sum(dis) / len(dis), 4),
        "corr_net_vs_max_solar_kw": None if corr_solar is None else round(corr_solar, 4),
        "corr_net_vs_max_battery_soc_kwh": None if corr_soc is None else round(corr_soc, 4),
    }


def fleet_time_distribution(houses: list[dict]) -> dict:
    if not houses:
        return {"period_start": None, "period_end": None, "months": []}

    period_start = min((h.get("start_date") for h in houses if h.get("start_date")), default=None)
    period_end = max((h.get("end_date") for h in houses if h.get("end_date")), default=None)

    month_rows: dict[str, int] = {}
    month_net: dict[str, float] = {}
    month_active_homes: dict[str, int] = {}

    for h in houses:
        seen_months: set[str] = set()
        for month, count in (h.get("monthly_rows") or {}).items():
            month_rows[month] = month_rows.get(month, 0) + int(count)
            seen_months.add(month)
        for month, net in (h.get("monthly_net_margin") or {}).items():
            month_net[month] = month_net.get(month, 0.0) + float(net)
        for month in seen_months:
            month_active_homes[month] = month_active_homes.get(month, 0) + 1

    months = sorted(set(month_rows) | set(month_net) | set(month_active_homes))
    month_points = [
        {
            "month": m,
            "rows": month_rows.get(m, 0),
            "net_margin": round(month_net.get(m, 0.0), 6),
            "active_homes": month_active_homes.get(m, 0),
        }
        for m in months
    ]
    return {"period_start": period_start, "period_end": period_end, "months": month_points}


def _rank_deciles(values: list[float]) -> list[int]:
    pairs = sorted(enumerate(values), key=lambda x: x[1])
    n = len(values)
    out = [0] * n
    for rank, (idx, _) in enumerate(pairs):
        out[idx] = min(10, (rank * 10) // max(n, 1) + 1)
    return out


def predictor_analysis(houses: list[dict]) -> dict:
    if not houses:
        return {"rows": []}

    features: list[tuple[str, str]] = [
        ("max_solar_kw", "Max solar (kW)"),
        ("max_battery_soc_kwh", "Max battery SoC (kWh)"),
        ("max_battery_charge_kw", "Max battery charge (kW)"),
        ("max_battery_discharge_kw", "Max battery discharge (kW)"),
        ("avg_import_price", "Avg import price"),
        ("std_import_price", "Import price std"),
        ("avg_export_price", "Avg export price"),
        ("std_export_price", "Export price std"),
        ("avg_price_spread", "Avg export-import spread"),
        ("spread_positive_pct", "Spread positive (%)"),
        ("avg_load_kw", "Avg load (kW)"),
        ("std_load_kw", "Load std (kW)"),
        ("evening_load_share_pct", "Evening load share (%)"),
        ("total_import_kwh", "Total import (kWh)"),
        ("total_export_kwh", "Total export (kWh)"),
        ("total_solar_kwh", "Total solar (kWh)"),
        ("battery_throughput_kwh", "Battery throughput (kWh)"),
        ("import_active_pct", "Import active (%)"),
        ("export_active_pct", "Export active (%)"),
        ("battery_active_pct", "Battery active (%)"),
        ("export_to_import_ratio", "Export/import ratio"),
        ("solar_self_consumption_pct", "Solar self-consumption (%)"),
    ]
    for ck in ABS_CENSUS_KEYS:
        if any(h.get(ck) is not None for h in houses):
            features.append((ck, ABS_CENSUS_LABELS.get(ck, ck.replace("_", " ").title())))

    target_margin = [h["net_margin"] for h in houses]
    margin_deciles = _rank_deciles(target_margin)
    rows: list[dict] = []
    for key, label in features:
        xs: list[float] = []
        ys_margin: list[float] = []
        ys_decile: list[float] = []
        for i, h in enumerate(houses):
            v = h.get(key)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            xs.append(fv)
            ys_margin.append(target_margin[i])
            ys_decile.append(float(margin_deciles[i]))
        corr_margin = _pearson(xs, ys_margin)
        corr_decile = _pearson(xs, ys_decile)
        if corr_margin is None and corr_decile is None:
            continue
        rows.append(
            {
                "feature": key,
                "label": label,
                "sample_size": len(xs),
                "corr_net_margin": None if corr_margin is None else round(corr_margin, 4),
                "corr_profit_decile": None if corr_decile is None else round(corr_decile, 4),
                "abs_score": max(abs(corr_margin or 0.0), abs(corr_decile or 0.0)),
            }
        )
    rows.sort(key=lambda r: r["abs_score"], reverse=True)
    return {"rows": rows[:12]}


def _zscore_matrix(rows: list[list[float]]) -> list[list[float]]:
    if not rows:
        return []
    n_cols = len(rows[0])
    means = [sum(r[c] for r in rows) / len(rows) for c in range(n_cols)]
    stds: list[float] = []
    for c in range(n_cols):
        var = sum((r[c] - means[c]) ** 2 for r in rows) / len(rows)
        stds.append(math.sqrt(var) if var > 1e-12 else 1.0)
    return [[(r[c] - means[c]) / stds[c] for c in range(n_cols)] for r in rows]


def _kmeans(points: list[list[float]], k: int, iterations: int = 20) -> list[int]:
    if not points:
        return []
    k = max(1, min(k, len(points)))
    centroids = [points[(i * len(points)) // k][:] for i in range(k)]
    labels = [0] * len(points)
    for _ in range(iterations):
        changed = False
        for i, p in enumerate(points):
            best = min(range(k), key=lambda c: sum((p[d] - centroids[c][d]) ** 2 for d in range(len(p))))
            if labels[i] != best:
                labels[i] = best
                changed = True
        if not changed:
            break
        for c in range(k):
            members = [points[i] for i, lbl in enumerate(labels) if lbl == c]
            if not members:
                continue
            centroids[c] = [sum(m[d] for m in members) / len(members) for d in range(len(members[0]))]
    return labels


# After clustering, segments are sorted by avg net margin and labelled S1..Sk
# (S1 = weakest, Sk = strongest in *this* fleet). Copy is written for lay readers.
_SEGMENT_PLAIN_ENGLISH_BY_RANK: tuple[str, ...] = (
    "Homes in this bucket share a similar pattern of energy behaviour: how much power they draw and export, "
    "when they use it, and how busy the battery is. In your fleet, this pattern lines up with the "
    "weakest typical financial outcome (lowest average net margin) for the period analysed.",
    "Another distinct behaviour pattern, but with a clearer upside than the bottom group: "
    "typical net margins are better than S1, yet still below the middle of the pack for this run.",
    "A stronger-than-average pattern: these homes tend to finish the interval with meaningfully better "
    "typical net margins than S1 and S2, though not the very best group here.",
    "The standout behaviour group for this dataset: the pattern associated with the highest typical net margins. "
    "They are not guaranteed winners house-by-house, but as a group they averaged the best tariff outcome.",
)


def _plain_english_for_segment_rank(rank: int, n_segments: int) -> str:
    """rank 1 = S1 (lowest profit), rank n = Sn (highest)."""
    i = rank - 1
    if 0 <= i < len(_SEGMENT_PLAIN_ENGLISH_BY_RANK):
        return _SEGMENT_PLAIN_ENGLISH_BY_RANK[i]
    if rank <= 1:
        return "Lowest typical net margin among the behaviour groups in this run."
    if rank >= n_segments:
        return "Highest typical net margin among the behaviour groups in this run."
    return "Mid-ranked behaviour group for typical net margin in this run."


def segment_analysis(houses: list[dict], k: int = 4) -> dict:
    if not houses:
        return {"segments": []}
    base_keys = [
        "avg_load_kw",
        "evening_load_share_pct",
        "import_active_pct",
        "export_active_pct",
        "battery_active_pct",
        "avg_price_spread",
        "solar_self_consumption_pct",
        "export_to_import_ratio",
        "battery_throughput_kwh",
        "max_solar_kw",
        "max_battery_soc_kwh",
    ]
    census_keys = [ck for ck in ABS_CENSUS_KEYS if all(h.get(ck) is not None for h in houses)]
    keys = base_keys + census_keys

    usable: list[dict] = []
    matrix: list[list[float]] = []
    for h in houses:
        row: list[float] = []
        ok = True
        for key in keys:
            v = h.get(key)
            if v is None:
                ok = False
                break
            try:
                row.append(float(v))
            except (TypeError, ValueError):
                ok = False
                break
        if ok:
            usable.append(h)
            matrix.append(row)

    if not matrix:
        return {"segments": []}
    labels = _kmeans(_zscore_matrix(matrix), k=k, iterations=24)
    for i, h in enumerate(usable):
        h["segment_id"] = int(labels[i]) + 1

    segments: list[dict] = []
    for seg in sorted(set(labels)):
        members = [h for h in usable if h.get("segment_id") == seg + 1]
        m = len(members)
        segments.append(
            {
                "segment_id": seg + 1,
                "n_homes": m,
                "avg_net_margin": round(sum(h["net_margin"] for h in members) / m, 6),
                "avg_gross_cost": round(sum(h["gross_cost"] for h in members) / m, 6),
                "avg_gross_revenue": round(sum(h["gross_revenue"] for h in members) / m, 6),
                "avg_margin_pct_activity": round(
                    sum((h["margin_pct_activity"] or 0.0) for h in members) / m, 4
                ),
                "avg_margin_pct_revenue": round(
                    sum((h["margin_pct_revenue"] or 0.0) for h in members) / m, 4
                ),
                "total_net_margin": round(sum(h["net_margin"] for h in members), 6),
                "avg_max_solar_kw": round(sum(h["max_solar_kw"] for h in members) / m, 4),
                "avg_max_battery_soc_kwh": round(sum(h["max_battery_soc_kwh"] for h in members) / m, 4),
                "avg_import_price": round(sum(h["avg_import_price"] for h in members) / m, 5),
                "avg_export_price": round(sum(h["avg_export_price"] for h in members) / m, 5),
                "avg_export_active_pct": round(sum(h["export_active_pct"] for h in members) / m, 4),
                "avg_battery_active_pct": round(sum(h["battery_active_pct"] for h in members) / m, 4),
            }
        )
    segments.sort(key=lambda s: s["avg_net_margin"])
    n_seg = len(segments)
    label_by_id: dict[int, str] = {}
    for rank, seg in enumerate(segments, start=1):
        seg["label"] = f"S{rank}"
        seg["plain_english"] = _plain_english_for_segment_rank(rank, n_seg)
        label_by_id[seg["segment_id"]] = seg["label"]
    for h in usable:
        sid = h.get("segment_id")
        if sid in label_by_id:
            h["segment_label"] = label_by_id[sid]
    return {"segments": segments}


def sse_event(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


@app.route("/")
def index():
    n_files = len(discover_csv_paths())
    return render_template(
        "index.html",
        n_files=n_files,
        data_root=str(CATAN_RESULTS),
        demographic_doc=demographic_augmentation_documentation(),
    )


@app.route("/api/stream")
def api_stream():
    paths = discover_csv_paths()

    @stream_with_context
    def generate():
        if not paths:
            yield sse_event(
                {
                    "type": "error",
                    "message": f"No CSV files found under {CATAN_RESULTS}",
                }
            )
            return

        total = len(paths)
        houses: list[dict] = []
        yield sse_event({"type": "start", "total": total})

        # Parallelize per-home CSV processing to use all CPU cores.
        max_workers = min(total, max(1, (os.cpu_count() or 2) - 1))
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(summarize_csv, p): p for p in paths}
            done = 0
            for fut in as_completed(futures):
                done += 1
                csv_path = futures[fut]
                try:
                    row = fut.result()
                    row["ok"] = True
                    houses.append(row)
                    payload_house = row
                except Exception as e:  # noqa: BLE001 — per-file errors should not stop the run
                    payload_house = {
                        "house_id": csv_path.parent.name,
                        "path": str(csv_path.relative_to(BASE_DIR.parent)),
                        "ok": False,
                        "error": str(e),
                    }
                yield sse_event(
                    {
                        "type": "progress",
                        "current": done,
                        "total": total,
                        "house": payload_house,
                    }
                )

        ok_houses = [h for h in houses if h.get("ok", True)]
        census_meta = merge_census_into_houses(houses)
        fleet = fleet_means(ok_houses)
        deciles = decile_profiles(ok_houses)
        size_profile = size_profitability_profile(ok_houses)
        time_profile = fleet_time_distribution(ok_houses)
        predictors = predictor_analysis(ok_houses)
        segments = segment_analysis(ok_houses, k=4)
        fleet_df = houses_to_dataframe(houses)
        yield sse_event(
            {
                "type": "complete",
                "fleet": fleet,
                "houses": houses,
                "deciles": deciles,
                "size_profile": size_profile,
                "time_profile": time_profile,
                "predictors": predictors,
                "segments": segments,
                "census": census_meta,
                "fleet_dataframe": {
                    "n_rows": int(len(fleet_df)),
                    "columns": list(fleet_df.columns),
                    "documentation": census_meta.get("documentation") or demographic_augmentation_documentation(),
                },
            }
        )

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


if __name__ == "__main__":
    app.run(debug=True, port=5010, threaded=True)
