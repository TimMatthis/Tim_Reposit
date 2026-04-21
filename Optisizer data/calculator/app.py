"""
Optisizer fleet summary: sum net_cost positives/negatives per home, stream progress via SSE.

Optional ABS census (demographic augmentation, same POA join as Cluster/enrichment.py):
  See ``demographic_augmentation_documentation()`` for the full bridge + DataFrame notes.
  Bridge: ``house_postcodes.csv`` with ``house_id``, ``postcode`` (4-digit POA).

Optional VIC exclusion list: ``catan_results/VIC SITES.csv`` (header ``uid``) — those folder UUIDs
are skipped when discovering ``optimisation_intervals.csv`` under either Catan export tree.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import math
import os
import re
import shutil
import tempfile
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pandas as pd
from collections import defaultdict

from typing import Any

from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from werkzeug.utils import secure_filename

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent
OPTISIZER_DATA = BASE_DIR.parent
# Full optimisation exports (all regions including VIC).
CATAN_RESULTS = OPTISIZER_DATA / "catan_results"
def catan_directory_no_vic() -> Path:
    """Prefer ``catan_no_vic`` (underscore); fall back to legacy ``catan no vic`` (spaces)."""
    underscore = OPTISIZER_DATA / "catan_no_vic"
    spaced = OPTISIZER_DATA / "catan no vic"
    if underscore.is_dir():
        return underscore
    return spaced


def catan_sources() -> tuple[tuple[str, str, Path], ...]:
    return (
        (CATAN_SOURCE_WITH_VIC, "Full fleet — catan_results", CATAN_RESULTS),
        (CATAN_SOURCE_NO_VIC, "No VIC export — catan_no_vic", catan_directory_no_vic()),
    )

# Optional blocklist: folder UUIDs (one per row under header ``uid``) skipped for every fleet scan.
VIC_SITES_CSV_CANDIDATES: tuple[str, ...] = ("VIC SITES.csv", "vic sites.csv", "VIC_SITES.csv")

HOUSE_POSTCODES_CSV = BASE_DIR / "house_postcodes.csv"
CSV_NAME = "optimisation_intervals.csv"
COMPARISON_BATCH_JSON = OPTISIZER_DATA / "Comparison" / "batch_catan_comparison.json"
CACHE_DIR = BASE_DIR / ".summary_cache"
CACHE_DIR.mkdir(exist_ok=True)
CACHE_SCHEMA_VERSION = 7
INTERVAL_HOURS = 5.0 / 60.0
# Wall-clock bins for pooling interval-level $ (same semantics as hourly_gross_*).
CLOCK_SLOTS_PER_DAY = 288  # 24 * 60 / 5

# Optional third economics block in ``batch_catan_comparison.json``: retail / no trade-back
# counterfactual (same keys as ``actual`` / ``optimised``).
BATCH_NO_TRADE_SCENARIO_KEYS: tuple[str, ...] = (
    "retail_without_trade",
    "no_trade",
    "without_reposit",
    "retail_only",
    "counterfactual_retail_no_trade",
)

# Optional MSC counterfactual block in ``batch_catan_comparison.json``.
BATCH_MSC_SCENARIO_KEYS: tuple[str, ...] = (
    "msc",
    "msc_counterfactual",
    "counterfactual_msc",
    "counterfactual_msc_mode",
)

# Optional per-row counterfactual in ``optimisation_intervals.csv`` (same sign convention as ``net_cost``).
_NO_TRADE_NET_COST_HEADER_NAMES: tuple[str, ...] = (
    "net_cost_retail_without_trade",
    "net_cost_retail_no_trade",
    "net_cost_without_reposit",
    "net_cost_no_trade",
)

# MSC valuation assumptions (energy-only):
# - same import/export rate within each interval, so value comes from offset, not timing spread,
# - 3 time-of-use windows requested for DMO pricing.
MSC_TOU_PRICE_PEAK_PER_KWH = 0.45      # 4pm-8pm
MSC_TOU_PRICE_MIDDAY_PER_KWH = 0.12    # 10am-2pm
MSC_TOU_PRICE_SHOULDER_PER_KWH = 0.37  # all other times
MSC_COUNTERFACTUAL_MODE = "inverter_msc_mode"
MSC_CACHE_SCHEMA_VERSION = 2


def _msc_tou_price_per_kwh(ts_raw: str) -> float:
    """
    3-window MSC DMO valuation (same import/export within each interval):
    - 16:00-19:59 => 0.45 $/kWh
    - 10:00-13:59 => 0.12 $/kWh
    - all other times => 0.37 $/kWh
    """
    if len(ts_raw) >= 13 and ts_raw[11:13].isdigit():
        hour = int(ts_raw[11:13])
    else:
        hour = -1
    if 16 <= hour < 20:
        return MSC_TOU_PRICE_PEAK_PER_KWH
    if 10 <= hour < 14:
        return MSC_TOU_PRICE_MIDDAY_PER_KWH
    return MSC_TOU_PRICE_SHOULDER_PER_KWH


def _batch_no_trade_side(rec: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(rec, dict):
        return None
    for k in BATCH_NO_TRADE_SCENARIO_KEYS:
        side = rec.get(k)
        if not isinstance(side, dict) or side.get("net_energy_cost") is None:
            continue
        try:
            float(side["net_energy_cost"])
        except (TypeError, ValueError):
            continue
        return side
    return None


def _to_float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _kwh_from_side(side: dict[str, Any], total_key: str, gross_key: str, vwap_key: str) -> float | None:
    direct = _to_float_or_none(side.get(total_key))
    if direct is not None and direct >= 0.0:
        return direct
    gross = _to_float_or_none(side.get(gross_key))
    vwap = _to_float_or_none(side.get(vwap_key))
    if gross is None or vwap is None or vwap <= 0.0:
        return None
    derived = gross / vwap
    return derived if derived >= 0.0 else None


def _scenario_energy_snapshot(side: dict[str, Any] | None) -> dict[str, float] | None:
    if not isinstance(side, dict):
        return None
    gross_import = _to_float_or_none(side.get("gross_import_cost"))
    gross_export = _to_float_or_none(side.get("gross_export_revenue"))
    import_vwap = _to_float_or_none(side.get("import_vwap"))
    export_vwap = _to_float_or_none(side.get("export_vwap"))
    import_kwh = _kwh_from_side(side, "total_grid_import_kwh", "gross_import_cost", "import_vwap")
    export_kwh = _kwh_from_side(side, "total_grid_export_kwh", "gross_export_revenue", "export_vwap")
    net_energy_cost = _to_float_or_none(side.get("net_energy_cost"))
    if gross_import is None and import_kwh is not None and import_vwap is not None:
        gross_import = import_kwh * import_vwap
    if gross_export is None and export_kwh is not None and export_vwap is not None:
        gross_export = export_kwh * export_vwap
    if net_energy_cost is None and gross_import is not None and gross_export is not None:
        net_energy_cost = gross_import - gross_export
    if (
        gross_import is None
        and gross_export is None
        and import_kwh is None
        and export_kwh is None
        and net_energy_cost is None
    ):
        return None
    return {
        "gross_import_cost": float(gross_import or 0.0),
        "gross_export_revenue": float(gross_export or 0.0),
        "import_kwh": float(import_kwh or 0.0),
        "export_kwh": float(export_kwh or 0.0),
        "net_energy_cost": float(net_energy_cost or 0.0),
    }


def _batch_msc_side(rec: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(rec, dict):
        return None
    for k in BATCH_MSC_SCENARIO_KEYS:
        side = rec.get(k)
        if not isinstance(side, dict):
            continue
        if _to_float_or_none(side.get("net_energy_cost")) is None:
            continue
        return side
    return None


def _extract_battery_specs_for_msc(rec: dict[str, Any]) -> dict[str, float]:
    """
    Aggregate battery settings from ``system_config.inverters`` for MSC simulation.
    Falls back to conservative defaults when metadata is missing.
    """
    sc = rec.get("system_config")
    if not isinstance(sc, dict):
        return {
            "capacity_kwh": 0.0,
            "min_soc_kwh": 0.0,
            "initial_soc_kwh": 0.0,
            "charge_efficiency": 1.0,
            "discharge_efficiency": 1.0,
        }
    invs = sc.get("inverters", [])
    capacity = 0.0
    min_soc = 0.0
    initial_soc = 0.0
    has_initial = False
    charge_eff_num = 0.0
    discharge_eff_num = 0.0
    weight_sum = 0.0
    for inv in invs:
        if not isinstance(inv, dict):
            continue
        batt = inv.get("battery")
        if not isinstance(batt, dict):
            continue
        cap = max(_to_float_or_none(batt.get("capacity_kwh")) or 0.0, 0.0)
        if cap <= 0.0:
            continue
        capacity += cap
        min_soc += max(_to_float_or_none(batt.get("min_soc_kwh")) or 0.0, 0.0)
        ini = _to_float_or_none(batt.get("initial_soc_kwh"))
        if ini is not None:
            initial_soc += max(ini, 0.0)
            has_initial = True
        ch_eff = _to_float_or_none(batt.get("charge_efficiency"))
        dis_eff = _to_float_or_none(batt.get("discharge_efficiency"))
        ch_eff = min(max(ch_eff if ch_eff is not None else 1.0, 0.01), 1.0)
        dis_eff = min(max(dis_eff if dis_eff is not None else 1.0, 0.01), 1.0)
        charge_eff_num += ch_eff * cap
        discharge_eff_num += dis_eff * cap
        weight_sum += cap
    if capacity <= 0.0:
        return {
            "capacity_kwh": 0.0,
            "min_soc_kwh": 0.0,
            "initial_soc_kwh": 0.0,
            "charge_efficiency": 1.0,
            "discharge_efficiency": 1.0,
        }
    min_soc = min(max(min_soc, 0.0), capacity)
    initial_soc = min(max(initial_soc if has_initial else min_soc, min_soc), capacity)
    return {
        "capacity_kwh": capacity,
        "min_soc_kwh": min_soc,
        "initial_soc_kwh": initial_soc,
        "charge_efficiency": charge_eff_num / max(weight_sum, 1e-9),
        "discharge_efficiency": discharge_eff_num / max(weight_sum, 1e-9),
    }


def _find_uid_csv_for_msc(uid: str) -> Path | None:
    if not uid:
        return None
    # Prefer no-VIC export (same scope as most batch comparisons), then fall back to full fleet.
    preferred_sources = (CATAN_SOURCE_NO_VIC, CATAN_SOURCE_WITH_VIC)
    for source_id in preferred_sources:
        try:
            root = catan_root_for_source(source_id)
        except ValueError:
            continue
        p = root / uid / CSV_NAME
        if p.is_file():
            return p
    return None


def _msc_cache_path_for_csv(csv_path: Path) -> Path:
    cache_sub = CACHE_DIR / "msc_counterfactual"
    cache_sub.mkdir(parents=True, exist_ok=True)
    stable_key = hashlib.sha1(str(csv_path).encode("utf-8")).hexdigest()
    return cache_sub / f"{stable_key}.json"


def _simulate_inverter_msc_from_csv(
    csv_path: Path,
    rec: dict[str, Any],
) -> dict[str, Any] | None:
    stat = csv_path.stat()
    cache_path = _msc_cache_path_for_csv(csv_path)
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            meta = cached.get("meta", {})
            if (
                meta.get("mtime_ns") == stat.st_mtime_ns
                and meta.get("size") == stat.st_size
                and meta.get("schema") == MSC_CACHE_SCHEMA_VERSION
                and meta.get("price_peak_per_kwh") == round(MSC_TOU_PRICE_PEAK_PER_KWH, 6)
                and meta.get("price_midday_per_kwh") == round(MSC_TOU_PRICE_MIDDAY_PER_KWH, 6)
                and meta.get("price_shoulder_per_kwh") == round(MSC_TOU_PRICE_SHOULDER_PER_KWH, 6)
            ):
                summary = cached.get("summary")
                if isinstance(summary, dict):
                    return summary
        except (json.JSONDecodeError, OSError, ValueError):
            pass

    specs = _extract_battery_specs_for_msc(rec)
    cap = float(specs["capacity_kwh"])
    min_soc = float(specs["min_soc_kwh"])
    soc = float(specs["initial_soc_kwh"])
    charge_eff = float(specs["charge_efficiency"])
    discharge_eff = float(specs["discharge_efficiency"])

    total_import_kwh = 0.0
    total_export_kwh = 0.0
    total_load_kwh = 0.0
    total_solar_kwh = 0.0
    gross_import = 0.0
    gross_export = 0.0

    with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return None
        col = {name: i for i, name in enumerate(header)}
        idx_load = col.get("house_load_kw", -1)
        idx_solar = col.get("solar_output_kw", -1)
        idx_soc = col.get("battery_soc_kwh", -1)
        idx_ts = col.get("timestamp", -1)
        if idx_load < 0 and idx_solar < 0:
            return None

        def _f(row: list[str], idx: int) -> float:
            if idx < 0 or idx >= len(row):
                return 0.0
            s = row[idx]
            return float(s) if s else 0.0

        def _s(row: list[str], idx: int) -> str:
            if idx < 0 or idx >= len(row):
                return ""
            return row[idx]

        if cap > 0.0 and idx_soc >= 0:
            # Anchor initial SOC to measured first value when available.
            for row in reader:
                first_soc = _to_float_or_none(row[idx_soc] if idx_soc < len(row) else None)
                if first_soc is not None:
                    soc = min(max(float(first_soc), min_soc), cap)
                    break
            f.seek(0)
            reader = csv.reader(f)
            next(reader, None)

        for row in reader:
            ts_raw = _s(row, idx_ts)
            load_kwh = max(_f(row, idx_load), 0.0) * INTERVAL_HOURS
            solar_kwh = max(_f(row, idx_solar), 0.0) * INTERVAL_HOURS
            total_load_kwh += load_kwh
            total_solar_kwh += solar_kwh

            # Solar priority 1: serve house load.
            solar_to_load = min(solar_kwh, load_kwh)
            remaining_load = load_kwh - solar_to_load
            solar_excess = solar_kwh - solar_to_load

            # Solar priority 2: charge battery from excess only.
            if cap > 0.0 and solar_excess > 0.0 and charge_eff > 0.0:
                headroom = max(cap - soc, 0.0)
                charge_input_limit = headroom / charge_eff
                solar_to_battery_input = min(solar_excess, charge_input_limit)
                soc = min(cap, soc + solar_to_battery_input * charge_eff)
                solar_excess -= solar_to_battery_input

            # Discharge priority: serve remaining load only, never export from battery.
            if cap > 0.0 and remaining_load > 0.0 and discharge_eff > 0.0:
                deliverable_to_load = max(soc - min_soc, 0.0) * discharge_eff
                battery_to_load = min(remaining_load, deliverable_to_load)
                soc = max(min_soc, soc - (battery_to_load / discharge_eff))
                remaining_load -= battery_to_load

            total_import_kwh += max(remaining_load, 0.0)
            total_export_kwh += max(solar_excess, 0.0)
            p = _msc_tou_price_per_kwh(ts_raw)
            gross_import += max(remaining_load, 0.0) * p
            gross_export += max(solar_excess, 0.0) * p

    net_energy_cost = gross_import - gross_export
    export_offset_pct = (
        ((total_load_kwh - total_import_kwh) / total_load_kwh) * 100.0 if total_load_kwh > 0.0 else None
    )
    import_vwap = (gross_import / total_import_kwh) if total_import_kwh > 0.0 else None
    export_vwap = (gross_export / total_export_kwh) if total_export_kwh > 0.0 else None

    summary: dict[str, Any] = {
        "net_energy_cost": round(net_energy_cost, 6),
        "gross_import_cost": round(gross_import, 6),
        "gross_export_revenue": round(gross_export, 6),
        "total_grid_import_kwh": round(total_import_kwh, 6),
        "total_grid_export_kwh": round(total_export_kwh, 6),
        "total_house_load_kwh": round(total_load_kwh, 6),
        "total_solar_kwh": round(total_solar_kwh, 6),
        "import_vwap": None if import_vwap is None else round(import_vwap, 6),
        "export_vwap": None if export_vwap is None else round(export_vwap, 6),
        "export_offset_pct": None if export_offset_pct is None else round(export_offset_pct, 6),
        "counterfactual_mode": MSC_COUNTERFACTUAL_MODE,
        "msc_pricing_mode": "time_of_use_3_block",
        "msc_price_peak_per_kwh": round(MSC_TOU_PRICE_PEAK_PER_KWH, 6),
        "msc_price_midday_per_kwh": round(MSC_TOU_PRICE_MIDDAY_PER_KWH, 6),
        "msc_price_shoulder_per_kwh": round(MSC_TOU_PRICE_SHOULDER_PER_KWH, 6),
        "energy_only": True,
        "same_import_export_rate_per_interval": True,
        "time_of_use_valuation": True,
    }
    try:
        cache_path.write_text(
            json.dumps(
                {
                    "meta": {
                        "mtime_ns": stat.st_mtime_ns,
                        "size": stat.st_size,
                        "schema": MSC_CACHE_SCHEMA_VERSION,
                        "price_peak_per_kwh": round(MSC_TOU_PRICE_PEAK_PER_KWH, 6),
                        "price_midday_per_kwh": round(MSC_TOU_PRICE_MIDDAY_PER_KWH, 6),
                        "price_shoulder_per_kwh": round(MSC_TOU_PRICE_SHOULDER_PER_KWH, 6),
                    },
                    "summary": summary,
                }
            ),
            encoding="utf-8",
        )
    except OSError:
        pass
    return summary


def _build_msc_counterfactual_side(
    uid: str,
    rec: dict[str, Any],
) -> dict[str, Any] | None:
    existing = _batch_msc_side(rec)
    if existing is not None:
        return existing
    if not isinstance(rec, dict) or not uid:
        return None
    csv_path = _find_uid_csv_for_msc(uid)
    if csv_path is None:
        return None
    return _simulate_inverter_msc_from_csv(csv_path, rec)


def augment_batch_with_msc_counterfactual(batch: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for uid, rec in batch.items():
        if not isinstance(rec, dict):
            out[uid] = rec
            continue
        rec_copy = dict(rec)
        msc = _build_msc_counterfactual_side(str(uid), rec_copy)
        if msc is not None and _batch_msc_side(rec_copy) is None:
            rec_copy["msc_counterfactual"] = msc
        out[uid] = rec_copy
    return out


def msc_config_payload() -> dict[str, Any]:
    return {
        "mode": MSC_COUNTERFACTUAL_MODE,
        "pricing_mode": "time_of_use_3_block",
        "price_peak_per_kwh": round(MSC_TOU_PRICE_PEAK_PER_KWH, 6),
        "price_midday_per_kwh": round(MSC_TOU_PRICE_MIDDAY_PER_KWH, 6),
        "price_shoulder_per_kwh": round(MSC_TOU_PRICE_SHOULDER_PER_KWH, 6),
        "same_import_export_rate_per_interval": True,
        "energy_only": True,
    }


def clock_slot_5min_from_timestamp(ts_raw: str) -> int | None:
    """
    Map a CSV timestamp string to 0..287 from local wall time (HH:MM floored to 5 minutes).
    Accepts ISO date + 'T' or space + HH:MM (e.g. '2025-04-09 13:20:00+10:00').
    """
    if len(ts_raw) < 16:
        return None
    if ts_raw[10] not in ("T", " ", "t"):
        return None
    hh = ts_raw[11:13]
    if ts_raw[13] != ":":
        return None
    mm = ts_raw[14:16]
    if not (hh.isdigit() and mm.isdigit()):
        return None
    h = int(hh)
    m = int(mm)
    if h < 0 or h > 23 or m < 0 or m > 59:
        return None
    return h * 12 + (m // 5)

# SSE / UI: stable ids for Catan roots (also used as .summary_cache/<id>/ subfolders).
CATAN_SOURCE_WITH_VIC = "with_vic"
CATAN_SOURCE_NO_VIC = "no_vic"
def catan_root_for_source(source_id: str) -> Path:
    for sid, _label, root in catan_sources():
        if sid == source_id:
            return root
    raise ValueError(f"Unknown Catan source {source_id!r}; expected one of {[s[0] for s in catan_sources()]}")


def catan_source_meta(source_id: str) -> dict[str, str]:
    """Human label and repo-relative path for SSE / UI."""
    for sid, label, root in catan_sources():
        if sid == source_id:
            try:
                rel = str(root.relative_to(REPO_ROOT))
            except ValueError:
                rel = str(root)
            return {"data_source": sid, "data_source_label": label, "data_source_path": rel}
    root = catan_root_for_source(source_id)
    try:
        rel = str(root.relative_to(REPO_ROOT))
    except ValueError:
        rel = str(root)
    return {"data_source": source_id, "data_source_label": source_id, "data_source_path": rel}


def normalize_catan_source(req, default: str = CATAN_SOURCE_WITH_VIC) -> str:
    raw = (req.args.get("source") or default).strip()
    if raw not in {s[0] for s in catan_sources()}:
        return default
    return raw


def vic_exclusion_csv_path() -> Path | None:
    for name in VIC_SITES_CSV_CANDIDATES:
        p = CATAN_RESULTS / name
        if p.is_file():
            return p
    return None


def load_vic_site_exclusions() -> frozenset[str]:
    """Lowercase folder UUIDs (no hyphens) to omit from ``discover_csv_paths``."""
    path = vic_exclusion_csv_path()
    if not path:
        return frozenset()
    out: set[str] = set()
    try:
        with path.open(newline="", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                cell = (row[0] or "").strip()
                if not cell:
                    continue
                low = cell.lower().replace("-", "")
                if low == "uid":
                    continue
                out.add(low)
    except OSError:
        return frozenset()
    return frozenset(out)


def discover_csv_paths_with_meta(root: Path | None = None) -> tuple[list[Path], dict[str, int | str | None]]:
    """Return CSV paths under ``root`` and discovery stats (VIC blocklist skips)."""
    base = root if root is not None else CATAN_RESULTS
    bl_path = vic_exclusion_csv_path()
    bl_rel: str | None = None
    if bl_path:
        try:
            bl_rel = str(bl_path.relative_to(REPO_ROOT))
        except ValueError:
            bl_rel = str(bl_path)

    excl = load_vic_site_exclusions()
    empty_meta: dict[str, int | str | None] = {
        "vic_blocklist_file": bl_rel,
        "vic_blocklist_uids": len(excl),
        "skipped_vic_blocklist": 0,
        "scanned_folders": 0,
    }
    if not base.is_dir():
        return [], empty_meta

    all_paths = sorted(base.glob(f"*/{CSV_NAME}"))
    skipped = 0
    if excl:
        paths: list[Path] = []
        for p in all_paths:
            hid = p.parent.name.lower().replace("-", "")
            if hid in excl:
                skipped += 1
            else:
                paths.append(p)
    else:
        paths = list(all_paths)

    meta: dict[str, int | str | None] = {
        "vic_blocklist_file": bl_rel,
        "vic_blocklist_uids": len(excl),
        "skipped_vic_blocklist": skipped,
        "scanned_folders": len(all_paths),
    }
    return paths, meta


def discover_csv_paths(root: Path | None = None) -> list[Path]:
    paths, _meta = discover_csv_paths_with_meta(root)
    return paths


def catan_source_catalog() -> list[dict]:
    """For index template: each source id, label, repo-relative path, file count."""
    rows: list[dict] = []
    for sid, label, root in catan_sources():
        try:
            rel = str(root.relative_to(REPO_ROOT))
        except ValueError:
            rel = str(root)
        rows.append(
            {
                "id": sid,
                "label": label,
                "path": rel,
                "n_files": len(discover_csv_paths(root)),
                "exists": root.is_dir(),
            }
        )
    return rows


def default_catan_source(catalog: list[dict]) -> str:
    """Prefer full fleet when it has data; otherwise first non-empty source."""
    for s in catalog:
        if s["id"] == CATAN_SOURCE_WITH_VIC and s["n_files"] > 0:
            return CATAN_SOURCE_WITH_VIC
    for s in catalog:
        if s["n_files"] > 0:
            return str(s["id"])
    return CATAN_SOURCE_WITH_VIC


def vic_blocklist_summary() -> dict[str, bool | int | str | None]:
    """Template + UI: whether ``VIC SITES`` file exists and how many UIDs it lists."""
    path = vic_exclusion_csv_path()
    excl = load_vic_site_exclusions()
    rel: str | None = None
    if path:
        try:
            rel = str(path.relative_to(REPO_ROOT))
        except ValueError:
            rel = str(path)
    return {"active": path is not None, "file": rel, "n_uids": len(excl)}


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
            "house_id must equal the optimisation export subfolder name (UUID) under catan_results or catan_no_vic. "
            "postcode is the POA join key "
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
    "margin_pct_revenue",
    "margin_pct_activity",
    "net_margin",
    "gross_cost_no_trade",
    "gross_revenue_no_trade",
    "margin_pct_revenue_no_trade",
    "margin_pct_activity_no_trade",
    "net_margin_no_trade",
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
    skip_keys = frozenset(
        {
            "monthly_rows",
            "monthly_net_margin",
            "monthly_weighted_prices",
            "hourly_gross_cost",
            "hourly_gross_revenue",
            "monthly_gross_cost",
            "monthly_gross_revenue",
            "daily_weighted_prices",
            "slot_gross_cost",
            "slot_gross_revenue",
        }
    )
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


def summarize_csv(csv_path: Path, cache_key: str) -> dict:
    stat = csv_path.stat()
    cache_sub = CACHE_DIR / cache_key
    cache_sub.mkdir(parents=True, exist_ok=True)
    cache_path = cache_sub / f"{csv_path.parent.name}.json"
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
    gross_cost_nt = 0.0
    gross_revenue_nt = 0.0
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
    monthly_imp_price_num: dict[str, float] = {}
    monthly_imp_kwh: dict[str, float] = {}
    monthly_exp_price_num: dict[str, float] = {}
    monthly_exp_kwh: dict[str, float] = {}
    daily_imp_price_num: dict[str, float] = {}
    daily_imp_kwh: dict[str, float] = {}
    daily_exp_price_num: dict[str, float] = {}
    daily_exp_kwh: dict[str, float] = {}
    monthly_gross_cost: dict[str, float] = {}
    monthly_gross_revenue: dict[str, float] = {}
    hourly_gross_cost = [0.0] * 24
    hourly_gross_revenue = [0.0] * 24
    slot_gross_cost = [0.0] * CLOCK_SLOTS_PER_DAY
    slot_gross_revenue = [0.0] * CLOCK_SLOTS_PER_DAY
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
        idx_net_nt: int | None = None
        for _nm in _NO_TRADE_NET_COST_HEADER_NAMES:
            if _nm in col:
                idx_net_nt = col[_nm]
                break

        for row in reader:
            n_rows += 1
            v = _f(row, idx_net)
            ts_raw = _s(row, idx_ts)
            import_price = _f(row, idx_import_price)
            export_price = _f(row, idx_export_price)
            import_kw = _f(row, idx_import_kw)
            export_kw = _f(row, idx_export_kw)
            load_kw = _f(row, idx_load)
            solar_kw = _f(row, idx_solar)
            charge_kw = _f(row, idx_ch)
            discharge_kw = _f(row, idx_dis)
            if len(ts_raw) >= 10:
                date_key = ts_raw[:10]  # ISO date prefix: YYYY-MM-DD (lexicographically sortable)
                month_key = ts_raw[:7]  # ISO month prefix: YYYY-MM
                if not start_date or date_key < start_date:
                    start_date = date_key
                if not end_date or date_key > end_date:
                    end_date = date_key
                monthly_rows[month_key] = monthly_rows.get(month_key, 0) + 1
                monthly_net_margin[month_key] = monthly_net_margin.get(month_key, 0.0) + (-v)
                dk_i = max(import_kw, 0.0) * INTERVAL_HOURS
                dk_e = max(export_kw, 0.0) * INTERVAL_HOURS
                monthly_imp_price_num[month_key] = monthly_imp_price_num.get(month_key, 0.0) + import_price * dk_i
                monthly_imp_kwh[month_key] = monthly_imp_kwh.get(month_key, 0.0) + dk_i
                monthly_exp_price_num[month_key] = monthly_exp_price_num.get(month_key, 0.0) + export_price * dk_e
                monthly_exp_kwh[month_key] = monthly_exp_kwh.get(month_key, 0.0) + dk_e
                daily_imp_price_num[date_key] = daily_imp_price_num.get(date_key, 0.0) + import_price * dk_i
                daily_imp_kwh[date_key] = daily_imp_kwh.get(date_key, 0.0) + dk_i
                daily_exp_price_num[date_key] = daily_exp_price_num.get(date_key, 0.0) + export_price * dk_e
                daily_exp_kwh[date_key] = daily_exp_kwh.get(date_key, 0.0) + dk_e
                if v > 0:
                    monthly_gross_cost[month_key] = monthly_gross_cost.get(month_key, 0.0) + v
                elif v < 0:
                    monthly_gross_revenue[month_key] = monthly_gross_revenue.get(month_key, 0.0) + (-v)
            hour = int(ts_raw[11:13]) if len(ts_raw) >= 13 and ts_raw[11:13].isdigit() else -1
            if 0 <= hour < 24:
                if v > 0:
                    hourly_gross_cost[hour] += v
                elif v < 0:
                    hourly_gross_revenue[hour] += -v
            slot = clock_slot_5min_from_timestamp(ts_raw)
            if slot is not None:
                if v > 0:
                    slot_gross_cost[slot] += v
                elif v < 0:
                    slot_gross_revenue[slot] += -v

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
            if idx_net_nt is not None:
                vnt = _f(row, idx_net_nt)
                if vnt > 0:
                    gross_cost_nt += vnt
                elif vnt < 0:
                    gross_revenue_nt += -vnt
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
        "margin_pct_revenue": None if margin_pct_revenue is None else round(margin_pct_revenue, 4),
        "margin_pct_activity": None if margin_pct_activity is None else round(margin_pct_activity, 4),
        "net_margin": round(net_margin, 6),
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
        "hourly_gross_cost": [round(x, 6) for x in hourly_gross_cost],
        "hourly_gross_revenue": [round(x, 6) for x in hourly_gross_revenue],
        "slot_gross_cost": [round(x, 6) for x in slot_gross_cost],
        "slot_gross_revenue": [round(x, 6) for x in slot_gross_revenue],
        "monthly_gross_cost": {k: round(vv, 6) for k, vv in monthly_gross_cost.items()},
        "monthly_gross_revenue": {k: round(vv, 6) for k, vv in monthly_gross_revenue.items()},
    }
    if idx_net_nt is not None:
        net_margin_nt = gross_revenue_nt - gross_cost_nt
        activity_nt = gross_revenue_nt + gross_cost_nt
        mpr_nt = (net_margin_nt / gross_revenue_nt * 100.0) if gross_revenue_nt > 0 else None
        mpa_nt = (net_margin_nt / activity_nt * 100.0) if activity_nt > 0 else None
        result["gross_cost_no_trade"] = round(gross_cost_nt, 6)
        result["gross_revenue_no_trade"] = round(gross_revenue_nt, 6)
        result["net_margin_no_trade"] = round(net_margin_nt, 6)
        result["margin_pct_revenue_no_trade"] = None if mpr_nt is None else round(mpr_nt, 4)
        result["margin_pct_activity_no_trade"] = None if mpa_nt is None else round(mpa_nt, 4)

    month_keys_wp = sorted(set(monthly_imp_kwh.keys()) | set(monthly_exp_kwh.keys()))
    monthly_weighted_prices: dict[str, dict[str, float]] = {}
    for mk in month_keys_wp:
        monthly_weighted_prices[mk] = {
            "import_num": round(monthly_imp_price_num.get(mk, 0.0), 6),
            "import_kwh": round(monthly_imp_kwh.get(mk, 0.0), 6),
            "export_num": round(monthly_exp_price_num.get(mk, 0.0), 6),
            "export_kwh": round(monthly_exp_kwh.get(mk, 0.0), 6),
        }
    result["monthly_weighted_prices"] = monthly_weighted_prices
    day_keys_wp = sorted(set(daily_imp_kwh.keys()) | set(daily_exp_kwh.keys()))
    daily_weighted_prices: dict[str, dict[str, float]] = {}
    for dk in day_keys_wp:
        daily_weighted_prices[dk] = {
            "import_num": round(daily_imp_price_num.get(dk, 0.0), 6),
            "import_kwh": round(daily_imp_kwh.get(dk, 0.0), 6),
            "export_num": round(daily_exp_price_num.get(dk, 0.0), 6),
            "export_kwh": round(daily_exp_kwh.get(dk, 0.0), 6),
        }
    result["daily_weighted_prices"] = daily_weighted_prices
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


def billing_months_count(h: dict) -> int:
    """
    Months used to spread a lump-sum fee. Prefer months that actually have interval rows
    (``monthly_net_margin`` keys); else inclusive calendar months from start_date to end_date; else 1.
    """
    mnm = h.get("monthly_net_margin")
    if isinstance(mnm, dict) and mnm:
        return max(1, len(mnm))
    sd = (h.get("start_date") or "")[:10]
    ed = (h.get("end_date") or "")[:10]
    if len(sd) == 10 and len(ed) == 10:
        try:
            d0 = date.fromisoformat(sd)
            d1 = date.fromisoformat(ed)
            if d1 < d0:
                d0, d1 = d1, d0
            return max(1, (d1.year - d0.year) * 12 + (d1.month - d0.month) + 1)
        except ValueError:
            pass
    return 1


def headline_margin_analysis(
    ok_houses: list[dict],
    target_margin_pct: float,
    basis: str,
) -> dict:
    """
    Split fleet by whether each home already meets a headline margin %, using the same
    definitions as ``summarize_csv``: margin % of *activity* (gross_cost + gross_revenue)
    or of *gross_revenue* only. For homes below the hurdle, compute the lump-sum dollar
    gap to reach that margin on the same denominator, then an equal monthly amount over
    ``billing_months_count`` (modelling a recurring fee or uplift that accrues dollar-for-dollar
    to ``net_margin`` over the optimisation window).
    """
    basis_key = (basis or "activity").lower().strip()
    if basis_key not in ("activity", "revenue"):
        basis_key = "activity"

    achievers: list[dict] = []
    shortfall_homes: list[dict] = []
    ineligible: list[dict] = []

    for h in ok_houses:
        house_id = str(h.get("house_id") or "")
        path = str(h.get("path") or "")
        gc = float(h.get("gross_cost") or 0.0)
        gr = float(h.get("gross_revenue") or 0.0)
        activity = gc + gr
        nm = float(h.get("net_margin") or 0.0)
        mp_act = h.get("margin_pct_activity")
        mp_rev = h.get("margin_pct_revenue")

        if basis_key == "activity":
            if activity <= 0.0 or mp_act is None:
                ineligible.append(
                    {
                        "house_id": house_id,
                        "path": path,
                        "reason": "zero_or_missing_activity_or_margin_pct_activity",
                    }
                )
                continue
            current_pct = float(mp_act)
            denom = activity
        else:
            if gr <= 0.0 or mp_rev is None:
                ineligible.append(
                    {
                        "house_id": house_id,
                        "path": path,
                        "reason": "zero_or_missing_revenue_or_margin_pct_revenue",
                    }
                )
                continue
            current_pct = float(mp_rev)
            denom = gr

        net_margin_required = (target_margin_pct / 100.0) * denom
        shortfall_total = max(0.0, net_margin_required - nm)
        n_mo = billing_months_count(h)
        monthly_fee = shortfall_total / float(n_mo)

        batt_soc = h.get("max_battery_soc_kwh")
        del_mult = h.get("delivery_multiplier")
        base_row = {
            "house_id": house_id,
            "path": path,
            "gross_cost": round(gc, 6),
            "gross_revenue": round(gr, 6),
            "activity": round(activity, 6),
            "net_margin": round(nm, 6),
            "margin_pct_activity": None if mp_act is None else round(float(mp_act), 4),
            "margin_pct_revenue": None if mp_rev is None else round(float(mp_rev), 4),
            "current_margin_pct": round(current_pct, 4),
            "net_margin_required_for_hurdle": round(net_margin_required, 6),
            "shortfall_total": round(shortfall_total, 6),
            "n_billing_months": n_mo,
            "monthly_fee": round(monthly_fee, 6),
            "max_battery_soc_kwh": None if batt_soc is None else round(float(batt_soc), 2),
            "delivery_multiplier": None if del_mult is None else round(float(del_mult), 4),
        }

        if current_pct + 1e-9 >= target_margin_pct:
            achievers.append(base_row)
        else:
            shortfall_homes.append(base_row)

    shortfall_homes.sort(key=lambda r: r["monthly_fee"], reverse=True)

    sum_shortfall = sum(r["shortfall_total"] for r in shortfall_homes)
    sum_monthly = sum(r["monthly_fee"] for r in shortfall_homes)

    basis_human = (
        "net margin ÷ (gross grid charges + gross feed-in credits)"
        if basis_key == "activity"
        else "net margin ÷ gross feed-in credits"
    )

    return {
        "basis": basis_key,
        "basis_description": basis_human,
        "target_margin_pct": round(target_margin_pct, 4),
        "n_homes_input": len(ok_houses),
        "n_natural_achievers": len(achievers),
        "n_below_hurdle": len(shortfall_homes),
        "n_ineligible": len(ineligible),
        "achievers": achievers,
        "shortfall_homes": shortfall_homes,
        "ineligible": ineligible,
        "totals": {
            "shortfall_dollars_sum": round(sum_shortfall, 4),
            "monthly_fee_sum_across_shortfall_homes": round(sum_monthly, 4),
        },
        "note": (
            "Monthly fee spreads each home's dollar gap evenly over months with interval data "
            "(or inclusive calendar months from start_date–end_date). It is the amount that, "
            "if recognised as net margin each month in the same way as the CSV net_margin, "
            "would lift the home to the headline % on the chosen basis."
        ),
    }


def _load_fleet_delivery_rates() -> dict[str, Any]:
    """
    Load the prediction accuracy deep dive from the batch comparison JSON and return
    the pricing haircut data: fleet-wide multiplier and per-battery-segment multipliers.
    Returns empty dict with ``available: False`` if data is missing.
    """
    if not COMPARISON_BATCH_JSON.is_file():
        return {"available": False}
    try:
        batch = json.loads(COMPARISON_BATCH_JSON.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {"available": False}
    if not isinstance(batch, dict) or len(batch) < 5:
        return {"available": False}
    batch = augment_batch_with_msc_counterfactual(batch)
    dive = prediction_accuracy_deep_dive(batch)
    hc = dive.get("pricing_haircut", {})
    if not hc.get("available"):
        return {"available": False}
    return {
        "available": True,
        "fleet_multiplier": hc["fleet_multiplier"],
        "fleet_p25_mult": hc["fleet_p25_mult"],
        "fleet_p75_mult": hc["fleet_p75_mult"],
        "by_battery": hc.get("by_battery", []),
        "delivery_rate": dive.get("delivery_rate", {}),
        "n_homes_in_model": dive.get("n_homes", 0),
        "sign_flip": dive.get("sign_flip", {}),
    }


def _battery_multiplier_for_home(
    h: dict, by_battery: list[dict], fleet_mult: float
) -> float:
    """
    Find the best-matching battery-segment delivery multiplier for a home.
    Falls back to fleet-wide multiplier if no battery info available.
    """
    batt_kwh = float(h.get("max_battery_soc_kwh") or 0.0)
    if batt_kwh <= 0 or not by_battery:
        return fleet_mult
    for seg in by_battery:
        lo = seg.get("range_lo", 0)
        hi = seg.get("range_hi", 999999)
        if lo is None:
            lo = 0
        if hi is None:
            hi = 999999
        if lo <= batt_kwh <= hi:
            return seg.get("multiplier", fleet_mult)
    return fleet_mult


def reality_adjusted_houses(
    ok_houses: list[dict], delivery_info: dict[str, Any]
) -> list[dict]:
    """
    Create copies of each home's summary with economics scaled to expected actuals.
    Adjusts ``net_margin``, ``gross_cost``, ``gross_revenue``, and recomputes margins.
    The delivery multiplier is applied to ``net_margin`` directly (it represents the
    fraction of predicted profit actually captured; negative means the home loses money).
    """
    if not delivery_info.get("available"):
        return ok_houses
    fleet_mult = delivery_info["fleet_multiplier"]
    by_batt = delivery_info.get("by_battery", [])
    batt_segments: list[dict] = []
    for seg in by_batt:
        lo = seg.get("range_lo")
        hi = seg.get("range_hi")
        if lo is not None and hi is not None:
            batt_segments.append({"range_lo": float(lo), "range_hi": float(hi), "multiplier": seg["multiplier"]})

    adjusted: list[dict] = []
    for h in ok_houses:
        adj = dict(h)
        nm = float(h.get("net_margin") or 0.0)
        gc = float(h.get("gross_cost") or 0.0)
        gr = float(h.get("gross_revenue") or 0.0)
        activity = gc + gr

        mult = _battery_multiplier_for_home(h, batt_segments, fleet_mult)

        # Net margin scaled: if model predicts $500 profit and mult is -0.5,
        # expected actual is -$250 (loss).
        adj_nm = nm * mult if nm > 0 else nm
        adj["net_margin"] = round(adj_nm, 6)
        adj["delivery_multiplier"] = round(mult, 4)

        if activity > 0:
            adj["margin_pct_activity"] = round(adj_nm / activity * 100, 4)
        if gr > 0:
            adj["margin_pct_revenue"] = round(adj_nm / gr * 100, 4)
        adjusted.append(adj)
    return adjusted


QUOTE_UPLOAD_ROOT = CACHE_DIR / "solax_quote_uploads"
OPTIMISER_INTERVALS_HEADER = [
    "interval",
    "timestamp",
    "import_price",
    "export_price",
    "grid_import_kw",
    "grid_export_kw",
    "house_load_kw",
    "solar_output_kw",
    "battery_charge_kw",
    "battery_discharge_kw",
    "battery_soc_kwh",
    "net_cost",
]


def _norm_csv_header(s: str) -> str:
    t = (s or "").strip().lower()
    t = re.sub(r"\s+", "_", t)
    return t


def _flex_pick_col(by_norm: dict[str, int], candidates: tuple[str, ...]) -> int | None:
    for c in candidates:
        k = _norm_csv_header(c)
        if k in by_norm:
            return by_norm[k]
    return None


def flexible_usage_csv_to_optimiser_csv(
    src: Path,
    dest: Path,
    default_import_price: float,
    default_export_price: float,
) -> dict[str, Any]:
    """
    Map a usage-style CSV (e.g. Solax export) into ``optimisation_intervals.csv`` layout
    expected by ``summarize_csv``. If ``net_cost`` is absent, approximate grid cashflow as
    ``import_price * import_kwh - export_price * export_kwh`` per row (battery not priced).
    """
    warnings: list[str] = []
    with src.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return {"ok": False, "error": "empty_file", "warnings": warnings}
        by_norm = {_norm_csv_header(h): i for i, h in enumerate(header)}

        def col(*names: str) -> int | None:
            return _flex_pick_col(by_norm, names)

        idx_ts = col("timestamp", "time", "datetime", "date_time", "local_time", "date")
        if idx_ts is None:
            return {"ok": False, "error": "missing_timestamp_column", "warnings": warnings}

        idx_net = col("net_cost", "net_energy_cost", "interval_cost")
        idx_imp_kw = col(
            "grid_import_kw",
            "import_kw",
            "grid_import",
            "import_power_kw",
            "mains_import_kw",
        )
        idx_imp_kwh = col("import_kwh", "grid_import_kwh", "import_energy_kwh")
        idx_exp_kw = col(
            "grid_export_kw",
            "export_kw",
            "export_power_kw",
            "feed_in_kw",
            "feedin_kw",
        )
        idx_exp_kwh = col("export_kwh", "grid_export_kwh", "export_energy_kwh", "feed_in_kwh")
        idx_pv_kw = col("solar_output_kw", "pv_kw", "solar_kw", "generation_kw", "pv_power_kw")
        idx_load_kw = col("house_load_kw", "load_kw", "consumption_kw", "house_consumption_kw")
        idx_ip = col("import_price", "buy_price", "retail_price", "import_tariff")
        idx_ep = col("export_price", "feed_in_price", "sell_price", "export_tariff", "fit")
        idx_bch = col("battery_charge_kw", "batt_charge_kw")
        idx_bdis = col("battery_discharge_kw", "batt_discharge_kw")
        idx_soc = col("battery_soc_kwh", "soc_kwh", "battery_soc", "batt_soc_kwh")

        if idx_net is None and idx_imp_kw is None and idx_imp_kwh is None:
            return {"ok": False, "error": "need_net_cost_or_import_energy", "warnings": warnings}

        if idx_net is None:
            warnings.append(
                "net_cost was computed as import_price*import_kwh - export_price*export_kwh; "
                "battery arbitrage not included — prefer optimiser CSV with net_cost when available."
            )

        dest.parent.mkdir(parents=True, exist_ok=True)
        dt = INTERVAL_HOURS
        n_out = 0
        with dest.open("w", newline="", encoding="utf-8") as out:
            w = csv.writer(out)
            w.writerow(OPTIMISER_INTERVALS_HEADER)
            for row in reader:
                if not row or all(not (c or "").strip() for c in row):
                    continue
                def cell(i: int | None) -> str:
                    if i is None or i >= len(row):
                        return ""
                    return row[i]

                ts = cell(idx_ts).strip()
                if len(ts) < 10:
                    continue

                ip = float(cell(idx_ip) or default_import_price) if idx_ip is not None else default_import_price
                ep = float(cell(idx_ep) or default_export_price) if idx_ep is not None else default_export_price

                if idx_imp_kw is not None and cell(idx_imp_kw).strip():
                    imp_kw = float(cell(idx_imp_kw) or 0.0)
                elif idx_imp_kwh is not None and cell(idx_imp_kwh).strip():
                    imp_kw = float(cell(idx_imp_kwh) or 0.0) / dt
                else:
                    imp_kw = 0.0

                if idx_exp_kw is not None and cell(idx_exp_kw).strip():
                    exp_kw = float(cell(idx_exp_kw) or 0.0)
                elif idx_exp_kwh is not None and cell(idx_exp_kwh).strip():
                    exp_kw = float(cell(idx_exp_kwh) or 0.0) / dt
                else:
                    exp_kw = 0.0

                load_kw = float(cell(idx_load_kw) or 0.0) if idx_load_kw is not None else 0.0
                solar_kw = float(cell(idx_pv_kw) or 0.0) if idx_pv_kw is not None else 0.0
                bch = float(cell(idx_bch) or 0.0) if idx_bch is not None else 0.0
                bdis = float(cell(idx_bdis) or 0.0) if idx_bdis is not None else 0.0
                soc = float(cell(idx_soc) or 0.0) if idx_soc is not None else 0.0

                if idx_net is not None and cell(idx_net).strip() != "":
                    try:
                        net_cost = float(cell(idx_net))
                    except ValueError:
                        net_cost = 0.0
                else:
                    imp_kwh = max(imp_kw, 0.0) * dt
                    exp_kwh = max(exp_kw, 0.0) * dt
                    net_cost = ip * imp_kwh - ep * exp_kwh

                w.writerow(
                    [
                        n_out,
                        ts,
                        ip,
                        ep,
                        imp_kw,
                        exp_kw,
                        load_kw,
                        solar_kw,
                        bch,
                        bdis,
                        soc,
                        net_cost,
                    ]
                )
                n_out += 1

    if n_out == 0:
        return {"ok": False, "error": "no_data_rows", "warnings": warnings}
    return {"ok": True, "n_rows": n_out, "warnings": warnings}


def _implied_monthly_fee_for_home(analysis: dict[str, Any], house_id: str) -> float | None:
    for r in analysis.get("shortfall_homes") or []:
        if r.get("house_id") == house_id:
            return float(r.get("monthly_fee") or 0.0)
    for r in analysis.get("achievers") or []:
        if r.get("house_id") == house_id:
            return 0.0
    return None


def single_home_margin_quote(
    house: dict[str, Any],
    target_margin_pct: float,
    basis: str,
    delivery_info: dict[str, Any] | None = None,
    battery_kwh_override: float | None = None,
) -> dict[str, Any]:
    """
    Run simulated + reality-adjusted headline margin on a single summarised home dict
    (as returned by ``summarize_csv``).
    """
    h = dict(house)
    if battery_kwh_override is not None and battery_kwh_override > 0:
        h["max_battery_soc_kwh"] = float(battery_kwh_override)

    if delivery_info is None:
        delivery_info = _load_fleet_delivery_rates()

    sim = headline_margin_analysis([h], target_margin_pct, basis)
    adj_houses = reality_adjusted_houses([h], delivery_info)
    adj = headline_margin_analysis(adj_houses, target_margin_pct, basis)

    hid = str(h.get("house_id") or "")
    adj_summary: dict[str, Any] = {"available": delivery_info.get("available", False)}
    if delivery_info.get("available"):
        adj_summary.update({
            "fleet_multiplier": delivery_info.get("fleet_multiplier"),
            "fleet_p25_mult": delivery_info.get("fleet_p25_mult"),
            "fleet_p75_mult": delivery_info.get("fleet_p75_mult"),
            "sign_flip": delivery_info.get("sign_flip", {}),
        })

    adj0 = adj_houses[0] if adj_houses else {}
    return {
        "house_id": hid,
        "target_margin_pct": target_margin_pct,
        "basis": basis,
        "summary_sim": {
            "net_margin": h.get("net_margin"),
            "gross_cost": h.get("gross_cost"),
            "gross_revenue": h.get("gross_revenue"),
            "margin_pct_activity": h.get("margin_pct_activity"),
            "margin_pct_revenue": h.get("margin_pct_revenue"),
            "max_battery_soc_kwh": h.get("max_battery_soc_kwh"),
            "n_billing_months": billing_months_count(h),
            "start_date": h.get("start_date"),
            "end_date": h.get("end_date"),
        },
        "summary_adjusted": {
            "net_margin": adj0.get("net_margin"),
            "margin_pct_activity": adj0.get("margin_pct_activity"),
            "margin_pct_revenue": adj0.get("margin_pct_revenue"),
        },
        "headline_sim": sim,
        "headline_adjusted": adj,
        "reality_adjusted_meta": adj_summary,
        "implied_monthly_fee_sim": _implied_monthly_fee_for_home(sim, hid),
        "implied_monthly_fee_adjusted": _implied_monthly_fee_for_home(adj, hid),
        "delivery_multiplier": adj0.get("delivery_multiplier"),
    }


def fleet_aggregate_price_curve(ok_houses: list[dict]) -> dict:
    """
    Pool all optimisation intervals across homes: per calendar month and per calendar day,
    volume-weighted mean import price ($/kWh on import energy) and export price ($/kWh on export energy).
    """
    agg_in_num: dict[str, float] = defaultdict(float)
    agg_in_den: dict[str, float] = defaultdict(float)
    agg_ex_num: dict[str, float] = defaultdict(float)
    agg_ex_den: dict[str, float] = defaultdict(float)
    n_with_monthly = 0
    for h in ok_houses:
        mw = h.get("monthly_weighted_prices")
        if not isinstance(mw, dict) or not mw:
            continue
        n_with_monthly += 1
        for mk, b in mw.items():
            if not isinstance(b, dict):
                continue
            agg_in_num[mk] += float(b.get("import_num") or 0.0)
            agg_in_den[mk] += float(b.get("import_kwh") or 0.0)
            agg_ex_num[mk] += float(b.get("export_num") or 0.0)
            agg_ex_den[mk] += float(b.get("export_kwh") or 0.0)
    months_sorted = sorted(set(agg_in_num.keys()) | set(agg_ex_num.keys()))
    points: list[dict] = []
    for mk in months_sorted:
        iden = agg_in_den[mk]
        eden = agg_ex_den[mk]
        ip = (agg_in_num[mk] / iden) if iden > 0 else None
        ep = (agg_ex_num[mk] / eden) if eden > 0 else None
        points.append(
            {
                "month": mk,
                "avg_import_price": None if ip is None else round(ip, 6),
                "avg_export_price": None if ep is None else round(ep, 6),
                "fleet_import_kwh": round(iden, 4),
                "fleet_export_kwh": round(eden, 4),
            }
        )

    d_in_num: dict[str, float] = defaultdict(float)
    d_in_den: dict[str, float] = defaultdict(float)
    d_ex_num: dict[str, float] = defaultdict(float)
    d_ex_den: dict[str, float] = defaultdict(float)
    n_with_daily = 0
    for h in ok_houses:
        dw = h.get("daily_weighted_prices")
        if not isinstance(dw, dict) or not dw:
            continue
        n_with_daily += 1
        for dk, b in dw.items():
            if not isinstance(b, dict):
                continue
            d_in_num[dk] += float(b.get("import_num") or 0.0)
            d_in_den[dk] += float(b.get("import_kwh") or 0.0)
            d_ex_num[dk] += float(b.get("export_num") or 0.0)
            d_ex_den[dk] += float(b.get("export_kwh") or 0.0)
    days_sorted = sorted(set(d_in_num.keys()) | set(d_ex_num.keys()))
    day_points: list[dict] = []
    for dk in days_sorted:
        iden_d = d_in_den[dk]
        eden_d = d_ex_den[dk]
        ipd = (d_in_num[dk] / iden_d) if iden_d > 0 else None
        epd = (d_ex_num[dk] / eden_d) if eden_d > 0 else None
        day_points.append(
            {
                "day": dk,
                "avg_import_price": None if ipd is None else round(ipd, 6),
                "avg_export_price": None if epd is None else round(epd, 6),
                "fleet_import_kwh": round(iden_d, 4),
                "fleet_export_kwh": round(eden_d, 4),
            }
        )

    return {
        "months": points,
        "days": day_points,
        "n_homes": len(ok_houses),
        "n_homes_with_monthly_prices": n_with_monthly,
        "n_homes_with_daily_prices": n_with_daily,
        "period_start": months_sorted[0] if months_sorted else None,
        "period_end": months_sorted[-1] if months_sorted else None,
        "daily_period_start": days_sorted[0] if days_sorted else None,
        "daily_period_end": days_sorted[-1] if days_sorted else None,
    }


def merge_trade_timing_cohort(houses: list[dict]) -> dict:
    """
    Pool all 5-minute intervals in a cohort: sums of positive net_cost (grid charges) and
    of feed-in credits (-net_cost when negative), by clock hour (0-23), by 5-minute wall-clock
    slot (288 bins), and calendar month (YYYY-MM).
    """
    hc = [0.0] * 24
    hr = [0.0] * 24
    sc = [0.0] * CLOCK_SLOTS_PER_DAY
    sr = [0.0] * CLOCK_SLOTS_PER_DAY
    mc: dict[str, float] = defaultdict(float)
    mr: dict[str, float] = defaultdict(float)
    for h in houses:
        hhc = h.get("hourly_gross_cost")
        if isinstance(hhc, list) and len(hhc) == 24:
            for i in range(24):
                hc[i] += float(hhc[i] or 0.0)
        hhr = h.get("hourly_gross_revenue")
        if isinstance(hhr, list) and len(hhr) == 24:
            for i in range(24):
                hr[i] += float(hhr[i] or 0.0)
        sgc = h.get("slot_gross_cost")
        if isinstance(sgc, list) and len(sgc) == CLOCK_SLOTS_PER_DAY:
            for i in range(CLOCK_SLOTS_PER_DAY):
                sc[i] += float(sgc[i] or 0.0)
        sgr = h.get("slot_gross_revenue")
        if isinstance(sgr, list) and len(sgr) == CLOCK_SLOTS_PER_DAY:
            for i in range(CLOCK_SLOTS_PER_DAY):
                sr[i] += float(sgr[i] or 0.0)
        mgc = h.get("monthly_gross_cost")
        if isinstance(mgc, dict):
            for k, val in mgc.items():
                mc[k] += float(val or 0.0)
        mgr = h.get("monthly_gross_revenue")
        if isinstance(mgr, dict):
            for k, val in mgr.items():
                mr[k] += float(val or 0.0)
    sum_c = sum(hc)
    sum_r = sum(hr)
    by_hour: list[dict] = []
    for hour in range(24):
        cost_share = (hc[hour] / sum_c * 100.0) if sum_c > 0 else None
        rev_share = (hr[hour] / sum_r * 100.0) if sum_r > 0 else None
        by_hour.append(
            {
                "hour": hour,
                "gross_cost": round(hc[hour], 4),
                "gross_revenue": round(hr[hour], 4),
                "gross_cost_share_pct": None if cost_share is None else round(cost_share, 3),
                "gross_revenue_share_pct": None if rev_share is None else round(rev_share, 3),
            }
        )
    months_sorted = sorted(set(mc.keys()) | set(mr.keys()))
    sum_mc = sum(mc.values())
    sum_mr = sum(mr.values())
    by_month: list[dict] = []
    for mk in months_sorted:
        c = mc.get(mk, 0.0)
        r = mr.get(mk, 0.0)
        by_month.append(
            {
                "month": mk,
                "gross_cost": round(c, 4),
                "gross_revenue": round(r, 4),
                "gross_cost_share_pct": None
                if sum_mc <= 0
                else round(c / sum_mc * 100.0, 3),
                "gross_revenue_share_pct": None
                if sum_mr <= 0
                else round(r / sum_mr * 100.0, 3),
            }
        )
    sum_sc = sum(sc)
    sum_sr = sum(sr)
    by_slot: list[dict] = []
    for i in range(CLOCK_SLOTS_PER_DAY):
        hh = i // 12
        mm = (i % 12) * 5
        time_label = f"{hh:02d}:{mm:02d}"
        c = sc[i]
        r = sr[i]
        cost_share = (c / sum_sc * 100.0) if sum_sc > 0 else None
        rev_share = (r / sum_sr * 100.0) if sum_sr > 0 else None
        by_slot.append(
            {
                "slot": i,
                "time": time_label,
                "gross_cost": round(c, 4),
                "gross_revenue": round(r, 4),
                "gross_cost_share_pct": None if cost_share is None else round(cost_share, 3),
                "gross_revenue_share_pct": None if rev_share is None else round(rev_share, 3),
            }
        )
    return {
        "by_hour": by_hour,
        "by_slot": by_slot,
        "by_month": by_month,
        "total_gross_cost": round(sum_c, 4),
        "total_gross_revenue": round(sum_r, 4),
        "n_homes": len(houses),
    }


def fleet_trade_timing(ok_houses: list[dict]) -> dict:
    loss = [h for h in ok_houses if (h.get("net_margin") or 0.0) < 0]
    profit = [h for h in ok_houses if (h.get("net_margin") or 0.0) >= 0]
    return {
        "loss_makers": merge_trade_timing_cohort(loss),
        "profitable": merge_trade_timing_cohort(profit),
        "all_homes": merge_trade_timing_cohort(ok_houses),
    }


def fleet_means(houses: list[dict]) -> dict:
    if not houses:
        return {}
    n = len(houses)

    def mean(key: str) -> float:
        return sum(h[key] for h in houses) / n

    rev_vals = [h["margin_pct_revenue"] for h in houses if h["margin_pct_revenue"] is not None]
    act_vals = [h["margin_pct_activity"] for h in houses if h["margin_pct_activity"] is not None]
    nt_net = [float(h["net_margin_no_trade"]) for h in houses if h.get("net_margin_no_trade") is not None]
    nt_act = [h["margin_pct_activity_no_trade"] for h in houses if h.get("margin_pct_activity_no_trade") is not None]

    out_fm: dict[str, Any] = {
        "n_homes": n,
        "avg_gross_cost": round(mean("gross_cost"), 6),
        "avg_gross_revenue": round(mean("gross_revenue"), 6),
        "avg_net_margin": round(mean("net_margin"), 6),
        "avg_margin_pct_revenue": None if not rev_vals else round(sum(rev_vals) / len(rev_vals), 4),
        "avg_margin_pct_activity": None if not act_vals else round(sum(act_vals) / len(act_vals), 4),
    }
    if nt_net:
        out_fm["n_homes_with_retail_without_trade_csv"] = len(nt_net)
        out_fm["avg_net_margin_no_trade"] = round(sum(nt_net) / len(nt_net), 6)
        out_fm["avg_margin_pct_activity_no_trade"] = (
            None if not nt_act else round(sum(float(x) for x in nt_act) / len(nt_act), 4)
        )
    return out_fm


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
                    "avg_net_margin_no_trade": None,
                    "avg_margin_pct_activity_no_trade": None,
                }
            )
            continue

        m = len(bucket)
        net_vals = [h["net_margin"] for h in bucket]
        act_vals = [h["margin_pct_activity"] for h in bucket if h["margin_pct_activity"] is not None]
        nt_net_vals = [float(h["net_margin_no_trade"]) for h in bucket if h.get("net_margin_no_trade") is not None]
        nt_act_vals = [
            h["margin_pct_activity_no_trade"] for h in bucket if h.get("margin_pct_activity_no_trade") is not None
        ]
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
                "avg_net_margin_no_trade": None
                if not nt_net_vals
                else round(sum(nt_net_vals) / len(nt_net_vals), 6),
                "avg_margin_pct_activity_no_trade": None
                if not nt_act_vals
                else round(sum(float(x) for x in nt_act_vals) / len(nt_act_vals), 4),
                "avg_max_solar_kw": round(sum(h["max_solar_kw"] for h in bucket) / m, 6),
                "avg_max_battery_soc_kwh": round(sum(h["max_battery_soc_kwh"] for h in bucket) / m, 6),
            }
        )
    return out


def _extract_system_size(rec: dict[str, Any]) -> dict[str, float]:
    """Pull solar kW + battery kWh from ``system_config.inverters``."""
    sc = rec.get("system_config")
    if not isinstance(sc, dict):
        return {"solar_kw": 0.0, "battery_kwh": 0.0}
    invs = sc.get("inverters", [])
    solar = sum(inv.get("solar", {}).get("max_dc_output_power_kw", 0.0) for inv in invs if isinstance(inv, dict))
    batt = sum(inv.get("battery", {}).get("capacity_kwh", 0.0) for inv in invs if isinstance(inv, dict))
    return {"solar_kw": solar, "battery_kwh": batt}


def _margin_pct_activity_from_batch_side(side: dict[str, Any]) -> float | None:
    """
    Headline **activity** margin % from batch economics: net margin ÷ (gross import cost + gross export revenue),
    same basis as ``summarize_csv`` / fleet headline margin (both gross terms treated as positive activity).
    """
    if not isinstance(side, dict):
        return None
    try:
        gi = float(side["gross_import_cost"])
        ge = float(side["gross_export_revenue"])
    except (KeyError, TypeError, ValueError):
        return None
    activity = gi + ge
    if activity <= 0.0:
        return None
    net = ge - gi
    return (net / activity) * 100.0


def _system_size_rank_score(battery_kwh: float, solar_kw: float) -> float:
    """Scalar for equal-count ranking: emphasises both battery and solar (kWh × kW scale)."""
    b = max(float(battery_kwh), 0.0)
    s = max(float(solar_kw), 0.0)
    return (b + 0.25) * (s + 0.25)


def margin_delta_by_system_size_deciles(
    batch: dict[str, Any],
    restrict_uids: frozenset[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Equal-count **system-size deciles**: homes ranked by configured battery × solar (from ``system_config``),
    then split into 10 buckets. Within each, report how **activity margin %** on **actual** bills differs from
    **simulated** (optimised) margin % — i.e. ``actual_margin_pct - simulated_margin_pct`` in percentage points.
    """
    rows: list[dict[str, float | str]] = []
    for uid, rec in batch.items():
        if restrict_uids is not None and uid not in restrict_uids:
            continue
        if not isinstance(rec, dict) or rec.get("error"):
            continue
        opt = rec.get("optimised")
        act = rec.get("actual")
        if not isinstance(opt, dict) or not isinstance(act, dict):
            continue
        m_sim = _margin_pct_activity_from_batch_side(opt)
        m_act = _margin_pct_activity_from_batch_side(act)
        if m_sim is None or m_act is None:
            continue
        nt_side = _batch_no_trade_side(rec)
        m_nt = _margin_pct_activity_from_batch_side(nt_side) if nt_side else None
        msc_side = _batch_msc_side(rec)
        m_msc = _margin_pct_activity_from_batch_side(msc_side) if msc_side else None
        sz = _extract_system_size(rec)
        bk = float(sz["battery_kwh"])
        sk = float(sz["solar_kw"])
        score = _system_size_rank_score(bk, sk)
        row_d: dict[str, float | str] = {
            "house_id": uid,
            "battery_kwh": bk,
            "solar_kw": sk,
            "size_score": score,
            "margin_sim_pct": float(m_sim),
            "margin_act_pct": float(m_act),
            "delta_margin_pp": float(m_act) - float(m_sim),
        }
        if m_nt is not None:
            row_d["margin_nt_pct"] = float(m_nt)
            row_d["delta_act_vs_nt_pp"] = float(m_act) - float(m_nt)
        if m_msc is not None:
            row_d["margin_msc_pct"] = float(m_msc)
            row_d["delta_act_vs_msc_pp"] = float(m_act) - float(m_msc)
        rows.append(row_d)

    if not rows:
        return []

    rows.sort(key=lambda r: float(r["size_score"]))
    n = len(rows)
    buckets: list[list[dict[str, float | str]]] = [[] for _ in range(10)]
    for i, r in enumerate(rows):
        idx = (i * 10) // n
        buckets[idx].append(r)

    out: list[dict[str, Any]] = []
    for dec_i, bucket in enumerate(buckets, start=1):
        if not bucket:
            out.append(
                {
                    "decile": dec_i,
                    "label": f"SD{dec_i}",
                    "n_homes": 0,
                    "size_score_min": None,
                    "size_score_max": None,
                    "avg_battery_kwh": None,
                    "avg_solar_kw": None,
                    "mean_delta_margin_pp": None,
                    "median_delta_margin_pp": None,
                    "mean_margin_sim_pct": None,
                    "mean_margin_act_pct": None,
                    "mean_margin_nt_pct": None,
                    "mean_margin_msc_pct": None,
                    "mean_delta_act_vs_nt_pp": None,
                    "median_delta_act_vs_nt_pp": None,
                    "mean_delta_act_vs_msc_pp": None,
                    "median_delta_act_vs_msc_pp": None,
                }
            )
            continue
        m = len(bucket)
        deltas = sorted(float(r["delta_margin_pp"]) for r in bucket)
        scores = [float(r["size_score"]) for r in bucket]
        nt_margins = sorted(float(r["margin_nt_pct"]) for r in bucket if r.get("margin_nt_pct") is not None)
        d_ant = sorted(float(r["delta_act_vs_nt_pp"]) for r in bucket if r.get("delta_act_vs_nt_pp") is not None)
        msc_margins = sorted(float(r["margin_msc_pct"]) for r in bucket if r.get("margin_msc_pct") is not None)
        d_amsc = sorted(float(r["delta_act_vs_msc_pp"]) for r in bucket if r.get("delta_act_vs_msc_pp") is not None)
        out.append(
            {
                "decile": dec_i,
                "label": f"SD{dec_i}",
                "plain_english": (
                    f"System-size decile {dec_i} of 10: SD1 = smallest (battery+ε)×(solar+ε) rank scores, "
                    f"SD10 = largest. Δ margin = actual % − simulated % on activity basis."
                ),
                "n_homes": m,
                "size_score_min": round(min(scores), 3),
                "size_score_max": round(max(scores), 3),
                "avg_battery_kwh": round(sum(float(r["battery_kwh"]) for r in bucket) / m, 2),
                "avg_solar_kw": round(sum(float(r["solar_kw"]) for r in bucket) / m, 2),
                "mean_delta_margin_pp": round(sum(deltas) / m, 2),
                "median_delta_margin_pp": round(_quantile_linear(deltas, 0.5), 2),
                "mean_margin_sim_pct": round(sum(float(r["margin_sim_pct"]) for r in bucket) / m, 2),
                "mean_margin_act_pct": round(sum(float(r["margin_act_pct"]) for r in bucket) / m, 2),
                "mean_margin_nt_pct": round(sum(nt_margins) / len(nt_margins), 2) if nt_margins else None,
                "mean_margin_msc_pct": round(sum(msc_margins) / len(msc_margins), 2) if msc_margins else None,
                "mean_delta_act_vs_nt_pp": round(sum(d_ant) / len(d_ant), 2) if d_ant else None,
                "median_delta_act_vs_nt_pp": round(_quantile_linear(d_ant, 0.5), 2) if d_ant else None,
                "mean_delta_act_vs_msc_pp": round(sum(d_amsc) / len(d_amsc), 2) if d_amsc else None,
                "median_delta_act_vs_msc_pp": round(_quantile_linear(d_amsc, 0.5), 2) if d_amsc else None,
            }
        )
    return out


_PROFIT_THRESHOLD = 10.0


def _delivery_rate_stats(rows: list[dict[str, float]]) -> dict[str, Any]:
    """
    Compute delivery-rate distribution (actual/simulated, 1.0 = perfect).
    Filters to rows where sim_profit > threshold to avoid divide-by-tiny-number noise.
    """
    eligible = [r for r in rows if r["sim_profit"] > _PROFIT_THRESHOLD]
    if not eligible:
        return {"n": 0}
    rates = [r["act_profit"] / r["sim_profit"] for r in eligible]
    sr = sorted(rates)
    n = len(sr)
    mean_r = sum(rates) / n
    med_r = _quantile_linear(sr, 0.5)
    shortfalls = [(1 - r) * 100 for r in rates]
    n_profit_to_loss = sum(1 for r in eligible if r["act_profit"] < 0)
    n_both_profit = sum(1 for r in eligible if r["act_profit"] > 0)
    return {
        "n": n,
        "mean_delivery_pct": round(mean_r * 100, 1),
        "median_delivery_pct": round(med_r * 100, 1),
        "p10_delivery_pct": round(_quantile_linear(sr, 0.10) * 100, 1),
        "p25_delivery_pct": round(_quantile_linear(sr, 0.25) * 100, 1),
        "p75_delivery_pct": round(_quantile_linear(sr, 0.75) * 100, 1),
        "p90_delivery_pct": round(_quantile_linear(sr, 0.90) * 100, 1),
        "mean_shortfall_pct": round(sum(shortfalls) / n, 1),
        "median_shortfall_pct": round((1 - med_r) * 100, 1),
        "n_profit_to_loss": n_profit_to_loss,
        "pct_profit_to_loss": round(n_profit_to_loss / n * 100, 1),
        "n_both_profit": n_both_profit,
    }


def _prediction_bands(rows: list[dict[str, Any]], n_bands: int = 5) -> list[dict[str, Any]]:
    """
    Bin homes by simulated-profit range; within each band report actual-outcome distribution.
    Returns list sorted by band lower bound.
    """
    eligible = [r for r in rows if r["sim_profit"] > _PROFIT_THRESHOLD]
    if not eligible:
        return []
    eligible.sort(key=lambda r: r["sim_profit"])
    band_size = max(1, len(eligible) // n_bands)
    bands: list[dict[str, Any]] = []
    for i in range(0, len(eligible), band_size):
        chunk = eligible[i : i + band_size]
        if not chunk:
            break
        sims = [float(r["sim_profit"]) for r in chunk]
        acts = [float(r["act_profit"]) for r in chunk]
        sacts = sorted(acts)
        rates = sorted(float(r["act_profit"]) / float(r["sim_profit"]) for r in chunk)
        n_loss = sum(1 for a in acts if a < 0)
        nts = [float(r["nt_profit"]) for r in chunk if r.get("nt_profit") is not None]
        mscs = [float(r["msc_profit"]) for r in chunk if r.get("msc_profit") is not None]
        band: dict[str, Any] = {
            "label": f"${min(sims):,.0f}–${max(sims):,.0f}",
            "sim_lo": round(min(sims), 0),
            "sim_hi": round(max(sims), 0),
            "n": len(chunk),
            "mean_sim": round(sum(sims) / len(chunk), 0),
            "mean_actual": round(sum(acts) / len(chunk), 0),
            "median_actual": round(_quantile_linear(sacts, 0.5), 0),
            "p25_actual": round(_quantile_linear(sacts, 0.25), 0),
            "p75_actual": round(_quantile_linear(sacts, 0.75), 0),
            "median_delivery_pct": round(_quantile_linear(rates, 0.5) * 100, 1),
            "pct_loss": round(n_loss / len(chunk) * 100, 1),
        }
        if nts:
            band["mean_retail_without_trade"] = round(sum(nts) / len(nts), 0)
        if mscs:
            band["mean_msc_counterfactual"] = round(sum(mscs) / len(mscs), 0)
        bands.append(band)
    return bands


def _size_segmented_accuracy(rows: list[dict[str, float]], key: str, n_segments: int = 3) -> list[dict[str, Any]]:
    """
    Divide rows into ``n_segments`` equal groups by a size metric (``key`` in each row dict,
    e.g. ``battery_kwh``). Return delivery-rate stats per segment.
    """
    eligible = [r for r in rows if r.get(key, 0) > 0 and r["sim_profit"] > _PROFIT_THRESHOLD]
    if not eligible:
        return []
    eligible.sort(key=lambda r: r[key])
    seg_size = max(1, len(eligible) // n_segments)
    segments: list[dict[str, Any]] = []
    for i in range(0, len(eligible), seg_size):
        chunk = eligible[i : i + seg_size]
        if not chunk:
            break
        if len(chunk) < max(3, seg_size // 3) and segments:
            chunk = eligible[i - seg_size : len(eligible)]
            segments.pop()
        vals = [r[key] for r in chunk]
        rates = sorted(r["act_profit"] / r["sim_profit"] for r in chunk)
        acts = [r["act_profit"] for r in chunk]
        sims = [r["sim_profit"] for r in chunk]
        n_loss = sum(1 for a in acts if a < 0)
        segments.append({
            "label": f"{min(vals):.1f}–{max(vals):.1f}",
            "range_lo": round(min(vals), 1),
            "range_hi": round(max(vals), 1),
            "n": len(chunk),
            "mean_sim": round(sum(sims) / len(chunk), 0),
            "mean_actual": round(sum(acts) / len(chunk), 0),
            "median_delivery_pct": round(_quantile_linear(rates, 0.5) * 100, 1),
            "mean_delivery_pct": round(sum(rates) / len(rates) * 100, 1),
            "p25_delivery_pct": round(_quantile_linear(rates, 0.25) * 100, 1),
            "p75_delivery_pct": round(_quantile_linear(rates, 0.75) * 100, 1),
            "pct_flip_to_loss": round(n_loss / len(chunk) * 100, 1),
        })
    return segments


def _pricing_haircut(
    delivery: dict[str, Any],
    by_battery: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Synthesise a single pricing-discount-factor object from delivery-rate stats
    and battery-segmented accuracy. Returns multipliers (0..1+ scale) and confidence bands.
    """
    if delivery.get("n", 0) == 0:
        return {"available": False}
    fleet_median = delivery["median_delivery_pct"] / 100.0
    fleet_mean = delivery["mean_delivery_pct"] / 100.0
    fleet_p25 = delivery["p25_delivery_pct"] / 100.0
    fleet_p75 = delivery["p75_delivery_pct"] / 100.0

    by_size: list[dict[str, Any]] = []
    for seg in by_battery:
        by_size.append({
            "label": seg["label"] + " kWh",
            "range_lo": seg.get("range_lo"),
            "range_hi": seg.get("range_hi"),
            "n": seg["n"],
            "multiplier": round(seg["median_delivery_pct"] / 100.0, 3),
            "p25_mult": round(seg["p25_delivery_pct"] / 100.0, 3),
            "p75_mult": round(seg["p75_delivery_pct"] / 100.0, 3),
            "pct_flip_to_loss": seg["pct_flip_to_loss"],
        })

    return {
        "available": True,
        "fleet_multiplier": round(fleet_median, 3),
        "fleet_multiplier_mean": round(fleet_mean, 3),
        "fleet_p25_mult": round(fleet_p25, 3),
        "fleet_p75_mult": round(fleet_p75, 3),
        "by_battery": by_size,
    }


def _optimism_predictor_correlations(
    rows: list[dict[str, float | str]],
    batch: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Correlate delivery rate (actual/sim) with available per-home features from the batch
    (system size, load, solar, export share, VWAPs, etc.). Returns ranked list of features
    by absolute correlation.
    """
    eligible = [r for r in rows if float(r["sim_profit"]) > _PROFIT_THRESHOLD]
    if len(eligible) < 5:
        return []
    rates = [float(r["act_profit"]) / float(r["sim_profit"]) for r in eligible]

    feature_defs: list[tuple[str, str]] = [
        ("battery_kwh", "Battery capacity (kWh)"),
        ("solar_kw", "Solar array size (kW)"),
    ]
    opt_features: list[tuple[str, str]] = [
        ("total_house_load_kwh", "Annual household load (kWh)"),
        ("total_solar_kwh", "Annual solar generation (kWh)"),
        ("total_grid_export_kwh", "Annual grid export (kWh)"),
        ("total_grid_import_kwh", "Annual grid import (kWh)"),
        ("total_battery_charge_kwh", "Battery charge throughput (kWh)"),
        ("export_offset_pct", "Export offset %"),
        ("export_vwap", "Export VWAP ($/kWh)"),
        ("import_vwap", "Import VWAP ($/kWh)"),
    ]

    results: list[dict[str, Any]] = []

    for key, label in feature_defs:
        vals = [float(r.get(key, 0)) for r in eligible]
        if all(v == 0 for v in vals):
            continue
        c = _pearson(vals, rates)
        if c is not None:
            results.append({"feature": key, "label": label, "corr": round(c, 4), "n": len(vals)})

    for key, label in opt_features:
        vals: list[float] = []
        rate_subset: list[float] = []
        for i, r in enumerate(eligible):
            uid = str(r["house_id"])
            rec = batch.get(uid, {})
            opt = rec.get("optimised", {}) if isinstance(rec, dict) else {}
            if not isinstance(opt, dict):
                continue
            try:
                v = float(opt[key])
            except (KeyError, TypeError, ValueError):
                continue
            vals.append(v)
            rate_subset.append(rates[i])
        if len(vals) < 5:
            continue
        c = _pearson(vals, rate_subset)
        if c is not None:
            results.append({"feature": key, "label": label, "corr": round(c, 4), "n": len(vals)})

    results.sort(key=lambda r: abs(r["corr"]), reverse=True)
    return results


def _quantile_linear(sorted_xs: list[float], q: float) -> float:
    """Linear interpolation quantile; ``q`` in [0, 1]. ``sorted_xs`` must be sorted ascending."""
    if not sorted_xs:
        return float("nan")
    n = len(sorted_xs)
    if n == 1:
        return sorted_xs[0]
    pos = (n - 1) * max(0.0, min(1.0, q))
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_xs[lo]
    return sorted_xs[lo] * (hi - pos) + sorted_xs[hi] * (pos - lo)


def _size_decile_bubbles_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        b: dict[str, Any] = {
            "house_id": str(r["house_id"]),
            "sim_profit": round(float(r["sim_profit"]), 2),
            "act_profit": round(float(r["act_profit"]), 2),
            "size_decile": int(r["size_decile"]),
            "size_decile_label": f"SD{int(r['size_decile'])}",
            "battery_kwh": round(float(r["battery_kwh"]), 2),
            "solar_kw": round(float(r["solar_kw"]), 2),
        }
        if r.get("nt_profit") is not None:
            b["nt_profit"] = round(float(r["nt_profit"]), 2)
        out.append(b)
    return out


def _attach_size_deciles_to_prediction_rows(rows: list[dict[str, float | str]]) -> None:
    """
    Mutate ``rows`` in place: add ``size_score`` and ``size_decile`` (1..10) using the same
    equal-count ranking as ``margin_delta_by_system_size_deciles`` (battery × solar score).
    """
    if not rows:
        return
    for r in rows:
        r["size_score"] = _system_size_rank_score(float(r["battery_kwh"]), float(r["solar_kw"]))
    order = sorted(range(len(rows)), key=lambda i: float(rows[i]["size_score"]))
    n = len(rows)
    for rank, idx in enumerate(order):
        rows[idx]["size_decile"] = (rank * 10) // n + 1


def prediction_accuracy_deep_dive(
    batch: dict[str, Any],
    restrict_uids: frozenset[str] | None = None,
) -> dict[str, Any]:
    """
    Compare batch ``optimised`` vs ``actual`` ``net_energy_cost`` as *household profit*
    (negated so positive = money made). Quantify calibration (correlation, MAE/RMSE),
    optimism bias (simulated profit minus actual), and how often reality is worse than the model.

    When the batch record includes optional counterfactuals (retail/no-trade and/or MSC),
    the same profit sign convention applies and optional multi-scenario summaries are emitted.
    """
    energy_checks = {
        "actual": {
            "n_homes": 0,
            "total_import_kwh": 0.0,
            "total_export_kwh": 0.0,
            "total_gross_import_cost_usd": 0.0,
            "total_gross_export_revenue_usd": 0.0,
            "total_net_energy_cost_usd": 0.0,
        },
        "msc": {
            "n_homes": 0,
            "total_import_kwh": 0.0,
            "total_export_kwh": 0.0,
            "total_gross_import_cost_usd": 0.0,
            "total_gross_export_revenue_usd": 0.0,
            "total_net_energy_cost_usd": 0.0,
        },
    }
    n_homes_with_both_energy_sides = 0
    rows: list[dict[str, float | str]] = []
    for uid, rec in batch.items():
        if restrict_uids is not None and uid not in restrict_uids:
            continue
        if not isinstance(rec, dict) or rec.get("error"):
            continue
        opt = rec.get("optimised")
        act = rec.get("actual")
        if not isinstance(opt, dict) or not isinstance(act, dict):
            continue
        try:
            sn = float(opt["net_energy_cost"])
            an = float(act["net_energy_cost"])
        except (KeyError, TypeError, ValueError):
            continue
        sim_p = -sn
        act_p = -an
        err = act_p - sim_p
        optimism = sim_p - act_p
        sz = _extract_system_size(rec)
        nt_side = _batch_no_trade_side(rec)
        nt_p: float | None = None
        if nt_side is not None:
            try:
                nt_p = -float(nt_side["net_energy_cost"])
            except (KeyError, TypeError, ValueError):
                nt_p = None
        msc_side = _batch_msc_side(rec)
        msc_p: float | None = None
        if msc_side is not None:
            try:
                msc_p = -float(msc_side["net_energy_cost"])
            except (KeyError, TypeError, ValueError):
                msc_p = None
        act_snap = _scenario_energy_snapshot(act)
        msc_snap = _scenario_energy_snapshot(msc_side)
        if act_snap is not None:
            energy_checks["actual"]["n_homes"] += 1
            energy_checks["actual"]["total_import_kwh"] += act_snap["import_kwh"]
            energy_checks["actual"]["total_export_kwh"] += act_snap["export_kwh"]
            energy_checks["actual"]["total_gross_import_cost_usd"] += act_snap["gross_import_cost"]
            energy_checks["actual"]["total_gross_export_revenue_usd"] += act_snap["gross_export_revenue"]
            energy_checks["actual"]["total_net_energy_cost_usd"] += act_snap["net_energy_cost"]
        if msc_snap is not None:
            energy_checks["msc"]["n_homes"] += 1
            energy_checks["msc"]["total_import_kwh"] += msc_snap["import_kwh"]
            energy_checks["msc"]["total_export_kwh"] += msc_snap["export_kwh"]
            energy_checks["msc"]["total_gross_import_cost_usd"] += msc_snap["gross_import_cost"]
            energy_checks["msc"]["total_gross_export_revenue_usd"] += msc_snap["gross_export_revenue"]
            energy_checks["msc"]["total_net_energy_cost_usd"] += msc_snap["net_energy_cost"]
        if act_snap is not None and msc_snap is not None:
            n_homes_with_both_energy_sides += 1
        row_obj: dict[str, float | str] = {
            "house_id": uid,
            "sim_profit": sim_p,
            "act_profit": act_p,
            "error": err,
            "optimism": optimism,
            "battery_kwh": sz["battery_kwh"],
            "solar_kw": sz["solar_kw"],
        }
        if nt_p is not None:
            row_obj["nt_profit"] = nt_p
        if msc_p is not None:
            row_obj["msc_profit"] = msc_p
        rows.append(row_obj)

    n = len(rows)
    n_scanned = len(batch)
    if n == 0:
        return {
            "n_homes": 0,
            "n_batch_keys_scanned": n_scanned,
            "restricted_to_export_set": restrict_uids is not None,
            "summary": {},
            "scenario_checksums": {},
            "percentiles_optimism_usd": {},
            "scatter": [],
            "optimism_histogram": {"labels": [], "counts": []},
            "worst_optimism": [],
            "narrative": [],
            "margin_delta_by_size_decile": [],
            "size_decile_bubbles": [],
        }

    _attach_size_deciles_to_prediction_rows(rows)

    sims = [float(r["sim_profit"]) for r in rows]
    acts = [float(r["act_profit"]) for r in rows]
    errors = [float(r["error"]) for r in rows]
    optimisms = [float(r["optimism"]) for r in rows]

    mean_sim = sum(sims) / n
    mean_act = sum(acts) / n
    mean_err = sum(errors) / n
    mean_opt = sum(optimisms) / n
    sorted_err = sorted(errors)
    sorted_opt = sorted(optimisms)
    med_err = _quantile_linear(sorted_err, 0.5)
    med_opt = _quantile_linear(sorted_opt, 0.5)
    var_err = sum((e - mean_err) ** 2 for e in errors) / max(n - 1, 1)
    std_err = math.sqrt(max(var_err, 0.0))
    mae = sum(abs(e) for e in errors) / n
    rmse = math.sqrt(sum(e * e for e in errors) / n)
    corr = _pearson(sims, acts)
    denom_mape = sum(max(abs(s), 1.0) for s in sims)
    mean_abs_pct = (sum(abs(errors[i]) / max(abs(sims[i]), 1.0) for i in range(n)) / n) * 100.0

    eps = 1e-4
    n_too_opt = sum(1 for e in errors if e < -eps)
    n_better = sum(1 for e in errors if e > eps)
    n_tie = n - n_too_opt - n_better

    nt_subset = [r for r in rows if r.get("nt_profit") is not None]
    n_nt = len(nt_subset)
    mean_nt = sum(float(r["nt_profit"]) for r in nt_subset) / n_nt if n_nt else None
    mean_uplift_act_vs_nt = (
        sum(float(r["act_profit"]) - float(r["nt_profit"]) for r in nt_subset) / n_nt if n_nt else None
    )
    mean_sim_vs_nt = (
        sum(float(r["sim_profit"]) - float(r["nt_profit"]) for r in nt_subset) / n_nt if n_nt else None
    )
    msc_subset = [r for r in rows if r.get("msc_profit") is not None]
    n_msc = len(msc_subset)
    mean_msc = sum(float(r["msc_profit"]) for r in msc_subset) / n_msc if n_msc else None
    mean_customer_savings_vs_msc = (
        sum(float(r["act_profit"]) - float(r["msc_profit"]) for r in msc_subset) / n_msc if n_msc else None
    )
    median_customer_savings_vs_msc = (
        _quantile_linear(
            sorted(float(r["act_profit"]) - float(r["msc_profit"]) for r in msc_subset),
            0.5,
        )
        if n_msc
        else None
    )
    mean_sim_vs_msc = (
        sum(float(r["sim_profit"]) - float(r["msc_profit"]) for r in msc_subset) / n_msc if n_msc else None
    )

    pctiles = {
        "p10": round(_quantile_linear(sorted_opt, 0.10), 2),
        "p25": round(_quantile_linear(sorted_opt, 0.25), 2),
        "p50": round(_quantile_linear(sorted_opt, 0.50), 2),
        "p75": round(_quantile_linear(sorted_opt, 0.75), 2),
        "p90": round(_quantile_linear(sorted_opt, 0.90), 2),
    }

    worst = sorted(rows, key=lambda r: float(r["optimism"]), reverse=True)[:40]
    worst_out: list[dict[str, Any]] = []
    for r in worst:
        wo: dict[str, Any] = {
            "house_id": str(r["house_id"]),
            "sim_profit": round(float(r["sim_profit"]), 2),
            "act_profit": round(float(r["act_profit"]), 2),
            "optimism_usd": round(float(r["optimism"]), 2),
        }
        if r.get("nt_profit") is not None:
            wo["nt_profit"] = round(float(r["nt_profit"]), 2)
            wo["reposit_uplift_usd"] = round(float(r["act_profit"]) - float(r["nt_profit"]), 2)
        if r.get("msc_profit") is not None:
            wo["msc_profit"] = round(float(r["msc_profit"]), 2)
            wo["customer_savings_vs_msc_usd"] = round(float(r["act_profit"]) - float(r["msc_profit"]), 2)
        worst_out.append(wo)

    lo_o, hi_o = min(optimisms), max(optimisms)
    hist_labels: list[str] = []
    hist_counts: list[int] = []
    nb = min(16, max(4, n // 8))
    if hi_o <= lo_o:
        hist_labels = [f"{lo_o:.0f}"]
        hist_counts = [n]
    else:
        w = (hi_o - lo_o) / nb
        hist_counts = [0] * nb
        for v in optimisms:
            idx = int((v - lo_o) / w) if w > 0 else 0
            if idx >= nb:
                idx = nb - 1
            hist_counts[idx] += 1
        for i in range(nb):
            a = lo_o + i * w
            b = lo_o + (i + 1) * w
            hist_labels.append(f"{a:.0f}–{b:.0f}")

    scatter: list[dict[str, Any]] = []
    for r in rows:
        pt: dict[str, Any] = {
            "house_id": str(r["house_id"]),
            "x": round(float(r["sim_profit"]), 2),
            "y": round(float(r["act_profit"]), 2),
            "battery_kwh": round(float(r["battery_kwh"]), 1),
            "solar_kw": round(float(r["solar_kw"]), 1),
        }
        if r.get("nt_profit") is not None:
            pt["nt_profit"] = round(float(r["nt_profit"]), 2)
        if r.get("msc_profit") is not None:
            pt["msc_profit"] = round(float(r["msc_profit"]), 2)
            pt["customer_savings_vs_msc_usd"] = round(float(r["act_profit"]) - float(r["msc_profit"]), 2)
        scatter.append(pt)

    # --- Delivery-rate analysis (actual/sim %) ---
    delivery = _delivery_rate_stats(rows)

    # --- Prediction bands: bin by sim profit, show actual outcome range ---
    pred_bands = _prediction_bands(rows, n_bands=5)

    # --- System-size segmented accuracy (4 segments for finer granularity) ---
    n_seg = min(5, max(3, n // 50))
    by_battery = _size_segmented_accuracy(rows, "battery_kwh", n_segments=n_seg)
    by_solar = _size_segmented_accuracy(rows, "solar_kw", n_segments=n_seg)

    # --- Pricing haircut ---
    haircut = _pricing_haircut(delivery, by_battery)

    # --- Optimism-predictor correlations ---
    optimism_predictors = _optimism_predictor_correlations(rows, batch)

    margin_delta_size_dec = margin_delta_by_system_size_deciles(batch, restrict_uids)

    # --- Sign flip summary (same threshold as delivery/bands for consistency) ---
    eligible_for_sign = [r for r in rows if float(r["sim_profit"]) > _PROFIT_THRESHOLD]
    n_eligible_sign = len(eligible_for_sign)
    n_sim_pos_act_neg = sum(1 for r in eligible_for_sign if float(r["act_profit"]) < 0)
    n_sim_pos_act_pos = sum(1 for r in eligible_for_sign if float(r["act_profit"]) > 0)
    n_sim_below_thresh = n - n_eligible_sign
    pct_flip = round(n_sim_pos_act_neg / max(n_eligible_sign, 1) * 100, 1)

    corr_sq = None if corr is None else round(corr * corr, 4)
    narrative_lines: list[str] = []

    # 1. Pricing headline
    if haircut.get("available"):
        m = haircut["fleet_multiplier"]
        if m < 0:
            narrative_lines.append(
                f"Pricing reality: the model predicts profit, but the median home actually loses money. "
                f"For every $1 of predicted profit, the median actual outcome is "
                f"${abs(m) * 100:.0f} of loss. "
                f"The middle 50% of outcomes range from "
                f"{haircut['fleet_p25_mult'] * 100:.0f}% to {haircut['fleet_p75_mult'] * 100:.0f}% of the prediction."
            )
        elif m < 1:
            narrative_lines.append(
                f"Pricing factor: the model overstates performance. For every $1 predicted, "
                f"expect ${m * 100:.0f} cents at the median. "
                f"The middle 50% of homes deliver between "
                f"{haircut['fleet_p25_mult'] * 100:.0f}% and {haircut['fleet_p75_mult'] * 100:.0f}% of predicted value."
            )
        else:
            narrative_lines.append(
                f"The model slightly understates performance: median delivery rate is {m * 100:.0f}% of predicted."
            )

    # 2. Delivery-rate detail
    if delivery["n"] > 0:
        med = delivery['median_delivery_pct']
        if med < 0:
            narrative_lines.append(
                f"Delivery rate: the median home achieves {med:.1f}% of predicted profit "
                f"(i.e. a net loss). Mean delivery is {delivery['mean_delivery_pct']:.1f}%. "
                f"Interquartile range: {delivery['p25_delivery_pct']:.1f}% to {delivery['p75_delivery_pct']:.1f}%."
            )
        else:
            narrative_lines.append(
                f"Delivery rate: actuals deliver {med:.1f}% of predicted value "
                f"at the median (mean {delivery['mean_delivery_pct']:.1f}%). "
                f"Interquartile range: {delivery['p25_delivery_pct']:.1f}% to {delivery['p75_delivery_pct']:.1f}%."
            )

    # 3. Sign-flip headline
    if n_eligible_sign > 0:
        narrative_lines.append(
            f"Of {n_eligible_sign} homes the model predicted would profit (>${_PROFIT_THRESHOLD:.0f}), "
            f"{n_sim_pos_act_neg} ({pct_flip}%) actually lost money. "
            f"Only {n_sim_pos_act_pos} ({round(n_sim_pos_act_pos / max(n_eligible_sign, 1) * 100, 1)}%) remained profitable."
        )

    # 4. Optimism
    if mean_opt > 0:
        narrative_lines.append(
            f"On average the model was too optimistic by ${mean_opt:,.0f} per home "
            f"(median gap ${med_opt:,.0f})."
        )
    else:
        narrative_lines.append(
            f"On average simulated profit was ${-mean_opt:,.0f} lower than actual (median gap ${-med_opt:,.0f}). "
            "The model tended to understate how well homes did."
        )
    if corr is not None:
        narrative_lines.append(
            f"Sim–actual correlation is {corr:.2f}"
            + (f" (R² = {corr_sq})." if corr_sq is not None else ".")
        )

    if n_nt > 0 and mean_nt is not None and mean_uplift_act_vs_nt is not None:
        mean_act_nt = sum(float(r["act_profit"]) for r in nt_subset) / n_nt
        narrative_lines.append(
            f"Retail-without-Reposit-trade counterfactual is present for {n_nt} homes: "
            f"mean counterfactual profit ${mean_nt:,.0f} vs mean billed (with Reposit) ${mean_act_nt:,.0f}; "
            f"mean uplift from trading (billed minus retail-only) is ${mean_uplift_act_vs_nt:,.0f} per home."
            + (
                f" Mean simulated profit vs that baseline is ${mean_sim_vs_nt:,.0f}."
                if mean_sim_vs_nt is not None
                else ""
            )
        )
    if n_msc > 0 and mean_msc is not None and mean_customer_savings_vs_msc is not None:
        mean_act_msc = sum(float(r["act_profit"]) for r in msc_subset) / n_msc
        narrative_lines.append(
            f"MSC counterfactual is present for {n_msc} homes: mean MSC baseline profit ${mean_msc:,.0f} "
            f"vs mean billed (actual) ${mean_act_msc:,.0f}; mean customer savings vs MSC is "
            f"${mean_customer_savings_vs_msc:,.0f} per home."
            + (
                f" Mean simulated profit vs MSC is ${mean_sim_vs_msc:,.0f}."
                if mean_sim_vs_msc is not None
                else ""
            )
        )

    # 5. Size insight
    if by_battery and len(by_battery) >= 2:
        lo = by_battery[0]
        hi = by_battery[-1]
        narrative_lines.append(
            f"Battery size matters: small batteries ({lo['label']} kWh) deliver "
            f"{lo['median_delivery_pct']:.0f}% of predicted value vs "
            f"{hi['median_delivery_pct']:.0f}% for large ({hi['label']} kWh). "
            f"Flip-to-loss rates are {lo['pct_flip_to_loss']:.0f}% and {hi['pct_flip_to_loss']:.0f}% respectively."
        )

    # 6. Top optimism predictor
    if optimism_predictors:
        top = optimism_predictors[0]
        direction = "higher" if top["corr"] > 0 else "lower"
        narrative_lines.append(
            f"Strongest predictor of delivery rate: {top['label']} "
            f"(r = {top['corr']:+.2f}, n = {top['n']}). "
            f"Homes with {direction} {top['label'].lower()} tend to capture more of the predicted value."
        )

    if margin_delta_size_dec:
        filled = [b for b in margin_delta_size_dec if b.get("n_homes", 0) > 0]
        if filled:
            worst = min(filled, key=lambda b: float(b.get("mean_delta_margin_pp") or 0.0))
            best = max(filled, key=lambda b: float(b.get("mean_delta_margin_pp") or 0.0))
            narrative_lines.append(
                f"Activity margin gap (actual − sim, pp): by system-size decile (battery×solar rank), "
                f"worst average gap is {worst['label']} ({worst['mean_delta_margin_pp']:.1f} pp, n={worst['n_homes']}); "
                f"least bad is {best['label']} ({best['mean_delta_margin_pp']:.1f} pp, n={best['n_homes']})."
            )

    narrative_lines.append(
        f"MAE ${mae:,.0f}; RMSE ${rmse:,.0f}. "
        f"Mean absolute relative gap vs |sim| is {mean_abs_pct:.1f}%."
    )

    checks_out: dict[str, Any] = {
        "n_homes_with_both_scenarios": n_homes_with_both_energy_sides,
        "actual": {},
        "msc": {},
    }
    for scenario in ("actual", "msc"):
        d = energy_checks[scenario]
        import_kwh = d["total_import_kwh"]
        export_kwh = d["total_export_kwh"]
        checks_out[scenario] = {
            "n_homes": d["n_homes"],
            "total_import_kwh": round(import_kwh, 3),
            "total_export_kwh": round(export_kwh, 3),
            "total_gross_import_cost_usd": round(d["total_gross_import_cost_usd"], 2),
            "total_gross_export_revenue_usd": round(d["total_gross_export_revenue_usd"], 2),
            "total_net_energy_cost_usd": round(d["total_net_energy_cost_usd"], 2),
            "weighted_avg_import_price_per_kwh": None
            if import_kwh <= 0.0
            else round(d["total_gross_import_cost_usd"] / import_kwh, 6),
            "weighted_avg_export_price_per_kwh": None
            if export_kwh <= 0.0
            else round(d["total_gross_export_revenue_usd"] / export_kwh, 6),
        }

    return {
        "n_homes": n,
        "n_batch_keys_scanned": n_scanned,
        "restricted_to_export_set": restrict_uids is not None,
        "summary": {
            "mean_simulated_profit": round(mean_sim, 2),
            "mean_actual_profit": round(mean_act, 2),
            "mean_error_actual_minus_sim": round(mean_err, 2),
            "median_error_actual_minus_sim": round(med_err, 2),
            "mean_optimism_sim_minus_actual_usd": round(mean_opt, 2),
            "median_optimism_sim_minus_actual_usd": round(med_opt, 2),
            "std_error_usd": round(std_err, 2),
            "mae_usd": round(mae, 2),
            "rmse_usd": round(rmse, 2),
            "correlation_sim_vs_actual_profit": None if corr is None else round(corr, 4),
            "corr_squared_proxy": corr_sq,
            "mean_abs_gap_pct_of_abs_sim": round(mean_abs_pct, 2),
            "pct_sim_too_optimistic": round(n_too_opt / n * 100.0, 2),
            "pct_actual_better_than_sim": round(n_better / n * 100.0, 2),
            "pct_tie": round(n_tie / n * 100.0, 2),
            "count_too_optimistic": n_too_opt,
            "count_actual_better": n_better,
            "count_tie": n_tie,
            "n_homes_with_retail_without_trade": n_nt,
            "mean_retail_without_trade_profit_usd": None if not n_nt or mean_nt is None else round(mean_nt, 2),
            "mean_reposit_uplift_actual_minus_without_usd": None
            if not n_nt or mean_uplift_act_vs_nt is None
            else round(mean_uplift_act_vs_nt, 2),
            "mean_simulated_minus_without_usd": None
            if not n_nt or mean_sim_vs_nt is None
            else round(mean_sim_vs_nt, 2),
            "n_homes_with_msc_counterfactual": n_msc,
            "mean_msc_counterfactual_profit_usd": None if not n_msc or mean_msc is None else round(mean_msc, 2),
            "mean_customer_savings_vs_msc_usd": None
            if not n_msc or mean_customer_savings_vs_msc is None
            else round(mean_customer_savings_vs_msc, 2),
            "median_customer_savings_vs_msc_usd": None
            if not n_msc or median_customer_savings_vs_msc is None
            else round(median_customer_savings_vs_msc, 2),
            "mean_simulated_minus_msc_usd": None
            if not n_msc or mean_sim_vs_msc is None
            else round(mean_sim_vs_msc, 2),
        },
        "delivery_rate": delivery,
        "sign_flip": {
            "n_eligible": n_eligible_sign,
            "n_flip_to_loss": n_sim_pos_act_neg,
            "pct_flip_to_loss": pct_flip,
            "n_both_profit": n_sim_pos_act_pos,
            "n_below_threshold": n_sim_below_thresh,
        },
        "pricing_haircut": haircut,
        "optimism_predictors": optimism_predictors,
        "prediction_bands": pred_bands,
        "by_battery_kwh": by_battery,
        "by_solar_kw": by_solar,
        "percentiles_optimism_usd": pctiles,
        "scatter": scatter,
        "optimism_histogram": {"labels": hist_labels, "counts": hist_counts},
        "worst_optimism": worst_out,
        "narrative": narrative_lines,
        "scenario_checksums": checks_out,
        "margin_delta_by_size_decile": margin_delta_size_dec,
        "size_decile_bubbles": _size_decile_bubbles_payload(rows),
    }


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


PREDICTOR_BASE_FEATURES: tuple[tuple[str, str], ...] = (
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
)


def _predictor_feature_defs(houses: list[dict]) -> list[tuple[str, str]]:
    feats: list[tuple[str, str]] = list(PREDICTOR_BASE_FEATURES)
    for ck in ABS_CENSUS_KEYS:
        if any(h.get(ck) is not None for h in houses):
            feats.append((ck, ABS_CENSUS_LABELS.get(ck, ck.replace("_", " ").title())))
    return feats


def predictor_analysis(houses: list[dict]) -> dict:
    if not houses:
        return {"rows": []}

    features = _predictor_feature_defs(houses)

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


def loss_houses_predictors(loss_houses: list[dict], max_rows: int = 12) -> dict:
    """Correlations with net_margin among loss-making homes only (deeper loss = more negative)."""
    if len(loss_houses) < 3:
        return {"rows": []}
    features = _predictor_feature_defs(loss_houses)
    target_margin = [h["net_margin"] for h in loss_houses]
    rows: list[dict] = []
    for key, label in features:
        xs: list[float] = []
        ys_margin: list[float] = []
        for i, h in enumerate(loss_houses):
            v = h.get(key)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            xs.append(fv)
            ys_margin.append(target_margin[i])
        corr_margin = _pearson(xs, ys_margin)
        if corr_margin is None:
            continue
        rows.append(
            {
                "feature": key,
                "label": label,
                "sample_size": len(xs),
                "corr_net_margin": round(corr_margin, 4),
                "corr_profit_decile": None,
                "abs_score": abs(corr_margin),
            }
        )
    rows.sort(key=lambda r: r["abs_score"], reverse=True)
    return {"rows": rows[:max_rows]}


def _median_sorted(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    m = len(s) // 2
    if len(s) % 2:
        return float(s[m])
    return float(s[m - 1] + s[m]) / 2.0


def _float_vals(houses: list[dict], key: str) -> list[float]:
    out: list[float] = []
    for h in houses:
        v = h.get(key)
        if v is None:
            continue
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            continue
    return out


def _mean_f(vals: list[float]) -> float | None:
    if not vals:
        return None
    return sum(vals) / len(vals)


LOSS_VS_PROFIT_METRICS: tuple[tuple[str, str], ...] = (
    ("gross_cost", "Gross cost ($)"),
    ("gross_revenue", "Gross revenue / credits ($)"),
    ("avg_import_price", "Avg import price ($/kWh)"),
    ("avg_export_price", "Avg export price ($/kWh)"),
    ("avg_price_spread", "Avg export−import spread ($/kWh)"),
    ("spread_positive_pct", "Spread positive (%)"),
    ("total_import_kwh", "Total import (kWh)"),
    ("total_export_kwh", "Total export (kWh)"),
    ("total_load_kwh", "Total load (kWh)"),
    ("total_solar_kwh", "Total solar (kWh)"),
    ("battery_throughput_kwh", "Battery throughput (kWh)"),
    ("export_to_import_ratio", "Export / import ratio"),
    ("import_active_pct", "Import-active intervals (%)"),
    ("export_active_pct", "Export-active intervals (%)"),
    ("battery_active_pct", "Battery-active intervals (%)"),
    ("max_solar_kw", "Max solar (kW)"),
    ("max_battery_soc_kwh", "Max battery SoC (kWh)"),
    ("avg_load_kw", "Avg load (kW)"),
    ("evening_load_share_pct", "Evening load share (%)"),
    ("solar_self_consumption_pct", "Solar self-consumption (%)"),
)


def loss_making_deep_dive(houses: list[dict]) -> dict:
    """
    Compare loss-making homes (net_margin < 0) to profitable homes and surface structural drivers
    relative to fleet-wide medians.
    """
    if not houses:
        return {"empty": True, "n_loss": 0, "n_profit": 0, "n_fleet": 0}

    loss = [h for h in houses if (h.get("net_margin") or 0.0) < 0]
    profit = [h for h in houses if (h.get("net_margin") or 0.0) >= 0]
    n_loss, n_profit, n_fleet = len(loss), len(profit), len(houses)
    pct_loss = round(n_loss / n_fleet * 100.0, 2) if n_fleet else 0.0

    if not loss:
        return {
            "empty": True,
            "n_loss": 0,
            "n_profit": n_profit,
            "n_fleet": n_fleet,
            "pct_fleet_loss": pct_loss,
        }

    sum_loss_net = sum(h["net_margin"] for h in loss)
    medians: dict[str, float | None] = {}
    for key, _ in LOSS_VS_PROFIT_METRICS:
        medians[key] = _median_sorted(_float_vals(houses, key))

    compare: list[dict] = []
    for key, label in LOSS_VS_PROFIT_METRICS:
        lv = _float_vals(loss, key)
        pv = _float_vals(profit, key)
        lm = _mean_f(lv)
        pm = _mean_f(pv)
        row: dict = {
            "key": key,
            "label": label,
            "loss_mean": None if lm is None else round(lm, 6),
            "profit_mean": None if pm is None else round(pm, 6),
            "ratio": None,
            "pct_vs_profit": None,
        }
        if lm is not None and pm is not None and abs(pm) > 1e-12:
            row["ratio"] = round(lm / pm, 4)
            row["pct_vs_profit"] = round((lm / pm - 1.0) * 100.0, 2)
        compare.append(row)

    driver_defs: list[tuple[str, str, str, str]] = [
        ("high_grid_cost", "Grid charges at or above fleet median", "gross_cost", "high"),
        ("low_feed_in_credits", "Feed-in credits at or below fleet median", "gross_revenue", "low"),
        ("high_import_price", "Avg import price at or above fleet median", "avg_import_price", "high"),
        ("low_export_price", "Avg export price at or below fleet median", "avg_export_price", "low"),
        ("weak_price_spread", "Avg export−import spread at or below fleet median", "avg_price_spread", "low"),
        ("heavy_grid_imports", "Total grid import energy at or above fleet median", "total_import_kwh", "high"),
        ("light_grid_exports", "Total grid export energy at or below fleet median", "total_export_kwh", "low"),
        ("low_export_import_ratio", "Export/import ratio at or below fleet median", "export_to_import_ratio", "low"),
        ("few_positive_spreads", "Share of positive spread intervals at or below fleet median", "spread_positive_pct", "low"),
    ]

    drivers: list[dict] = []
    for did, dlabel, mkey, direction in driver_defs:
        mval = medians.get(mkey)
        if mval is None:
            continue
        cnt = 0
        for h in loss:
            v = h.get(mkey)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if direction == "high" and fv >= mval:
                cnt += 1
            elif direction == "low" and fv <= mval:
                cnt += 1
        drivers.append(
            {
                "id": did,
                "label": dlabel,
                "count": cnt,
                "pct_of_loss": round(cnt / n_loss * 100.0, 2) if n_loss else 0.0,
            }
        )
    drivers.sort(key=lambda d: d["pct_of_loss"], reverse=True)

    narrative: list[str] = []
    gc = next((r for r in compare if r["key"] == "gross_cost"), None)
    gr = next((r for r in compare if r["key"] == "gross_revenue"), None)
    if gc and gc.get("pct_vs_profit") is not None and gc["pct_vs_profit"] > 3:
        narrative.append(
            f"Loss-making homes average {gc['pct_vs_profit']:.0f}% higher gross grid cost than profitable homes."
        )
    if gr and gr.get("pct_vs_profit") is not None and gr["pct_vs_profit"] < -3:
        narrative.append(
            f"They average {abs(gr['pct_vs_profit']):.0f}% lower gross feed-in credits than profitable homes."
        )
    ip = next((r for r in compare if r["key"] == "avg_import_price"), None)
    ep = next((r for r in compare if r["key"] == "avg_export_price"), None)
    if ip and ip.get("pct_vs_profit") is not None and ip["pct_vs_profit"] > 2:
        narrative.append(
            f"They face import prices about {ip['pct_vs_profit']:.0f}% higher on average than profitable homes."
        )
    if ep and ep.get("pct_vs_profit") is not None and ep["pct_vs_profit"] < -2:
        narrative.append(
            f"Export prices average {abs(ep['pct_vs_profit']):.0f}% lower than for profitable homes."
        )
    ti = next((r for r in compare if r["key"] == "total_import_kwh"), None)
    te = next((r for r in compare if r["key"] == "total_export_kwh"), None)
    if ti and ti.get("pct_vs_profit") is not None and ti["pct_vs_profit"] > 5:
        narrative.append(
            f"Total energy imported from the grid is about {ti['pct_vs_profit']:.0f}% higher on average than for profitable homes."
        )
    if te and te.get("pct_vs_profit") is not None and te["pct_vs_profit"] < -5:
        narrative.append(
            f"Total energy exported to the grid is about {abs(te['pct_vs_profit']):.0f}% lower on average than for profitable homes."
        )
    if not narrative:
        narrative.append(
            "Loss homes differ from profitable homes on cost, credits, and grid-flow metrics; "
            "use the comparison table and driver counts below to see where this fleet concentrates."
        )

    seg_counts: dict[str, int] = {}
    for h in loss:
        lab = h.get("segment_label")
        k = str(lab) if lab else "Not clustered"
        seg_counts[k] = seg_counts.get(k, 0) + 1
    segment_mix = sorted(
        [{"segment": k, "count": v, "pct": round(v / n_loss * 100.0, 2)} for k, v in seg_counts.items()],
        key=lambda x: -x["count"],
    )

    worst = sorted(loss, key=lambda h: h["net_margin"])[:80]
    worst_rows: list[dict] = []
    for h in worst:
        worst_rows.append(
            {
                "house_id": h.get("house_id", ""),
                "segment_label": h.get("segment_label"),
                "net_margin": round(float(h["net_margin"]), 6),
                "margin_pct_revenue": h.get("margin_pct_revenue"),
                "gross_cost": round(float(h.get("gross_cost") or 0.0), 6),
                "gross_revenue": round(float(h.get("gross_revenue") or 0.0), 6),
                "avg_price_spread": None if h.get("avg_price_spread") is None else round(float(h["avg_price_spread"]), 6),
                "total_import_kwh": round(float(h.get("total_import_kwh") or 0.0), 4),
                "total_export_kwh": round(float(h.get("total_export_kwh") or 0.0), 4),
                "avg_import_price": round(float(h.get("avg_import_price") or 0.0), 6),
                "avg_export_price": round(float(h.get("avg_export_price") or 0.0), 6),
            }
        )

    loss_predictors = loss_houses_predictors(loss, max_rows=12)

    return {
        "empty": False,
        "n_loss": n_loss,
        "n_profit": n_profit,
        "n_fleet": n_fleet,
        "pct_fleet_loss": pct_loss,
        "sum_net_margin_loss": round(sum_loss_net, 6),
        "avg_net_margin_loss": round(sum_loss_net / n_loss, 6),
        "avg_net_margin_profit": None
        if not profit
        else round(sum(h["net_margin"] for h in profit) / len(profit), 6),
        "compare": compare,
        "drivers": drivers,
        "narrative": narrative,
        "segment_mix": segment_mix,
        "worst_homes": worst_rows,
        "loss_predictors": loss_predictors,
    }


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
        rev_pct_vals = [h["margin_pct_revenue"] for h in members if h.get("margin_pct_revenue") is not None]
        act_pct_vals = [h["margin_pct_activity"] for h in members if h.get("margin_pct_activity") is not None]
        total_rev = sum(h["gross_revenue"] for h in members)
        total_net = sum(h["net_margin"] for h in members)
        total_activity = sum(h["gross_revenue"] + h["gross_cost"] for h in members)
        segment_nm_pct_rev = (total_net / total_rev * 100.0) if total_rev > 0 else None
        segment_nm_pct_act = (total_net / total_activity * 100.0) if total_activity > 0 else None
        segments.append(
            {
                "segment_id": seg + 1,
                "n_homes": m,
                "avg_net_margin": round(sum(h["net_margin"] for h in members) / m, 6),
                "avg_gross_cost": round(sum(h["gross_cost"] for h in members) / m, 6),
                "avg_gross_revenue": round(sum(h["gross_revenue"] for h in members) / m, 6),
                "avg_margin_pct_activity": None
                if not act_pct_vals
                else round(sum(act_pct_vals) / len(act_pct_vals), 4),
                "avg_margin_pct_revenue": None
                if not rev_pct_vals
                else round(sum(rev_pct_vals) / len(rev_pct_vals), 4),
                "segment_net_margin_pct_revenue": None
                if segment_nm_pct_rev is None
                else round(segment_nm_pct_rev, 4),
                "segment_net_margin_pct_activity": None
                if segment_nm_pct_act is None
                else round(segment_nm_pct_act, 4),
                "total_net_margin": round(total_net, 6),
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


# ---------------------------------------------------------------------------
# Calibrated prediction: persisted segment model + sim->actual calibration.
# Used by /calibrated-prediction subset page. Separate from in-fleet
# `segment_analysis` so labels are stable across runs (pinned S-rank map).
# ---------------------------------------------------------------------------

SEGMENT_FEATURE_ORDER: tuple[str, ...] = (
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
)

CALIBRATION_ARTEFACT = CACHE_DIR / "calibration_model_latest.json"
CALIBRATION_SCHEMA_VERSION = 1
_PROFIT_ELIGIBILITY_THRESHOLD = _PROFIT_THRESHOLD  # reuse $10 gate from delivery stats


def _extract_home_feature_vector(h: dict) -> tuple[list[float | None], list[str]]:
    """Return raw float values in SEGMENT_FEATURE_ORDER plus list of missing/invalid dims."""
    missing: list[str] = []
    vals: list[float | None] = []
    for key in SEGMENT_FEATURE_ORDER:
        v = h.get(key)
        if v is None:
            missing.append(key)
            vals.append(None)
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            missing.append(key)
            vals.append(None)
    return vals, missing


def _population_mean_std(matrix: list[list[float]]) -> tuple[list[float], list[float]]:
    """Per-column population mean / std with std floored to 1.0 on near-zero variance.

    Matches `_zscore_matrix` convention (no Bessel correction).
    """
    if not matrix:
        return [], []
    n = len(matrix)
    n_cols = len(matrix[0])
    means = [sum(r[c] for r in matrix) / n for c in range(n_cols)]
    stds: list[float] = []
    for c in range(n_cols):
        var = sum((r[c] - means[c]) ** 2 for r in matrix) / n
        stds.append(math.sqrt(var) if var > 1e-12 else 1.0)
    return means, stds


def _feature_medians(matrix: list[list[float]]) -> list[float]:
    if not matrix:
        return []
    n_cols = len(matrix[0])
    meds: list[float] = []
    for c in range(n_cols):
        col_vals = sorted(r[c] for r in matrix)
        meds.append(_quantile_linear(col_vals, 0.5))
    return meds


def _centroid_distances(z: list[float], centroids: list[list[float]]) -> list[float]:
    out: list[float] = []
    for c in centroids:
        s = 0.0
        for d in range(len(z)):
            diff = z[d] - c[d]
            s += diff * diff
        out.append(math.sqrt(s))
    return out


def _kmeans_with_centroids(
    points: list[list[float]], k: int, iterations: int = 24
) -> tuple[list[int], list[list[float]]]:
    """Deterministic k-means with evenly-spaced init (matches `_kmeans`); also returns centroids."""
    if not points:
        return [], []
    k = max(1, min(k, len(points)))
    centroids = [points[(i * len(points)) // k][:] for i in range(k)]
    labels = [0] * len(points)
    for _ in range(iterations):
        changed = False
        for i, p in enumerate(points):
            best = min(
                range(k),
                key=lambda c: sum((p[d] - centroids[c][d]) ** 2 for d in range(len(p))),
            )
            if labels[i] != best:
                labels[i] = best
                changed = True
        if not changed:
            break
        for c in range(k):
            members = [points[i] for i, lbl in enumerate(labels) if lbl == c]
            if not members:
                continue
            centroids[c] = [
                sum(m[d] for m in members) / len(members) for d in range(len(members[0]))
            ]
    return labels, centroids


def fit_segment_model(
    houses: list[dict], k: int = 4, iterations: int = 24
) -> dict[str, Any]:
    """Fit a persistable behaviour-segment model over homes with complete feature vectors.

    Homes are pre-sorted by ``house_id`` ascending for deterministic k-means init
    (no RNG). S-rank (1..K) pins each raw centroid to its avg-net_margin rank so
    labels survive artefact refreshes when membership is stable.
    """
    usable: list[dict] = []
    raw: list[list[float]] = []
    ordered = sorted(
        (h for h in houses if h.get("house_id")), key=lambda h: str(h["house_id"])
    )
    for h in ordered:
        vec, missing = _extract_home_feature_vector(h)
        if missing:
            continue
        usable.append(h)
        raw.append([float(x) for x in vec])  # type: ignore[arg-type]
    if len(raw) < max(k, 4):
        return {"available": False, "reason": "insufficient_homes", "n": len(raw), "k": k}

    means, stds = _population_mean_std(raw)
    medians = _feature_medians(raw)
    z = [[(r[c] - means[c]) / stds[c] for c in range(len(r))] for r in raw]
    labels, centroids = _kmeans_with_centroids(z, k=k, iterations=iterations)

    # Rank raw labels by avg net_margin of members to produce stable S-rank.
    margins_by_label: dict[int, list[float]] = {}
    for i, lbl in enumerate(labels):
        nm = float(usable[i].get("net_margin") or 0.0)
        margins_by_label.setdefault(lbl, []).append(nm)
    order_by_margin = sorted(
        margins_by_label.keys(),
        key=lambda lbl: sum(margins_by_label[lbl]) / len(margins_by_label[lbl]),
    )
    centroid_to_rank = {
        raw_lbl: rank for rank, raw_lbl in enumerate(order_by_margin, start=1)
    }

    # Ambiguity guardrails derived from assigned-home distances to their own centroid.
    assigned_dist = [
        math.sqrt(
            sum((z[i][d] - centroids[labels[i]][d]) ** 2 for d in range(len(z[i])))
        )
        for i in range(len(z))
    ]
    dist_sorted = sorted(assigned_dist)
    distance_p90 = _quantile_linear(dist_sorted, 0.9) if dist_sorted else 0.0
    tau_src = sorted(d * d for d in assigned_dist)
    tau = _quantile_linear(tau_src, 0.5) if tau_src else 1.0
    if tau <= 1e-9:
        tau = 1.0

    # Per-rank population meta.
    S_meta: dict[str, dict[str, Any]] = {}
    uids_by_rank: dict[int, list[str]] = {}
    for i, lbl in enumerate(labels):
        rank = centroid_to_rank[lbl]
        uids_by_rank.setdefault(rank, []).append(str(usable[i]["house_id"]))
    for rank, uids in uids_by_rank.items():
        nms = margins_by_label[order_by_margin[rank - 1]]
        S_meta[f"S{rank}"] = {
            "members": len(uids),
            "avg_net_margin": round(sum(nms) / len(nms), 4),
        }

    uid_list = [str(h["house_id"]) for h in usable]
    fleet_hash_input = json.dumps(
        {
            "uids": uid_list,
            "features": list(SEGMENT_FEATURE_ORDER),
            "k": k,
            "iterations": iterations,
        },
        sort_keys=True,
    )
    fleet_hash = hashlib.sha256(fleet_hash_input.encode("utf-8")).hexdigest()[:16]

    return {
        "available": True,
        "schema": CALIBRATION_SCHEMA_VERSION,
        "fleet_hash": fleet_hash,
        "feature_order": list(SEGMENT_FEATURE_ORDER),
        "means": [round(x, 8) for x in means],
        "stds": [round(x, 8) for x in stds],
        "medians": [round(x, 8) for x in medians],
        "scaler_convention": "population_variance",
        "k": k,
        "iterations": iterations,
        "init_rule": "points[(i*n)//k] with uids sorted ascending",
        "centroids_z": [[round(x, 8) for x in c] for c in centroids],
        "centroid_to_S_rank": {
            str(raw_lbl): rank for raw_lbl, rank in centroid_to_rank.items()
        },
        "S_rank_meta": S_meta,
        "ambiguity_thresholds": {
            "distance_p90": round(distance_p90, 6),
            "top2_ratio_warn": 0.9,
        },
        "tau": round(tau, 6),
        "census_policy": "A_non_census_only",
        "uids": uid_list,
        "n_homes": len(usable),
    }


def apply_segment_model(houses: list[dict], model: dict[str, Any]) -> None:
    """Annotate each home with ``segment_id_ref`` + ``segment_label_ref`` from a persisted model.

    Does not overwrite the in-fleet ``segment_id`` (which comes from this run's own k-means).
    """
    if not model.get("available"):
        return
    means = model["means"]
    stds = model["stds"]
    centroids = model["centroids_z"]
    rank_map = {int(k): int(v) for k, v in model["centroid_to_S_rank"].items()}
    for h in houses:
        vec, missing = _extract_home_feature_vector(h)
        if missing:
            continue
        z = [(float(vec[i]) - means[i]) / stds[i] for i in range(len(vec))]  # type: ignore[arg-type]
        dists = _centroid_distances(z, centroids)
        raw = min(range(len(dists)), key=lambda c: dists[c])
        h["segment_id_ref"] = rank_map[raw]
        h["segment_label_ref"] = f"S{rank_map[raw]}"


def assign_home_to_segment(
    features: dict[str, Any], model: dict[str, Any]
) -> dict[str, Any]:
    """Nearest-centroid assignment in z-space; emits top-2, ambiguity flags and soft weights."""
    if not model.get("available"):
        return {"assigned": False, "reason": "model_unavailable"}
    means = model["means"]
    stds = model["stds"]
    medians = model["medians"]
    centroids = model["centroids_z"]
    rank_map = {int(k): int(v) for k, v in model["centroid_to_S_rank"].items()}
    feature_order = model["feature_order"]

    imputed: list[str] = []
    vec: list[float] = []
    for i, key in enumerate(feature_order):
        v = features.get(key)
        try:
            vec.append(float(v))
        except (TypeError, ValueError):
            imputed.append(key)
            vec.append(float(medians[i]))
    ratio_imputed = len(imputed) / max(1, len(feature_order))
    low_conf = ratio_imputed >= (1.0 / 3.0)

    z = [(vec[i] - means[i]) / stds[i] for i in range(len(vec))]
    dists = _centroid_distances(z, centroids)
    idxs = sorted(range(len(dists)), key=lambda c: dists[c])
    primary_raw = idxs[0]
    secondary_raw = idxs[1] if len(idxs) > 1 else idxs[0]
    primary_rank = rank_map[primary_raw]
    secondary_rank = rank_map[secondary_raw]
    d1 = dists[primary_raw]
    d2 = dists[secondary_raw] if secondary_raw != primary_raw else d1 * 2 + 1.0

    thresholds = model.get("ambiguity_thresholds", {})
    ratio_warn = float(thresholds.get("top2_ratio_warn", 0.9))
    distance_p90 = float(thresholds.get("distance_p90", 0.0))
    ambiguous = (d1 / d2 >= ratio_warn) if d2 > 0 else False
    far_from_fleet = d1 > distance_p90 if distance_p90 > 0 else False

    tau = float(model.get("tau", 1.0)) or 1.0
    raw_weights = [math.exp(-(d * d) / tau) for d in dists]
    tot = sum(raw_weights) or 1.0
    weights_by_rank: dict[int, float] = {}
    for raw_idx, w in enumerate(raw_weights):
        rr = rank_map[raw_idx]
        weights_by_rank[rr] = weights_by_rank.get(rr, 0.0) + w / tot

    return {
        "assigned": True,
        "low_confidence": bool(low_conf),
        "ambiguous": bool(ambiguous),
        "far_from_fleet": bool(far_from_fleet),
        "imputed_dims": imputed,
        "ratio_imputed": round(ratio_imputed, 3),
        "primary_segment": primary_rank,
        "primary_label": f"S{primary_rank}",
        "primary_distance": round(d1, 6),
        "secondary_segment": secondary_rank,
        "secondary_label": f"S{secondary_rank}",
        "secondary_distance": round(d2, 6),
        "distance_ratio": round(d1 / d2, 4) if d2 > 0 else None,
        "soft_weights_by_rank": {
            f"S{r}": round(w, 4) for r, w in sorted(weights_by_rank.items())
        },
        "all_distances": [round(d, 6) for d in dists],
        "distance_p90_fleet": round(distance_p90, 6),
    }


# -- Calibration model zoo ---------------------------------------------------


def _solve_ols(X: list[list[float]], y: list[float]) -> list[float] | None:
    """OLS via normal equations + Gauss-Jordan. Returns coefficients or None on singularity."""
    if not X or not y or len(X) != len(y):
        return None
    n = len(X)
    p = len(X[0])
    XtX = [[0.0] * p for _ in range(p)]
    Xty = [0.0] * p
    for i in range(n):
        row = X[i]
        yi = y[i]
        for a in range(p):
            Xty[a] += row[a] * yi
            for b in range(a, p):
                XtX[a][b] += row[a] * row[b]
    for a in range(p):
        for b in range(a):
            XtX[a][b] = XtX[b][a]
    aug = [XtX[i] + [Xty[i]] for i in range(p)]
    for col in range(p):
        piv = col
        maxv = abs(aug[col][col])
        for r in range(col + 1, p):
            if abs(aug[r][col]) > maxv:
                maxv = abs(aug[r][col])
                piv = r
        if maxv < 1e-12:
            return None
        if piv != col:
            aug[col], aug[piv] = aug[piv], aug[col]
        pv = aug[col][col]
        for c in range(col, p + 1):
            aug[col][c] /= pv
        for r in range(p):
            if r == col:
                continue
            factor = aug[r][col]
            if factor == 0:
                continue
            for c in range(col, p + 1):
                aug[r][c] -= factor * aug[col][c]
    return [aug[i][p] for i in range(p)]


def _fit_identity(_rows: list[dict]) -> dict:
    return {}


def _predict_identity(_state: dict, row: dict) -> float:
    return float(row["sim_profit"])


def _fit_global_ratio(rows: list[dict]) -> dict:
    eligible = [r for r in rows if r["sim_profit"] > _PROFIT_ELIGIBILITY_THRESHOLD]
    if not eligible:
        return {"k": 1.0}
    rates = sorted(r["act_profit"] / r["sim_profit"] for r in eligible)
    return {"k": float(_quantile_linear(rates, 0.5))}


def _predict_global_ratio(state: dict, row: dict) -> float:
    return float(state.get("k", 1.0)) * float(row["sim_profit"])


def _fit_global_linear(rows: list[dict]) -> dict:
    X = [[1.0, float(r["sim_profit"])] for r in rows]
    y = [float(r["act_profit"]) for r in rows]
    b = _solve_ols(X, y)
    if b is None:
        return {"a": 0.0, "b": 1.0, "failed": True}
    return {"a": float(b[0]), "b": float(b[1])}


def _predict_global_linear(state: dict, row: dict) -> float:
    return float(state.get("a", 0.0)) + float(state.get("b", 1.0)) * float(row["sim_profit"])


def _fit_per_segment_ratio(rows: list[dict], min_n: int = 20) -> dict:
    by_seg: dict[int, list[float]] = {}
    for r in rows:
        if r["sim_profit"] > _PROFIT_ELIGIBILITY_THRESHOLD:
            by_seg.setdefault(int(r["S_rank"]), []).append(
                r["act_profit"] / r["sim_profit"]
            )
    global_rates = sorted(
        r["act_profit"] / r["sim_profit"]
        for r in rows
        if r["sim_profit"] > _PROFIT_ELIGIBILITY_THRESHOLD
    )
    global_k = float(_quantile_linear(global_rates, 0.5)) if global_rates else 1.0
    by_seg_k: dict[str, float] = {}
    for s, rr in by_seg.items():
        if len(rr) >= min_n:
            by_seg_k[str(s)] = float(_quantile_linear(sorted(rr), 0.5))
    return {"by_seg": by_seg_k, "global_k": global_k}


def _predict_per_segment_ratio(state: dict, row: dict) -> float:
    k = state.get("by_seg", {}).get(str(int(row["S_rank"])), state.get("global_k", 1.0))
    return float(k) * float(row["sim_profit"])


def _fit_per_segment_linear(rows: list[dict], min_n: int = 20) -> dict:
    by_seg: dict[int, list[dict]] = {}
    for r in rows:
        by_seg.setdefault(int(r["S_rank"]), []).append(r)
    gX = [[1.0, float(r["sim_profit"])] for r in rows]
    gy = [float(r["act_profit"]) for r in rows]
    g_ab = _solve_ols(gX, gy) or [0.0, 1.0]
    by_seg_ab: dict[str, list[float]] = {}
    for s, rr in by_seg.items():
        if len(rr) < min_n:
            continue
        X = [[1.0, float(r["sim_profit"])] for r in rr]
        y = [float(r["act_profit"]) for r in rr]
        ab = _solve_ols(X, y)
        if ab is not None:
            by_seg_ab[str(s)] = [float(ab[0]), float(ab[1])]
    return {"by_seg": by_seg_ab, "global_ab": [float(g_ab[0]), float(g_ab[1])]}


def _predict_per_segment_linear(state: dict, row: dict) -> float:
    ab = state.get("by_seg", {}).get(str(int(row["S_rank"])), state.get("global_ab", [0.0, 1.0]))
    return float(ab[0]) + float(ab[1]) * float(row["sim_profit"])


def _fit_segment_feature_linear(rows: list[dict], segments: list[int]) -> dict:
    """Single OLS with one-hot S-rank (baseline = lowest rank) + interactions with sim_profit."""
    sorted_segs = sorted(segments)
    if not sorted_segs:
        return {"b": [0.0, 1.0], "baseline": 0, "other": [], "failed": True}
    baseline = sorted_segs[0]
    other = [s for s in sorted_segs if s != baseline]

    def make_row(r: dict) -> list[float]:
        row = [1.0, float(r["sim_profit"])]
        row.extend(1.0 if int(r["S_rank"]) == s else 0.0 for s in other)
        row.extend(
            (1.0 if int(r["S_rank"]) == s else 0.0) * float(r["sim_profit"])
            for s in other
        )
        return row

    X = [make_row(r) for r in rows]
    y = [float(r["act_profit"]) for r in rows]
    b = _solve_ols(X, y)
    return {
        "b": b if b is not None else [0.0, 1.0] + [0.0] * (2 * len(other)),
        "baseline": int(baseline),
        "other": [int(s) for s in other],
        "failed": b is None,
    }


def _predict_segment_feature_linear(state: dict, row: dict) -> float:
    if state.get("failed"):
        return float(row["sim_profit"])
    b = state["b"]
    other = state["other"]
    s = int(row["S_rank"])
    sim = float(row["sim_profit"])
    val = float(b[0]) + float(b[1]) * sim
    for i, seg in enumerate(other):
        d = 1.0 if s == seg else 0.0
        val += float(b[2 + i]) * d
    for i, seg in enumerate(other):
        d = 1.0 if s == seg else 0.0
        val += float(b[2 + len(other) + i]) * d * sim
    return val


MODEL_ZOO: dict[str, tuple] = {
    "identity": (_fit_identity, _predict_identity),
    "global_ratio": (_fit_global_ratio, _predict_global_ratio),
    "global_linear": (_fit_global_linear, _predict_global_linear),
    "per_segment_ratio": (_fit_per_segment_ratio, _predict_per_segment_ratio),
    "per_segment_linear": (_fit_per_segment_linear, _predict_per_segment_linear),
    "segment_feature_linear": (None, _predict_segment_feature_linear),
}


def _fit_model(name: str, train: list[dict], segments: list[int]) -> dict:
    if name == "segment_feature_linear":
        return _fit_segment_feature_linear(train, segments)
    fit_fn, _ = MODEL_ZOO[name]
    return fit_fn(train)  # type: ignore[misc]


def _predict_model(name: str, state: dict, row: dict) -> float:
    _, pred_fn = MODEL_ZOO[name]
    return float(pred_fn(state, row))


# -- Calibration dataset + CV ------------------------------------------------


def build_calibration_dataset(
    houses: list[dict], batch: dict[str, Any], segment_model: dict[str, Any]
) -> dict[str, Any]:
    """Intersect batch UIDs with fleet homes having complete segment features; attach S-rank."""
    if not segment_model.get("available"):
        return {"available": False, "reason": "segment_model_unavailable", "n": 0, "rows": []}
    means = segment_model["means"]
    stds = segment_model["stds"]
    centroids = segment_model["centroids_z"]
    rank_map = {int(k): int(v) for k, v in segment_model["centroid_to_S_rank"].items()}
    houses_by_uid = {str(h.get("house_id") or ""): h for h in houses if h.get("house_id")}

    rows: list[dict[str, Any]] = []
    for uid in sorted(batch.keys()):
        rec = batch.get(uid)
        if not isinstance(rec, dict) or rec.get("error"):
            continue
        opt = rec.get("optimised") or {}
        act = rec.get("actual") or {}
        if not isinstance(opt, dict) or not isinstance(act, dict):
            continue
        try:
            sn = float(opt["net_energy_cost"])
            an = float(act["net_energy_cost"])
        except (KeyError, TypeError, ValueError):
            continue
        h = houses_by_uid.get(uid)
        if not h:
            continue
        vec, missing = _extract_home_feature_vector(h)
        if missing:
            continue
        z = [(float(vec[i]) - means[i]) / stds[i] for i in range(len(vec))]  # type: ignore[arg-type]
        dists = _centroid_distances(z, centroids)
        raw = min(range(len(dists)), key=lambda c: dists[c])
        s_rank = rank_map[raw]
        sim_p = -sn
        act_p = -an
        sz = _extract_system_size(rec)
        rows.append(
            {
                "house_id": uid,
                "sim_profit": round(sim_p, 4),
                "act_profit": round(act_p, 4),
                "residual": round(act_p - sim_p, 4),
                "S_rank": int(s_rank),
                "battery_kwh": float(sz.get("battery_kwh", 0.0)),
                "solar_kw": float(sz.get("solar_kw", 0.0)),
            }
        )
    return {
        "available": True,
        "n": len(rows),
        "rows": rows,
        "k": int(segment_model.get("k", 4)),
    }


def _hash_fold(uid: str, n_folds: int) -> int:
    h = hashlib.sha1(uid.encode("utf-8")).digest()
    return h[0] % max(1, n_folds)


def _residual_stats(res: list[float]) -> dict[str, float]:
    if not res:
        return {"n": 0}
    srt = sorted(res)
    n = len(srt)
    mean = sum(srt) / n
    var = sum((x - mean) ** 2 for x in srt) / max(n - 1, 1) if n > 1 else 0.0
    return {
        "n": n,
        "p05": round(_quantile_linear(srt, 0.05), 2),
        "p10": round(_quantile_linear(srt, 0.10), 2),
        "p25": round(_quantile_linear(srt, 0.25), 2),
        "p50": round(_quantile_linear(srt, 0.50), 2),
        "p75": round(_quantile_linear(srt, 0.75), 2),
        "p90": round(_quantile_linear(srt, 0.90), 2),
        "p95": round(_quantile_linear(srt, 0.95), 2),
        "mean": round(mean, 2),
        "std": round(math.sqrt(max(var, 0.0)), 2),
    }


def cv_evaluate(dataset: dict, n_folds: int = 5) -> dict:
    """5-fold CV by house_id hash; MAE on $ profit is the headline selector."""
    rows = dataset.get("rows", [])
    if not rows:
        return {"n_rows": 0, "models": [], "winner": "identity", "beats_baseline": False}
    segments = sorted({int(r["S_rank"]) for r in rows})
    folds: list[list[dict]] = [[] for _ in range(n_folds)]
    for r in rows:
        folds[_hash_fold(str(r["house_id"]), n_folds)].append(r)

    results: list[dict] = []
    for name in MODEL_ZOO.keys():
        fold_maes: list[float] = []
        fold_rmses: list[float] = []
        residuals_by_seg: dict[int, list[float]] = {}
        residuals_all: list[float] = []
        for f in range(n_folds):
            test = folds[f]
            train = [r for g in range(n_folds) if g != f for r in folds[g]]
            if not train or not test:
                continue
            state = _fit_model(name, train, segments)
            abs_errs: list[float] = []
            sq_errs: list[float] = []
            for r in test:
                yhat = _predict_model(name, state, r)
                e = float(r["act_profit"]) - yhat
                abs_errs.append(abs(e))
                sq_errs.append(e * e)
                residuals_all.append(e)
                residuals_by_seg.setdefault(int(r["S_rank"]), []).append(e)
            if abs_errs:
                fold_maes.append(sum(abs_errs) / len(abs_errs))
                fold_rmses.append(math.sqrt(sum(sq_errs) / len(sq_errs)))
        if not fold_maes:
            continue
        mean_mae = sum(fold_maes) / len(fold_maes)
        if len(fold_maes) > 1:
            sd_mae = math.sqrt(
                sum((m - mean_mae) ** 2 for m in fold_maes) / (len(fold_maes) - 1)
            )
        else:
            sd_mae = 0.0
        mean_rmse = sum(fold_rmses) / len(fold_rmses)
        results.append(
            {
                "name": name,
                "mean_cv_mae": round(mean_mae, 2),
                "sd_cv_mae": round(sd_mae, 2),
                "mean_cv_rmse": round(mean_rmse, 2),
                "stability": round(sd_mae / mean_mae, 3) if mean_mae > 0 else 0.0,
                "n_folds": len(fold_maes),
                "residuals_overall": _residual_stats(residuals_all),
                "residuals_by_seg": {
                    f"S{s}": _residual_stats(rs) for s, rs in sorted(residuals_by_seg.items())
                },
            }
        )

    stable = [r for r in results if r["stability"] <= 0.5]
    pool = stable if stable else results
    winner = min(pool, key=lambda r: r["mean_cv_mae"]) if pool else None
    baseline = next((r for r in results if r["name"] == "identity"), None)
    beats_baseline = False
    if winner and baseline and winner["name"] != "identity":
        if winner["mean_cv_mae"] <= baseline["mean_cv_mae"] * 0.95:
            beats_baseline = True
    if winner and not beats_baseline and baseline:
        winner = baseline

    return {
        "n_rows": len(rows),
        "n_folds": n_folds,
        "segments": segments,
        "models": results,
        "winner": winner["name"] if winner else "identity",
        "beats_baseline": bool(beats_baseline),
    }


def predict_with_reliability(
    features: dict,
    sim_profit: float,
    artefact: dict,
) -> dict:
    """Run segment assignment then calibrated prediction + reliability band.

    Uses soft-weighted top-K residuals (from CV report) when assignment is confident,
    else hard-segment residuals widened to p05/p95 with an explicit low-confidence flag.
    """
    seg_model = artefact.get("segment_model") or {}
    calib = artefact.get("calibration") or {}
    winner_name = str(calib.get("winner") or "identity")
    state = calib.get("winner_state") or {}
    cv_report = calib.get("cv_report") or {}
    models_meta = {m["name"]: m for m in cv_report.get("models", [])}
    winner_meta = models_meta.get(winner_name, {})
    residuals_by_seg = winner_meta.get("residuals_by_seg", {})
    residuals_overall = winner_meta.get("residuals_overall", {})

    assignment = assign_home_to_segment(features, seg_model)
    out: dict[str, Any] = {
        "assignment": assignment,
        "winner_model": winner_name,
        "winner_cv_mae": winner_meta.get("mean_cv_mae"),
        "beats_baseline": bool(cv_report.get("beats_baseline", False)),
        "calibration_n_rows": calib.get("n_rows", 0),
    }

    if not assignment.get("assigned"):
        out.update(
            {
                "sim_profit": round(float(sim_profit), 2),
                "expected_actual_profit": round(float(sim_profit), 2),
                "delta_vs_sim": 0.0,
                "reliability": {"available": False, "reason": "no_segment_model"},
            }
        )
        return out

    row_for_predict = {
        "sim_profit": float(sim_profit),
        "S_rank": int(assignment["primary_segment"]),
    }
    try:
        yhat = _predict_model(winner_name, state, row_for_predict)
    except Exception:  # noqa: BLE001 — defensive against malformed artefacts
        yhat = float(sim_profit)

    # Reliability band.
    reliability: dict[str, Any] = {"available": False}
    primary = int(assignment["primary_segment"])
    secondary = int(assignment["secondary_segment"])
    low_conf = bool(assignment.get("low_confidence")) or bool(
        assignment.get("far_from_fleet")
    )
    ambiguous = bool(assignment.get("ambiguous"))

    def _band_from_residuals(stats: dict, qlo: str, qhi: str) -> tuple[float, float] | None:
        if not stats or stats.get("n", 0) < 5:
            return None
        lo = stats.get(qlo)
        hi = stats.get(qhi)
        if lo is None or hi is None:
            return None
        return float(lo), float(hi)

    if low_conf:
        stats = residuals_by_seg.get(f"S{primary}") or residuals_overall
        band = _band_from_residuals(stats, "p05", "p95") or _band_from_residuals(
            residuals_overall, "p05", "p95"
        )
        reliability = {
            "available": band is not None,
            "mode": "hard_segment_widened_p05_p95",
            "segment": f"S{primary}",
            "residual_band": {"low": band[0], "high": band[1]} if band else None,
        }
    elif ambiguous:
        # Blend top-2 segments with their soft weights.
        w = assignment.get("soft_weights_by_rank", {})
        primary_w = float(w.get(f"S{primary}", 0.5))
        secondary_w = float(w.get(f"S{secondary}", 0.5))
        ws = primary_w + secondary_w
        if ws <= 0:
            ws = 1.0
        primary_w /= ws
        secondary_w /= ws
        stats_p = residuals_by_seg.get(f"S{primary}") or residuals_overall
        stats_s = residuals_by_seg.get(f"S{secondary}") or residuals_overall
        band_p = _band_from_residuals(stats_p, "p10", "p90")
        band_s = _band_from_residuals(stats_s, "p10", "p90")
        if band_p and band_s:
            lo = primary_w * band_p[0] + secondary_w * band_s[0]
            hi = primary_w * band_p[1] + secondary_w * band_s[1]
            reliability = {
                "available": True,
                "mode": "soft_weighted_top2_p10_p90",
                "segments": [f"S{primary}", f"S{secondary}"],
                "weights": {f"S{primary}": round(primary_w, 3), f"S{secondary}": round(secondary_w, 3)},
                "residual_band": {"low": round(lo, 2), "high": round(hi, 2)},
            }
        else:
            band = band_p or band_s
            reliability = {
                "available": band is not None,
                "mode": "hard_segment_p10_p90",
                "segment": f"S{primary}",
                "residual_band": {"low": band[0], "high": band[1]} if band else None,
            }
    else:
        stats = residuals_by_seg.get(f"S{primary}") or residuals_overall
        band = _band_from_residuals(stats, "p10", "p90")
        reliability = {
            "available": band is not None,
            "mode": "hard_segment_p10_p90",
            "segment": f"S{primary}",
            "residual_band": {"low": band[0], "high": band[1]} if band else None,
        }

    expected = round(float(yhat), 2)
    band = (reliability or {}).get("residual_band") or None
    interval: dict[str, Any] | None = None
    if band:
        interval = {
            "low": round(expected + float(band["low"]), 2),
            "high": round(expected + float(band["high"]), 2),
            "center": expected,
        }

    out.update(
        {
            "sim_profit": round(float(sim_profit), 2),
            "expected_actual_profit": expected,
            "delta_vs_sim": round(expected - float(sim_profit), 2),
            "reliability": reliability,
            "prediction_interval": interval,
            "n_segments_model": int(seg_model.get("k", 0)),
        }
    )
    return out


# -- Artefact persistence + refresh -----------------------------------------


def save_calibration_artefact(payload: dict) -> Path:
    CACHE_DIR.mkdir(exist_ok=True)
    CALIBRATION_ARTEFACT.write_text(json.dumps(payload), encoding="utf-8")
    return CALIBRATION_ARTEFACT


def load_calibration_artefact() -> dict | None:
    if not CALIBRATION_ARTEFACT.is_file():
        return None
    try:
        raw = json.loads(CALIBRATION_ARTEFACT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None
    if not isinstance(raw, dict) or raw.get("schema") != CALIBRATION_SCHEMA_VERSION:
        return None
    return raw


def _summarise_fleet_houses(source_id: str) -> list[dict]:
    """Run `summarize_csv` across all CSVs under the given Catan source."""
    root = catan_root_for_source(source_id)
    paths, _disc = discover_csv_paths_with_meta(root)
    if not paths:
        return []
    houses: list[dict] = []
    max_workers = min(len(paths), max(1, (os.cpu_count() or 2) - 1))
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(summarize_csv, p, source_id): p for p in paths}
        for fut in as_completed(futures):
            try:
                row = fut.result()
                row["ok"] = True
                houses.append(row)
            except Exception:  # noqa: BLE001 — per-file errors are non-fatal
                pass
    return [h for h in houses if h.get("ok", True)]


def refresh_calibration_reference(source_id: str | None = None) -> dict:
    """Rebuild the persisted segment model + calibration winner from disk.

    Uses ``CATAN_SOURCE_NO_VIC`` by default to match the batch comparison scope.
    """
    src = source_id or CATAN_SOURCE_NO_VIC
    ok_houses = _summarise_fleet_houses(src)
    if not ok_houses:
        return {"ok": False, "error": "no_houses_summarised", "source": src}

    seg_model = fit_segment_model(ok_houses, k=4, iterations=24)
    if not seg_model.get("available"):
        return {
            "ok": False,
            "error": seg_model.get("reason", "segment_fit_failed"),
            "source": src,
        }

    batch: dict[str, Any] = {}
    if COMPARISON_BATCH_JSON.is_file():
        try:
            loaded = json.loads(COMPARISON_BATCH_JSON.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                batch = loaded
        except (OSError, json.JSONDecodeError, UnicodeError):
            batch = {}

    dataset = build_calibration_dataset(ok_houses, batch, seg_model)
    cv_report: dict[str, Any]
    winner_name = "identity"
    winner_state: dict[str, Any] | None = None
    if dataset.get("available") and dataset.get("n", 0) >= 20:
        cv_report = cv_evaluate(dataset, n_folds=5)
        winner_name = cv_report.get("winner", "identity")
        winner_state = _fit_model(winner_name, dataset["rows"], cv_report.get("segments", []))
    else:
        cv_report = {
            "n_rows": dataset.get("n", 0),
            "models": [],
            "warning": "insufficient_calibration_data_min_20_rows",
            "winner": "identity",
        }

    artefact = {
        "schema": CALIBRATION_SCHEMA_VERSION,
        "created_utc": date.today().isoformat(),
        "source": src,
        "segment_model": seg_model,
        "calibration": {
            "winner": winner_name,
            "winner_state": winner_state,
            "cv_report": cv_report,
            "n_rows": dataset.get("n", 0),
        },
    }
    save_calibration_artefact(artefact)
    try:
        rel_path = str(CALIBRATION_ARTEFACT.relative_to(BASE_DIR.parent))
    except ValueError:
        rel_path = str(CALIBRATION_ARTEFACT)
    return {"ok": True, "artefact_path": rel_path, "artefact": artefact}


# -- New-home feature extraction for calibrated prediction inputs -----------


def summarize_csv_for_new_home(csv_path: Path, cache_key: str = "calibrated_input") -> dict:
    """Reuse `summarize_csv` on an uploaded CSV and keep only the fields needed downstream."""
    return summarize_csv(csv_path, cache_key)


def new_home_features_from_uid(uid: str, source_id: str) -> dict | None:
    """Locate optimisation_intervals.csv under the chosen Catan source and summarise it."""
    root = catan_root_for_source(source_id)
    csv_path = root / uid / CSV_NAME
    if not csv_path.is_file():
        return None
    try:
        return summarize_csv(csv_path, source_id)
    except Exception:  # noqa: BLE001
        return None


def sse_event(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


@app.route("/")
def index():
    catalog = catan_source_catalog()
    n_files = sum(s["n_files"] for s in catalog)
    default_source = default_catan_source(catalog)
    return render_template(
        "index.html",
        n_files=n_files,
        catan_sources=catalog,
        default_source=default_source,
        vic_blocklist=vic_blocklist_summary(),
        demographic_doc=demographic_augmentation_documentation(),
    )


@app.route("/api/stream")
def api_stream():
    source_id = normalize_catan_source(request)
    root = catan_root_for_source(source_id)

    @stream_with_context
    def generate():
        paths, disc_meta = discover_csv_paths_with_meta(root)
        if not paths:
            yield sse_event(
                {
                    "type": "error",
                    "message": f"No CSV files found under {root} (source={source_id})",
                }
            )
            return

        total = len(paths)
        houses: list[dict] = []
        yield sse_event(
            {
                "type": "start",
                "total": total,
                "discovery": disc_meta,
                **catan_source_meta(source_id),
            }
        )

        # Parallelize per-home CSV processing to use all CPU cores.
        max_workers = min(total, max(1, (os.cpu_count() or 2) - 1))
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(summarize_csv, p, source_id): p for p in paths}
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
        loss_dive = loss_making_deep_dive(ok_houses)
        fleet_price_curve = fleet_aggregate_price_curve(ok_houses)
        trade_timing = fleet_trade_timing(ok_houses)
        fleet_df = houses_to_dataframe(houses)
        yield sse_event(
            {
                "type": "complete",
                "discovery": disc_meta,
                **catan_source_meta(source_id),
                "fleet": fleet,
                "houses": houses,
                "deciles": deciles,
                "size_profile": size_profile,
                "time_profile": time_profile,
                "predictors": predictors,
                "segments": segments,
                "loss_dive": loss_dive,
                "fleet_price_curve": fleet_price_curve,
                "trade_timing": trade_timing,
                "msc_counterfactual": msc_config_payload(),
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


@app.route("/fleet-prices")
def fleet_prices_page():
    catalog = catan_source_catalog()
    default_source = default_catan_source(catalog)
    return render_template(
        "fleet_prices.html",
        catan_sources=catalog,
        default_source=default_source,
    )


@app.route("/api/fleet-prices")
def api_fleet_prices():
    """Aggregate volume-weighted import/export $/kWh by month and by day across all homes for one Catan export set."""
    source_id = normalize_catan_source(request)
    root = catan_root_for_source(source_id)
    paths, disc_meta = discover_csv_paths_with_meta(root)
    if not paths:
        curve = fleet_aggregate_price_curve([])
        return jsonify(
            {
                **catan_source_meta(source_id),
                "discovery": disc_meta,
                **curve,
                "error": "no_csv",
            }
        )
    houses: list[dict] = []
    max_workers = min(len(paths), max(1, (os.cpu_count() or 2) - 1))
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(summarize_csv, p, source_id): p for p in paths}
        for fut in as_completed(futures):
            try:
                row = fut.result()
                row["ok"] = True
                houses.append(row)
            except Exception:  # noqa: BLE001
                pass
    ok_houses = [h for h in houses if h.get("ok", True)]
    curve = fleet_aggregate_price_curve(ok_houses)
    return jsonify({**catan_source_meta(source_id), "discovery": disc_meta, **curve})


@app.route("/fleet-headline-margin")
def fleet_headline_margin_page():
    catalog = catan_source_catalog()
    default_source = default_catan_source(catalog)
    return render_template(
        "fleet_headline_margin.html",
        catan_sources=catalog,
        default_source=default_source,
    )


@app.route("/api/fleet-headline-margin")
def api_fleet_headline_margin():
    """
    For a headline margin % (same definition as fleet summaries), split homes into natural
    achievers vs below-hurdle; for the latter, implied monthly fee to close the dollar gap
    over months with data.
    """
    source_id = normalize_catan_source(request)
    root = catan_root_for_source(source_id)
    paths, disc_meta = discover_csv_paths_with_meta(root)
    try:
        target_margin_pct = float(request.args.get("target_pct", "5"))
    except (TypeError, ValueError):
        target_margin_pct = 5.0
    basis = (request.args.get("basis") or "activity").lower().strip()
    if basis not in ("activity", "revenue"):
        basis = "activity"

    if not paths:
        empty = headline_margin_analysis([], target_margin_pct, basis)
        return jsonify(
            {
                **catan_source_meta(source_id),
                "discovery": disc_meta,
                **empty,
                "error": "no_csv",
            }
        )

    houses: list[dict] = []
    max_workers = min(len(paths), max(1, (os.cpu_count() or 2) - 1))
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(summarize_csv, p, source_id): p for p in paths}
        for fut in as_completed(futures):
            try:
                row = fut.result()
                row["ok"] = True
                houses.append(row)
            except Exception:  # noqa: BLE001
                pass
    ok_houses = [h for h in houses if h.get("ok", True)]
    sim_analysis = headline_margin_analysis(ok_houses, target_margin_pct, basis)

    delivery_info = _load_fleet_delivery_rates()
    adjusted_houses = reality_adjusted_houses(ok_houses, delivery_info)
    adj_analysis = headline_margin_analysis(adjusted_houses, target_margin_pct, basis)

    adj_summary: dict[str, Any] = {"available": delivery_info.get("available", False)}
    if delivery_info.get("available"):
        adj_summary.update({
            "fleet_multiplier": delivery_info["fleet_multiplier"],
            "fleet_p25_mult": delivery_info["fleet_p25_mult"],
            "fleet_p75_mult": delivery_info["fleet_p75_mult"],
            "n_homes_in_model": delivery_info.get("n_homes_in_model", 0),
            "sign_flip": delivery_info.get("sign_flip", {}),
            "analysis": adj_analysis,
        })

    return jsonify({
        **catan_source_meta(source_id),
        "discovery": disc_meta,
        **sim_analysis,
        "reality_adjusted": adj_summary,
    })


@app.route("/api/batch-comparison")
def api_batch_comparison():
    """
    Static batch file: per-UID economics (see Comparison/batch_catan_comparison.json).

    Each record normally has ``optimised`` (simulated / best-case Reposit) and ``actual``
    (billed / with Reposit). An optional third block — same keys as ``actual`` — may appear
    under ``retail_without_trade`` (aliases: ``no_trade``, ``without_reposit``, ``retail_only``,
    ``counterfactual_retail_no_trade``) for **retail without trade-back** counterfactual totals.
    """
    if not COMPARISON_BATCH_JSON.is_file():
        return jsonify({"loaded": False, "error": "file_missing", "uids": {}})
    try:
        raw = json.loads(COMPARISON_BATCH_JSON.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return jsonify({"loaded": False, "error": "read_or_parse_failed", "uids": {}})
    if not isinstance(raw, dict):
        return jsonify({"loaded": False, "error": "bad_shape", "uids": {}})
    raw = augment_batch_with_msc_counterfactual(raw)
    try:
        rel = str(COMPARISON_BATCH_JSON.relative_to(REPO_ROOT))
    except ValueError:
        rel = str(COMPARISON_BATCH_JSON)
    return jsonify(
        {
            "loaded": True,
            "relative_path": rel,
            "n_uids": len(raw),
            "msc_config": msc_config_payload(),
            "uids": raw,
        }
    )


@app.route("/api/prediction-accuracy")
def api_prediction_accuracy():
    """
    Model-style summary: how well simulated ``net_energy_cost`` (as negated profit) predicts
    billed outcomes; optimism bias and spread. Optional ``source`` limits to homes present
    in that Catan export set.
    """
    if not COMPARISON_BATCH_JSON.is_file():
        return jsonify({"loaded": False, "error": "file_missing"})
    try:
        batch = json.loads(COMPARISON_BATCH_JSON.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return jsonify({"loaded": False, "error": "read_or_parse_failed"})
    if not isinstance(batch, dict):
        return jsonify({"loaded": False, "error": "bad_shape"})
    batch = augment_batch_with_msc_counterfactual(batch)

    restrict: frozenset[str] | None = None
    disc_meta: dict[str, int | str | None] = {}
    meta_extra: dict[str, str | int | dict] = {}
    if request.args.get("source"):
        source_id = normalize_catan_source(request)
        root = catan_root_for_source(source_id)
        paths, disc_meta = discover_csv_paths_with_meta(root)
        restrict = frozenset(p.parent.name for p in paths)
        meta_extra = {**catan_source_meta(source_id), "discovery": disc_meta}

    dive = prediction_accuracy_deep_dive(batch, restrict)
    return jsonify(
        {
            "loaded": True,
            "msc_config": msc_config_payload(),
            **meta_extra,
            **dive,
        }
    )


# ---------------------------------------------------------------------------
# Calibrated prediction routes (subset page under fleet summary)
# ---------------------------------------------------------------------------

MAX_CALIBRATED_UPLOAD_BYTES = 48 * 1024 * 1024
CALIBRATED_UPLOAD_ROOT = CACHE_DIR / "calibrated_prediction_uploads"


def _artefact_status_payload() -> dict:
    artefact = load_calibration_artefact()
    if not artefact:
        return {"available": False}
    seg = artefact.get("segment_model", {})
    calib = artefact.get("calibration", {})
    cv_report = calib.get("cv_report", {}) or {}
    models = cv_report.get("models", []) or []
    model_summary = [
        {
            "name": m.get("name"),
            "mean_cv_mae": m.get("mean_cv_mae"),
            "sd_cv_mae": m.get("sd_cv_mae"),
            "stability": m.get("stability"),
        }
        for m in models
    ]
    return {
        "available": True,
        "created_utc": artefact.get("created_utc"),
        "source": artefact.get("source"),
        "segment_model": {
            "available": bool(seg.get("available")),
            "k": seg.get("k"),
            "fleet_hash": seg.get("fleet_hash"),
            "n_homes": seg.get("n_homes"),
            "feature_order": seg.get("feature_order"),
            "S_rank_meta": seg.get("S_rank_meta"),
            "ambiguity_thresholds": seg.get("ambiguity_thresholds"),
        },
        "calibration": {
            "winner": calib.get("winner"),
            "n_rows": calib.get("n_rows"),
            "beats_baseline": cv_report.get("beats_baseline"),
            "segments": cv_report.get("segments"),
            "models": model_summary,
        },
    }


@app.route("/calibrated-prediction")
def calibrated_prediction_page():
    catalog = catan_source_catalog()
    default_source = default_catan_source(catalog)
    return render_template(
        "calibrated_prediction.html",
        catan_sources=catalog,
        default_source=default_source,
    )


@app.route("/api/calibration/status")
def api_calibration_status():
    return jsonify(_artefact_status_payload())


@app.route("/api/calibration/refresh", methods=["POST"])
def api_calibration_refresh():
    source_id = normalize_catan_source(request, default=CATAN_SOURCE_NO_VIC)
    result = refresh_calibration_reference(source_id)
    if not result.get("ok"):
        return jsonify(result), 400
    return jsonify({"ok": True, "status": _artefact_status_payload()})


def _predict_from_summary(house: dict, artefact: dict) -> dict:
    sim_profit = float(house.get("net_margin") or 0.0)
    features = {k: house.get(k) for k in SEGMENT_FEATURE_ORDER}
    payload = predict_with_reliability(features, sim_profit, artefact)
    payload["input_summary"] = {
        "house_id": house.get("house_id"),
        "n_rows": house.get("n_rows"),
        "start_date": house.get("start_date"),
        "end_date": house.get("end_date"),
        "gross_cost": house.get("gross_cost"),
        "gross_revenue": house.get("gross_revenue"),
        "net_margin": house.get("net_margin"),
        "max_battery_soc_kwh": house.get("max_battery_soc_kwh"),
        "max_solar_kw": house.get("max_solar_kw"),
        "avg_load_kw": house.get("avg_load_kw"),
        "evening_load_share_pct": house.get("evening_load_share_pct"),
    }
    return payload


@app.route("/api/calibrated-prediction/by-uid", methods=["POST"])
def api_calibrated_prediction_by_uid():
    artefact = load_calibration_artefact()
    if not artefact:
        return jsonify({"error": "no_reference_artefact", "hint": "POST /api/calibration/refresh first"}), 409
    uid = (request.form.get("uid") or request.args.get("uid") or "").strip()
    if not uid:
        return jsonify({"error": "missing_uid"}), 400
    source_id = normalize_catan_source(request, default=CATAN_SOURCE_NO_VIC)
    house = new_home_features_from_uid(uid, source_id)
    if not house:
        return jsonify({"error": "uid_not_found", "source": source_id, "uid": uid}), 404
    return jsonify(_predict_from_summary(house, artefact))


@app.route("/api/calibrated-prediction/upload", methods=["POST"])
def api_calibrated_prediction_upload():
    artefact = load_calibration_artefact()
    if not artefact:
        return jsonify({"error": "no_reference_artefact", "hint": "POST /api/calibration/refresh first"}), 409

    fstor = request.files.get("file")
    if not fstor or not fstor.filename:
        return jsonify({"error": "missing_file"}), 400
    fmt = (request.form.get("format") or "optimiser").lower().strip()
    if fmt not in ("optimiser", "flexible"):
        fmt = "optimiser"
    try:
        default_import_price = float(request.form.get("default_import_price", "0.35"))
    except (TypeError, ValueError):
        default_import_price = 0.35
    try:
        default_export_price = float(request.form.get("default_export_price", "0.05"))
    except (TypeError, ValueError):
        default_export_price = 0.05

    CALIBRATED_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    up_id = uuid.uuid4().hex
    upload_dir = CALIBRATED_UPLOAD_ROOT / up_id
    upload_dir.mkdir(parents=True, exist_ok=True)
    raw_path = upload_dir / secure_filename(fstor.filename or "upload.csv")
    try:
        total = 0
        with raw_path.open("wb") as out:
            while True:
                chunk = fstor.stream.read(1 << 20)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_CALIBRATED_UPLOAD_BYTES:
                    return jsonify({"error": "file_too_large"}), 400
                out.write(chunk)

        out_csv = upload_dir / CSV_NAME
        conversion: dict[str, Any] = {}
        if fmt == "flexible":
            conversion = flexible_usage_csv_to_optimiser_csv(
                raw_path, out_csv, default_import_price, default_export_price
            )
            if not conversion.get("ok"):
                return (
                    jsonify(
                        {
                            "error": conversion.get("error", "convert_failed"),
                            "warnings": conversion.get("warnings", []),
                        }
                    ),
                    400,
                )
        else:
            with raw_path.open(newline="", encoding="utf-8", errors="replace") as rf:
                hdr = next(csv.reader(rf), None)
            if not hdr or "net_cost" not in hdr:
                return jsonify({"error": "optimiser_csv_must_include_net_cost_column"}), 400
            shutil.copyfile(raw_path, out_csv)

        try:
            house = summarize_csv(out_csv, "calibrated_prediction")
        except Exception as e:  # noqa: BLE001 — surfaced to client
            return jsonify({"error": "summarise_failed", "detail": str(e)}), 400

        payload = _predict_from_summary(house, artefact)
        payload["conversion"] = conversion if fmt == "flexible" else None
        return jsonify(payload)
    finally:
        shutil.rmtree(upload_dir, ignore_errors=True)


MAX_SOLAX_QUOTE_UPLOAD_BYTES = 48 * 1024 * 1024


@app.route("/solax-quote")
def solax_quote_page():
    return render_template("solax_quote.html")


@app.route("/api/solax-quote/schema")
def api_solax_quote_schema():
    """Document CSV layouts accepted by ``/api/solax-quote``."""
    return jsonify(
        {
            "optimiser_csv": {
                "filename": CSV_NAME,
                "required_columns": ["net_cost", "timestamp"],
                "optional_columns": OPTIMISER_INTERVALS_HEADER[2:]
                + list(_NO_TRADE_NET_COST_HEADER_NAMES),
                "description": (
                    "Same row layout as fleet optimisation_intervals.csv (after running the optimiser on a usage profile). "
                    "Optional counterfactual column ``net_cost_retail_no_trade`` (or aliases in _NO_TRADE_NET_COST_HEADER_NAMES) "
                    "uses the same sign convention as ``net_cost`` for retail / no-trade-back pricing per interval."
                ),
            },
            "flexible_usage_csv": {
                "required": "A timestamp column plus either net_cost OR import energy (kW or kWh per interval) with default or per-row prices.",
                "timestamp_aliases": ["timestamp", "time", "datetime", "date_time", "local_time", "date"],
                "net_cost_aliases": ["net_cost", "net_energy_cost", "interval_cost"],
                "import_aliases_kw": ["grid_import_kw", "import_kw", "grid_import", "import_power_kw", "mains_import_kw"],
                "import_aliases_kwh": ["import_kwh", "grid_import_kwh", "import_energy_kwh"],
                "export_aliases_kw": ["grid_export_kw", "export_kw", "export_power_kw", "feed_in_kw", "feedin_kw"],
                "export_aliases_kwh": ["export_kwh", "grid_export_kwh", "export_energy_kwh", "feed_in_kwh"],
                "price_aliases": {
                    "import_price": ["import_price", "buy_price", "retail_price", "import_tariff"],
                    "export_price": ["export_price", "feed_in_price", "sell_price", "export_tariff", "fit"],
                },
                "optional_load_solar_battery": [
                    "house_load_kw / load_kw / consumption_kw",
                    "solar_output_kw / pv_kw / solar_kw / generation_kw",
                    "battery_charge_kw",
                    "battery_discharge_kw",
                    "battery_soc_kwh / soc_kwh",
                ],
                "interval_hours": INTERVAL_HOURS,
                "note": "When net_cost is missing, each row is priced as import_price*import_kwh - export_price*export_kwh (grid only). Prefer optimiser CSV for dispatch-aware net_cost.",
            },
        }
    )


@app.route("/api/solax-quote", methods=["POST"])
def api_solax_quote():
    """
    Upload a year (or any window) of interval CSV, summarise like the fleet tool, then return
    implied monthly fee for a target headline margin on **simulated** and **reality-adjusted** economics.
    """
    fstor = request.files.get("file")
    if not fstor or not fstor.filename:
        return jsonify({"error": "missing_file"}), 400

    try:
        target_margin_pct = float(request.form.get("target_pct", "25"))
    except (TypeError, ValueError):
        target_margin_pct = 25.0
    basis = (request.form.get("basis") or "activity").lower().strip()
    if basis not in ("activity", "revenue"):
        basis = "activity"
    fmt = (request.form.get("format") or "optimiser").lower().strip()
    if fmt not in ("optimiser", "flexible"):
        fmt = "optimiser"
    try:
        default_import_price = float(request.form.get("default_import_price", "0.35"))
    except (TypeError, ValueError):
        default_import_price = 0.35
    try:
        default_export_price = float(request.form.get("default_export_price", "0.05"))
    except (TypeError, ValueError):
        default_export_price = 0.05
    batt_override_raw = request.form.get("battery_kwh_override") or ""
    battery_kwh_override: float | None = None
    if batt_override_raw.strip():
        try:
            battery_kwh_override = float(batt_override_raw)
        except ValueError:
            return jsonify({"error": "bad_battery_kwh_override"}), 400

    QUOTE_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    uid = uuid.uuid4().hex
    upload_dir = QUOTE_UPLOAD_ROOT / uid
    upload_dir.mkdir(parents=True, exist_ok=True)
    raw_path = upload_dir / secure_filename(fstor.filename or "upload.csv")
    try:
        total = 0
        with raw_path.open("wb") as out:
            while True:
                chunk = fstor.stream.read(1 << 20)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_SOLAX_QUOTE_UPLOAD_BYTES:
                    return jsonify({"error": "file_too_large"}), 400
                out.write(chunk)

        out_csv = upload_dir / CSV_NAME
        conversion: dict[str, Any] = {}
        if fmt == "flexible":
            conversion = flexible_usage_csv_to_optimiser_csv(
                raw_path, out_csv, default_import_price, default_export_price
            )
            if not conversion.get("ok"):
                return jsonify({"error": conversion.get("error", "convert_failed"), "warnings": conversion.get("warnings", [])}), 400
        else:
            with raw_path.open(newline="", encoding="utf-8", errors="replace") as rf:
                r0 = csv.reader(rf)
                hdr = next(r0, None)
            if not hdr or "net_cost" not in hdr:
                return jsonify({"error": "optimiser_csv_must_include_net_cost_column"}), 400
            shutil.copyfile(raw_path, out_csv)

        try:
            house = summarize_csv(out_csv, "solax_quote")
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": "summarise_failed", "detail": str(e)}), 400

        quote = single_home_margin_quote(
            house,
            target_margin_pct,
            basis,
            battery_kwh_override=battery_kwh_override,
        )
        quote["conversion"] = conversion if fmt == "flexible" else None
        return jsonify(quote)
    finally:
        shutil.rmtree(upload_dir, ignore_errors=True)


if __name__ == "__main__":
    app.run(debug=True, port=5010, threaded=True)
