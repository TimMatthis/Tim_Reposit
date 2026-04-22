#!/usr/bin/env python3
"""
MSC peak-import verification script (read-only).

Replays the MSC (Modelled Solar-only Counterfactual) simulation per interval
for every home in the selected Catan source, but additionally buckets each
interval's grid import kWh and $ by YYYY-MM x TOU window
(AM peak / PM peak / midday / shoulder) so we can confirm that the large
winter bars in the "Monthly aggregate bill comparison" chart are driven by
real peak-window imports, not by a pricing / accounting artefact.

Outputs:
  - Console table: fleet month x TOU-window import kWh + $ summary.
  - CSV at ``Optisizer data/diagnostics/msc_peak_import_check.csv``
    (long format: one row per month x window).
  - CSV at ``Optisizer data/diagnostics/msc_peak_import_check_totals.csv``
    (one row per month, for tying back to the on-page chart).

Nothing in the web app is modified; this is a pure diagnostic.

Usage:
  python -m calculator.scripts.msc_peak_import_check                # fleet, no_vic
  python -m calculator.scripts.msc_peak_import_check --source with_vic
  python -m calculator.scripts.msc_peak_import_check --uids <uid1>,<uid2>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any


# -- Import production internals from app.py without changing them ------------

# The calculator package lives at "Optisizer data/calculator/app.py" which has
# a space in the path, so we manipulate sys.path to import it directly.
_SCRIPT_DIR = Path(__file__).resolve().parent
_CALCULATOR_DIR = _SCRIPT_DIR.parent
if str(_CALCULATOR_DIR) not in sys.path:
    sys.path.insert(0, str(_CALCULATOR_DIR))

import app as calc_app  # noqa: E402


# -- TOU-window classification (mirrors _msc_import_price_per_kwh) ------------


WINDOW_LABELS = ("peak_am", "peak_pm", "midday", "shoulder")


def _window_for_hour(hour: int, cfg: dict[str, Any]) -> str:
    """Mirror of ``_msc_import_price_per_kwh``'s branch order (first match wins)."""
    if int(cfg.get("peak_am_start_hour", 0)) <= hour < int(cfg.get("peak_am_end_hour", 0)):
        return "peak_am"
    if int(cfg["peak_start_hour"]) <= hour < int(cfg["peak_end_hour"]):
        return "peak_pm"
    if int(cfg["midday_start_hour"]) <= hour < int(cfg["midday_end_hour"]):
        return "midday"
    return "shoulder"


def _rate_for_window(window: str, cfg: dict[str, Any]) -> float:
    if window == "peak_am":
        return float(
            cfg.get("import_rate_peak_am_per_kwh", cfg["import_rate_peak_per_kwh"])
        )
    if window == "peak_pm":
        return float(cfg["import_rate_peak_per_kwh"])
    if window == "midday":
        return float(cfg["import_rate_midday_per_kwh"])
    return float(cfg["import_rate_shoulder_per_kwh"])


# -- Per-home worker ---------------------------------------------------------


def _empty_month_entry() -> dict[str, float]:
    """Per-month accumulators: kWh + $ per window plus totals."""
    d: dict[str, float] = {}
    for w in WINDOW_LABELS:
        d[f"kwh_{w}"] = 0.0
        d[f"cost_{w}"] = 0.0
    d["gross_import_cost"] = 0.0
    d["gross_export_revenue"] = 0.0
    d["import_kwh_total"] = 0.0
    d["export_kwh_total"] = 0.0
    d["load_kwh_total"] = 0.0
    d["solar_kwh_total"] = 0.0
    return d


def _simulate_one(
    csv_path_str: str,
    specs: dict[str, float],
    cfg: dict[str, Any],
) -> tuple[str, dict[str, dict[str, float]] | None, str | None]:
    """
    Replay MSC for one home, returning a dict keyed by YYYY-MM of accumulators.

    The per-interval physics is an exact mirror of the main simulator
    (``_simulate_inverter_msc_from_csv_with_specs`` in ``app.py``). We do not
    call that function here because we need to bucket imports by TOU window
    during the same pass; the main simulator keeps only per-month totals.
    """
    csv_path = Path(csv_path_str)
    try:
        stat = csv_path.stat()
    except OSError as e:
        return csv_path_str, None, f"stat_failed: {e}"

    cap = float(specs["capacity_kwh"])
    min_soc = float(specs["min_soc_kwh"])
    soc = float(specs["initial_soc_kwh"])
    charge_eff = float(specs["charge_efficiency"])
    discharge_eff = float(specs["discharge_efficiency"])

    interval_hours = float(calc_app.INTERVAL_HOURS)

    # Precompute rates per window so we're not re-reading the dict every interval.
    rate_by_window = {w: _rate_for_window(w, cfg) for w in WINDOW_LABELS}
    fit_rate = float(cfg["export_feed_in_tariff_per_kwh"])
    peak_am_start = int(cfg.get("peak_am_start_hour", 0))
    peak_am_end = int(cfg.get("peak_am_end_hour", 0))
    peak_pm_start = int(cfg["peak_start_hour"])
    peak_pm_end = int(cfg["peak_end_hour"])
    midday_start = int(cfg["midday_start_hour"])
    midday_end = int(cfg["midday_end_hour"])

    monthly: dict[str, dict[str, float]] = {}

    try:
        f = csv_path.open(newline="", encoding="utf-8", errors="replace")
    except OSError as e:
        return csv_path_str, None, f"open_failed: {e}"

    with f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return csv_path_str, None, "empty_csv"
        col = {name: i for i, name in enumerate(header)}
        idx_load = col.get("house_load_kw", -1)
        idx_solar = col.get("solar_output_kw", -1)
        idx_soc = col.get("battery_soc_kwh", -1)
        idx_ts = col.get("timestamp", -1)
        if idx_load < 0 and idx_solar < 0:
            return csv_path_str, None, "missing_load_and_solar_columns"

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
            for row in reader:
                if len(row) > idx_soc and row[idx_soc]:
                    try:
                        first = float(row[idx_soc])
                        soc = min(max(first, min_soc), cap)
                        break
                    except ValueError:
                        continue
            f.seek(0)
            reader = csv.reader(f)
            next(reader, None)

        for row in reader:
            ts_raw = _s(row, idx_ts)
            load_kwh = max(_f(row, idx_load), 0.0) * interval_hours
            solar_kwh = max(_f(row, idx_solar), 0.0) * interval_hours

            # Solar priority 1: solar to load.
            solar_to_load = min(solar_kwh, load_kwh)
            remaining_load = load_kwh - solar_to_load
            solar_excess = solar_kwh - solar_to_load

            # Solar priority 2: excess solar charges battery.
            if cap > 0.0 and solar_excess > 0.0 and charge_eff > 0.0:
                headroom = max(cap - soc, 0.0)
                charge_input_limit = headroom / charge_eff
                solar_to_batt = min(solar_excess, charge_input_limit)
                soc = min(cap, soc + solar_to_batt * charge_eff)
                solar_excess -= solar_to_batt

            # Battery discharge: to load only.
            if cap > 0.0 and remaining_load > 0.0 and discharge_eff > 0.0:
                deliverable = max(soc - min_soc, 0.0) * discharge_eff
                batt_to_load = min(remaining_load, deliverable)
                soc = max(min_soc, soc - (batt_to_load / discharge_eff))
                remaining_load -= batt_to_load

            grid_import_kwh = max(remaining_load, 0.0)
            grid_export_kwh = max(solar_excess, 0.0)

            # TOU window from hour-of-day (same rules as the pricing function).
            if len(ts_raw) >= 13 and ts_raw[11:13].isdigit():
                hour = int(ts_raw[11:13])
            else:
                hour = -1
            if peak_am_start <= hour < peak_am_end:
                window = "peak_am"
            elif peak_pm_start <= hour < peak_pm_end:
                window = "peak_pm"
            elif midday_start <= hour < midday_end:
                window = "midday"
            else:
                window = "shoulder"

            import_cost = grid_import_kwh * rate_by_window[window]
            export_revenue = grid_export_kwh * fit_rate

            month_key = ts_raw[:7] if len(ts_raw) >= 7 else ""
            m = monthly.get(month_key)
            if m is None:
                m = _empty_month_entry()
                monthly[month_key] = m
            m[f"kwh_{window}"] += grid_import_kwh
            m[f"cost_{window}"] += import_cost
            m["gross_import_cost"] += import_cost
            m["gross_export_revenue"] += export_revenue
            m["import_kwh_total"] += grid_import_kwh
            m["export_kwh_total"] += grid_export_kwh
            m["load_kwh_total"] += load_kwh
            m["solar_kwh_total"] += solar_kwh

    # Drop empty month key (rows with no timestamp) for cleanliness.
    monthly.pop("", None)
    return csv_path_str, monthly, None


# -- Fleet driver ------------------------------------------------------------


def _merge_into(
    fleet: dict[str, dict[str, float]],
    home_monthly: dict[str, dict[str, float]],
    active_by_month: dict[str, int],
) -> None:
    for month, entry in home_monthly.items():
        acc = fleet.get(month)
        if acc is None:
            acc = _empty_month_entry()
            fleet[month] = acc
        for k, v in entry.items():
            acc[k] = acc.get(k, 0.0) + v
        active_by_month[month] = active_by_month.get(month, 0) + 1


def _load_batch(batch_path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(batch_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}


def _specs_for_uid(batch: dict[str, Any], uid: str) -> dict[str, float]:
    rec = batch.get(uid)
    if isinstance(rec, dict):
        return calc_app._extract_battery_specs_for_msc(rec)  # noqa: SLF001
    return calc_app._extract_battery_specs_for_msc({})  # noqa: SLF001


def _source_paths(source_id: str) -> list[Path]:
    root = calc_app.catan_root_for_source(source_id)
    return calc_app.discover_csv_paths(root)


def _maybe_filter_uids(paths: list[Path], uids_filter: list[str] | None) -> list[Path]:
    if not uids_filter:
        return paths
    want = {u.strip().lower().replace("-", "") for u in uids_filter if u.strip()}
    out = []
    for p in paths:
        key = p.parent.name.lower().replace("-", "")
        if key in want:
            out.append(p)
    return out


def _run_serial(
    paths: list[Path],
    batch: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[dict[str, dict[str, float]], dict[str, int], list[tuple[str, str]]]:
    fleet: dict[str, dict[str, float]] = {}
    active_by_month: dict[str, int] = {}
    errors: list[tuple[str, str]] = []
    for i, p in enumerate(paths, 1):
        uid = p.parent.name
        specs = _specs_for_uid(batch, uid)
        _, monthly, err = _simulate_one(str(p), specs, cfg)
        if err:
            errors.append((uid, err))
            continue
        if monthly:
            _merge_into(fleet, monthly, active_by_month)
        if i % 25 == 0 or i == len(paths):
            print(f"  ... processed {i}/{len(paths)} homes", file=sys.stderr)
    return fleet, active_by_month, errors


def _run_parallel(
    paths: list[Path],
    batch: dict[str, Any],
    cfg: dict[str, Any],
    max_workers: int,
) -> tuple[dict[str, dict[str, float]], dict[str, int], list[tuple[str, str]]]:
    fleet: dict[str, dict[str, float]] = {}
    active_by_month: dict[str, int] = {}
    errors: list[tuple[str, str]] = []
    path_to_uid = {str(p): p.parent.name for p in paths}
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futs = []
        for p in paths:
            uid = path_to_uid[str(p)]
            specs = _specs_for_uid(batch, uid)
            futs.append(pool.submit(_simulate_one, str(p), specs, cfg))
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                csv_path_str, monthly, err = fut.result()
            except Exception as e:  # noqa: BLE001
                errors.append(("?", f"worker_exception: {e}"))
                continue
            uid = path_to_uid.get(csv_path_str, "?")
            if err:
                errors.append((uid, err))
                continue
            if monthly:
                _merge_into(fleet, monthly, active_by_month)
            if done % 25 == 0 or done == len(paths):
                print(f"  ... processed {done}/{len(paths)} homes", file=sys.stderr)
    return fleet, active_by_month, errors


# -- Reporting ---------------------------------------------------------------


def _print_table(
    fleet: dict[str, dict[str, float]],
    active_by_month: dict[str, int],
    winter_months: set[str],
) -> None:
    months = sorted(fleet.keys())
    if not months:
        print("No data aggregated.")
        return

    hdr = (
        f"{'month':<10}"
        f"{'n':>5}"
        f"{'impKWh':>12}"
        f"{'gross$':>12}"
        f"{'expKWh':>12}"
        f"{'fit$':>10}"
        f"{'net$':>12}"
        f"{'AMp$':>10}"
        f"{'PMp$':>10}"
        f"{'mid$':>10}"
        f"{'shldr$':>10}"
        f"{'PMp%':>7}"
    )
    print("\nFleet MSC monthly breakdown (imports by TOU window)")
    print(hdr)
    print("-" * len(hdr))
    total_import_kwh = 0.0
    total_gross_import = 0.0
    total_export_kwh = 0.0
    total_export_revenue = 0.0
    total_by_win = {w: 0.0 for w in WINDOW_LABELS}
    for m in months:
        e = fleet[m]
        gi = e["gross_import_cost"]
        ik = e["import_kwh_total"]
        ek = e["export_kwh_total"]
        er = e["gross_export_revenue"]
        net = gi - er
        amp = e["cost_peak_am"]
        pmp = e["cost_peak_pm"]
        mid = e["cost_midday"]
        sh = e["cost_shoulder"]
        pmp_pct = (pmp / gi * 100.0) if gi > 0 else 0.0
        flag = " *" if m in winter_months else ""
        print(
            f"{m:<10}"
            f"{active_by_month.get(m, 0):>5}"
            f"{ik:>12,.0f}"
            f"{gi:>12,.0f}"
            f"{ek:>12,.0f}"
            f"{er:>10,.0f}"
            f"{net:>12,.0f}"
            f"{amp:>10,.0f}"
            f"{pmp:>10,.0f}"
            f"{mid:>10,.0f}"
            f"{sh:>10,.0f}"
            f"{pmp_pct:>6.1f}%"
            f"{flag}"
        )
        total_import_kwh += ik
        total_gross_import += gi
        total_export_kwh += ek
        total_export_revenue += er
        for w in WINDOW_LABELS:
            total_by_win[w] += e[f"cost_{w}"]

    print("-" * len(hdr))
    total_net = total_gross_import - total_export_revenue
    pmp_pct_all = (total_by_win["peak_pm"] / total_gross_import * 100.0) if total_gross_import > 0 else 0.0
    print(
        f"{'TOTAL':<10}"
        f"{'':>5}"
        f"{total_import_kwh:>12,.0f}"
        f"{total_gross_import:>12,.0f}"
        f"{total_export_kwh:>12,.0f}"
        f"{total_export_revenue:>10,.0f}"
        f"{total_net:>12,.0f}"
        f"{total_by_win['peak_am']:>10,.0f}"
        f"{total_by_win['peak_pm']:>10,.0f}"
        f"{total_by_win['midday']:>10,.0f}"
        f"{total_by_win['shoulder']:>10,.0f}"
        f"{pmp_pct_all:>6.1f}%"
    )

    # Quick sanity: per-month sum of window $ must equal gross_import to cents.
    print("\nSanity check: per-window $ sum vs gross_import_cost (should be ~0)")
    max_abs = 0.0
    for m in months:
        e = fleet[m]
        s = e["cost_peak_am"] + e["cost_peak_pm"] + e["cost_midday"] + e["cost_shoulder"]
        diff = s - e["gross_import_cost"]
        if abs(diff) > abs(max_abs):
            max_abs = diff
    print(f"  max |window$ - gross_import$| across months: {max_abs:+.6f}")


def _write_csvs(
    out_dir: Path,
    fleet: dict[str, dict[str, float]],
    active_by_month: dict[str, int],
    cfg: dict[str, Any],
    source_id: str,
) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    long_path = out_dir / "msc_peak_import_check.csv"
    totals_path = out_dir / "msc_peak_import_check_totals.csv"

    rate_by_window = {w: _rate_for_window(w, cfg) for w in WINDOW_LABELS}

    months = sorted(fleet.keys())
    with long_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "source",
                "month",
                "window",
                "rate_per_kwh_usd",
                "import_kwh",
                "import_cost_usd",
                "n_homes_active",
                "fleet_monthly_import_kwh",
                "fleet_monthly_gross_import_usd",
                "fleet_monthly_export_kwh",
                "fleet_monthly_export_credit_usd",
                "fleet_monthly_net_bill_usd",
            ]
        )
        for m in months:
            e = fleet[m]
            gi = e["gross_import_cost"]
            ik = e["import_kwh_total"]
            ek = e["export_kwh_total"]
            er = e["gross_export_revenue"]
            net = gi - er
            for win in WINDOW_LABELS:
                w.writerow(
                    [
                        source_id,
                        m,
                        win,
                        f"{rate_by_window[win]:.6f}",
                        f"{e[f'kwh_{win}']:.6f}",
                        f"{e[f'cost_{win}']:.6f}",
                        active_by_month.get(m, 0),
                        f"{ik:.6f}",
                        f"{gi:.6f}",
                        f"{ek:.6f}",
                        f"{er:.6f}",
                        f"{net:.6f}",
                    ]
                )

    with totals_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "source",
                "month",
                "n_homes_active",
                "fleet_import_kwh",
                "fleet_gross_import_usd",
                "fleet_export_kwh",
                "fleet_export_credit_usd",
                "fleet_net_bill_usd",
                "peak_am_cost_usd",
                "peak_pm_cost_usd",
                "midday_cost_usd",
                "shoulder_cost_usd",
                "peak_am_kwh",
                "peak_pm_kwh",
                "midday_kwh",
                "shoulder_kwh",
            ]
        )
        for m in months:
            e = fleet[m]
            gi = e["gross_import_cost"]
            ik = e["import_kwh_total"]
            ek = e["export_kwh_total"]
            er = e["gross_export_revenue"]
            w.writerow(
                [
                    source_id,
                    m,
                    active_by_month.get(m, 0),
                    f"{ik:.6f}",
                    f"{gi:.6f}",
                    f"{ek:.6f}",
                    f"{er:.6f}",
                    f"{gi - er:.6f}",
                    f"{e['cost_peak_am']:.6f}",
                    f"{e['cost_peak_pm']:.6f}",
                    f"{e['cost_midday']:.6f}",
                    f"{e['cost_shoulder']:.6f}",
                    f"{e['kwh_peak_am']:.6f}",
                    f"{e['kwh_peak_pm']:.6f}",
                    f"{e['kwh_midday']:.6f}",
                    f"{e['kwh_shoulder']:.6f}",
                ]
            )

    return long_path, totals_path


# -- Main --------------------------------------------------------------------


def _default_diagnostics_dir() -> Path:
    return calc_app.OPTISIZER_DATA / "diagnostics"


def _default_winter_months() -> set[str]:
    # Matches the Apr-2025 to Apr-2026 window in the chart the user asked about.
    return {"2025-05", "2025-06", "2025-07", "2025-08"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument(
        "--source",
        default=calc_app.CATAN_SOURCE_NO_VIC,
        help="Catan source id (default: no_vic, matches the MSC page default).",
    )
    p.add_argument(
        "--uids",
        default="",
        help="Comma-separated UID filter; if set, only these UIDs are simulated.",
    )
    p.add_argument(
        "--out-dir",
        default=str(_default_diagnostics_dir()),
        help="Directory for CSV outputs.",
    )
    p.add_argument(
        "--serial",
        action="store_true",
        help="Force single-process execution (default: ProcessPoolExecutor).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
        help="Number of worker processes (default: cpu_count - 1).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    cfg = calc_app._msc_cfg_defaults()  # noqa: SLF001

    print("MSC configuration:")
    print(
        f"  AM peak  {cfg['peak_am_start_hour']:02d}-{cfg['peak_am_end_hour']:02d} "
        f"@ ${cfg['import_rate_peak_am_per_kwh']:.4f}/kWh"
    )
    print(
        f"  PM peak  {cfg['peak_start_hour']:02d}-{cfg['peak_end_hour']:02d} "
        f"@ ${cfg['import_rate_peak_per_kwh']:.4f}/kWh"
    )
    print(
        f"  midday   {cfg['midday_start_hour']:02d}-{cfg['midday_end_hour']:02d} "
        f"@ ${cfg['import_rate_midday_per_kwh']:.4f}/kWh"
    )
    print(f"  shoulder (rest)    @ ${cfg['import_rate_shoulder_per_kwh']:.4f}/kWh")
    print(f"  FiT (flat)         @ ${cfg['export_feed_in_tariff_per_kwh']:.4f}/kWh")

    try:
        paths = _source_paths(args.source)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    if not paths:
        print(f"error: no CSVs found under source {args.source!r}", file=sys.stderr)
        return 2

    uid_filter = [u for u in (args.uids.split(",") if args.uids else []) if u.strip()]
    if uid_filter:
        paths = _maybe_filter_uids(paths, uid_filter)
        if not paths:
            print(f"error: no CSVs matched --uids {args.uids!r}", file=sys.stderr)
            return 2

    batch = _load_batch(calc_app.COMPARISON_BATCH_JSON)
    if not batch:
        print(
            f"warning: could not load batch {calc_app.COMPARISON_BATCH_JSON}; "
            "battery specs will default to zero capacity.",
            file=sys.stderr,
        )

    print(f"\nSimulating {len(paths)} homes from source {args.source!r}...")

    if args.serial or len(paths) < 4:
        fleet, active, errors = _run_serial(paths, batch, cfg)
    else:
        fleet, active, errors = _run_parallel(
            paths, batch, cfg, max_workers=max(1, args.workers)
        )

    if errors:
        print(f"\n{len(errors)} home(s) skipped or failed:", file=sys.stderr)
        for uid, err in errors[:20]:
            print(f"  {uid}: {err}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... +{len(errors) - 20} more", file=sys.stderr)

    _print_table(fleet, active, _default_winter_months())

    out_dir = Path(args.out_dir)
    long_path, totals_path = _write_csvs(out_dir, fleet, active, cfg, args.source)
    print(f"\nWrote: {long_path}")
    print(f"Wrote: {totals_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
