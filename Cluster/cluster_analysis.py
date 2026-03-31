"""
Time-Series Cluster Analysis — Energy Usage Profiles
=====================================================
Pipeline:
  1. Load raw CSV  (one row per home, ~105,120 interval columns)
  2. Compute the Average Daily Profile  (288 points per home)
  3. Min-Max scale each profile individually  (shape-based clustering)
  4. KShape clustering via tslearn  (5 clusters)
  5. Visualise cluster centroids + individual traces
  6. Export home → cluster mapping as CSV
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import davies_bouldin_score, silhouette_score
from tslearn.clustering import KShape
from tslearn.utils import to_time_series_dataset


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INTERVALS_PER_DAY = 288          # default: 5-minute intervals in 24 hours
INTERVAL_MINUTES = 5
N_CLUSTERS = 5
RANDOM_SEED = 42
KSHAPE_N_INIT = 3
KSHAPE_MAX_ITER = 30

def _build_time_labels(interval_minutes: int) -> list[str]:
    labels: list[str] = []
    for minutes_since_midnight in range(0, 24 * 60, interval_minutes):
        h, m = divmod(minutes_since_midnight, 60)
        labels.append(f"{h:02d}:{m:02d}")
    return labels


TIME_LABELS = _build_time_labels(INTERVAL_MINUTES)

CENTROID_COLORS = [
    "#FF4D6D",  # bright red-pink
    "#74B3CE",  # bright sky blue
    "#2EC4B6",  # bright teal
    "#FFD166",  # bright yellow
    "#FF9F1C",  # bright orange
]


# ---------------------------------------------------------------------------
# Step 1 – Load data
# ---------------------------------------------------------------------------

def _set_time_resolution(interval_minutes: int) -> None:
    """
    Update global time resolution metadata used for profile shape and chart labels.
    """
    global INTERVAL_MINUTES, INTERVALS_PER_DAY, TIME_LABELS
    INTERVAL_MINUTES = int(interval_minutes)
    INTERVALS_PER_DAY = int(24 * 60 / INTERVAL_MINUTES)
    TIME_LABELS = _build_time_labels(INTERVAL_MINUTES)


def load_data(csv_path: str) -> tuple[pd.Series, pd.DataFrame]:
    """
    Load the raw CSV.

    Expected layout: first column is a home identifier; remaining columns are
    5-minute energy readings ordered chronologically across the full year.

    Returns
    -------
    home_ids : pd.Series
    readings : pd.DataFrame  (homes × time-intervals, numeric only)
    """
    _set_time_resolution(5)
    print(f"[1/5] Loading data from '{csv_path}' …")
    df = pd.read_csv(csv_path, index_col=0, engine="c")

    home_ids = pd.Series(df.index, name="home_id").reset_index(drop=True)
    try:
        readings = df.astype(np.float32, copy=False)
    except ValueError:
        # Fallback for mixed/dirty numeric columns.
        readings = df.apply(pd.to_numeric, errors="coerce").astype(np.float32, copy=False)

    print(f"      {len(home_ids)} homes × {readings.shape[1]} interval columns loaded.")
    return home_ids, readings


def load_average_daily_profiles(csv_path: str) -> tuple[pd.Series, np.ndarray]:
    """
    Fast path for large wide CSV files:
      - load numeric interval columns as float32
      - reshape to (homes, days, 288)
      - average across days

    Returns
    -------
    home_ids : pd.Series
    profiles : np.ndarray  shape (n_homes, 288), dtype float32
    """
    header = pd.read_csv(csv_path, nrows=0)
    long_format_cols = {"SiteUID", "timekey", "Consumption_kWh"}

    if long_format_cols.issubset(set(header.columns)):
        print(f"[1/5] Loading long-format data from '{csv_path}' …")
        df = pd.read_csv(csv_path, usecols=["SiteUID", "timekey", "Consumption_kWh"])
        df["Consumption_kWh"] = pd.to_numeric(df["Consumption_kWh"], errors="coerce")
        df = df.dropna(subset=["SiteUID", "timekey", "Consumption_kWh"])

        # Infer cadence from unique timestamps in the file.
        unique_timekeys = np.sort(df["timekey"].astype(np.int64).unique())
        if len(unique_timekeys) < 2:
            raise ValueError("Not enough timestamp values in 'timekey' to infer interval cadence.")

        step_seconds = int(np.median(np.diff(unique_timekeys)))
        if step_seconds <= 0 or 86400 % step_seconds != 0:
            raise ValueError(
                f"Unsupported cadence inferred from 'timekey': {step_seconds} seconds."
            )

        interval_minutes = step_seconds // 60
        _set_time_resolution(interval_minutes)
        print(
            f"      Detected long format with {INTERVALS_PER_DAY} intervals/day "
            f"({INTERVAL_MINUTES}-minute cadence)."
        )

        dt = pd.to_datetime(df["timekey"], unit="s")
        slot = (dt.dt.hour * 60 + dt.dt.minute) // INTERVAL_MINUTES
        df["slot"] = slot.astype(np.int32)

        grouped = (
            df.groupby(["SiteUID", "slot"], sort=True)["Consumption_kWh"]
            .mean()
            .unstack("slot")
            .reindex(columns=range(INTERVALS_PER_DAY))
        )

        home_ids = pd.Series(grouped.index.astype(str), name="home_id").reset_index(drop=True)
        profiles = grouped.to_numpy(dtype=np.float32, copy=False)

        print(f"      {len(home_ids)} homes loaded from long-format rows.")
        print(f"      Profiles shape: {profiles.shape}")
        return home_ids, np.nan_to_num(profiles, nan=0.0).astype(np.float32, copy=False)

    # Fallback: existing wide matrix format (one row per home).
    home_ids, readings = load_data(csv_path)
    profiles = compute_average_daily_profile(readings)
    return home_ids, profiles


# ---------------------------------------------------------------------------
# Step 2 – Average Daily Profile
# ---------------------------------------------------------------------------

def compute_average_daily_profile(readings: pd.DataFrame) -> np.ndarray:
    """
    Fold the year-long series into 288 representative slots by averaging every
    slot index across all days (i.e. column-wise mean over groups of INTERVALS_PER_DAY).

    Returns
    -------
    profiles : np.ndarray  shape (n_homes, 288)
    """
    print("[2/5] Computing average daily profiles …")

    n_homes, n_cols = readings.shape
    n_days = n_cols // INTERVALS_PER_DAY
    usable_cols = n_days * INTERVALS_PER_DAY

    if usable_cols < n_cols:
        print(f"      Trimming {n_cols - usable_cols} trailing columns "
              f"to fit {n_days} complete days.")

    raw = readings.iloc[:, :usable_cols].to_numpy(dtype=np.float32, copy=False)  # (n_homes, n_days * 288)

    # Reshape to (n_homes, n_days, 288) then mean over the days axis
    reshaped = raw.reshape(n_homes, n_days, INTERVALS_PER_DAY)
    if np.isnan(raw).any():
        profiles = np.nanmean(reshaped, axis=1, dtype=np.float32)
    else:
        profiles = reshaped.mean(axis=1, dtype=np.float32)

    print(f"      Profiles shape: {profiles.shape}")
    return profiles


# ---------------------------------------------------------------------------
# Step 3 – Per-row Min-Max scaling
# ---------------------------------------------------------------------------

def minmax_scale_rows(profiles: np.ndarray) -> np.ndarray:
    """
    Scale each home's profile independently to [0, 1].

    This removes absolute magnitude differences so that clustering captures
    behavioural *shape* rather than total consumption volume.
    """
    print("[3/5] Applying per-row Min-Max scaling …")

    row_min = np.nanmin(profiles, axis=1, keepdims=True)
    row_max = np.nanmax(profiles, axis=1, keepdims=True)
    denom = row_max - row_min

    # Flat profiles (zero range) remain 0 after scaling.
    zero_range = denom == 0
    safe_denom = np.where(zero_range, 1.0, denom)
    scaled = (profiles - row_min) / safe_denom

    if zero_range.any():
        print(f"      Warning: {zero_range.sum()} flat profiles detected.")
        scaled[zero_range.ravel(), :] = 0.0

    return np.nan_to_num(scaled, nan=0.0).astype(np.float32, copy=False)


# ---------------------------------------------------------------------------
# Step 4 – KShape clustering
# ---------------------------------------------------------------------------

def run_kshape(scaled_profiles: np.ndarray) -> tuple[KShape, np.ndarray]:
    """
    Fit KShape on the scaled daily profiles.

    KShape uses a normalised cross-correlation measure, making it naturally
    suited to shape-based time-series clustering.

    Returns
    -------
    model  : fitted KShape instance
    labels : np.ndarray of int, length n_homes
    """
    print(f"[4/5] Running KShape clustering (k={N_CLUSTERS}) …")

    # tslearn expects shape (n_series, n_timesteps, n_dimensions)
    ts_dataset = to_time_series_dataset(scaled_profiles.astype(np.float32, copy=False))

    model = KShape(
        n_clusters=N_CLUSTERS,
        n_init=KSHAPE_N_INIT,
        max_iter=KSHAPE_MAX_ITER,
        random_state=RANDOM_SEED,
        verbose=False,
    )
    labels = model.fit_predict(ts_dataset)

    for k in range(N_CLUSTERS):
        count = (labels == k).sum()
        print(f"      Cluster {k}: {count} homes")

    return model, labels


# ---------------------------------------------------------------------------
# Step 4a – Auto-detect optimal k via silhouette sweep
# ---------------------------------------------------------------------------

def find_optimal_k(
    scaled_profiles: np.ndarray,
    k_min: int = 2,
    k_max: int = 10,
    progress_cb=None,
) -> tuple[int, list[dict]]:
    """
    Run KShape for each k in [k_min, k_max] and return the k with the
    highest mean silhouette score, plus the full sweep log.
    """
    ts_dataset = to_time_series_dataset(scaled_profiles.astype(np.float32, copy=False))
    k_max = min(k_max, len(scaled_profiles) - 1)
    sweep_results: list[dict] = []

    print(f"[auto] Sweeping k={k_min}..{k_max} to find optimal cluster count …")

    for k in range(k_min, k_max + 1):
        if progress_cb:
            progress_cb(k, k_max)

        model = KShape(
            n_clusters=k,
            n_init=KSHAPE_N_INIT,
            max_iter=KSHAPE_MAX_ITER,
            random_state=RANDOM_SEED,
            verbose=False,
        )
        labels = model.fit_predict(ts_dataset)

        n_unique = len(np.unique(labels))
        if n_unique < 2:
            sil = -1.0
        else:
            sil = float(silhouette_score(scaled_profiles, labels, metric="euclidean"))

        entry = {
            "k": k,
            "silhouette": round(sil, 4),
            "inertia": round(float(model.inertia_), 4),
        }
        sweep_results.append(entry)
        print(f"      k={k}  silhouette={sil:.4f}  inertia={model.inertia_:.4f}")

    best = max(sweep_results, key=lambda r: r["silhouette"])
    print(f"[auto] Best k = {best['k']}  (silhouette {best['silhouette']:.4f})")

    return best["k"], sweep_results


# ---------------------------------------------------------------------------
# Step 4b – Cluster quality metrics
# ---------------------------------------------------------------------------

def compute_metrics(scaled_profiles: np.ndarray, labels: np.ndarray, model: KShape) -> dict:
    """
    Return a dict of clustering quality scores for the chosen k.

    Silhouette and Davies-Bouldin are computed on the flat (euclidean) profiles,
    which is a standard and fast approximation when the cluster count is already
    fixed. Inertia comes directly from the fitted KShape model.

    Interpretation guide
    --------------------
    silhouette   : -1 → +1   higher is better  (>0.5 = strong, 0.2–0.5 = reasonable)
    davies_bouldin: 0 → ∞    lower is better   (<1.0 = strong separation)
    inertia      : 0 → ∞     lower is better   (use relative change across k runs)
    """
    n_clusters = len(np.unique(labels))

    if n_clusters < 2:
        return {"silhouette": None, "davies_bouldin": None, "inertia": float(model.inertia_)}

    from sklearn.metrics import silhouette_samples

    sil  = float(
        silhouette_score(
            scaled_profiles,
            labels,
            metric="euclidean",
            sample_size=min(len(labels), 200),
            random_state=RANDOM_SEED,
        )
    )
    db   = float(davies_bouldin_score(scaled_profiles, labels))
    ine  = float(model.inertia_)

    sample_scores = silhouette_samples(scaled_profiles, labels, metric="euclidean")
    per_cluster: dict[int, dict] = {}
    for k in range(n_clusters):
        mask = labels == k
        cluster_scores = sample_scores[mask]
        mean_sil = float(cluster_scores.mean())
        if mean_sil >= 0.5:
            k_rating = "Strong"
        elif mean_sil >= 0.25:
            k_rating = "Reasonable"
        elif mean_sil >= 0.0:
            k_rating = "Weak"
        else:
            k_rating = "Poor"
        per_cluster[k] = {"silhouette": round(mean_sil, 4), "rating": k_rating}
        print(f"      Cluster {k} silhouette: {mean_sil:.4f}  ({k_rating})")

    if sil >= 0.5:
        rating = "Strong"
    elif sil >= 0.25:
        rating = "Reasonable"
    elif sil >= 0.0:
        rating = "Weak"
    else:
        rating = "Poor"

    print(f"      Overall Silhouette : {sil:.4f}  ({rating})")
    print(f"      Davies-Bouldin     : {db:.4f}")
    print(f"      Inertia            : {ine:.4f}")

    return {
        "silhouette":      round(sil, 4),
        "davies_bouldin":  round(db,  4),
        "inertia":         round(ine, 4),
        "rating":          rating,
        "per_cluster":     per_cluster,
    }


# ---------------------------------------------------------------------------
# Step 5 – Visualise
# ---------------------------------------------------------------------------

def _slot_for_hour(hour: int) -> int:
    return int((hour * 60) // INTERVAL_MINUTES)

# Auto-label cluster archetype based on centroid peak time
def _behaviour_label(centroid: np.ndarray) -> str:
    peak_h = int(np.argmax(centroid)) * INTERVAL_MINUTES / 60   # convert slot → hours
    if peak_h < 6:   return "Late Night Activity"
    if peak_h < 10:  return "Morning Peaker"
    if peak_h < 14:  return "Daytime User"
    if peak_h < 18:  return "Afternoon Activity"
    if peak_h < 22:  return "Evening Peaker"
    return "Night Owl"


def _draw_tod_bands(ax: plt.Axes) -> None:
    tod_bands = [
        (_slot_for_hour(0),  _slot_for_hour(6),  "Night",   "#0a0f1e", 0.95),
        (_slot_for_hour(6),  _slot_for_hour(9),  "Morning", "#0f1f10", 0.95),
        (_slot_for_hour(9),  _slot_for_hour(16), "Daytime", "#0f0f1f", 0.95),
        (_slot_for_hour(16), _slot_for_hour(19), "Evening", "#1f0f0a", 0.95),
        (_slot_for_hour(19), INTERVALS_PER_DAY,  "Night",   "#0a0f1e", 0.95),
    ]
    for start, end, label, colour, alpha in tod_bands:
        ax.axvspan(start, end, color=colour, alpha=alpha, zorder=0)
        mid = (start + end) / 2
        ax.text(
            mid, 0.985, label,
            ha="center", va="top",
            fontsize=7, color="#55607a", fontstyle="italic",
            transform=ax.get_xaxis_transform(),
        )
    for slot in (_slot_for_hour(6), _slot_for_hour(9), _slot_for_hour(16), _slot_for_hour(19)):
        ax.axvline(slot, color="#2a2f45", linewidth=0.9, linestyle="-", alpha=1.0, zorder=1)


def _annotate_peak(ax: plt.Axes, centroid: np.ndarray, color: str) -> None:
    peak_idx  = int(np.argmax(centroid))
    peak_time = TIME_LABELS[peak_idx]
    peak_val  = centroid[peak_idx]

    ax.axvline(peak_idx, color=color, linewidth=1.0, linestyle="--", alpha=0.5, zorder=3)

    # Position the label to the left if the peak is in the right half
    offset_x = -30 if peak_idx > INTERVALS_PER_DAY // 2 else 10
    ax.annotate(
        f"Peak {peak_time}",
        xy=(peak_idx, peak_val),
        xytext=(peak_idx + offset_x, min(peak_val + 0.08, 0.95)),
        fontsize=8, fontweight="bold", color=color, zorder=6,
        bbox=dict(boxstyle="round,pad=0.25", fc="#0f1117", ec=color, lw=0.8, alpha=0.85),
        arrowprops=dict(arrowstyle="-|>", color=color, lw=0.8),
    )


def visualise(
    scaled_profiles: np.ndarray,
    model: KShape,
    labels: np.ndarray,
    output_path: str,
    dpi: int = 150,
) -> None:
    """
    One subplot per cluster showing:
      - distinct time-of-day background zones with vertical dividers
      - faint individual home traces for data density
      - 10th–90th percentile spread band
      - filled area + bold centroid line
      - peak time annotation with callout box
      - auto-detected behaviour label as subtitle
    """
    print("[5/5] Generating visualisation …")

    plt.style.use("dark_background")

    n_homes_total = len(labels)
    x             = np.arange(INTERVALS_PER_DAY)
    tick_step_slots = max(1, int(120 // INTERVAL_MINUTES))  # every 2 hours
    x_ticks_pos   = list(range(0, INTERVALS_PER_DAY, tick_step_slots))
    x_ticks_lbl   = [TIME_LABELS[i] for i in x_ticks_pos]

    ncols = min(N_CLUSTERS, 3)
    nrows = (N_CLUSTERS + ncols - 1) // ncols

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(7.5 * ncols, 5 * nrows),
        sharey=True,   # share Y only; each subplot gets its own X labels
    )
    fig.patch.set_facecolor("#0f1117")
    axes_flat = np.array(axes).flatten()

    for k in range(N_CLUSTERS):
        ax    = axes_flat[k]
        ax.set_facecolor("#13151f")

        mask           = labels == k
        cluster_traces = scaled_profiles[mask]
        centroid       = model.cluster_centers_[k].ravel()
        color          = CENTROID_COLORS[k % len(CENTROID_COLORS)]
        n_in_cluster   = mask.sum()
        pct            = n_in_cluster / n_homes_total * 100
        behaviour      = _behaviour_label(centroid)

        # 1 — Time-of-day background zones + dividers
        _draw_tod_bands(ax)

        # 2 — Individual home traces (faint, in cluster colour)
        max_traces = 80   # cap for performance; sample if cluster is large
        traces_to_draw = cluster_traces
        if len(cluster_traces) > max_traces:
            idx = np.random.choice(len(cluster_traces), max_traces, replace=False)
            traces_to_draw = cluster_traces[idx]
        for trace in traces_to_draw:
            ax.plot(x, trace, color=color, linewidth=0.4, alpha=0.07, zorder=2)

        # 3 — 10th–90th percentile spread band
        if len(cluster_traces) > 1:
            lo = np.percentile(cluster_traces, 10, axis=0)
            hi = np.percentile(cluster_traces, 90, axis=0)
            ax.fill_between(x, lo, hi, color=color, alpha=0.22, zorder=3, label="10–90th pct")

        # 4 — Filled glow under centroid
        ax.fill_between(x, 0, centroid, color=color, alpha=0.20, zorder=4)

        # 5 — Centroid line
        ax.plot(x, centroid, color=color, linewidth=2.8, zorder=5, label="Centroid", solid_capstyle="round")

        # 6 — Peak annotation
        _annotate_peak(ax, centroid, color)

        # 7 — Y-axis gridlines
        ax.yaxis.grid(True, linestyle="--", linewidth=0.4, color="#2a2f45", alpha=0.8, zorder=1)
        ax.set_axisbelow(False)

        # 8 — Title + behaviour subtitle
        ax.set_title(
            f"Cluster {k}  —  {behaviour}",
            fontsize=10.5, fontweight="bold", color=color, pad=10,
        )

        # 9 — Axes
        ax.set_xticks(x_ticks_pos)
        ax.set_xticklabels(x_ticks_lbl, rotation=45, ha="right", fontsize=7, color="#8892a4")
        ax.tick_params(axis="y", labelsize=7.5, colors="#8892a4")
        ax.set_ylabel("Scaled Usage (0–1)", fontsize=8, color="#8892a4")
        ax.set_ylim(-0.02, 1.08)
        ax.set_xlim(0, INTERVALS_PER_DAY - 1)

        for spine in ax.spines.values():
            spine.set_edgecolor("#2c3050")
            spine.set_linewidth(0.8)

        # 10 — Legend
        legend = ax.legend(loc="upper left", fontsize=7.5, framealpha=0.4, labelcolor="white")
        legend.get_frame().set_facecolor("#1a1d27")
        legend.get_frame().set_edgecolor("#2c3050")

        # 11 — Home count badge (bottom-right)
        ax.text(
            0.98, 0.03,
            f"{n_in_cluster} homes  ({pct:.0f}%)",
            transform=ax.transAxes,
            ha="right", va="bottom",
            fontsize=8, color="#6a7490",
            bbox=dict(boxstyle="round,pad=0.3", fc="#1a1d27", ec="#2c3050", lw=0.7, alpha=0.8),
        )

    # Hide unused subplots
    for idx in range(N_CLUSTERS, len(axes_flat)):
        axes_flat[idx].set_visible(False)

    fig.suptitle(
        "Energy Usage — Archetypal Daily Profiles by Cluster",
        fontsize=15, fontweight="bold", color="#e2e8f0", y=1.01,
    )

    plt.tight_layout(h_pad=3.0, w_pad=2.0)
    plt.savefig(output_path, dpi=dpi, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    plt.style.use("default")
    print(f"      Figure saved → '{output_path}'")


# ---------------------------------------------------------------------------
# Step 6 – Export cluster mapping
# ---------------------------------------------------------------------------

def export_mapping(home_ids: pd.Series, labels: np.ndarray, output_path: str) -> None:
    mapping = pd.DataFrame({
        "home_id": home_ids,
        "cluster_id": labels,
    })
    mapping.to_csv(output_path, index=False)
    print(f"      Mapping saved → '{output_path}'")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Time-Series KShape Cluster Analysis for Energy Usage Data"
    )
    parser.add_argument(
        "csv_path",
        help="Path to the input CSV file (homes × interval columns).",
    )
    parser.add_argument(
        "--clusters", "-k",
        type=int,
        default=N_CLUSTERS,
        dest="n_clusters",
        help=f"Number of clusters (default: {N_CLUSTERS}).",
    )
    parser.add_argument(
        "--out-dir", "-o",
        default=".",
        dest="out_dir",
        help="Directory to write output files (default: current directory).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    global N_CLUSTERS
    N_CLUSTERS = args.n_clusters

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    figure_path  = str(out_dir / "cluster_profiles.png")
    mapping_path = str(out_dir / "home_cluster_mapping.csv")

    # ---- Pipeline ----
    home_ids, profiles = load_average_daily_profiles(args.csv_path)
    scaled             = minmax_scale_rows(profiles)
    model, labels      = run_kshape(scaled)

    visualise(scaled, model, labels, figure_path)
    export_mapping(home_ids, labels, mapping_path)

    print("\nDone.")


if __name__ == "__main__":
    main()
