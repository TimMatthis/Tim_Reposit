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
    interval_minutes = int(interval_minutes)
    if interval_minutes <= 0:
        raise ValueError(
            f"interval_minutes must be a positive integer, got {interval_minutes}. "
            "Check that your timekey column contains valid timestamps with a supported cadence "
            "(e.g. 30-minute or 5-minute intervals)."
        )
    INTERVAL_MINUTES = interval_minutes
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


def load_average_daily_profiles(
    csv_path: str,
    min_days_per_season: int = 14,
) -> tuple[pd.Series, np.ndarray, dict]:
    """
    Load long-format CSV and compute separate summer and winter average daily
    profiles for each site.

    Southern Hemisphere seasons
    ---------------------------
    Summer : October – March  (months 10, 11, 12, 1, 2, 3)
    Winter : April – September  (months 4, 5, 6, 7, 8, 9)

    Each site produces up to two profiles.  A site-season pair is excluded when
    it has fewer than ``min_days_per_season`` distinct calendar days of data, to
    avoid noisy short-window averages.

    Profile identifiers use the format  ``SiteUID__summer`` / ``SiteUID__winter``
    so that downstream code can always recover the originating site by splitting
    on the last ``__``.

    Returns
    -------
    home_ids    : pd.Series  — one entry per site-season profile
    profiles    : np.ndarray  shape (n_profiles, INTERVALS_PER_DAY), float32
    postcode_map: dict  (SiteUID__season) -> Postcode string
    """
    header = pd.read_csv(csv_path, nrows=0)
    long_format_cols = {"SiteUID", "timekey", "Consumption_kWh"}
    has_postcode = "Postcode" in header.columns

    if long_format_cols.issubset(set(header.columns)):
        print(f"[1/5] Loading long-format data from '{csv_path}' …")
        load_cols = ["SiteUID", "timekey", "Consumption_kWh"]
        if has_postcode:
            load_cols.append("Postcode")
        df = pd.read_csv(csv_path, usecols=load_cols)
        df["Consumption_kWh"] = pd.to_numeric(df["Consumption_kWh"], errors="coerce")
        df = df.dropna(subset=["SiteUID", "timekey", "Consumption_kWh"])

        # Infer cadence from unique timestamps in the file.
        # Support both Unix epoch integers and human-readable datetime strings.
        sample = df["timekey"].iloc[0]
        try:
            int(sample)
            timekey_is_epoch = True
        except (ValueError, TypeError):
            timekey_is_epoch = False

        if timekey_is_epoch:
            epoch_series = df["timekey"].astype(np.int64)
            dt = pd.to_datetime(epoch_series, unit="s")
        else:
            dt = pd.to_datetime(df["timekey"], utc=True)
            # Explicit seconds-since-epoch — avoids pandas version differences
            # with astype(np.int64) on tz-aware series (returns nanoseconds in
            # some versions but may behave differently in pandas 2.x).
            epoch_series = (
                (dt - pd.Timestamp("1970-01-01", tz="UTC"))
                .dt.total_seconds()
                .astype(np.int64)
            )

        unique_timekeys = np.sort(epoch_series.unique())
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

        # ── Season assignment (Southern Hemisphere) ───────────────────────
        month = dt.dt.month
        df["season"] = month.map(
            lambda m: "summer" if m in (10, 11, 12, 1, 2, 3) else "winter"
        )

        # ── Time-slot within the day ──────────────────────────────────────
        slot = (dt.dt.hour * 60 + dt.dt.minute) // INTERVAL_MINUTES
        df["slot"] = slot.astype(np.int32)

        # ── Filter sparse site-season pairs ──────────────────────────────
        df["_date"] = dt.dt.date
        day_counts = df.groupby(["SiteUID", "season"])["_date"].nunique()
        valid_pairs = (
            day_counts[day_counts >= min_days_per_season]
            .reset_index()[["SiteUID", "season"]]
        )
        n_sites_total = df["SiteUID"].nunique()
        df = df.merge(valid_pairs, on=["SiteUID", "season"])
        n_profiles_kept = len(valid_pairs)
        n_profiles_max = n_sites_total * 2
        if n_profiles_kept < n_profiles_max:
            print(
                f"      {n_profiles_max - n_profiles_kept} site-season profiles excluded "
                f"(<{min_days_per_season} days of data)."
            )

        # ── Average by (site, season, slot) ──────────────────────────────
        grouped = (
            df.groupby(["SiteUID", "season", "slot"], sort=True)["Consumption_kWh"]
            .mean()
            .unstack("slot")
            .reindex(columns=range(INTERVALS_PER_DAY))
        )
        # grouped.index is a MultiIndex (SiteUID, season).

        # ── Postcode map keyed by "SiteUID__season" ───────────────────────
        postcode_map: dict = {}
        if has_postcode:
            pc_lookup = (
                df[["SiteUID", "Postcode"]]
                .drop_duplicates(subset="SiteUID")
                .set_index("SiteUID")["Postcode"]
                .astype(str)
            )
            postcode_map = {
                f"{site}__{season}": pc_lookup.get(site, "Unknown")
                for site, season in grouped.index
            }
            n_sites_with_pc = len(pc_lookup)
            print(
                f"      Postcode column detected — "
                f"{n_sites_with_pc} site→postcode mappings loaded."
            )

        # ── Flatten MultiIndex → "SiteUID__season" strings ───────────────
        grouped.index = [
            f"{site}__{season}" for site, season in grouped.index
        ]
        home_ids = pd.Series(
            grouped.index.astype(str), name="home_id"
        ).reset_index(drop=True)
        profiles = grouped.to_numpy(dtype=np.float32, copy=False)

        n_unique_sites = df["SiteUID"].nunique()
        print(
            f"      {n_unique_sites} sites → {len(home_ids)} seasonal profiles "
            f"({len(home_ids) / max(n_unique_sites, 1):.1f} seasons/site avg)."
        )
        print(f"      Profiles shape: {profiles.shape}")
        return (
            home_ids,
            np.nan_to_num(profiles, nan=0.0).astype(np.float32, copy=False),
            postcode_map,
        )

    # Fallback: existing wide matrix format (one row per home).
    home_ids, readings = load_data(csv_path)
    profiles = compute_average_daily_profile(readings)
    return home_ids, profiles, {}


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
            sil = float(silhouette_score(scaled_profiles, labels, metric="correlation"))

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

    Silhouette is computed with correlation distance (1 − Pearson r), which
    matches KShape's shape-based internal measure and gives more accurate scores
    than Euclidean distance for time-series clusters. Davies-Bouldin uses
    Euclidean (sklearn default). Inertia comes directly from the fitted KShape
    model.

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
            metric="correlation",
            sample_size=min(len(labels), 200),
            random_state=RANDOM_SEED,
        )
    )
    db   = float(davies_bouldin_score(scaled_profiles, labels))
    ine  = float(model.inertia_)

    sample_scores = silhouette_samples(scaled_profiles, labels, metric="correlation")
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

def export_mapping(
    home_ids: pd.Series,
    labels: np.ndarray,
    output_path: str,
    postcode_map: dict | None = None,
) -> None:
    mapping = pd.DataFrame({
        "home_id": home_ids,
        "cluster_id": labels,
    })

    # When seasonal profiling is active, home_id has the form "SiteUID__season".
    # Split into separate columns so the CSV is easy to filter by season.
    if mapping["home_id"].str.contains("__", regex=False).any():
        split = mapping["home_id"].str.rsplit("__", n=1)
        mapping.insert(1, "site_uid", split.str[0])
        mapping.insert(2, "season",   split.str[1])

    if postcode_map:
        mapping["Postcode"] = mapping["home_id"].map(postcode_map).fillna("Unknown")

    mapping.to_csv(output_path, index=False)
    print(f"      Mapping saved → '{output_path}'")


# ---------------------------------------------------------------------------
# Step 7 – Postcode distribution analysis
# ---------------------------------------------------------------------------

def postcode_cluster_analysis(
    home_ids: pd.Series,
    labels: np.ndarray,
    postcode_map: dict,
    heatmap_path: str,
    min_homes: int = 3,
) -> dict:
    """
    Analyse whether postcode is a proxy for energy usage cluster.

    Builds a contingency table (postcode × cluster), runs a chi-squared test
    to check if the association is statistically significant, and saves a
    normalised heatmap PNG showing the % of each postcode's homes per cluster.

    Parameters
    ----------
    home_ids      : pd.Series of SiteUIDs in the same order as labels
    labels        : cluster label per home (np.ndarray)
    postcode_map  : dict  SiteUID -> Postcode string
    heatmap_path  : path to write the heatmap PNG
    min_homes     : postcodes with fewer homes are merged into 'Other'

    Returns
    -------
    dict with keys:
        chi2, p_value, dof, interpretation,
        top_postcodes_per_cluster, n_postcodes, heatmap_file,
        contingency_rows  (list of dicts for template rendering)
    """
    from scipy.stats import chi2_contingency

    print("[6/6] Running postcode distribution analysis …")

    mapping = pd.DataFrame({"home_id": home_ids.astype(str), "cluster_id": labels})
    mapping["Postcode"] = mapping["home_id"].map(postcode_map).fillna("Unknown").astype(str)

    # Merge rare postcodes into 'Other' to reduce noise.
    counts_per_postcode = mapping["Postcode"].value_counts()
    rare = counts_per_postcode[counts_per_postcode < min_homes].index
    mapping.loc[mapping["Postcode"].isin(rare), "Postcode"] = "Other"

    n_clusters = int(labels.max()) + 1
    cluster_cols = list(range(n_clusters))

    contingency = (
        mapping.groupby(["Postcode", "cluster_id"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=cluster_cols, fill_value=0)
    )

    # Sort postcodes by total homes descending; keep 'Other' at the bottom.
    contingency["_total"] = contingency.sum(axis=1)
    other_row = contingency.loc[["Other"]] if "Other" in contingency.index else None
    contingency = contingency[contingency.index != "Other"].sort_values("_total", ascending=False)
    if other_row is not None:
        contingency = pd.concat([contingency, other_row])
    contingency = contingency.drop(columns=["_total"])

    # Row-normalised table: % of each postcode's homes per cluster.
    row_totals = contingency.sum(axis=1).replace(0, 1)
    normalised = contingency.div(row_totals, axis=0) * 100

    # Chi-squared test on raw counts (exclude 'Other' to keep it clean).
    test_table = contingency[contingency.index != "Other"]
    if test_table.shape[0] >= 2 and test_table.shape[1] >= 2:
        chi2, p_value, dof, _ = chi2_contingency(test_table.values)
        chi2 = round(float(chi2), 4)
        p_value = float(p_value)
        if p_value < 0.001:
            interpretation = "Very strong evidence — postcode is a meaningful proxy for energy usage pattern."
        elif p_value < 0.01:
            interpretation = "Strong evidence — postcode correlates significantly with usage cluster."
        elif p_value < 0.05:
            interpretation = "Moderate evidence — some postcode–cluster association exists."
        else:
            interpretation = "Weak evidence — postcode does not appear to predict usage pattern in this dataset."
    else:
        chi2, p_value, dof = None, None, None
        interpretation = "Not enough postcodes to run statistical test."

    # Top 5 postcodes per cluster by concentration (row-normalised %).
    top_postcodes_per_cluster: dict[int, list[dict]] = {}
    for col in cluster_cols:
        col_series = normalised[col].drop("Other", errors="ignore")
        top = col_series.nlargest(5)
        top_postcodes_per_cluster[col] = [
            {"postcode": pc, "pct": round(float(pct), 1)}
            for pc, pct in top.items()
        ]

    # Heatmap PNG.
    _save_postcode_heatmap(normalised, heatmap_path, n_clusters)

    # Rows for template table rendering.
    contingency_rows = []
    for postcode, row in normalised.iterrows():
        raw_counts = contingency.loc[postcode]
        contingency_rows.append({
            "postcode": postcode,
            "total": int(raw_counts.sum()),
            "cluster_pcts": [round(float(row.get(c, 0)), 1) for c in cluster_cols],
            "dominant_cluster": int(row.idxmax()),
        })

    print(f"      {len(contingency)} postcodes analysed.")
    if p_value is not None:
        print(f"      Chi-squared p-value: {p_value:.4f} — {interpretation}")

    return {
        "chi2": chi2,
        "p_value": round(p_value, 6) if p_value is not None else None,
        "dof": int(dof) if dof is not None else None,
        "interpretation": interpretation,
        "top_postcodes_per_cluster": top_postcodes_per_cluster,
        "n_postcodes": len(contingency),
        "heatmap_file": Path(heatmap_path).name,
        "contingency_rows": contingency_rows,
        "n_clusters": n_clusters,
    }


def _save_postcode_heatmap(normalised: "pd.DataFrame", output_path: str, n_clusters: int) -> None:
    """Save a dark-themed heatmap of postcode × cluster (row-normalised %)."""
    n_postcodes = len(normalised)
    fig_height = max(4, min(n_postcodes * 0.4 + 1, 28))
    fig, ax = plt.subplots(figsize=(max(6, n_clusters * 1.5), fig_height))
    fig.patch.set_facecolor("#0f1117")
    ax.set_facecolor("#13151f")

    data = normalised.values
    im = ax.imshow(data, aspect="auto", cmap="YlOrRd", vmin=0, vmax=100)

    ax.set_xticks(range(n_clusters))
    ax.set_xticklabels([f"Cluster {c}" for c in range(n_clusters)], color="white", fontsize=9)
    ax.set_yticks(range(n_postcodes))
    ax.set_yticklabels(normalised.index.tolist(), color="white", fontsize=8)
    ax.tick_params(colors="white")

    for spine in ax.spines.values():
        spine.set_edgecolor("#333")

    # Annotate cells with % value.
    for i in range(n_postcodes):
        for j in range(n_clusters):
            val = data[i, j]
            text_color = "black" if val > 55 else "white"
            ax.text(j, i, f"{val:.0f}%", ha="center", va="center",
                    color=text_color, fontsize=7)

    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.ax.yaxis.set_tick_params(color="white")
    cbar.ax.set_ylabel("% of postcode homes", color="white", fontsize=8)
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="white")

    ax.set_title("Postcode × Cluster Distribution (% of postcode's homes per cluster)",
                 color="white", fontsize=10, pad=10)
    ax.set_xlabel("Cluster", color="white", fontsize=9)
    ax.set_ylabel("Postcode", color="white", fontsize=9)

    plt.tight_layout()
    fig.savefig(output_path, dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"      Heatmap saved → '{output_path}'")


# ---------------------------------------------------------------------------
# Step 8 – Cluster predictor (Random Forest on enriched site features)
# ---------------------------------------------------------------------------

def cluster_predictor(
    enriched_df: "pd.DataFrame",
    home_ids: pd.Series,
    labels: np.ndarray,
    importance_path: str,
) -> dict:
    """
    Train a Random Forest classifier to predict cluster membership from
    site-level metadata features derived by enrichment.py.

    Returns a dict with feature importances, accuracy metrics, and the
    path to a saved feature importance bar chart.

    Parameters
    ----------
    enriched_df     : DataFrame indexed by SiteUID with feature columns
    home_ids        : pd.Series of SiteUIDs (same order as labels)
    labels          : cluster label per home (np.ndarray)
    importance_path : file path to write the importance bar chart PNG

    Returns
    -------
    dict with keys:
        accuracy, n_used, n_dropped, feature_importances (list of dicts),
        per_cluster_accuracy (dict), top_feature, top_feature_importance,
        interpretation, importance_file
    """
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import LabelEncoder

    print("[7/7] Training cluster predictor …")

    if enriched_df is None or enriched_df.empty:
        print("      No enrichment data available — predictor skipped.")
        return {}

    # Build feature matrix by joining enriched features to cluster labels.
    # When seasonal profiling is active, home_ids have the form "SiteUID__season".
    # Strip the suffix so we can join against enriched_df (indexed by bare SiteUID).
    raw_ids = home_ids.astype(str)
    if raw_ids.str.contains("__", regex=False).any():
        site_ids = raw_ids.str.rsplit("__", n=1).str[0]
    else:
        site_ids = raw_ids

    label_df = pd.DataFrame({
        "home_id": site_ids.values,
        "cluster_id": labels,
    }).set_index("home_id")

    merged = label_df.join(enriched_df, how="inner")
    if merged.empty:
        print("      No SiteUID overlap between enrichment and cluster labels — predictor skipped.")
        return {}

    # Select numeric features only; drop climate_label (string) and cols with
    # too many missing values (>50%).
    feature_cols = [
        c for c in enriched_df.columns
        if c != "climate_label"
        and pd.api.types.is_numeric_dtype(enriched_df[c])
        and enriched_df[c].notna().mean() > 0.5
    ]

    if not feature_cols:
        print("      No usable numeric features for predictor.")
        return {}

    X = merged[feature_cols].copy()
    y = merged["cluster_id"].values

    # Impute missing values with column medians.
    for col in X.columns:
        if X[col].isna().any():
            X[col] = X[col].fillna(X[col].median())

    n_used = len(X)
    n_dropped = len(merged) - n_used
    print(f"      Features: {feature_cols}")
    print(f"      Training on {n_used} sites ({n_dropped} dropped due to missing data).")

    if n_used < 10:
        print("      Too few sites for predictor — skipped.")
        return {}

    rf = RandomForestClassifier(
        n_estimators=200,
        class_weight="balanced",
        random_state=RANDOM_SEED,
        n_jobs=-1,
    )

    # Cross-validated accuracy (5-fold, or fewer if small dataset).
    n_folds = min(5, n_used // max(len(np.unique(y)), 2))
    n_folds = max(n_folds, 2)
    cv_scores = cross_val_score(rf, X.values, y, cv=n_folds, scoring="accuracy")
    accuracy = float(np.mean(cv_scores))

    # Fit on full data for importances and per-cluster metrics.
    rf.fit(X.values, y)
    importances = rf.feature_importances_

    # Per-cluster accuracy via in-sample predictions (indicative only).
    y_pred = rf.predict(X.values)
    n_clusters = len(np.unique(y))
    per_cluster_accuracy: dict[int, float] = {}
    for k in range(n_clusters):
        mask = y == k
        if mask.sum() > 0:
            per_cluster_accuracy[k] = round(float((y_pred[mask] == k).mean()), 3)

    # Feature importance table.
    importance_list = sorted(
        [{"feature": f, "importance": round(float(imp), 4)}
         for f, imp in zip(feature_cols, importances)],
        key=lambda x: x["importance"],
        reverse=True,
    )
    top_feature = importance_list[0]["feature"] if importance_list else "N/A"
    top_importance = importance_list[0]["importance"] if importance_list else 0.0

    # Plain-English interpretation.
    if accuracy >= 0.70:
        interp = (
            f"Strong predictability ({accuracy:.0%} accuracy): site metadata alone "
            f"explains most cluster membership. '{_feature_label(top_feature)}' "
            f"is the dominant factor ({top_importance:.0%} of model weight)."
        )
    elif accuracy >= 0.50:
        interp = (
            f"Moderate predictability ({accuracy:.0%} accuracy): metadata captures "
            f"some cluster signal. '{_feature_label(top_feature)}' contributes most "
            f"({top_importance:.0%}) but behavioural factors likely matter more."
        )
    else:
        interp = (
            f"Weak predictability ({accuracy:.0%} accuracy): cluster membership is "
            f"driven more by occupant behaviour than site characteristics. "
            f"Consider adding occupancy or tariff data."
        )

    print(f"      Cross-validated accuracy: {accuracy:.2%}")
    print(f"      Top feature: {top_feature} ({top_importance:.2%})")
    print(f"      {interp}")

    _save_importance_chart(importance_list, importance_path, accuracy)

    return {
        "accuracy": round(accuracy, 4),
        "accuracy_pct": round(accuracy * 100, 1),
        "n_used": n_used,
        "n_dropped": n_dropped,
        "n_folds": n_folds,
        "feature_importances": importance_list,
        "per_cluster_accuracy": per_cluster_accuracy,
        "top_feature": _feature_label(top_feature),
        "top_feature_importance": round(top_importance * 100, 1),
        "interpretation": interp,
        "importance_file": Path(importance_path).name,
    }


def _feature_label(feature: str) -> str:
    """Return a human-readable label for a feature column name."""
    labels = {
        "Solar_kW":                   "Solar system size (kW)",
        "Battery_kWh":                "Battery capacity (kWh)",
        "is_apartment":               "Dwelling type (apartment vs house)",
        "climate_zone":               "Climate zone",
        "median_household_income":    "Median household income (weekly $)",
        "avg_household_size":         "Average household size",
        "avg_persons_per_bedroom":    "Average persons per bedroom",
        "median_weekly_rent":         "Median weekly rent ($)",
        "pct_65_plus":                "% aged 65+ (retirement age)",
        "pct_not_in_labour_force":    "% not in labour force (home during day)",
        "pct_part_time_employed":     "% employed part-time (partially home)",
        "pct_university_educated":    "% with university degree (WFH proxy)",
        "seifa_score":                "Area socioeconomic index (SEIFA)",
        "pct_dwellings_house":        "% separate houses in postcode",
        "pct_wfh":                    "% working from home in postcode",
        "median_income":              "Median household income",
    }
    return labels.get(feature, feature)


def _save_importance_chart(
    importance_list: list[dict],
    output_path: str,
    accuracy: float,
) -> None:
    """Save a dark-themed horizontal bar chart of feature importances."""
    if not importance_list:
        return

    features = [_feature_label(d["feature"]) for d in reversed(importance_list)]
    values = [d["importance"] for d in reversed(importance_list)]

    fig, ax = plt.subplots(figsize=(8, max(3, len(features) * 0.55 + 1.2)))
    fig.patch.set_facecolor("#0f1117")
    ax.set_facecolor("#13151f")

    colors = ["#FF4D6D" if v == max(values) else "#74B3CE" for v in values]
    bars = ax.barh(features, values, color=colors, height=0.6, zorder=2)

    for bar, val in zip(bars, values):
        ax.text(
            val + 0.005, bar.get_y() + bar.get_height() / 2,
            f"{val:.1%}", va="center", ha="left", color="white", fontsize=8,
        )

    ax.set_xlim(0, max(values) * 1.25)
    ax.set_xlabel("Feature importance", color="white", fontsize=9)
    ax.tick_params(colors="white")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
    for spine in ax.spines.values():
        spine.set_edgecolor("#333")
    ax.grid(axis="x", color="#2a2d3e", zorder=1)

    ax.set_title(
        f"What predicts cluster membership?\n"
        f"Cross-validated accuracy: {accuracy:.0%}",
        color="white", fontsize=10, pad=10,
    )

    plt.tight_layout()
    fig.savefig(output_path, dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"      Feature importance chart saved → '{output_path}'")


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
