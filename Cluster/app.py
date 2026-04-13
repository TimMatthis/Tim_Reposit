"""
Flask front end for the Energy Cluster Analysis pipeline.
Uses Server-Sent Events to stream live progress to the browser.
"""

import json
import os
import threading
import time
import uuid
from pathlib import Path

import cluster_analysis as ca
import pandas as pd

from cluster_analysis import (
    cluster_predictor,
    compute_metrics,
    export_mapping,
    find_optimal_k,
    load_average_daily_profiles,
    minmax_scale_rows,
    postcode_cluster_analysis,
    run_kshape,
    visualise,
)
from enrichment import enrich_sites
from flask import (
    Flask,
    Response,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

UPLOAD_DIR = Path("uploads")
OUTPUT_DIR = Path("outputs")
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
ALLOWED_EXTENSIONS = {"csv"}
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500 MB

# In-memory job store  { job_id: { status, progress, message, result } }
_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ---------------------------------------------------------------------------
# Background analysis worker
# ---------------------------------------------------------------------------

def _set(job_id: str, **kwargs) -> None:
    with _jobs_lock:
        _jobs[job_id].update(kwargs)


def run_analysis_job(
    job_id: str,
    csv_path: Path,
    n_clusters: int,
    figure_path: Path,
    mapping_path: Path,
    auto_detect: bool = False,
    abs_csv_path: Path | None = None,
) -> None:
    try:
        _set(job_id, progress=5, message="Scanning CSV for distinct sites…")
        header = pd.read_csv(str(csv_path), nrows=0)
        if "SiteUID" in header.columns:
            site_col = pd.read_csv(str(csv_path), usecols=["SiteUID"])["SiteUID"]
            n_rows = len(site_col)
            n_sites = site_col.nunique()
            _set(job_id, progress=10,
                 message=f"Found {n_sites:,} distinct sites across {n_rows:,} rows. Loading profiles…")
        else:
            n_sites = None
            n_rows = None
            _set(job_id, progress=10, message="Loading data…")

        # Enrich sites with address-derived and optional ABS features.
        _set(job_id, progress=12, message="Enriching sites with address and postcode features…")
        try:
            enriched_df = enrich_sites(
                str(csv_path),
                abs_csv_path=str(abs_csv_path) if abs_csv_path else None,
            )
        except Exception as enrich_exc:
            print(f"      Enrichment failed (non-fatal): {enrich_exc}")
            enriched_df = None

        _set(job_id, progress=15, message="Loading and aggregating seasonal daily profiles…")
        home_ids, profiles, postcode_map = load_average_daily_profiles(str(csv_path))

        # KShape z-normalises internally so MinMax pre-scaling is not needed for
        # clustering. We retain a MinMax-scaled copy only for the visualisation,
        # where [0,1] traces are easier to compare across clusters.
        scaled_for_viz = minmax_scale_rows(profiles)

        sweep_results = None

        if auto_detect:
            def sweep_cb(k, k_max):
                pct = 30 + int(35 * (k - 2) / max(k_max - 2, 1))
                _set(job_id, progress=pct, message=f"Sweep: testing k={k} of {k_max}…")

            _set(job_id, progress=30, message="Auto-detecting optimal cluster count…")
            n_clusters, sweep_results = find_optimal_k(profiles, k_min=2, k_max=10, progress_cb=sweep_cb)

        _set(job_id, progress=65, message=f"Running KShape clustering (k={n_clusters}) — this may take a minute…")
        ca.N_CLUSTERS = n_clusters
        model, labels = run_kshape(profiles)

        _set(job_id, progress=80, message="Computing quality metrics…")
        metrics = compute_metrics(profiles, labels, model)

        _set(job_id, progress=88, message="Generating cluster visualisation…")
        visualise(scaled_for_viz, model, labels, str(figure_path))

        _set(job_id, progress=96, message="Saving results…")
        export_mapping(home_ids, labels, str(mapping_path), postcode_map=postcode_map)

        cluster_counts = {k: int((labels == k).sum()) for k in range(n_clusters)}

        # Postcode analysis — only if Postcode data was present in the CSV.
        postcode_result = None
        if postcode_map:
            _set(job_id, progress=97, message="Analysing postcode distribution across clusters…")
            heatmap_path = OUTPUT_DIR / f"{job_id}_postcode_heatmap.png"
            try:
                postcode_result = postcode_cluster_analysis(
                    home_ids, labels, postcode_map, str(heatmap_path)
                )
            except Exception as pc_exc:
                print(f"      Postcode analysis skipped: {pc_exc}")

        # Cluster predictor — only if enrichment data is available.
        predictor_result = None
        if enriched_df is not None and not enriched_df.empty:
            _set(job_id, progress=99, message="Training cluster predictor on site features…")
            importance_path = OUTPUT_DIR / f"{job_id}_feature_importance.png"
            try:
                predictor_result = cluster_predictor(
                    enriched_df, home_ids, labels, str(importance_path)
                )
            except Exception as pred_exc:
                print(f"      Cluster predictor skipped: {pred_exc}")

        _set(
            job_id,
            status="done",
            progress=100,
            message="Analysis complete!",
            result={
                "figure_file":    figure_path.name,
                "mapping_file":   mapping_path.name,
                "n_clusters":     n_clusters,
                "n_homes":        len(home_ids),
                "n_sites_raw":    n_sites,
                "n_rows_raw":     n_rows,
                "cluster_counts": cluster_counts,
                "metrics":        metrics,
                "sweep":          sweep_results,
                "postcode":       postcode_result,
                "predictor":      predictor_result,
            },
        )

    except Exception as exc:
        _set(job_id, status="error", progress=0, message=str(exc))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    page = render_template("index.html")
    # Defensive guard: Flask requires a non-None return value from views.
    return page if page is not None else ""


@app.route("/run", methods=["POST"])
def run():
    file        = request.files.get("csv_file")
    abs_file    = request.files.get("abs_csv_file")
    n_clusters  = int(request.form.get("n_clusters", 5))
    auto_detect = request.form.get("auto_detect") == "1"

    if not file or file.filename == "":
        flash("Please select a CSV file before running.", "error")
        return redirect(url_for("index"))

    if not allowed_file(file.filename):
        flash("Only .csv files are accepted.", "error")
        return redirect(url_for("index"))

    job_id = uuid.uuid4().hex[:8]

    csv_path     = UPLOAD_DIR / f"{job_id}_input.csv"
    figure_path  = OUTPUT_DIR / f"{job_id}_cluster_profiles.png"
    mapping_path = OUTPUT_DIR / f"{job_id}_home_cluster_mapping.csv"

    file.save(csv_path)

    # Save optional ABS enrichment CSV if supplied.
    abs_csv_path = None
    if abs_file and abs_file.filename and allowed_file(abs_file.filename):
        abs_csv_path = UPLOAD_DIR / f"{job_id}_abs_enrichment.csv"
        abs_file.save(abs_csv_path)

    with _jobs_lock:
        _jobs[job_id] = {"status": "running", "progress": 0, "message": "Starting…", "result": None}

    thread = threading.Thread(
        target=run_analysis_job,
        args=(job_id, csv_path, n_clusters, figure_path, mapping_path, auto_detect, abs_csv_path),
        daemon=True,
    )
    thread.start()

    return redirect(url_for("progress_page", job_id=job_id))


@app.route("/progress/<job_id>")
def progress_page(job_id: str):
    if job_id not in _jobs:
        flash("Job not found.", "error")
        return redirect(url_for("index"))
    return render_template("progress.html", job_id=job_id)


@app.route("/stream/<job_id>")
def stream(job_id: str):
    """Server-Sent Events endpoint — pushes job state to the browser."""
    def generate():
        last_progress = -1
        while True:
            with _jobs_lock:
                job = _jobs.get(job_id)

            if not job:
                yield f"data: {json.dumps({'status': 'error', 'message': 'Job not found.'})}\n\n"
                break

            if job["progress"] != last_progress or job["status"] in ("done", "error"):
                last_progress = job["progress"]
                payload = {
                    "status":   job["status"],
                    "progress": job["progress"],
                    "message":  job["message"],
                }
                yield f"data: {json.dumps(payload)}\n\n"

            if job["status"] in ("done", "error"):
                break

            time.sleep(0.4)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/results/<job_id>")
def results_page(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job or job["status"] != "done" or not job["result"]:
        flash("Results not ready or job not found.", "error")
        return redirect(url_for("index"))

    r = job["result"]
    return render_template(
        "results.html",
        figure_file=r["figure_file"],
        mapping_file=r["mapping_file"],
        n_clusters=r["n_clusters"],
        n_homes=r["n_homes"],
        n_sites_raw=r.get("n_sites_raw"),
        n_rows_raw=r.get("n_rows_raw"),
        cluster_counts=r["cluster_counts"],
        metrics=r["metrics"],
        sweep=r.get("sweep"),
        postcode=r.get("postcode"),
        predictor=r.get("predictor"),
    )


@app.route("/outputs/<filename>")
def serve_output(filename: str):
    return send_from_directory(OUTPUT_DIR.resolve(), filename)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
