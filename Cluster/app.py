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
from cluster_analysis import (
    compute_metrics,
    export_mapping,
    load_average_daily_profiles,
    minmax_scale_rows,
    run_kshape,
    visualise,
)
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
) -> None:
    try:
        _set(job_id, progress=5, message="Starting data load…")
        _set(job_id, progress=20, message="Loading and aggregating daily profiles…")
        home_ids, profiles = load_average_daily_profiles(str(csv_path))

        _set(job_id, progress=35, message="Applying Min-Max scaling…")
        scaled = minmax_scale_rows(profiles)

        _set(job_id, progress=42, message=f"Running KShape clustering (k={n_clusters}) — this may take a minute…")
        ca.N_CLUSTERS = n_clusters
        model, labels = run_kshape(scaled)

        _set(job_id, progress=80, message="Computing quality metrics…")
        metrics = compute_metrics(scaled, labels, model)

        _set(job_id, progress=88, message="Generating cluster visualisation…")
        visualise(scaled, model, labels, str(figure_path))

        _set(job_id, progress=96, message="Saving results…")
        export_mapping(home_ids, labels, str(mapping_path))

        cluster_counts = {k: int((labels == k).sum()) for k in range(n_clusters)}

        _set(
            job_id,
            status="done",
            progress=100,
            message="Analysis complete!",
            result={
                "figure_file":   figure_path.name,
                "mapping_file":  mapping_path.name,
                "n_clusters":    n_clusters,
                "n_homes":       len(home_ids),
                "cluster_counts": cluster_counts,
                "metrics":       metrics,
            },
        )

    except Exception as exc:
        _set(job_id, status="error", progress=0, message=str(exc))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/run", methods=["POST"])
def run():
    file       = request.files.get("csv_file")
    n_clusters = int(request.form.get("n_clusters", 5))

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

    with _jobs_lock:
        _jobs[job_id] = {"status": "running", "progress": 0, "message": "Starting…", "result": None}

    thread = threading.Thread(
        target=run_analysis_job,
        args=(job_id, csv_path, n_clusters, figure_path, mapping_path),
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
        cluster_counts=r["cluster_counts"],
        metrics=r["metrics"],
    )


@app.route("/outputs/<filename>")
def serve_output(filename: str):
    return send_from_directory(OUTPUT_DIR.resolve(), filename)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
