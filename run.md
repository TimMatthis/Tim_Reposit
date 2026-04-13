# Run the project hub (landing page for all tools)

**One command** (from anywhere — uses the script’s directory as repo root):

```bash
/home/reposit/www_playground/start-hub.sh
```

Or after `cd` into the repo:

```bash
./start-hub.sh
```

That starts **utility_api** (index + APIs), waits for it, then POSTs **start** for Cluster (5000), Optisizer (5010), and NEM+BOM (5020). Hub log: `.hub-server.log` in the repo root.

---

### Manual hub only (no auto-start of Flask apps)

From the **repository root** (`www_playground`):

```bash
cd utility_api && node server.js
```

Then open the hub in a browser (static files + launcher API; default port **3847**):

- **http://127.0.0.1:3847/**  
- or **http://127.0.0.1:3847/index.html**

Use that URL instead of `file:///…/index.html` so the “Start and open …” buttons can reach `/api/*`.

Override port: `PORT=4000 node server.js` (and open `http://127.0.0.1:4000/`).

If your shell is not already in this repo, use the full path:

```bash
cd /path/to/www_playground/utility_api && node server.js
```

## NEM prices + BOM weather (port 5020)

That URL is a **separate Flask app**. It is **not** started by `utility_api` until you click **“Start and open NEM + BOM app”** on the hub (with `utility_api` running), **or** you start it manually:

```bash
cd /path/to/www_playground/nem_weather_price_ml && python3 app.py
```

Then open **http://127.0.0.1:5020/** again. If the terminal shows an import error (e.g. `No module named 'flask'`), install deps: `pip install -r nem_weather_price_ml/requirements.txt` (or use a venv).

Pipeline data (optional but needed for charts): from repo root run  
`python3 nem_weather_price_ml/scripts/run_pipeline.py`  
so `data/processed/features_daily.parquet` exists; the home page still loads without it, but `/api/features` will error until then.
