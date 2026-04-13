#!/usr/bin/env bash
# Start project hub (utility_api) on :3847, then start Cluster, Optisizer, and NEM+BOM Flask apps.
# Usage from repo root:  ./start-hub.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${PORT:-3847}"
API="http://127.0.0.1:${PORT}"

if curl -sf "${API}/api/health" >/dev/null 2>&1; then
  echo "Hub already responding at ${API}"
else
  echo "Starting utility_api (logs: ${ROOT}/.hub-server.log) …"
  nohup bash -c "cd \"${ROOT}/utility_api\" && exec node server.js" >>"${ROOT}/.hub-server.log" 2>&1 &
  echo -n "Waiting for ${API}/api/health"
  for _ in $(seq 1 60); do
    if curl -sf "${API}/api/health" >/dev/null 2>&1; then
      echo " — ok."
      break
    fi
    echo -n "."
    sleep 0.25
  done
  if ! curl -sf "${API}/api/health" >/dev/null 2>&1; then
    echo ""
    echo "Hub did not become ready. See ${ROOT}/.hub-server.log"
    exit 1
  fi
fi

echo "Starting backend apps (no-op if already running)…"
curl -sf -X POST "${API}/api/cluster/start"     >/dev/null && echo "  · Cluster (5000)"     || true
curl -sf -X POST "${API}/api/optisizer/start"   >/dev/null && echo "  · Optisizer (5010)"   || true
curl -sf -X POST "${API}/api/nem-weather/start" >/dev/null && echo "  · NEM + BOM (5020)"   || true

echo ""
echo "Index page:  ${API}/"
echo "  Cluster     → http://127.0.0.1:5000/"
echo "  Optisizer   → http://127.0.0.1:5010/"
echo "  NEM + BOM   → http://127.0.0.1:5020/"
echo ""
echo "Stop hub: pkill -f 'utility_api/server.js'  (Flask apps are separate Python processes.)"
