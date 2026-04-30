/**
 * Top-3 packages API (monthly fee constrained).
 * No dependencies — run: node server.js  →  http://localhost:3847
 *
 * POST /api/top-packages  Content-Type: application/json  { "monthlyFee": 93 }
 * GET  /api/top-packages?monthlyFee=93
 *
 * monthlyFee is any integer dollar amount; part-worth is piecewise-linear between
 * conjoint levels (same as the calculator price sliders), clamped outside the range.
 *
 * Optional POST body key "utilities" — same shape as defaults.json to override part-worths.
 */

const fs = require('fs');
const http = require('http');
const net = require('net');
const path = require('path');
const { spawn } = require('child_process');
const { URL } = require('url');
const { analyzeSitePhotos } = require('./noBillPhotoAnalyze');

const CLUSTER_DIR  = path.resolve(__dirname, '../Cluster');
const CLUSTER_PORT = 5000;

const OPTISIZER_DIR  = path.resolve(__dirname, '../Optisizer data/calculator');
const OPTISIZER_PORT = 5010;

const NEM_WEATHER_DIR  = path.resolve(__dirname, '../nem_weather_price_ml');
const NEM_WEATHER_PORT = 5020;

// Track the spawned Flask process so we don't start duplicates.
let clusterProc = null;
let optisizerProc = null;
let nemWeatherProc = null;

function isPortOpen(port) {
  return new Promise(resolve => {
    const sock = new net.Socket();
    sock.setTimeout(600);
    sock.on('connect', () => { sock.destroy(); resolve(true); });
    sock.on('error',   () => { sock.destroy(); resolve(false); });
    sock.on('timeout', () => { sock.destroy(); resolve(false); });
    sock.connect(port, '127.0.0.1');
  });
}

function startCluster() {
  // Already running (process handle still alive or port already open).
  if (clusterProc && !clusterProc.exitCode && clusterProc.exitCode !== 0) {
    return { started: false, message: 'Process handle already exists.' };
  }
  clusterProc = spawn('python3', ['app.py'], {
    cwd: CLUSTER_DIR,
    detached: false,
    stdio: 'ignore',
  });
  clusterProc.on('exit', () => { clusterProc = null; });
  return { started: true, message: 'Flask server starting…' };
}

function startOptisizer() {
  if (optisizerProc && !optisizerProc.exitCode && optisizerProc.exitCode !== 0) {
    return { started: false, message: 'Process handle already exists.' };
  }
  optisizerProc = spawn('python3', ['app.py'], {
    cwd: OPTISIZER_DIR,
    detached: false,
    stdio: 'ignore',
  });
  optisizerProc.on('exit', () => { optisizerProc = null; });
  return { started: true, message: 'Flask server starting…' };
}

function startNemWeather() {
  if (nemWeatherProc && !nemWeatherProc.exitCode && nemWeatherProc.exitCode !== 0) {
    return { started: false, message: 'Process handle already exists.' };
  }
  nemWeatherProc = spawn('python3', ['app.py'], {
    cwd: NEM_WEATHER_DIR,
    detached: false,
    stdio: 'ignore',
    env: { ...process.env, PORT: String(NEM_WEATHER_PORT) },
  });
  nemWeatherProc.on('exit', () => { nemWeatherProc = null; });
  return { started: true, message: 'Flask server starting…' };
}

const PORT = process.env.PORT || 3847;
const ATTR_ORDER = [
  'installationCost',
  'monthlyFee',
  'signupIncentive',
  'fairUseCap',
  'contractTerm'
];

const defaultUtilities = JSON.parse(
  fs.readFileSync(path.join(__dirname, 'defaults.json'), 'utf8')
);

function cloneUtilities(u) {
  return JSON.parse(JSON.stringify(u));
}

function deepMergeUtilities(base, override) {
  const out = cloneUtilities(base);
  if (!override || typeof override !== 'object') return out;
  for (const key of Object.keys(override)) {
    if (!out[key]) continue;
    const o = override[key];
    if (o.options) out[key].options = [...o.options];
    if (o.utilities) out[key].utilities = [...o.utilities].map(Number);
  }
  return out;
}

function getMaxAchievablePartWorthSum(utilities) {
  let sum = 0;
  for (const attr of ATTR_ORDER) {
    const arr = utilities[attr]?.utilities;
    if (!arr?.length) continue;
    sum += Math.max(...arr);
  }
  return sum;
}

function totalIndexedUtilityFromRawSum(rawSum, utilities) {
  const maxSum = getMaxAchievablePartWorthSum(utilities);
  if (maxSum <= 0) return 0;
  return (rawSum / maxSum) * 100;
}

function formatDisplay(attr, rawValue) {
  if (attr === 'installationCost' || attr === 'signupIncentive') return `$${rawValue}`;
  if (attr === 'monthlyFee') return `$${rawValue}/mo`;
  if (attr === 'fairUseCap' && typeof rawValue === 'number') return `${rawValue} kWh/day`;
  return String(rawValue);
}

function getSortedPriceUtilityPairs(options, utilsArr) {
  const pairs = options.map((p, i) => ({ p: Number(p), u: Number(utilsArr[i]) || 0 }));
  pairs.sort((a, b) => a.p - b.p);
  return pairs;
}

function interpolatedPartWorthFromPairs(pairs, price) {
  if (!pairs.length) return 0;
  const x = Number(price);
  if (Number.isNaN(x)) return pairs[0].u;
  if (x <= pairs[0].p) return pairs[0].u;
  const last = pairs[pairs.length - 1];
  if (x >= last.p) return last.u;
  for (let i = 0; i < pairs.length - 1; i++) {
    if (x >= pairs[i].p && x <= pairs[i + 1].p) {
      const t = (x - pairs[i].p) / (pairs[i + 1].p - pairs[i].p);
      return pairs[i].u + t * (pairs[i + 1].u - pairs[i].u);
    }
  }
  return last.u;
}

function interpolatedMonthlyPartWorth(utilities, dollars) {
  const m = utilities.monthlyFee;
  const pairs = getSortedPriceUtilityPairs(m.options, m.utilities);
  return interpolatedPartWorthFromPairs(pairs, dollars);
}

function resolveMonthlyFeeDollars(input) {
  if (input === undefined || input === null || input === '') {
    throw new Error('monthlyFee is required (integer dollars per month, e.g. 79 or 93)');
  }
  let n;
  if (typeof input === 'number' && Number.isFinite(input)) {
    n = Math.round(input);
  } else {
    const s = String(input)
      .trim()
      .replace(/^\$/, '')
      .replace(/\/mo$/i, '')
      .trim();
    n = Math.round(Number(s));
  }
  if (!Number.isFinite(n)) {
    throw new Error(`Invalid monthlyFee "${input}". Use an integer dollar amount (e.g. 79).`);
  }
  return n;
}

const DISCRETE_ATTRS = ATTR_ORDER.filter(a => a !== 'monthlyFee');

function generateDiscreteCombinations(utilities) {
  const combinations = [];
  function gen(attrs, current) {
    if (attrs.length === 0) {
      combinations.push({ ...current });
      return;
    }
    const [first, ...rest] = attrs;
    const n = utilities[first].options.length;
    for (let i = 0; i < n; i++) gen(rest, { ...current, [first]: i });
  }
  gen(DISCRETE_ATTRS, {});
  return combinations;
}

function buildPackageDetail(utilities, combo, lockedAttrs) {
  const packageDetail = {};
  for (const attr of ATTR_ORDER) {
    if (attr === 'monthlyFee') {
      const dollars = combo.monthlyFee;
      const pw = interpolatedMonthlyPartWorth(utilities, dollars);
      packageDetail[attr] = {
        value: dollars,
        display: formatDisplay('monthlyFee', dollars),
        partWorth: Number(pw.toFixed(4)),
        interpolated: true,
        locked: lockedAttrs.includes(attr)
      };
      continue;
    }
    const index = combo[attr];
    const rawValue = utilities[attr].options[index];
    const partWorth = utilities[attr].utilities[index];
    packageDetail[attr] = {
      index,
      value: rawValue,
      display: formatDisplay(attr, rawValue),
      partWorth: partWorth == null ? null : Number(partWorth.toFixed(4)),
      locked: lockedAttrs.includes(attr)
    };
  }
  return packageDetail;
}

function findTopPackages(utilities, monthlyFeeDollars, topN = 3) {
  const baseCombos = generateDiscreteCombinations(utilities);
  const monthlyUw = interpolatedMonthlyPartWorth(utilities, monthlyFeeDollars);
  const scored = baseCombos.map(combo => {
    const fullCombo = { ...combo, monthlyFee: monthlyFeeDollars };
    let rawSum = monthlyUw;
    for (const attr of DISCRETE_ATTRS) {
      rawSum += utilities[attr].utilities[combo[attr]];
    }
    return {
      combo: fullCombo,
      rawPartWorthSum: rawSum,
      indexedUtility: totalIndexedUtilityFromRawSum(rawSum, utilities)
    };
  });
  scored.sort((a, b) => b.indexedUtility - a.indexedUtility);
  const lockedAttrs = ['monthlyFee'];
  return scored.slice(0, topN).map((row, i) => ({
    rank: i + 1,
    indexedUtility: Number(row.indexedUtility.toFixed(4)),
    rawPartWorthSum: Number(row.rawPartWorthSum.toFixed(4)),
    combo: row.combo,
    package: buildPackageDetail(utilities, row.combo, lockedAttrs)
  }));
}

function readJsonBody(req, maxBytes = 524288) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', chunk => {
      raw += chunk;
      if (raw.length > maxBytes) {
        reject(new Error('Request body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!raw.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch (e) {
        reject(new Error('Invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}

const ALLOWED_PHOTO_MIMES = new Set(['image/jpeg', 'image/png', 'image/webp']);
const MAX_PHOTO_IMAGES = 8;
const MAX_PHOTO_JSON_BYTES = 22 * 1024 * 1024;

function validatePhotoPayload(body) {
  const images = body?.images;
  if (!Array.isArray(images) || images.length === 0) {
    throw new Error('images must be a non-empty array');
  }
  if (images.length > MAX_PHOTO_IMAGES) {
    throw new Error(`At most ${MAX_PHOTO_IMAGES} images per request`);
  }
  const out = [];
  for (let i = 0; i < images.length; i++) {
    const row = images[i];
    const mime = typeof row?.mime === 'string' ? row.mime.trim().toLowerCase() : '';
    const dataBase64 = typeof row?.dataBase64 === 'string' ? row.dataBase64.trim() : '';
    if (!ALLOWED_PHOTO_MIMES.has(mime)) {
      throw new Error(`Image ${i + 1}: mime must be image/jpeg, image/png, or image/webp`);
    }
    if (!dataBase64) {
      throw new Error(`Image ${i + 1}: dataBase64 is required`);
    }
    if (dataBase64.length > 6 * 1024 * 1024) {
      throw new Error(`Image ${i + 1}: payload too large (resize before upload)`);
    }
    out.push({ mime, dataBase64 });
  }
  return out;
}

function handleNoBillAnalyzePhotos(req, res) {
  if (req.method !== 'POST') {
    sendJson(res, 405, { ok: false, error: 'Method not allowed' });
    return;
  }
  readJsonBody(req, MAX_PHOTO_JSON_BYTES)
    .then(async body => {
      const images = validatePhotoPayload(body);
      const apiKey = process.env.OPENAI_API_KEY || '';
      const result = await analyzeSitePhotos(images, apiKey);
      if (!result.ok) {
        sendJson(res, 502, { ok: false, error: result.error || 'Analysis failed' });
        return;
      }
      sendJson(res, 200, {
        ok: true,
        mock: Boolean(result.mock),
        analysis: result.analysis,
        rawText: result.rawText,
        promptRef: 'utility_api/noBillPhotoPrompt.js',
      });
    })
    .catch(e => sendJson(res, 400, { ok: false, error: e.message || String(e) }));
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  });
  res.end(body);
}

function handleTopPackages(method, url, req, res) {
  const run = body => {
    try {
      const monthlyInput =
        body.monthlyFee !== undefined ? body.monthlyFee : url.searchParams.get('monthlyFee');
      const utilities = deepMergeUtilities(defaultUtilities, body.utilities);
      const feeDollars = resolveMonthlyFeeDollars(
        typeof monthlyInput === 'string' && /^\d+$/.test(monthlyInput.trim())
          ? Number(monthlyInput.trim())
          : monthlyInput === '' || monthlyInput === undefined
            ? undefined
            : monthlyInput
      );
      const topPackages = findTopPackages(utilities, feeDollars, 3);
      sendJson(res, 200, {
        ok: true,
        constraints: {
          monthlyFee: {
            value: feeDollars,
            display: formatDisplay('monthlyFee', feeDollars),
            interpolated: true
          }
        },
        topPackages,
        meta: {
          scale:
            'indexedUtility is 0–100 vs best achievable package for the part-worths in use',
          attributes: ATTR_ORDER
        }
      });
    } catch (e) {
      sendJson(res, 400, { ok: false, error: e.message || String(e) });
    }
  };

  if (method === 'GET') {
    run({});
    return;
  }
  if (method === 'POST') {
    readJsonBody(req)
      .then(run)
      .catch(e => sendJson(res, 400, { ok: false, error: e.message || String(e) }));
    return;
  }
  sendJson(res, 405, { ok: false, error: 'Method not allowed' });
}

// Serve static files from the workspace root so the landing page works
// at http://localhost:3847/ without file:// CORS issues.
const STATIC_ROOT = path.resolve(__dirname, '..');
const MIME_TYPES = {
  '.html': 'text/html',
  '.css':  'text/css',
  '.js':   'application/javascript',
  '.json': 'application/json',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif':  'image/gif',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
  '.csv':  'text/csv',
  '.md':   'text/markdown',
  '.txt':  'text/plain',
};

function serveStatic(urlPath, res) {
  const decoded = decodeURIComponent(urlPath);
  const filePath = path.join(STATIC_ROOT, decoded);
  const resolved = path.resolve(filePath);

  if (!resolved.startsWith(STATIC_ROOT)) {
    sendJson(res, 403, { ok: false, error: 'Forbidden' });
    return;
  }

  fs.stat(resolved, (err, stats) => {
    if (err) {
      sendJson(res, 404, { ok: false, error: 'Not found' });
      return;
    }
    // Directory → serve index.html inside it (works on Windows and Unix)
    if (stats.isDirectory()) {
      const indexPath = path.join(resolved, 'index.html');
      fs.stat(indexPath, (err2, stats2) => {
        if (err2 || !stats2.isFile()) {
          sendJson(res, 404, { ok: false, error: 'Not found' });
          return;
        }
        res.writeHead(200, { 'Content-Type': 'text/html' });
        fs.createReadStream(indexPath).pipe(res);
      });
      return;
    }
    if (!stats.isFile()) {
      sendJson(res, 404, { ok: false, error: 'Not found' });
      return;
    }
    const ext = path.extname(resolved).toLowerCase();
    const mime = MIME_TYPES[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime });
    fs.createReadStream(resolved).pipe(res);
  });
}

const server = http.createServer((req, res) => {
  let url;
  try {
    url = new URL(req.url || '/', `http://127.0.0.1:${PORT}`);
  } catch {
    sendJson(res, 400, { ok: false, error: 'Bad URL' });
    return;
  }

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Max-Age': '86400'
    });
    res.end();
    return;
  }

  if (url.pathname === '/api/health') {
    sendJson(res, 200, {
      ok: true,
      service: 'utility-top-packages',
      /** Hub uses this to avoid calling /api/optisizer/* on pre–Optisizer utility_api builds (those return 404). */
      optisizerLauncher: true,
      /** Hub uses this for NEM + BOM Flask explorer (port 5020). */
      nemWeatherLauncher: true,
      monthlyFee: {
        mode: 'anyIntegerDollars',
        partWorth: 'piecewiseLinearInterpolatedBetweenConjointLevels'
      }
    });
    return;
  }

  if (url.pathname === '/api/top-packages') {
    handleTopPackages(req.method, url, req, res);
    return;
  }

  if (url.pathname === '/api/cluster/start' && req.method === 'POST') {
    isPortOpen(CLUSTER_PORT).then(already => {
      if (already) {
        sendJson(res, 200, { ok: true, status: 'already_running' });
      } else {
        const result = startCluster();
        sendJson(res, 200, { ok: true, status: 'starting', ...result });
      }
    });
    return;
  }

  if (url.pathname === '/api/cluster/status') {
    isPortOpen(CLUSTER_PORT).then(open => {
      sendJson(res, 200, { ok: true, running: open });
    });
    return;
  }

  if (url.pathname === '/api/optisizer/start' && req.method === 'POST') {
    isPortOpen(OPTISIZER_PORT).then(already => {
      if (already) {
        sendJson(res, 200, { ok: true, status: 'already_running' });
      } else {
        const result = startOptisizer();
        sendJson(res, 200, { ok: true, status: 'starting', ...result });
      }
    });
    return;
  }

  if (url.pathname === '/api/optisizer/status') {
    isPortOpen(OPTISIZER_PORT).then(open => {
      sendJson(res, 200, { ok: true, running: open });
    });
    return;
  }

  if (url.pathname === '/api/nem-weather/start' && req.method === 'POST') {
    isPortOpen(NEM_WEATHER_PORT).then(already => {
      if (already) {
        sendJson(res, 200, { ok: true, status: 'already_running' });
      } else {
        const result = startNemWeather();
        sendJson(res, 200, { ok: true, status: 'starting', ...result });
      }
    });
    return;
  }

  if (url.pathname === '/api/nem-weather/status') {
    isPortOpen(NEM_WEATHER_PORT).then(open => {
      sendJson(res, 200, { ok: true, running: open });
    });
    return;
  }

  if (url.pathname === '/api/consent/request-code') {
    if (req.method !== 'POST') { sendJson(res, 405, { ok: false, error: 'Method not allowed' }); return; }
    readJsonBody(req).then(() => {
      sendJson(res, 200, { ok: true, message: 'Code sent (dev mode — use any 6 digits)' });
    }).catch(e => sendJson(res, 400, { ok: false, error: e.message }));
    return;
  }

  if (url.pathname === '/api/consent/verify-code') {
    if (req.method !== 'POST') { sendJson(res, 405, { ok: false, error: 'Method not allowed' }); return; }
    readJsonBody(req).then(() => {
      sendJson(res, 200, { ok: true, message: 'Code verified (dev mode)' });
    }).catch(e => sendJson(res, 400, { ok: false, error: e.message }));
    return;
  }

  if (url.pathname === '/api/no-bill/health' && req.method === 'GET') {
    sendJson(res, 200, {
      ok: true,
      route: 'no-bill',
      openaiConfigured: Boolean(process.env.OPENAI_API_KEY && String(process.env.OPENAI_API_KEY).trim()),
    });
    return;
  }

  if (url.pathname === '/api/no-bill/analyze-photos') {
    handleNoBillAnalyzePhotos(req, res);
    return;
  }

  // Fall through: serve static files from workspace root
  if (req.method === 'GET') {
    serveStatic(url.pathname, res);
    return;
  }

  sendJson(res, 404, { ok: false, error: 'Not found' });
});

server.listen(PORT, () => {
  console.log(`\nProject Hub → http://localhost:${PORT}`);
  console.log(`API         → http://localhost:${PORT}/api/health`);
  console.log(`Cluster     → http://localhost:${PORT}/api/cluster/status`);
  console.log(`Optisizer   → http://localhost:${PORT}/api/optisizer/status`);
  console.log(`NEM weather → http://localhost:${PORT}/api/nem-weather/status`);
  console.log(`No-Bill AI  → GET  http://localhost:${PORT}/api/no-bill/health`);
  console.log(`            → POST http://localhost:${PORT}/api/no-bill/analyze-photos`);
  if (!process.env.OPENAI_API_KEY) {
    console.log('            → OPENAI_API_KEY not set — photo analysis returns mock JSON');
  }
  console.log();
});
