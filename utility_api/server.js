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
const path = require('path');
const { URL } = require('url');

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
      if (raw.length > maxBytes) reject(new Error('Request body too large'));
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

  sendJson(res, 404, { ok: false, error: 'Not found' });
});

server.listen(PORT, () => {
  console.log(`Utility top-packages API → http://localhost:${PORT}`);
  console.log(`monthlyFee: any integer $/mo (interpolated). GET /api/health to verify policy.`);
  console.log(`GET  /api/top-packages?monthlyFee=93`);
  console.log(`POST /api/top-packages  {"monthlyFee":93}`);
});
