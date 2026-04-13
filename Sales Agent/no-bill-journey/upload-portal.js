/**
 * Site photos + vision API. Static pages (e.g. port 8080) call the utility Node server (default 3847).
 */
(function () {
  const STORAGE_KEY = 'repositNoBillApiBase';
  const DEFAULT_UTILITY_PORT = '3847';

  function normalizeBase(s) {
    if (s == null) return '';
    let t = String(s).trim().replace(/\/+$/, '');
    if (t === 'http://' || t === 'https://') return '';
    return t;
  }

  function metaApiBase() {
    const el = document.querySelector('meta[name="reposit-no-bill-api"]');
    if (!el || !el.content) return '';
    return normalizeBase(el.content);
  }

  function queryApiBase() {
    const q = new URLSearchParams(window.location.search);
    const full = q.get('api');
    if (full && full.trim()) return normalizeBase(full);
    const port = q.get('utilityPort');
    if (port && /^\d+$/.test(port)) {
      return normalizeBase(window.location.protocol + '//' + window.location.hostname + ':' + port);
    }
    return '';
  }

  /** When the HTML is served from the utility server, API is same-origin. */
  function computeDefaultBase() {
    if (window.location.protocol === 'file:') {
      return 'http://127.0.0.1:' + DEFAULT_UTILITY_PORT;
    }
    if (String(window.location.port) === DEFAULT_UTILITY_PORT) {
      return '';
    }
    return normalizeBase(
      window.location.protocol + '//' + window.location.hostname + ':' + DEFAULT_UTILITY_PORT
    );
  }

  function storedBase() {
    try {
      return normalizeBase(sessionStorage.getItem(STORAGE_KEY));
    } catch (_) {
      return '';
    }
  }

  function setStoredBase(base) {
    try {
      const n = normalizeBase(base);
      if (n) {
        sessionStorage.setItem(STORAGE_KEY, n);
      } else {
        sessionStorage.removeItem(STORAGE_KEY);
      }
    } catch (_) { /* ignore */ }
  }

  function inputBase() {
    const el = document.getElementById('nb-api-base');
    return el ? normalizeBase(el.value) : '';
  }

  /**
   * Bases to try when testing the connection (order matters).
   */
  function buildCandidateBases() {
    const seen = new Set();
    const out = [];
    function add(b) {
      const n = normalizeBase(b);
      const key = n === '' ? '__same__' : n;
      if (seen.has(key)) return;
      seen.add(key);
      out.push(n);
    }

    add(inputBase());
    add(storedBase());
    add(metaApiBase());
    add(queryApiBase());
    add(computeDefaultBase());
    if (window.location.hostname === 'localhost') {
      add('http://127.0.0.1:' + DEFAULT_UTILITY_PORT);
    }
    if (String(window.location.port) === DEFAULT_UTILITY_PORT) {
      add('');
    }
    return out;
  }

  /** Base used for Analyze: explicit input > saved session > meta/query > default. */
  function resolveBaseForAnalyze() {
    const fromInput = inputBase();
    if (fromInput) return fromInput;
    const st = storedBase();
    if (st) return st;
    const m = metaApiBase();
    if (m) return m;
    const q = queryApiBase();
    if (q) return q;
    return computeDefaultBase();
  }

  function displayResolvedLabel() {
    const el = document.getElementById('nb-api-resolved');
    if (!el) return;
    const b = resolveBaseForAnalyze();
    el.textContent = b ? b : window.location.origin || '(same origin)';
  }

  const MIME_MAP = {
    'image/jpeg': 'image/jpeg',
    'image/jpg': 'image/jpeg',
    'image/png': 'image/png',
    'image/webp': 'image/webp',
  };

  const MAX_EDGE = 1600;
  const JPEG_QUALITY = 0.82;

  function loadImageFromFile(file) {
    return new Promise((resolve, reject) => {
      const url = URL.createObjectURL(file);
      const img = new Image();
      img.onload = function () {
        URL.revokeObjectURL(url);
        resolve(img);
      };
      img.onerror = function () {
        URL.revokeObjectURL(url);
        reject(new Error('Could not read image'));
      };
      img.src = url;
    });
  }

  function canvasToJpegBase64(img) {
    let w = img.naturalWidth || img.width;
    let h = img.naturalHeight || img.height;
    if (w <= 0 || h <= 0) {
      throw new Error('Invalid image dimensions');
    }
    const scale = Math.min(1, MAX_EDGE / Math.max(w, h));
    const tw = Math.round(w * scale);
    const th = Math.round(h * scale);
    const canvas = document.createElement('canvas');
    canvas.width = tw;
    canvas.height = th;
    const ctx = canvas.getContext('2d');
    if (!ctx) {
      throw new Error('Canvas not supported');
    }
    ctx.drawImage(img, 0, 0, tw, th);
    const dataUrl = canvas.toDataURL('image/jpeg', JPEG_QUALITY);
    const parts = dataUrl.split(',');
    if (parts.length < 2) {
      throw new Error('Could not encode image');
    }
    return { dataBase64: parts[1], mime: 'image/jpeg' };
  }

  async function filesToPayload(files) {
    const out = [];
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const mime = MIME_MAP[file.type];
      if (!mime) {
        throw new Error('Unsupported type: ' + (file.type || file.name) + ' — use JPG, PNG, or WebP');
      }
      const img = await loadImageFromFile(file);
      out.push(canvasToJpegBase64(img));
    }
    return out;
  }

  function setBusy(btn, busy, label) {
    btn.disabled = busy;
    if (label != null) {
      btn.textContent = label;
    }
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function renderAnalysis(data) {
    const el = document.getElementById('nb-analysis-root');
    if (!el) return;

    if (!data || !data.analysis) {
      const err = data?.error || data?.rawText || 'No analysis returned';
      el.innerHTML =
        '<div class="nb-analysis nb-analysis--error"><p class="nb-analysis-title">Could not use this result</p><pre class="nb-analysis-pre">' +
        escapeHtml(typeof err === 'string' ? err : JSON.stringify(err, null, 2)) +
        '</pre></div>';
      return;
    }

    const a = data.analysis;
    const mock = data.mock ? ' <span class="nb-badge-mock">Demo / mock</span>' : '';
    const readiness = a.summary?.overallReadiness || 'unknown';
    const badgeClass =
      readiness === 'ready'
        ? 'nb-readiness nb-readiness--ready'
        : readiness === 'needs_more_photos'
          ? 'nb-readiness nb-readiness--more'
          : 'nb-readiness nb-readiness--hold';

    let html = '<div class="nb-analysis">';
    html += '<p class="nb-analysis-title">AI site check' + mock + '</p>';
    html += '<p class="' + badgeClass + '">' + escapeHtml(readiness) + '</p>';
    if (a.summary?.oneLineVerdict) {
      html += '<p class="nb-analysis-verdict">' + escapeHtml(a.summary.oneLineVerdict) + '</p>';
    }

    if (Array.isArray(a.perImage) && a.perImage.length) {
      html += '<h3 class="nb-analysis-sub">Per image</h3><ul class="nb-analysis-per-image">';
      a.perImage.forEach(function (row) {
        html += '<li><strong>Image ' + (Number(row.index) + 1 || '?') + '</strong>';
        if (row.whatYouSee) {
          html += ' — ' + escapeHtml(row.whatYouSee);
        }
        if (row.categories && typeof row.categories === 'object') {
          html += '<ul class="nb-cat-list">';
          Object.keys(row.categories).forEach(function (k) {
            html +=
              '<li><span class="nb-cat-key">' +
              escapeHtml(k) +
              '</span> <span class="nb-cat-val">' +
              escapeHtml(String(row.categories[k])) +
              '</span></li>';
          });
          html += '</ul>';
        }
        if (Array.isArray(row.issues) && row.issues.length) {
          html += '<p class="nb-issues">Issues: ' + row.issues.map(escapeHtml).join('; ') + '</p>';
        }
        html += '</li>';
      });
      html += '</ul>';
    }

    if (Array.isArray(a.missingShots) && a.missingShots.length) {
      html += '<h3 class="nb-analysis-sub">Still needed</h3><ul class="nb-missing">';
      a.missingShots.forEach(function (x) {
        html += '<li>' + escapeHtml(x) + '</li>';
      });
      html += '</ul>';
    }

    if (Array.isArray(a.recommendationsForCustomer) && a.recommendationsForCustomer.length) {
      html += '<h3 class="nb-analysis-sub">Suggestions</h3><ul class="nb-reco">';
      a.recommendationsForCustomer.forEach(function (x) {
        html += '<li>' + escapeHtml(x) + '</li>';
      });
      html += '</ul>';
    }

    if (Array.isArray(a.hazards) && a.hazards.length) {
      html += '<div class="nb-hazards"><strong>Hazards flagged</strong><ul>';
      a.hazards.forEach(function (x) {
        html += '<li>' + escapeHtml(x) + '</li>';
      });
      html += '</ul></div>';
    }

    if (a.engineerNotes) {
      html +=
        '<details class="nb-engineer"><summary>Internal notes</summary><p>' +
        escapeHtml(a.engineerNotes) +
        '</p></details>';
    }

    html += '<p class="nb-prompt-ref">Prompt: <code>utility_api/noBillPhotoPrompt.js</code></p>';
    html += '</div>';
    el.innerHTML = html;
  }

  function updateSubmitButton(analysis) {
    const submit = document.getElementById('nb-submit-design');
    if (!submit) return;
    const ready = analysis && analysis.summary && analysis.summary.overallReadiness === 'ready';
    submit.disabled = !ready;
    submit.setAttribute('aria-disabled', ready ? 'false' : 'true');
    submit.title = ready
      ? ''
      : 'Unlocks when the AI marks overall readiness as ready (adjust in CRM if needed).';
  }

  async function runTestConnection() {
    const status = document.getElementById('nb-api-status');
    const btn = document.getElementById('nb-api-test');
    if (btn) {
      setBusy(btn, true, 'Testing…');
    }
    if (status) {
      status.textContent = 'Trying API bases…';
      status.className = 'nb-api-status';
    }

    const candidates = buildCandidateBases();
    let lastErr = 'No reachable API';
    for (let i = 0; i < candidates.length; i++) {
      const base = candidates[i];
      const url = (base || '') + '/api/no-bill/health';
      try {
        const res = await fetch(url, { method: 'GET' });
        const json = await res.json().catch(() => ({}));
        if (res.ok && json.ok) {
          const input = document.getElementById('nb-api-base');
          if (input) {
            input.value = base;
          }
          setStoredBase(base);
          if (status) {
            status.textContent =
              'Connected: ' +
              (base || window.location.origin) +
              (json.openaiConfigured ? ' · OpenAI key loaded' : ' · mock vision until OPENAI_API_KEY is set');
            status.className = 'nb-api-status nb-api-status--ok';
          }
          displayResolvedLabel();
          if (btn) {
            setBusy(btn, false, 'Test connection');
          }
          return;
        }
        lastErr = json.error || res.statusText || 'Bad response';
      } catch (e) {
        lastErr = e.message || String(e);
      }
    }

    if (status) {
      status.textContent =
        'Could not reach No-Bill API. Start: cd utility_api && node server.js — then Test again. Last error: ' +
        lastErr;
      status.className = 'nb-api-status nb-api-status--err';
    }
    displayResolvedLabel();
    if (btn) {
      setBusy(btn, false, 'Test connection');
    }
  }

  async function runAnalyze() {
    const input = document.getElementById('nb-photo-input');
    const btn = document.getElementById('nb-analyze-photos');
    const status = document.getElementById('nb-photo-status');
    const files = input && input.files ? Array.from(input.files) : [];
    if (!files.length) {
      if (status) {
        status.textContent = 'Choose one or more photos first.';
      }
      return;
    }

    const root = document.getElementById('nb-analysis-root');
    if (root) {
      root.innerHTML = '';
    }
    if (status) {
      status.textContent = 'Resizing images and calling API…';
    }
    setBusy(btn, true, 'Analyzing…');

    const base = resolveBaseForAnalyze();
    const postUrl = (base || '') + '/api/no-bill/analyze-photos';

    try {
      const images = await filesToPayload(files);
      const res = await fetch(postUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ images }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok || !json.ok) {
        throw new Error(json.error || res.statusText || 'Request failed');
      }
      renderAnalysis(json);
      updateSubmitButton(json.analysis);
      if (status) {
        status.textContent = json.mock
          ? 'Mock analysis (set OPENAI_API_KEY on the utility server for live vision).'
          : 'Analysis complete.';
      }
    } catch (e) {
      const hint =
        e.message && /Failed to fetch|NetworkError/i.test(e.message)
          ? ' Check API base above and run Test connection — static port 8080 does not host /api by itself.'
          : '';
      if (status) {
        status.textContent = (e.message || String(e)) + hint;
      }
      renderAnalysis({ error: (e.message || String(e)) + hint });
      updateSubmitButton(null);
    } finally {
      setBusy(btn, false, 'Run photo check');
    }
  }

  function applyDefaultBaseToInput() {
    const input = document.getElementById('nb-api-base');
    if (!input) return;
    const d = computeDefaultBase();
    input.value = d;
    setStoredBase(d);
    displayResolvedLabel();
    const status = document.getElementById('nb-api-status');
    if (status) {
      status.textContent = 'Filled default for this browser host. Click Test connection.';
      status.className = 'nb-api-status';
    }
  }

  function wire() {
    const input = document.getElementById('nb-api-base');
    if (input) {
      const initial = storedBase() || metaApiBase() || queryApiBase();
      if (initial) {
        input.value = initial;
      }
      input.addEventListener('input', function () {
        setStoredBase(inputBase());
        displayResolvedLabel();
      });
      input.addEventListener('change', function () {
        setStoredBase(inputBase());
        displayResolvedLabel();
      });
    }

    displayResolvedLabel();

    const testBtn = document.getElementById('nb-api-test');
    if (testBtn) {
      testBtn.addEventListener('click', runTestConnection);
    }
    const defBtn = document.getElementById('nb-api-default');
    if (defBtn) {
      defBtn.addEventListener('click', applyDefaultBaseToInput);
    }

    const analyzeBtn = document.getElementById('nb-analyze-photos');
    if (analyzeBtn) {
      analyzeBtn.addEventListener('click', runAnalyze);
    }

    const photoInput = document.getElementById('nb-photo-input');
    if (photoInput) {
      photoInput.addEventListener('change', function () {
        const st = document.getElementById('nb-photo-status');
        if (st && photoInput.files && photoInput.files.length) {
          st.textContent = photoInput.files.length + ' file(s) selected.';
        }
      });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', wire);
  } else {
    wire();
  }
})();
