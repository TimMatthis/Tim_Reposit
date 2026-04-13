const { PHOTO_ANALYSIS_SYSTEM, buildUserMessage } = require('./noBillPhotoPrompt');

const OPENAI_URL = 'https://api.openai.com/v1/chat/completions';

function mockAnalysis(imageCount) {
  return {
    summary: {
      overallReadiness: 'needs_more_photos',
      oneLineVerdict:
        'Demo mode: set OPENAI_API_KEY on the utility API server to run real vision analysis.',
    },
    perImage: Array.from({ length: imageCount }, (_, i) => ({
      index: i,
      filenameHint: '',
      categories: {
        electricity_meter: 'unclear',
        main_switchboard: 'unclear',
        switchgear_labels: 'unclear',
        roof_site: 'unclear',
        inverter_location: 'unclear',
        battery_location: 'unclear',
        consumer_mains_cabling: 'unclear',
      },
      whatYouSee: 'Not analyzed (no API key).',
      issues: ['Vision API not configured'],
    })),
    missingShots: [
      'Main switchboard with door open (clear photo of breakers)',
      'Electricity meter / meter box',
      'Wide roof shot showing planes and shading',
    ],
    recommendationsForCustomer: [
      'Start the utility API with OPENAI_API_KEY set, then run Analyze photos again.',
      'See the photo guide for exact angles Reposit needs for a fixed-price guarantee.',
    ],
    hazards: [],
    engineerNotes:
      'Mock response from utility_api/noBillPhotoAnalyze.js — replace with live OpenAI call for production.',
  };
}

/**
 * @param {{ mime: string, dataBase64: string }[]} images
 * @param {string} [apiKey]
 * @returns {Promise<{ ok: boolean, analysis?: object, error?: string, mock?: boolean, rawText?: string }>}
 */
async function analyzeSitePhotos(images, apiKey) {
  const n = images.length;
  if (!apiKey || !String(apiKey).trim()) {
    return { ok: true, mock: true, analysis: mockAnalysis(n) };
  }

  const userText = buildUserMessage(n);
  const userContent = [
    { type: 'text', text: userText },
    ...images.map(img => ({
      type: 'image_url',
      image_url: {
        url: `data:${img.mime};base64,${img.dataBase64}`,
        detail: 'low',
      },
    })),
  ];

  const body = JSON.stringify({
    model: 'gpt-4o-mini',
    messages: [
      { role: 'system', content: PHOTO_ANALYSIS_SYSTEM },
      { role: 'user', content: userContent },
    ],
    max_tokens: 2500,
    response_format: { type: 'json_object' },
  });

  let res;
  try {
    res = await fetch(OPENAI_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey.trim()}`,
      },
      body,
    });
  } catch (e) {
    return { ok: false, error: `OpenAI request failed: ${e.message || e}` };
  }

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data.error?.message || data.message || res.statusText || 'OpenAI error';
    return { ok: false, error: msg };
  }

  const text = data.choices?.[0]?.message?.content;
  if (!text || typeof text !== 'string') {
    return { ok: false, error: 'Empty model response' };
  }

  try {
    const analysis = JSON.parse(text);
    return { ok: true, analysis };
  } catch {
    return { ok: true, analysis: null, rawText: text, error: 'Model returned non-JSON' };
  }
}

module.exports = { analyzeSitePhotos, mockAnalysis };
