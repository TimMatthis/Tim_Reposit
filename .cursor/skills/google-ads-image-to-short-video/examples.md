# Examples: Image to short Google Ads video

## Example A: Single landscape PNG to 15s spot

**Input:** One `1200×628` or `1920×1080` display-style creative with headline, support, CTA.

**Storyboard (16:9):**

1. 0–2s: Full frame, subtle zoom in on background only; logo corner static.
2. 2–7s: Headline animates in (two lines); VO reads headline.
3. 7–11s: Support line fades up; VO reads support.
4. 11–13s: CTA highlight pulse once (scale 1.0→1.03→1.0).
5. 13–15s: End card, VO repeats CTA phrase; hold brand.

**VO script (illustrative):**

> "Cut your evening peak bills with a battery that fits your usage. Reposit helps you shift load and capture value. Check if your system is compatible."

**Export:** `1920×1080`, H.264, AAC, ~15s.

---

## Example B: Vertical crop from horizontal art

**Input:** Horizontal master; need `1080×1920` for Shorts-style inventory.

**Approach:** Letterbox or smart crop **center-weighted** so headline block and CTA stay in the **middle 70%** height. Animate the same beats as Example A; verify thumb-stopping zone is not clipped.

---

## Example C: Three PNG variants as one message ladder

**Input:** Three persona variants (same aspect).

**Storyboard:** 4–5s each segment, hard cut or 0.3s crossfade between; single VO paragraph with pauses at cuts; end card uses final CTA only once.

---

## ffmpeg note

Exact filter graphs depend on whether you use **still frames + crossfade**, **pre-rendered segments**, or **one long filter_complex**. Prefer splitting complex work into **numbered PNG/JPG frames** or **short intermediate clips** to keep commands maintainable.
