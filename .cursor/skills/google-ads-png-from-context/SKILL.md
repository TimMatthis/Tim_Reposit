---
name: google-ads-png-from-context
description: Create Google Ads PNG image concepts from project source context while enforcing Reposit brand rules from the authoritative PDF brand guide ( colours, logo, layout ). Typography must use Geist only. Run mandatory brand-guide compliance plus Google Ads policy checks before shipping creatives.
---

# Google Ads PNG From Context

## Purpose

Use this skill to produce conversion-focused Google Ads image concepts from the current project context.

Default required size: `1200x628`.

## Authoritative brand template (mandatory)

Before generating or briefing any image (including prompts for `GenerateImage`, Pillow scripts, Figma exports, or external tools), treat this document as the single source of truth for Reposit visual identity:

**Brand guide PDF path (workspace):**  
`Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf`

### What you must take from the brand guide

- **Logo:** Marks, minimum size, exclusion zone, backgrounds allowed or forbidden, colour reversals (positive or negative lockups).
- **Colour:** Primary and secondary palettes as specified in the guide (including spot references such as Pantone where listed). Use the RGB or HEX conversions **printed in that guide** for screen or PNG output. Do not invent hex values when the guide specifies otherwise.
- **Layout and composition:** Grid, margins, hierarchy, illustration or photography style, and constraints that apply to marketing collateral (adapt sensibly to `1200x628` while respecting logo and colour rules).

### Typography exception (project override)

The 2017 PDF embeds legacy fonts (**Work Sans**, **Raleway**). **Do not use those families** for any deliverable governed by this skill.

**Forced rule:** Use **Geist only** for all typographic content on generated images:

- **Geist Sans** for headline, supporting lines, CTAs, disclaimers, and legal where readable at size.
- **Geist Mono** only when the brand guide explicitly calls for monospace or data-style presentation (otherwise omit).

Simulate hierarchy from the guide using **Geist Sans weights** (for example Regular or Medium for supporting copy, SemiBold or Bold for headlines and CTAs), not by introducing any third-party font.

If Geist files are not available in the environment, **install or bundle Geist** from the official Geist distribution before rasterising text. Do not substitute Arial, Inter, Poppins, Segoe UI, Work Sans, or Raleway as a workaround while claiming compliance with this skill.

### Standard raster typography schema (1200×628)

All Pillow-based PNG generators for this skill **must** use one shared schema so creatives stay visually consistent. Do not pick ad hoc pixel sizes per script.

**Source of truth (code):** `Landing pages/ImageLibrary/tools/google_ads_typography.py` (`FONT_SIZES`, `LINE_SPACING`, `LAYOUT`, `STANDARD_DISPLAY_AD_DISCLAIMER`, `load_geist`) for type and layout anchors; `Landing pages/ImageLibrary/tools/google_ads_compose.py` for decor overlays scaled **without** aspect-ratio distortion. Every Pillow generator should import from those modules.

**Canvas:** `1200×628` (`LAYOUT.canvas_w` × `LAYOUT.canvas_h`).

**Type sizes (px at 1:1 export):**

| Role | Size (px) | Geist weight | Typical use |
|------|-----------|--------------|-------------|
| Brand ribbon (text-only) | 22 | SemiBold | Single line when logo raster is missing |
| Tagline | 20 | Medium | Service line under logo (for example “Battery retrofit assessment”) |
| Audience | 18 | Medium | Segment label (for example battery brand or persona cue) |
| Headline | 46 | Bold | Primary promise |
| Support | 26 | Regular | Supporting copy |
| CTA | 24 | SemiBold | Label inside CTA pill |
| Disclaimer | 16 | Regular | Footer qualifier or legal |

**Wrapped-line rhythm (multi-line headline or support):**

- Headline: **52px** baseline step (`LINE_SPACING.headline`).
- Support: **32px** baseline step (`LINE_SPACING.support`).

**Shared layout anchors:** left padding **56px**, main text column max width **620px**, logo cap height **40px**, CTA pill height **56px**, bottom accent bar **6px** (`LAYOUT` fields). Adjust vertical stacking only when a layout variant requires it (for example logo present vs text-only brand line), but **keep font sizes and wrap rhythm unchanged**.

**Unified footer qualifier (PNG):** Use `STANDARD_DISPLAY_AD_DISCLAIMER` from `google_ads_typography.py` for every persona and battery-variant creative so subscript legal tone stays consistent (`Offer depends on your usage profile and eligibility.` unless compliance directs a single updated string there).

### HTML HubSpot offer emails

For **transactional HubSpot templates** pasted from this repo (see `Landing pages/Retrofit_CVP_Offer_Emails.html`):

- **Typography:** **Geist** as the first stack choice, **`Inter`** as the email-safe fallback, then system UI fonts. Load Geist via **Google Fonts** in the template `<head>` (`fonts.googleapis.com` `family=Geist` with weights needed for the design, typically **400–900**). Do **not** introduce **Work Sans** or **Raleway** in new HTML (2017 PDF legacy; not used on current CVP surfaces).
- **Colour:** Align with **`Retrofit_CVP_Landing.html`** tokens where possible (for example accent **#19c37d**, blue **#2a6b94**, ink **#0f172a**, hero blues **#2b78a7** family).
- **Logo:** Use approved HubSpot-hosted marks only (same URLs as landing). On `<img>`, preserve **aspect ratio** (set **width and height** attributes consistent with the SVG viewBox, or one dimension plus proportional height; avoid squashing decorative or logo raster art).

## Required inputs

Before generating any image, confirm all required inputs exist:

- Path to approved logo artwork (must match logo rules in the brand guide PDF unless the task specifies another approved asset).
- Colour values taken from or consistent with the brand guide PDF for the chosen medium (screen PNG).
- Confirmation that typography will be **Geist-only** per this skill.
- Campaign goal or offer.
- Audience or persona.

If any required input is missing, ask for it first.

## Source context workflow

1. Identify source files that define messaging, offer details, and tone.
2. Extract only high-signal context:
   - Main value proposition
   - Primary benefit
   - Proof point or trust cue
   - Clear CTA intent
3. Keep ad copy short and direct:
   - Headline: up to 40 characters preferred
   - Supporting line: up to 90 characters preferred
   - CTA text: 2-4 words
4. Create 2-4 distinct concept directions before generating final PNGs.

## Creative rules

- Prioritise one core promise per creative.
- Keep hierarchy clear: headline, support line, CTA, brand.
- Maintain high contrast for readability.
- Avoid clutter and excessive small text.
- Keep visual style aligned with source project tone **and** with the brand guide PDF (colour, logo, imagery discipline).
- Use **Geist-only** typography and colours consistent with the brand guide.

### Background and decorative imagery

Photography, diagrams, template artwork, or any raster layer that includes **logos, wordmarks, or brand geometry** (including embedded marks inside a larger PNG) **must keep its pixel aspect ratio**:

- Scale with **one uniform factor** only (equivalent to “contain” or “cover” using the same multiplier on width and height).
- **Forbidden:** resizing to independent target width and height when that does not match the source ratio (stretch, squash, letterboxing by distortion rather than by canvas).

**Pillow implementation:** `Landing pages/ImageLibrary/tools/google_ads_compose.py` (`overlay_faded_decor_right`, `resize_to_fit_preserving_aspect`). Use these helpers for right-hand decor overlays instead of raw `resize((w, h))` with unrelated dimensions.

Layout bounds for the decor slot sit on `LAYOUT.decor_preview_*` in `google_ads_typography.py`.

## Forced compliance gates (non-negotiable)

Do not deliver image concepts until **all** gates pass. If any gate fails, revise and re-run the full checklist.

### Gate A: Brand guide PDF

- Logo usage matches the PDF (placement, spacing, contrast, approved variants).
- Colours match PDF-defined primary or secondary palettes (including correct conversions for PNG).
- Overall execution does not contradict layout or imagery guidance in the PDF.
- **Raster decor and template artwork:** scaled uniformly so aspect ratio is preserved (see **Background and decorative imagery**). No non-uniform stretch of imagery that contains brand marks or diagrams.

### Gate B: Typography

- Every glyph on the creative is **Geist** (Sans by default; Mono only per guide-driven exceptions).
- No Work Sans, Raleway, Inter, system UI fonts, or other non-Geist families appear in the raster output.

### Gate C: Google Ads policy and honesty

- No misleading claims, fake urgency, or unverifiable superlatives.
- No prohibited or unsafe content.
- Copy aligns with destination page intent.
- CTA is clear and not deceptive.

If any gate fails, revise the concept and rerun all gates.

## Generation method

Use Cursor image generation (`GenerateImage`) when available, or an equivalent scripted raster pipeline (for example Pillow) that loads **Geist** from files.

Each generation prompt or script configuration must encode:

- Canvas size: `1200x628`
- Typography sizes and line rhythm **exactly** as in **Standard raster typography schema** (and `google_ads_typography.py` when using Pillow)
- **Decor / background rasters:** uniform scaling only; use `google_ads_compose.py` patterns so aspect ratio is never broken
- Visual style direction aligned with the brand guide PDF
- Exact headline, support line, and CTA
- Colour palette instructions **from the brand guide**
- **Geist-only** font and weight instructions
- Logo placement instruction **from the brand guide**
- Output format: PNG

## Output format

Return results in this structure:

```markdown
## Google Ads PNG Concepts

### Concept 1: [Name]
- Audience: [persona]
- Core message: [single promise]
- Copy:
  - Headline: "[text]"
  - Support: "[text]"
  - CTA: "[text]"
- Visual direction: [brief]
- Brand guide compliance: Pass/Fail ([notes])
- Geist-only typography: Pass/Fail
- Google Ads policy check: Pass/Fail
- Output: [generated PNG filename/path]

### Concept 2: [Name]
...
```

## Quality bar

- Every concept must look distinct.
- Every concept must preserve brand consistency **with the PDF guide**.
- Every concept must map to a clear campaign intent.
- Do not ship concepts with missing brand inputs or failed compliance gates.

## Additional resources

- Shared Pillow typography module: `Landing pages/ImageLibrary/tools/google_ads_typography.py`.
- Shared Pillow compositing (uniform-scale decor overlays): `Landing pages/ImageLibrary/tools/google_ads_compose.py`.
- Reusable prompts and examples: [examples.md](examples.md).
- Brand PDF: `Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf`.
