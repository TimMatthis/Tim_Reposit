# Google Ads PNG Prompt Examples

These templates implement [SKILL.md](SKILL.md): **authoritative brand PDF**, **Geist-only typography**, and **forced compliance gates A through C**.

## Authoritative brand PDF (mandatory reference)

`Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf`

Fill `[PALETTE]` and `[LOGO_INSTRUCTION]` from that PDF (RGB or HEX as printed there). Do not invent colours when the guide specifies otherwise.

## Typography (mandatory, no substitutions)

Use **Geist Sans** for all text unless the brand guide explicitly requires monospace or data styling, then **Geist Mono** for those tokens only.

Standard prompt block (paste under **Typography** in every template):

```text
Typography (Geist only):
- Font family: Geist Sans for headline, support line, CTA label, disclaimers
- Headline weight: SemiBold or Bold
- Support and footer weight: Regular or Medium
- CTA weight: SemiBold or Bold
- Geist Mono: omit unless brand guide specifies monospace or tabular data
- Forbidden on-image fonts: Work Sans, Raleway, Inter, Poppins, Segoe UI, Arial, system UI fonts, or any non-Geist family
```

**HTML HubSpot offer emails** (templates, not raster): Geist first, **Inter** as fallback, Google Fonts `<link>` in `<head>`; see [SKILL.md](SKILL.md) section **HTML HubSpot offer emails** and `Landing pages/Retrofit_CVP_Offer_Emails.html`.

Raster pipelines (Pillow): use **`Landing pages/ImageLibrary/tools/google_ads_typography.py`** (`FONT_SIZES`, `LINE_SPACING`, `LAYOUT`, `load_geist`) as the single pixel schema. **Decor / background PNGs:** scale with **`Landing pages/ImageLibrary/tools/google_ads_compose.py`** so aspect ratio is never broken (see SKILL.md “Background and decorative imagery”). Install Geist Sans `.ttf` files under `Landing pages/ImageLibrary/fonts/Geist/`. Generators: `generate_persona_google_ads.py`, `generate_pmax_sigenergy_solax_persona_ads.py`.

## Required variables

- `[AUDIENCE]`
- `[OFFER]`
- `[PRIMARY_BENEFIT]`
- `[PROOF_POINT]`
- `[CTA]`
- `[LOGO_INSTRUCTION]` (from brand PDF)
- `[PALETTE]` (from brand PDF, screen-safe values)

Optional:

- `[HEADLINE_WEIGHT]` (default `SemiBold or Bold`)
- `[BODY_WEIGHT]` (default `Regular or Medium`)

## Compliance block (paste into every prompt)

```text
Forced compliance (must pass before delivery):
- Gate A Brand guide PDF: Logo, colours, layout or imagery rules match the PDF
- Gate B Typography: Only Geist Sans or Geist Mono per rules above appears in the raster output
- Gate C Google Ads policy: No misleading claims, fake urgency, unverifiable superlatives, or prohibited content; CTA honest and aligned with landing intent
```

## Master prompt template

```text
Create a Google Ads image creative as a PNG.

Canvas size: 1200x628
Audience: [AUDIENCE]
Campaign goal: [OFFER]

Authoritative brand reference:
- Reposit brand guide PDF: Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf

Copy to include exactly:
- Headline: "[PRIMARY_BENEFIT]"
- Support line: "[PROOF_POINT]"
- CTA: "[CTA]"

Brand constraints:
- Use only these colours (from brand PDF): [PALETTE]
- Logo requirement (from brand PDF): [LOGO_INSTRUCTION]

Typography (Geist only):
- Font family: Geist Sans for all on-image text unless monospace is required by the brand PDF
- Headline weight: [HEADLINE_WEIGHT]
- Support line weight: [BODY_WEIGHT]
- CTA weight: [HEADLINE_WEIGHT]
- Geist Mono only if the brand guide specifies monospace or data-style presentation
- Do not use Work Sans, Raleway, Inter, Poppins, or system default sans fonts

Design direction:
- Clear visual hierarchy: headline, support line, CTA, logo
- High contrast and readability
- Minimal clutter
- Conversion-focused composition

Forced compliance (must pass before delivery):
- Gate A Brand guide PDF: Logo, colours, layout or imagery rules match the PDF
- Gate B Typography: Only Geist per rules above in final PNG
- Gate C Google Ads policy: Honest claims, no fake urgency, CTA matches destination intent

Output:
- PNG format
- One finished creative
```

## Variant template A: Benefit first

```text
Create a 1200x628 PNG Google Ads creative focused on one clear benefit.

Authoritative brand reference:
- Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf

Include text:
- Headline: "[PRIMARY_BENEFIT]"
- Support line: "[PROOF_POINT]"
- CTA: "[CTA]"

Visual style:
- Clean modern layout
- Product or lifestyle image area on one side
- Text block on the opposite side
- Strong CTA button contrast

Brand:
- Palette (from PDF): [PALETTE]
- Logo (from PDF): [LOGO_INSTRUCTION]

Typography (Geist only): Geist Sans; headline [HEADLINE_WEIGHT]; body [BODY_WEIGHT]; CTA [HEADLINE_WEIGHT]. Geist Mono only if PDF requires monospace. No Work Sans, Raleway, Inter, Poppins, or system fonts.

Forced compliance (must pass before delivery):
- Gate A Brand guide PDF
- Gate B Geist-only typography
- Gate C Google Ads policy (factual tone, supportable claims)
```

## Variant template B: Trust first

```text
Create a 1200x628 PNG Google Ads creative that leads with credibility and trust.

Authoritative brand reference:
- Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf

Include text:
- Headline: "[PRIMARY_BENEFIT]"
- Support line: "[PROOF_POINT]"
- CTA: "[CTA]"

Visual style:
- Professional, stable tone
- Extra whitespace for readability
- Emphasize trust cue near support line
- CTA remains visually dominant

Brand:
- Palette (from PDF): [PALETTE]
- Logo (from PDF): [LOGO_INSTRUCTION]

Typography (Geist only): Geist Sans throughout; headline [HEADLINE_WEIGHT]; support [BODY_WEIGHT]. No non-Geist fonts.

Forced compliance (must pass before delivery):
- Gate A Brand guide PDF
- Gate B Geist-only typography
- Gate C Google Ads policy (no unverifiable superlatives; claims specific and supportable)
```

## Variant template C: Time-bound without hype

```text
Create a 1200x628 PNG Google Ads creative for time-sensitive action without hype.

Authoritative brand reference:
- Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf

Include text:
- Headline: "[PRIMARY_BENEFIT]"
- Support line: "[PROOF_POINT]"
- CTA: "[CTA]"

Visual style:
- Strong focal point around CTA
- Time cue only if truthful and permitted by policy
- Simple composition with immediate clarity

Brand:
- Palette (from PDF): [PALETTE]
- Logo (from PDF): [LOGO_INSTRUCTION]

Typography (Geist only): Geist Sans; weights per SKILL.md. No Work Sans, Raleway, Inter, Poppins, or system fonts.

Forced compliance (must pass before delivery):
- Gate A Brand guide PDF
- Gate B Geist-only typography
- Gate C Google Ads policy (no fake countdowns; no false scarcity)
```

## Quick copy patterns

Use when source context is noisy:

- Headline formula: `Get [primary outcome] with [solution]`
- Support formula: `[proof point] for [audience]`
- CTA formula: `[Action] [Benefit]`

Typographic hierarchy stays **Geist Sans** with weight contrast only.

## Example filled prompt

Values below illustrate structure only. Replace `[PALETTE]` hex values with those printed in the brand PDF when they differ.

```text
Create a Google Ads image creative as a PNG.

Canvas size: 1200x628
Audience: Homeowners with high evening energy usage
Campaign goal: Increase qualified enquiries for battery retrofit assessments

Authoritative brand reference:
- Landing pages/ImageLibrary/SeedTemplates/brand/FINAL Reposit Brand Guide 2017 (1).pdf

Copy to include exactly:
- Headline: "Store Solar, Use It at Night"
- Support line: "Personalised assessment from your usage profile before any commitment"
- CTA: "Get My Estimate"

Brand constraints:
- Use only these colours (confirm against PDF): #0B1F3B, #1FA2FF, #F5F8FF, #FFFFFF
- Logo requirement (from PDF): Approved Reposit lockup, minimum clear space per guide, placement top-left on dark backgrounds

Typography (Geist only):
- Geist Sans SemiBold or Bold for headline and CTA
- Geist Sans Regular or Medium for support line
- Geist Mono omitted (not required by guide)
- Forbidden: Work Sans, Raleway, Inter, Poppins, Segoe UI, Arial

Design direction:
- Clear visual hierarchy: headline, support line, CTA, logo
- High contrast and readability
- Minimal clutter
- Conversion-focused composition

Forced compliance (must pass before delivery):
- Gate A Brand guide PDF: Logo and colours match PDF specifications
- Gate B Typography: Raster output uses Geist only
- Gate C Google Ads policy: No misleading claims; no fake urgency; match landing intent

Output:
- PNG format
- One finished creative
```
