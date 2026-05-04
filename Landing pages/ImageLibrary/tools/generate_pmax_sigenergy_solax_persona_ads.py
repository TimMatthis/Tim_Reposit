"""
Generate 1200x628 Performance Max PNGs: five personas x five variants, split across
Sigenergy and Solax homeowner messaging.

Follows `.cursor/skills/google-ads-png-from-context/SKILL.md`:
- Canvas 1200x628, PNG
- Typography: Geist Sans only (bundled under ../fonts/Geist)
- Colours: persona palettes aligned with existing landing persona ads
- Logo: official white wordmark raster under ../SeedTemplates/brand/logos/ (HubSpot source aligned with landing HTML)
- Decor overlay: uniform scale only (`google_ads_compose.overlay_faded_decor_right`)

Output:
- ../Ads/PMax/sigenergy/*.png (3 creatives per persona)
- ../Ads/PMax/solax/*.png (5 creatives per persona)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from google_ads_compose import overlay_faded_decor_right
from google_ads_typography import (
    FONT_SIZES,
    LINE_SPACING,
    LAYOUT,
    STANDARD_DISPLAY_AD_DISCLAIMER,
    load_geist,
)

ROOT = Path(__file__).resolve().parent.parent
OUT_SIG = ROOT / "Ads" / "PMax" / "sigenergy"
OUT_SOL = ROOT / "Ads" / "PMax" / "solax"
FONTS_DIR = ROOT / "fonts" / "Geist"
PREVIEW_IMG = ROOT / "SeedTemplates" / "brand" / "_preview_extracted" / "SolarBatteryFact1_crop_dark.png"
LOGO_PNG = ROOT / "SeedTemplates" / "brand" / "logos" / "Reposit_logo_white_h92.png"

W, H = LAYOUT.canvas_w, LAYOUT.canvas_h


PALETTES: dict[str, dict[str, str | float]] = {
    "battery-roi": {
        "ink": "#0c1729",
        "bg_bottom": "#134e5e",
        "accent": "#f59e0b",
        "accent2": "#0891b2",
        "panel": "#e6f3ff",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.14,
        "cta_dark": "#2d1604",
    },
    "energy-uncertainty": {
        "ink": "#0f172a",
        "bg_bottom": "#14532d",
        "accent": "#19c37d",
        "accent2": "#2a6b94",
        "panel": "#f0fdf4",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.1,
    },
    "fixed-income": {
        "ink": "#0f172a",
        "bg_bottom": "#1e3a8a",
        "accent": "#1d4ed8",
        "accent2": "#b45309",
        "panel": "#eff6ff",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.1,
    },
    "green-impact": {
        "ink": "#052e16",
        "bg_bottom": "#064e3b",
        "accent": "#059669",
        "accent2": "#065f46",
        "panel": "#ecfdf5",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.12,
    },
    "set-and-forget": {
        "ink": "#0f172a",
        "bg_bottom": "#4c1d95",
        "accent": "#5eead4",
        "accent2": "#7c3aed",
        "panel": "#f5f3ff",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.11,
        "cta_dark": "#0f172a",
    },
}


def hex_rgb(h: str) -> tuple[int, int, int]:
    h = h.lstrip("#")
    return tuple(int(h[i : i + 2], 16) for i in (0, 2, 4))


def draw_gradient_bg(
    draw: ImageDraw.ImageDraw,
    top: tuple[int, int, int],
    bottom: tuple[int, int, int],
) -> None:
    for y in range(H):
        t = y / max(H - 1, 1)
        r = int(top[0] * (1 - t) + bottom[0] * t)
        g = int(top[1] * (1 - t) + bottom[1] * t)
        b = int(top[2] * (1 - t) + bottom[2] * t)
        draw.line([(0, y), (W, y)], fill=(r, g, b))


def rounded_rect(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    radius: int,
    fill: tuple[int, int, int],
) -> None:
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle([x0, y0, x1, y1], radius=radius, fill=fill)


def load_logo_rgba(max_height: int = LAYOUT.logo_max_height) -> Image.Image | None:
    """White Reposit wordmark for dark gradients. Returns None if asset missing."""
    if not LOGO_PNG.exists():
        return None
    try:
        logo = Image.open(LOGO_PNG).convert("RGBA")
    except OSError:
        return None
    w0, h0 = logo.size
    if h0 <= 0:
        return None
    scale = max_height / h0
    new_size = (max(1, int(w0 * scale)), max(1, int(h0 * scale)))
    return logo.resize(new_size, Image.Resampling.LANCZOS)


def wrap_text(font: ImageFont.FreeTypeFont, text: str, max_width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    cur = ""
    for w in words:
        trial = (cur + " " + w).strip()
        bbox = font.getbbox(trial)
        if bbox[2] - bbox[0] <= max_width:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines if lines else [text]


def draw_ad(
    *,
    persona: str,
    battery: str,
    variant: int,
    headline: str,
    support: str,
    cta: str,
    disclaimer: str,
) -> None:
    pal = PALETTES[persona]
    ink = hex_rgb(str(pal["ink"]))
    accent = hex_rgb(str(pal["accent"]))
    accent2 = hex_rgb(str(pal["accent2"]))
    panel = hex_rgb(str(pal["panel"]))
    text_muted = hex_rgb(str(pal["text_muted"]))
    preview_opacity = float(pal["preview_opacity"])

    img = Image.new("RGBA", (W, H), panel)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw, ink, hex_rgb(str(pal["bg_bottom"])))
    overlay_faded_decor_right(
        img,
        PREVIEW_IMG,
        canvas_w=W,
        canvas_h=H,
        max_width_frac=LAYOUT.decor_preview_max_width_frac,
        max_height_frac=LAYOUT.decor_preview_max_height_frac,
        right_margin_px=LAYOUT.decor_preview_right_margin_px,
        opacity=float(preview_opacity),
    )

    pad_x = LAYOUT.pad_x
    max_text_w = LAYOUT.max_text_width

    font_tag = load_geist(FONTS_DIR, FONT_SIZES.tagline, "medium")
    font_aud = load_geist(FONTS_DIR, FONT_SIZES.audience, "medium")
    font_head = load_geist(FONTS_DIR, FONT_SIZES.headline, "bold")
    font_body = load_geist(FONTS_DIR, FONT_SIZES.support, "regular")
    font_cta = load_geist(FONTS_DIR, FONT_SIZES.cta, "semibold")
    font_foot = load_geist(FONTS_DIR, FONT_SIZES.disclaimer, "regular")

    logo = load_logo_rgba()
    y_cursor = LAYOUT.logo_top
    if logo is not None:
        img.alpha_composite(logo, (pad_x, y_cursor))
        y_cursor += logo.height + LAYOUT.logo_to_tagline_gap
    else:
        # Fallback if logo asset missing: text-only brand line
        font_fallback = load_geist(FONTS_DIR, FONT_SIZES.brand_ribbon, "semibold")
        draw.text(
            (pad_x, y_cursor),
            "REPOSIT POWER · Battery retrofit assessment",
            fill=(240, 245, 252),
            font=font_fallback,
        )
        y_cursor += 34

    tagline = "Battery retrofit assessment"
    draw.text((pad_x, y_cursor), tagline, fill=(226, 232, 240), font=font_tag)
    y_cursor += LAYOUT.gap_after_tagline

    audience_line = (
        "For Sigenergy homeowners" if battery == "sigenergy" else "For Solax homeowners"
    )
    draw.text((pad_x, y_cursor), audience_line, fill=(186, 198, 216), font=font_aud)
    y_cursor += LAYOUT.gap_after_audience

    headline_lines = wrap_text(font_head, headline, max_text_w)
    y = y_cursor + LAYOUT.pre_headline_gap
    lh = LINE_SPACING.headline
    for line in headline_lines:
        draw.text((pad_x, y), line, fill=(255, 255, 255), font=font_head)
        y += lh

    y += LAYOUT.post_headline_block_gap
    for line in wrap_text(font_body, support, max_text_w):
        draw.text((pad_x, y), line, fill=text_muted, font=font_body)
        y += LINE_SPACING.support

    y += LAYOUT.pre_cta_gap
    tb = font_cta.getbbox(cta)
    tw = tb[2] - tb[0]
    cta_w = max(280, tw + 72)
    cta_hex = pal.get("cta_dark")
    cta_text = hex_rgb(str(cta_hex)) if cta_hex else (255, 255, 255)
    ph = LAYOUT.cta_pill_height
    rounded_rect(
        draw,
        (pad_x, y, pad_x + cta_w, y + ph),
        LAYOUT.cta_pill_radius,
        accent,
    )
    draw.text(
        (pad_x + (cta_w - tw) // 2, y + LAYOUT.cta_vertical_padding_text),
        cta,
        fill=cta_text,
        font=font_cta,
    )

    if disclaimer:
        dy = H - LAYOUT.disclaimer_bottom_offset
        draw.text((pad_x, dy), disclaimer, fill=(180, 190, 205), font=font_foot)

    accent_bar_h = LAYOUT.accent_bar_height
    draw.rectangle([0, H - accent_bar_h, W, H], fill=accent2)

    out_dir = OUT_SIG if battery == "sigenergy" else OUT_SOL
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"pmax_{persona}_{battery}_{variant:02d}.png"
    path = out_dir / filename
    img.convert("RGB").save(path, "PNG", optimize=True)
    print("Wrote", path)


# Five creatives per persona per battery brand: Sigenergy 01..03 was first batch; Solax now 01..05.
# Total ads: 15 Sigenergy + 25 Solax = 40.
# Copy is honest: assessment first, no guaranteed savings, no fake urgency.
ADS: list[tuple[str, str, int, str, str, str, str]] = [
    # battery-roi
    (
        "battery-roi",
        "sigenergy",
        1,
        "More from your Sigenergy battery",
        "Personalised retrofit check from your usage. See if Reposit fits before you commit.",
        "Book my assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "sigenergy",
        2,
        "Same Sigenergy hardware, smarter runs",
        "Explore coordinated trading style value without new gear. Assessment first.",
        "See my options",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "sigenergy",
        3,
        "You bought storage. Put more to work",
        "For Sigenergy homes: clearer utilisation story after we review your profile.",
        "Start assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "solax",
        1,
        "Solax battery, deeper utilisation",
        "Find out if Reposit can help your Solax system shift load and capture value.",
        "Check compatibility",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "solax",
        2,
        "Make your Solax investment work harder",
        "No new hardware pitch. Usage led assessment and a clear next step.",
        "Review my usage",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "solax",
        3,
        "Solax at home? Get clearer next steps",
        "Retrofit assessment from your usage shows whether Reposit suits your Solax setup.",
        "Book my assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "solax",
        4,
        "Idle Solax cycles still cost money",
        "See if coordinated dispatch could fit your household before you commit.",
        "See my options",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "battery-roi",
        "solax",
        5,
        "Same Solax stack, smarter software layer",
        "Assessment led review. No swap out pitch, just an honest yes or no.",
        "Start assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    # energy-uncertainty
    (
        "energy-uncertainty",
        "sigenergy",
        1,
        "Sigenergy home, softer bill shocks",
        "Explore a predictable monthly fee shaped from your usage. Assessment first.",
        "See what I'd pay",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "sigenergy",
        2,
        "Fewer surprises to brace for",
        "Sigenergy owners: respectful numbers before pressure to proceed.",
        "Get clarity",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "sigenergy",
        3,
        "Household bills feel random lately",
        "If you run Sigenergy at home, see how one fee direction could look.",
        "Request assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "solax",
        1,
        "Solax plus calmer budgeting",
        "Plain language review of what a fixed fee direction might mean for you.",
        "See monthly shape",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "solax",
        2,
        "Energy costs spike month to month",
        "Solax homeowners welcome. Assessment before any commitment.",
        "Talk it through",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "solax",
        3,
        "Solax fitted, bills still swing wide",
        "Ask how one predictable monthly fee could be sized from your usage profile.",
        "See what I'd pay",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "solax",
        4,
        "Budget peace for Solax households",
        "Respectful numbers up front, no pressure to proceed on the spot.",
        "Get clarity",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "energy-uncertainty",
        "solax",
        5,
        "Tariff noise wearing you down",
        "Solax owners explore a simpler monthly shape after we review usage.",
        "Request assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    # fixed-income
    (
        "fixed-income",
        "sigenergy",
        1,
        "Sigenergy and steady household fees",
        "Respectful assessment with a clear monthly number before commitment.",
        "See monthly fee",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "sigenergy",
        2,
        "Predictable energy matters here",
        "If your Sigenergy system is already installed, ask how fees could simplify bills.",
        "Plain numbers first",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "sigenergy",
        3,
        "Fixed income, clearer energy math",
        "We read usage first, then show what a monthly fee direction might look like.",
        "Book assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "solax",
        1,
        "Solax home, simple fee conversation",
        "No jargon sprint. One respectful review and next steps you choose.",
        "Ask about fees",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "solax",
        2,
        "Retirees with Solax already fitted",
        "See whether a fixed fee direction suits your usage before you proceed.",
        "No pressure chat",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "solax",
        3,
        "Plain language Solax fee chat",
        "We explain terms simply and show a monthly number only when it fits.",
        "See monthly fee",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "solax",
        4,
        "Solax on the wall already",
        "Fixed income households welcome. Assessment first, your pace after.",
        "Plain numbers first",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "fixed-income",
        "solax",
        5,
        "Know the fee before you say yes",
        "Solax homes get the same respectful review and clear next steps.",
        "Book assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    # green-impact
    (
        "green-impact",
        "sigenergy",
        1,
        "Sigenergy can support the grid",
        "Explore coordinated flexibility after a personalised assessment.",
        "Join the fleet",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "sigenergy",
        2,
        "Home batteries, shared grid benefit",
        "Sigenergy owners: fleet style dispatch where savings and impact can align.",
        "Explore dispatch",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "sigenergy",
        3,
        "Climate minded and already stored",
        "If Sigenergy is on your wall, ask how participation could look for you.",
        "See impact path",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "solax",
        1,
        "Solax and neighbourhood scale impact",
        "Learn how coordinated battery use helps where the grid needs flexibility.",
        "Ask how it helps",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "solax",
        2,
        "Green choices with Solax fitted",
        "Pair household savings intent with dispatch that supports renewable uptake.",
        "Book assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "solax",
        3,
        "Solax homes, shared grid support",
        "Ask how coordinated flexibility helps when the network needs headroom.",
        "Join the fleet",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "solax",
        4,
        "Cleaner grid from your garage",
        "Solax batteries can pair savings intent with useful dispatch.",
        "Explore dispatch",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "green-impact",
        "solax",
        5,
        "Impact without a preachy lecture",
        "Personalised assessment for Solax owners who want outcomes they can explain.",
        "See impact path",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    # set-and-forget
    (
        "set-and-forget",
        "sigenergy",
        1,
        "Sigenergy with less energy admin",
        "One fee direction after we read usage. Fewer decisions month to month.",
        "Simplify my bills",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "sigenergy",
        2,
        "Make household energy boring",
        "Works with Sigenergy when the assessment shows a fit.",
        "Fix my plan",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "sigenergy",
        3,
        "Stop juggling tariff trivia",
        "Sigenergy homes: see whether Reposit can streamline how you pay for power.",
        "Less admin",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "solax",
        1,
        "Solax owners, set and simplify",
        "Assessment led path with fewer switches to track if you proceed.",
        "Book assessment",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "solax",
        2,
        "Prefer autopilot for energy bills",
        "Solax at home? Ask how one monthly shape could replace patchwork plans.",
        "Show me the path",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "solax",
        3,
        "Solax plus fewer bill decisions",
        "One fee direction after usage review when the assessment shows a fit.",
        "Simplify my bills",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "solax",
        4,
        "Hands off energy for busy homes",
        "Solax fitted households ask Reposit to simplify how you pay for power.",
        "Fix my plan",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
    (
        "set-and-forget",
        "solax",
        5,
        "Stop micromanaging retail tariffs",
        "Assessment led path with less admin if you choose to proceed.",
        "Less admin",
        STANDARD_DISPLAY_AD_DISCLAIMER,
    ),
]


def main() -> None:
    if sys.platform == "win32":
        os.environ.setdefault("PYTHONUTF8", "1")
    if len(ADS) != 40:
        raise SystemExit(f"Expected 40 ads, got {len(ADS)}")
    for persona, battery, variant, head, sup, cta, disc in ADS:
        draw_ad(
            persona=persona,
            battery=battery,
            variant=variant,
            headline=head,
            support=sup,
            cta=cta,
            disclaimer=disc,
        )


if __name__ == "__main__":
    main()
