"""
Generate 1200x628 persona-focused Google Ads style PNGs from brand colours in landing CSS.

Typography: Geist Sans only, sizes and rhythm from `google_ads_typography.py`.
Decor overlay: uniform scale via `google_ads_compose.py` (google-ads-png-from-context SKILL.md).

Output: ../Ads/Personas/*.png
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
OUT_DIR = ROOT / "Ads" / "Personas"
GEIST_DIR = ROOT / "fonts" / "Geist"
PREVIEW_IMG = ROOT / "SeedTemplates" / "brand" / "_preview_extracted" / "SolarBatteryFact1_crop_dark.png"

W, H = LAYOUT.canvas_w, LAYOUT.canvas_h


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


def draw_ad(spec: dict) -> None:
    ink = hex_rgb(spec["ink"])
    accent = hex_rgb(spec["accent"])
    accent2 = hex_rgb(spec["accent2"])
    panel = hex_rgb(spec["panel"])
    text_muted = hex_rgb(spec["text_muted"])

    img = Image.new("RGBA", (W, H), panel)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw, ink, hex_rgb(spec["bg_bottom"]))

    overlay_faded_decor_right(
        img,
        PREVIEW_IMG,
        canvas_w=W,
        canvas_h=H,
        max_width_frac=LAYOUT.decor_preview_max_width_frac,
        max_height_frac=LAYOUT.decor_preview_max_height_frac,
        right_margin_px=LAYOUT.decor_preview_right_margin_px,
        opacity=float(spec.get("preview_opacity", 0.11)),
    )

    pad_x = LAYOUT.pad_x
    max_text_w = LAYOUT.max_text_width

    font_brand = load_geist(GEIST_DIR, FONT_SIZES.brand_ribbon, "semibold")
    font_head = load_geist(GEIST_DIR, FONT_SIZES.headline, "bold")
    font_body = load_geist(GEIST_DIR, FONT_SIZES.support, "regular")
    font_cta = load_geist(GEIST_DIR, FONT_SIZES.cta, "semibold")
    font_foot = load_geist(GEIST_DIR, FONT_SIZES.disclaimer, "regular")

    brand_line = "REPOSIT POWER · Battery retrofit assessment"
    draw.text((pad_x, LAYOUT.brand_top_no_logo), brand_line, fill=(240, 245, 252), font=font_brand)

    headline_lines = wrap_text(font_head, spec["headline"], max_text_w)
    y = LAYOUT.headline_top_no_logo
    lh = LINE_SPACING.headline
    for line in headline_lines:
        draw.text((pad_x, y), line, fill=(255, 255, 255), font=font_head)
        y += lh

    y += LAYOUT.post_headline_block_gap
    for line in wrap_text(font_body, spec["support"], max_text_w):
        draw.text((pad_x, y), line, fill=text_muted, font=font_body)
        y += LINE_SPACING.support

    y += LAYOUT.pre_cta_gap
    tb = font_cta.getbbox(spec["cta"])
    tw = tb[2] - tb[0]
    cta_w = max(280, tw + 72)
    cta_text = spec.get("cta_text", (255, 255, 255))
    ph = LAYOUT.cta_pill_height
    rounded_rect(
        draw,
        (pad_x, y, pad_x + cta_w, y + ph),
        LAYOUT.cta_pill_radius,
        accent,
    )
    draw.text(
        (pad_x + (cta_w - tw) // 2, y + LAYOUT.cta_vertical_padding_text),
        spec["cta"],
        fill=cta_text,
        font=font_cta,
    )

    disclaimer = spec.get("disclaimer", "")
    if disclaimer:
        dy = H - LAYOUT.disclaimer_bottom_offset
        draw.text((pad_x, dy), disclaimer, fill=(180, 190, 205), font=font_foot)

    accent_bar_h = LAYOUT.accent_bar_height
    draw.rectangle([0, H - accent_bar_h, W, H], fill=accent2)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / spec["filename"]
    rgb = img.convert("RGB")
    rgb.save(path, "PNG", optimize=True)
    print("Wrote", path)


PERSONAS = [
    {
        "filename": "google-ad_persona-battery-roi.png",
        "ink": "#0c1729",
        "bg_bottom": "#134e5e",
        "accent": "#f59e0b",
        "accent2": "#0891b2",
        "panel": "#e6f3ff",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.14,
        "headline": "Same hardware. More value.",
        "support": (
            "Personalised retrofit assessment from your usage and battery data. "
            "No commitment until you choose."
        ),
        "cta": "Put my battery to work",
        "cta_text": hex_rgb("#2d1604"),
        "disclaimer": STANDARD_DISPLAY_AD_DISCLAIMER,
    },
    {
        "filename": "google-ad_persona-energy-uncertainty.png",
        "ink": "#0f172a",
        "bg_bottom": "#14532d",
        "accent": "#19c37d",
        "accent2": "#2a6b94",
        "panel": "#f0fdf4",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.1,
        "headline": "Stop bracing for bill surprises.",
        "support": (
            "See how one predictable monthly fee could be sized from your profile. "
            "Assessment first."
        ),
        "cta": "See what I'd pay",
        "disclaimer": STANDARD_DISPLAY_AD_DISCLAIMER,
    },
    {
        "filename": "google-ad_persona-fixed-income.png",
        "ink": "#0f172a",
        "bg_bottom": "#1e3a8a",
        "accent": "#1d4ed8",
        "accent2": "#b45309",
        "panel": "#eff6ff",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.1,
        "headline": "Fixed income. Predictable energy.",
        "support": (
            "Respectful assessment with a clear monthly number before any commitment."
        ),
        "cta": "See monthly fee",
        "disclaimer": STANDARD_DISPLAY_AD_DISCLAIMER,
    },
    {
        "filename": "google-ad_persona-green-impact.png",
        "ink": "#052e16",
        "bg_bottom": "#064e3b",
        "accent": "#059669",
        "accent2": "#065f46",
        "panel": "#ecfdf5",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.12,
        "headline": "Help the grid from home.",
        "support": (
            "Join coordinated battery flexibility. Savings and impact together, "
            "after a personalised assessment."
        ),
        "cta": "Join the fleet",
        "disclaimer": STANDARD_DISPLAY_AD_DISCLAIMER,
    },
    {
        "filename": "google-ad_persona-set-and-forget.png",
        "ink": "#0f172a",
        "bg_bottom": "#4c1d95",
        "accent": "#5eead4",
        "accent2": "#7c3aed",
        "panel": "#f5f3ff",
        "text_muted": "#cbd5e1",
        "preview_opacity": 0.11,
        "headline": "Make energy boring.",
        "support": (
            "One fixed fee direction after we read your usage. Less admin, fewer decisions."
        ),
        "cta": "Just fix it for me",
        "cta_text": hex_rgb("#0f172a"),
        "disclaimer": STANDARD_DISPLAY_AD_DISCLAIMER,
    },
]


def main() -> None:
    if sys.platform == "win32":
        os.environ.setdefault("PYTHONUTF8", "1")
    for spec in PERSONAS:
        draw_ad(spec)


if __name__ == "__main__":
    main()
