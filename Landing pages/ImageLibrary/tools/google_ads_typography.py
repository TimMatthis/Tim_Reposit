"""
Canonical Geist font sizes, line rhythm, and **one shared footer disclaimer** for 1200×628 Reposit Google Ads PNGs.

Single implementation of `.cursor/skills/google-ads-png-from-context/SKILL.md`
section “Standard raster typography schema”. Import these constants from every
Pillow generator so sizes stay aligned.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PIL import ImageFont


@dataclass(frozen=True)
class GoogleAdsFontSizes:
    """Pillow `size` in px at 1:1 scale (1200×628 export)."""

    brand_ribbon: int = 22  # SemiBold: text-only brand strip when logo absent
    tagline: int = 20  # Medium: service line under logo
    audience: int = 18  # Medium: segment label (for example battery brand)
    headline: int = 46  # Bold: primary promise
    support: int = 26  # Regular: supporting copy
    cta: int = 24  # SemiBold: CTA label inside pill
    disclaimer: int = 16  # Regular: legal or qualifier footer


FONT_SIZES = GoogleAdsFontSizes()


@dataclass(frozen=True)
class GoogleAdsLineSpacing:
    """Vertical step between baselines when text wraps to multiple lines."""

    headline: int = 52
    support: int = 32


LINE_SPACING = GoogleAdsLineSpacing()


# One footer line across all persona and PMax display PNGs for consistent compliance tone.
STANDARD_DISPLAY_AD_DISCLAIMER = "Offer depends on your usage profile and eligibility."


@dataclass(frozen=True)
class GoogleAdsLayout:
    """Shared left-column layout anchors for 1200×628 raster ads."""

    canvas_w: int = 1200
    canvas_h: int = 628
    pad_x: int = 56
    max_text_width: int = 620
    logo_top: int = 36
    logo_max_height: int = 40
    logo_to_tagline_gap: int = 10
    gap_after_tagline: int = 28  # vertical advance after tagline baseline
    gap_after_audience: int = 30  # vertical advance after audience baseline
    pre_headline_gap: int = 8
    post_headline_block_gap: int = 8
    pre_cta_gap: int = 18
    headline_top_no_logo: int = 112  # personas layout below single-line brand ribbon
    cta_pill_height: int = 56
    cta_pill_radius: int = 14
    cta_vertical_padding_text: int = 14  # text offset inside pill
    disclaimer_bottom_offset: int = 52
    brand_top_no_logo: int = 42
    accent_bar_height: int = 6
    # Right-side decor PNG (contain box; scale is uniform, never stretch):
    decor_preview_max_width_frac: float = 0.52
    decor_preview_max_height_frac: float = 0.92
    decor_preview_right_margin_px: int = 36


LAYOUT = GoogleAdsLayout()


def load_geist(font_dir: Path, size: int, weight: str) -> ImageFont.FreeTypeFont:
    """
    weight: regular | medium | semibold | bold
    """
    file_map = {
        "regular": "Geist-Regular.ttf",
        "medium": "Geist-Medium.ttf",
        "semibold": "Geist-SemiBold.ttf",
        "bold": "Geist-Bold.ttf",
    }
    name = file_map.get(weight, "Geist-Regular.ttf")
    path = font_dir / name
    if path.exists():
        return ImageFont.truetype(str(path), size=size)
    raise FileNotFoundError(
        f"Missing Geist font at {path}. Install Geist Sans under {font_dir} "
        "(see google-ads-png-from-context SKILL.md)."
    )
