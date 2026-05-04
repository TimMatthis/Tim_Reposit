"""
Shared compositing helpers for 1200×628 Google Ads PNGs.

Decorative background imagery must never be stretched on independent X/Y axes.
See google-ads-png-from-context SKILL.md (“Background and decorative imagery”).
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image


def resize_to_fit_preserving_aspect(
    im: Image.Image,
    max_w: int,
    max_h: int,
) -> Image.Image:
    """Uniform scale so the image fits inside max_w × max_h (contain)."""
    w0, h0 = im.size
    if w0 <= 0 or h0 <= 0:
        return im
    scale = min(max_w / w0, max_h / h0)
    nw = max(1, int(round(w0 * scale)))
    nh = max(1, int(round(h0 * scale)))
    if (nw, nh) == (w0, h0):
        return im
    return im.resize((nw, nh), Image.Resampling.LANCZOS)


def overlay_faded_decor_right(
    dest: Image.Image,
    asset_path: Path,
    *,
    canvas_w: int,
    canvas_h: int,
    max_width_frac: float,
    max_height_frac: float,
    right_margin_px: int,
    opacity: float,
) -> None:
    """
    Place a semi-transparent decor asset on the right, vertically centred.
    Scaling uses a single factor (contain within the max box): aspect ratio preserved.
    """
    if not asset_path.exists():
        return
    try:
        fg = Image.open(asset_path).convert("RGBA")
    except OSError:
        return

    max_w = max(1, int(round(canvas_w * max_width_frac)))
    max_h = max(1, int(round(canvas_h * max_height_frac)))
    fg = resize_to_fit_preserving_aspect(fg, max_w, max_h)

    alpha = int(max(0, min(1, opacity)) * 255)
    fg.putalpha(alpha)

    x = canvas_w - fg.width - right_margin_px
    y = (canvas_h - fg.height) // 2
    dest.alpha_composite(fg, (x, y))
