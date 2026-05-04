"""
Single horizontal video-format still (1920x1080) for Solax, matching PMax ad styling.

Use as a Demand Gen or YouTube horizontal key frame sample. Geist and disclaimers
follow google-ads-png-from-context. Optional silent MP4 when ffmpeg is on PATH.

Output:
- ../Ads/Video/samples/solax_video_sample_frame_1920.png
- ../Ads/Video/samples/solax_video_sample_1080.mp4 (if ffmpeg succeeds)
"""

from __future__ import annotations

import os
import shutil
import subprocess
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
from generate_pmax_sigenergy_solax_persona_ads import PALETTES, hex_rgb

ROOT = Path(__file__).resolve().parent.parent
FONTS_DIR = ROOT / "fonts" / "Geist"
PREVIEW_IMG = ROOT / "SeedTemplates" / "brand" / "_preview_extracted" / "SolarBatteryFact1_crop_dark.png"
LOGO_PNG = ROOT / "SeedTemplates" / "brand" / "logos" / "Reposit_logo_white_h92.png"
OUT_DIR = ROOT / "Ads" / "Video" / "samples"

VIDEO_W = 1920
VIDEO_H = 1080
SCALE = VIDEO_W / float(LAYOUT.canvas_w)
CONTENT_H = int(round(LAYOUT.canvas_h * SCALE))
OFFSET_Y = max(0, (VIDEO_H - CONTENT_H) // 2)

# One Solax-centric line set (aligned with PMax solax battery-roi variant 01).
PERSONA_PALETTE = "battery-roi"
HEADLINE = "Solax battery, deeper utilisation"
SUPPORT = (
    "Find out if Reposit can help your Solax system shift load and capture value."
)
CTA_LABEL = "Check compatibility"


def sc(x: float | int) -> int:
    return max(1, int(round(float(x) * SCALE)))


def load_logo(max_h: int) -> Image.Image | None:
    if not LOGO_PNG.exists():
        return None
    try:
        logo = Image.open(LOGO_PNG).convert("RGBA")
    except OSError:
        return None
    h0 = logo.height
    if h0 <= 0:
        return None
    s = max_h / h0
    return logo.resize((max(1, int(logo.width * s)), max(1, int(h0 * s))), Image.Resampling.LANCZOS)


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


def draw_gradient(
    draw: ImageDraw.ImageDraw,
    cw: int,
    ch: int,
    top: tuple[int, int, int],
    bottom: tuple[int, int, int],
) -> None:
    for y in range(ch):
        t = y / max(ch - 1, 1)
        r = int(top[0] * (1 - t) + bottom[0] * t)
        g = int(top[1] * (1 - t) + bottom[1] * t)
        b = int(top[2] * (1 - t) + bottom[2] * t)
        draw.line([(0, y), (cw, y)], fill=(r, g, b))


def rounded_rect(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    radius: int,
    fill: tuple[int, int, int],
) -> None:
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle([x0, y0, x1, y1], radius=radius, fill=fill)


def build_frame() -> Image.Image:
    pal = PALETTES[PERSONA_PALETTE]
    ink = hex_rgb(str(pal["ink"]))
    accent = hex_rgb(str(pal["accent"]))
    accent2 = hex_rgb(str(pal["accent2"]))
    panel = hex_rgb(str(pal["panel"]))
    text_muted = hex_rgb(str(pal["text_muted"]))
    preview_opacity = float(pal["preview_opacity"])

    img = Image.new("RGBA", (VIDEO_W, VIDEO_H), panel)
    draw = ImageDraw.Draw(img)

    ink_img = Image.new("RGB", (VIDEO_W, CONTENT_H), ink)
    idraw = ImageDraw.Draw(ink_img)
    draw_gradient(idraw, VIDEO_W, CONTENT_H, ink, hex_rgb(str(pal["bg_bottom"])))
    img.paste(ink_img.convert("RGBA"), (0, OFFSET_Y))

    content = Image.new("RGBA", (VIDEO_W, CONTENT_H), (0, 0, 0, 0))
    overlay_faded_decor_right(
        content,
        PREVIEW_IMG,
        canvas_w=VIDEO_W,
        canvas_h=CONTENT_H,
        max_width_frac=LAYOUT.decor_preview_max_width_frac,
        max_height_frac=LAYOUT.decor_preview_max_height_frac,
        right_margin_px=sc(LAYOUT.decor_preview_right_margin_px),
        opacity=float(preview_opacity),
    )
    img.alpha_composite(content, (0, OFFSET_Y))

    pad_x = sc(LAYOUT.pad_x)
    max_tw = sc(LAYOUT.max_text_width)

    font_tag = load_geist(FONTS_DIR, sc(FONT_SIZES.tagline), "medium")
    font_aud = load_geist(FONTS_DIR, sc(FONT_SIZES.audience), "medium")
    font_head = load_geist(FONTS_DIR, sc(FONT_SIZES.headline), "bold")
    font_body = load_geist(FONTS_DIR, sc(FONT_SIZES.support), "regular")
    font_cta = load_geist(FONTS_DIR, sc(FONT_SIZES.cta), "semibold")
    font_foot = load_geist(FONTS_DIR, sc(FONT_SIZES.disclaimer), "regular")

    lx = pad_x
    y_cursor = OFFSET_Y + sc(LAYOUT.logo_top)

    logo = load_logo(sc(LAYOUT.logo_max_height))
    if logo is not None:
        img.alpha_composite(logo, (lx, y_cursor))
        y_cursor += logo.height + sc(LAYOUT.logo_to_tagline_gap)
    else:
        font_fb = load_geist(FONTS_DIR, sc(FONT_SIZES.brand_ribbon), "semibold")
        draw.text(
            (lx, y_cursor),
            "REPOSIT POWER · Battery retrofit assessment",
            fill=(240, 245, 252),
            font=font_fb,
        )
        y_cursor += sc(34)

    draw.text((lx, y_cursor), "Battery retrofit assessment", fill=(226, 232, 240), font=font_tag)
    y_cursor += sc(LAYOUT.gap_after_tagline)

    draw.text((lx, y_cursor), "For Solax homeowners", fill=(186, 198, 216), font=font_aud)
    y_cursor += sc(LAYOUT.gap_after_audience)

    lh = sc(LINE_SPACING.headline)
    y = y_cursor + sc(LAYOUT.pre_headline_gap)
    for line in wrap_text(font_head, HEADLINE, max_tw):
        draw.text((lx, y), line, fill=(255, 255, 255), font=font_head)
        y += lh

    y += sc(LAYOUT.post_headline_block_gap)
    ls = sc(LINE_SPACING.support)
    for line in wrap_text(font_body, SUPPORT, max_tw):
        draw.text((lx, y), line, fill=text_muted, font=font_body)
        y += ls

    y += sc(LAYOUT.pre_cta_gap)
    tb = font_cta.getbbox(CTA_LABEL)
    tw = tb[2] - tb[0]
    cta_w = max(sc(280), tw + sc(72))
    cta_hex = pal.get("cta_dark")
    cta_text = hex_rgb(str(cta_hex)) if cta_hex else (255, 255, 255)
    ph = sc(LAYOUT.cta_pill_height)
    rr = sc(LAYOUT.cta_pill_radius)
    rounded_rect(draw, (lx, y, lx + cta_w, y + ph), rr, accent)
    vp = sc(LAYOUT.cta_vertical_padding_text)
    draw.text((lx + (cta_w - tw) // 2, y + vp), CTA_LABEL, fill=cta_text, font=font_cta)

    disc = STANDARD_DISPLAY_AD_DISCLAIMER
    if disc:
        dy = VIDEO_H - sc(LAYOUT.disclaimer_bottom_offset)
        draw.text((lx, dy), disc, fill=(180, 190, 205), font=font_foot)

    ab = sc(LAYOUT.accent_bar_height)
    draw.rectangle([0, VIDEO_H - ab, VIDEO_W, VIDEO_H], fill=accent2)

    return img


def maybe_mp4(png_path: Path, mp4_path: Path, seconds: float = 6.0) -> None:
    exe = shutil.which("ffmpeg")
    if not exe:
        print("ffmpeg not found on PATH; skipped MP4. Install ffmpeg to emit video.")
        return
    cmd = [
        exe,
        "-y",
        "-loop",
        "1",
        "-i",
        str(png_path),
        "-c:v",
        "libx264",
        "-t",
        str(seconds),
        "-pix_fmt",
        "yuv420p",
        "-r",
        "30",
        str(mp4_path),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Wrote", mp4_path)
    except subprocess.CalledProcessError as e:
        print("ffmpeg failed:", e.stderr or e.stdout or e)


def main() -> None:
    if sys.platform == "win32":
        os.environ.setdefault("PYTHONUTF8", "1")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    png_path = OUT_DIR / "solax_video_sample_frame_1920.png"
    mp4_path = OUT_DIR / "solax_video_sample_1080.mp4"

    frame = build_frame()
    frame.convert("RGB").save(png_path, "PNG", optimize=True)
    print("Wrote", png_path)

    maybe_mp4(png_path, mp4_path)


if __name__ == "__main__":
    main()
