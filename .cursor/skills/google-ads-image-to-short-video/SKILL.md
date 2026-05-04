---
name: google-ads-image-to-short-video
description: Converts static advert images into short Google Ads style videos with correct aspect ratios, motion design, and voiceover scripts fit to the image messaging. Use when the user wants video from PNG/display creatives, PMax or YouTube short spots, animated ads, TTS voiceover, or Demand Gen style clips derived from existing ad art.
---

# Google Ads Image to Short Video

## Purpose

Turn one or more **static advert images** (for example display or social-style PNGs) into **short, platform-ready videos** that preserve the **headline, support line, CTA, and brand** while adding **motion** and **voiceover** so the message reads clearly in a feed.

## Relationship to other project skills

When sources are Reposit campaign PNGs or need brand-locked execution:

- Read and follow [.cursor/skills/google-ads-png-from-context/SKILL.md](../google-ads-png-from-context/SKILL.md) for **Geist-only** type on any new frames, **brand guide PDF** paths, **colours**, **logo rules**, **disclaimer** patterns, and **Google Ads honesty** gates.
- If generating a **1920×1080** key frame or silent sample MP4 in this repo, reuse patterns in `Landing pages/ImageLibrary/tools/generate_video_sample_solax.py` (Pillow layout scaled from `google_ads_typography.LAYOUT` plus optional **ffmpeg**).

## Target formats (default deliverables)

Pick formats from the campaign brief. When the user does not specify, offer these **common Google ecosystems** targets:

| Use | Aspect | Typical resolution | Notes |
|-----|--------|-------------------|--------|
| Landscape (YouTube in-stream, many horizontal placements) | 16:9 | **1920×1080** | Matches existing repo sample tooling |
| Vertical (Shorts, some in-feed) | 9:16 | **1080×1920** | Safe zones: keep core text and logo inside central band |
| Square (feed) | 1:1 | **1080×1080** | Crop or letterbox from 16:9 source without distorting logos or faces |

**Length:** Prefer **6–30 seconds** for short spots unless the brief asks longer; stay within the placement limit the user names (confirm current Google Ads documentation for max duration per format if unknown).

**Technical:** **H.264** in **.mp4** is the usual handoff. **AAC** audio if voiceover is included. Aim for **≤ 30 fps** unless a platform spec says otherwise.

## Workflow

Copy this checklist and track progress:

```
Task progress:
- [ ] Ingest: paths to source image(s), campaign goal, persona, destination URL intent
- [ ] Extract message: headline, support, CTA, brand, disclaimer (if any on art)
- [ ] Storyboard: 3–6 beats (hook, promise, proof or detail, CTA)
- [ ] Motion plan: per-beat animation (no illegible micro-text)
- [ ] Voiceover script: timed to storyboard; mark pauses
- [ ] Build: sequence frames or motion (tool of choice + optional ffmpeg)
- [ ] Audio: generate or record VO; mix level so VO is clear, music optional and not drowning copy
- [ ] Compliance: brand + Google Ads policy + accessibility (captions if VO)
- [ ] Export: one MP4 per requested aspect; spot-check playback
```

### Step 1: Read the creative

- Identify **primary promise** (headline), **support**, **CTA**, **logo**, **palette**, and any **legal or qualifier** line.
- If multiple images: define order (for example problem then solution, or variant A/B for testing).

### Step 2: Storyboard and motion

Keep motion **readable and calm**: misused flashy edits hurt trust and can conflict with **photosensitive** best practices.

Typical **Google-style** motion (not all required on every spot):

- **Establish:** 0.5–1.5s logo or brand frame, slight scale or fade.
- **Headline:** line-by-line or word emphasis; avoid jittery bouncing text.
- **Support:** optional parallax or **Ken Burns** on background art with **uniform scale** (never squash-stretch logos or diagram axes).
- **CTA:** clear button or text emphasis in the last third of the edit.
- **End card:** hold CTA + brand **≥ 1s** where the brief allows.

Reuse imagery **without non-uniform stretch**; match `google_ads_compose` / brand rules when building new plates from PNG layers.

### Step 3: Voiceover

- **Script** = spoken headline and support, rewritten for **ear** (contractions OK, numbers clear, one idea per sentence).
- **Tone:** match persona (for example confident and plain for ROI; reassuring for uncertainty).
- **Pacing:** roughly **140–160 words per minute** for clarity; leave **0.3–0.5s** breath gaps at CTA beats.
- **TTS:** use environment-approved TTS or the user’s tool; export **wav or aac** aligned to timeline. **Music:** bed under VO at lower level if used; avoid lyrics that fight VO.

### Step 4: Assembly

- Align VO to storyboard; add **captions** (SRT or burnt-in) when the brief requires accessibility or sound-off viewing.
- **ffmpeg** example pattern (adjust paths and filters): build a concat list of frames or use filters for crossfade; **`-c:v libx264 -pix_fmt yuv420p`** for broad compatibility.
- If **only** extending the repo’s silent horizontal sample, run or adapt `generate_video_sample_solax.py` after installing **ffmpeg** on PATH.

### Step 5: Compliance gates

Do not ship until all pass:

1. **Brand** (when Reposit assets): logo, colours, Geist on any generated type, disclaimers consistent with PNG skill.
2. **Honesty:** no false urgency, unsubstantiated superlatives, or misleading claims; CTA matches landing intent.
3. **Safe motion:** no strobing; avoid rapid full-screen flashes.
4. **Specs:** resolution, aspect, duration, and file type match the named placement.

## Output handoff

Return a short summary for the user:

```markdown
## Short video handoff

- **Source image(s):** [paths]
- **Formats:** [e.g. 1920×1080 mp4]
- **Duration:** [s]
- **Storyboard beats:** [list]
- **VO script:** [full script]
- **Assets:** [mp4 path(s), optional srt/wav]
- **Compliance:** Brand [Pass/Fail], Policy [Pass/Fail], Specs [Pass/Fail]
```

## Additional resources

- Examples and beat templates: [examples.md](examples.md)
