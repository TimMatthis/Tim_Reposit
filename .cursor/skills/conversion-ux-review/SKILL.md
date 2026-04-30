---
name: conversion-ux-review
description: Review existing B2C landing pages, ecommerce pages, offer pages, and signup flows for sales conversion-focused UX. Use when the user asks for UX review, conversion optimisation, sales page critique, CRO suggestions, funnel improvements, or page changes intended to increase leads, purchases, bookings, or enquiries.
---

# Conversion UX Review

## Purpose

Use this skill to review existing B2C user experiences with the goal of increasing sales conversion. Prioritise practical improvements to clarity, trust, motivation, friction, and call-to-action performance.

## Review Workflow

1. Identify the page goal: purchase, lead, enquiry, booking, signup, trial, download, or offer claim.
2. Identify the likely audience intent and awareness level.
3. Review the full path to conversion, not just the visible design.
4. Prioritise issues that directly affect conversion before visual polish.
5. Recommend focused changes that can be implemented in the existing codebase.

## Conversion Review Checklist

- Value proposition: Is the main benefit clear within the first screen?
- Message match: Does the page match the likely source intent, ad promise, offer, or search query?
- CTA clarity: Is the primary action specific, visible, repeated at natural decision points, and low-friction?
- Offer strength: Is the deal, outcome, or reason to act concrete enough to motivate action?
- Trust: Are proof points, guarantees, reviews, credentials, security cues, or risk reducers visible near decisions?
- Friction: Are forms, steps, choices, copy, distractions, and navigation reduced to what the conversion needs?
- Objection handling: Are price, risk, effort, timing, eligibility, comparison, and uncertainty addressed before the CTA?
- Visual hierarchy: Does layout guide attention from problem to value to proof to action?
- Mobile conversion: Are CTA placement, form usability, loading, spacing, tap targets, and readability strong on small screens?
- Urgency and specificity: Are deadlines, availability, savings, bonuses, or next steps truthful and concrete?

## Output Format

Lead with the highest-impact findings. Use this format:

```markdown
## Conversion Review

### Highest-Impact Issues
- **Issue:** [Specific conversion problem]
  **Why it matters:** [Likely effect on buyer behaviour]
  **Recommendation:** [Concrete change]

### Quick Wins
- [Small change likely to improve conversion]

### Larger Opportunities
- [Bigger structural or strategic improvement]

### Copy Suggestions
- Current: "[existing copy if relevant]"
- Suggested: "[conversion-focused alternative]"
```

## Recommendation Rules

- Be specific about where the issue appears and what should change.
- Tie every recommendation to buyer behaviour, trust, clarity, motivation, or friction.
- Prefer fewer, stronger recommendations over a long list of generic CRO advice.
- Do not suggest dark patterns, fake scarcity, hidden costs, manipulative urgency, or misleading claims.
- When writing front-end copy, never use em dashes.
- If implementation is requested, reuse existing CSS classes first and keep styling in dedicated CSS files.

## Severity Guide

- **High:** Likely blocks or materially reduces conversions.
- **Medium:** Creates hesitation, confusion, or avoidable friction.
- **Low:** Polish or secondary improvement with limited direct conversion impact.
