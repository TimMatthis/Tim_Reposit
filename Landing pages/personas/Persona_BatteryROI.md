# Persona 3: Battery ROI

> "My battery cost a fortune. It should be earning one."

The technically literate, spreadsheet-driven buyer. Treats the household electricity system as an investment portfolio. The most data-hungry persona of the six — and the one most likely to read your fine print *before* your hero copy.

---

## 1. Snapshot

A homeowner aged **35–58**, almost always the household's "energy decision-maker", often male but increasingly women in finance/ops/engineering roles. Owns a recent solar + battery system (1–4 years old) — frequently with brand awareness of Tesla Powerwall, Sigenergy, AlphaESS, BYD, or Sungrow. Likely runs a HEMS or aggregator app and *checks it*. Reads Whirlpool / SolarQuotes / r/AusEnergy. Has done a payback calculation at least once. Is currently mildly bothered by the gap between the calculation and reality — they were promised payback in 6–8 years and the battery is on track for 11–14.

---

## 2. Core driver

**Sunk-cost activation.** This buyer paid for *capability they aren't using*. Their battery is a Ferrari that only does the school run. The conversion lever is not "save money" — it's "**unlock the asset you've already bought**". They will pay attention to anything that promises higher utilisation of existing capex, and they will scrutinise every claim.

---

## 3. Inner monologue

- "It charges from solar in the morning, drains by 9 pm, and then sits there until sunrise. That's half a day of dead asset."
- "If wholesale spot is hitting $14k/MWh during the evening peak, why isn't my battery making money?"
- "I'm sick of buying my own electrons back at retail markup."
- "Show me the actual revenue split, not just a marketing-friendly 'savings' figure."
- "I've already done the spreadsheet. Beat my spreadsheet."
- "If the controller talks to my Powerwall via Modbus, fine. If it needs a gateway, it's a no."

---

## 4. Tone principles

1. **Lead with the gap.** Don't pitch — describe the underutilisation, then offer the upgrade. "Your battery uses 30% of its capacity. Reposit uses 100%."
2. **Numbers, not adjectives.** Replace every "great" with a percentage, a dollar, or a duration.
3. **Respect their literacy.** They know what kWh, MWh, FCAS, and arbitrage mean. You can use these terms — sparingly, and never to obscure.
4. **Acknowledge the trade-off.** Be honest that revenue is shared with Reposit. Hiding it loses them; transparently structuring it wins them.
5. **No marketing speak.** Avoid "elevated experience", "next-generation", "smart". This buyer reads them as filler.

---

## 5. Vocabulary

### Use
- "Battery utilisation"
- "24/7 trading"
- "Wholesale market access"
- "Idle hardware → earning hardware"
- "Years off your payback"
- "Revenue, not just savings"
- "Your hardware. Our market access."
- "Self-consumption is the floor, not the ceiling"
- "Modbus / API / OEM-direct" *(in technical specs only)*

### Avoid
- "Magic" / "Effortless" / "Easy as pie" — feels patronising
- "Save big" / "Massive savings" — doesn't show your working
- "Don't worry about the technical stuff" *(you just lost them)*
- "Limited time" pseudo-urgency
- "Just trust us" framings

---

## 6. Hooks that work

1. **The 30% utilisation hook.** *"Your battery uses about 30% of its capacity for self-consumption. The other 70% sits idle."* Verifiable, specific, and the kind of claim this persona will fact-check (which builds trust when it holds up).
2. **The wholesale-spike hook.** *"Wholesale spot prices hit $14,500/MWh during peak. Your battery could trade — instead it's sitting full at 50% state of charge."* Technical, real, and aligns with what they read on Whirlpool.
3. **The accelerated payback hook.** Concrete: *"From an 11-year payback to ~7. Same hardware."* Numbers do all the work.
4. **The "no new hardware" hook.** *"We add a small controller, that's it."* Eliminates the second-largest objection (more capex) before it forms.
5. **The compare hook.** Two columns: "Your battery now" (self-consumption only) vs "Your battery, Reposit-connected" (24/7 trading). Identical hardware, two outcomes.

---

## 7. Trust requirements

Before this persona will share their data, they need to believe:

- The **integration with their specific brand of battery** is real and not "coming soon". They will check OEM compatibility before they click.
- The **revenue split is transparent** — they will not engage with a black-box payment promise. Show the structure: e.g. "Reposit takes a fee for grid services delivered, you get a fixed monthly bill below your current spend".
- The **market data is real**. If you cite NEM dispatch prices, link to AEMO. If you cite payback, show the assumptions.
- The **controller does not compromise warranty or safety** on their battery. Manufacturer-approved integrations matter more than vague reassurances.
- The **performance figures are personalised**. They expect "based on your usage" — not "based on a typical NSW household".

---

## 8. Anti-patterns

- **Hiding the math.** Blurred numbers, "click to reveal", or "up to" framing all read as marketing dishonesty.
- **Generic "save more" benefits.** They want revenue, not savings.
- **Tribe rhetoric.** "Join the smart energy revolution" reads as substanceless. Save it for Persona 4.
- **Marketing-only metrics.** "100% renewable" without context — they want $/MWh.
- **Talking around the controller.** Be specific about what's installed, where it sits in their wiring, and how it talks to their existing inverter/battery.

---

## 9. Subject + preheader patterns

| Subject | Preheader | Why it works |
|---|---|---|
| "Your battery's missing 70%. Here's the fix." | "From idle to 24/7 trading. ~4 years off your payback. Same hardware." | The specificity (70%, 4 years, same hardware) is the persona-fit signal. |
| "Wholesale hit $14k/MWh last week. Your battery slept through it." | "Reposit-connected batteries traded the spike. See yours, modelled." | Concrete, market-aware, links to a reality they already track. |
| "Your retrofit ROI: indicative numbers" | "Annual revenue + fixed bill comparison inside. Show your working — we did." | Acknowledges that they care about working, not conclusions. |

Avoid: any subject that makes a "save up to" claim without a concrete number. They'll mark it as marketing and skip.

---

## 10. Hero formula

```
PANE A     : Your battery right now   (chaos pane — flat
             charge/discharge curve, "idle 16 hrs/day")
PANE B     : Your battery, with Reposit   (calm pane — 24/7
             charge/discharge curve, "trading whenever the
             market signals")
H1         : Your battery cost a fortune.
             It should be earning one.
SUB        : One sentence naming the gap (self-consumption =
             ~30% utilisation). One sentence naming the
             mechanism (Reposit connects it to wholesale
             markets, trades 24/7). One sentence naming the
             outcome (fixed monthly bill below current spend
             + accelerated payback). No fluff.
TAGLINE    : "You and Reposit: Better together"
```

The chaos/calm pane is more powerful here as **two charts** than as two photos — this buyer reads charts. If you have the budget for a real before/after dispatch chart from anonymised data, use it.

---

## 11. CTA verbs

- ✅ **"Put My Battery to Work"** *(canonical)*
- ✅ **"Show Me the Numbers"**
- ✅ **"Run My ROI"**
- ❌ "Get Started" *(generic — wasted slot)*
- ❌ "Save Now" *(off-driver — they don't lead with savings)*
- ❌ "Claim My Plan" *(retailer grammar)*

The button verb is the buyer's actual intent: *activate dormant capex*.

---

## 12. Trust chips (3–4)

| Chip | Reduces objection |
|---|---|
| **Your hardware. Our market access.** | "Do I have to replace anything?" (No.) |
| **Trading revenue, shared back** | "Where's the money come from?" |
| **ROI you can actually see** | "Show me the working." |
| **No new hardware — small controller, that's it** | "What's the install footprint?" |

If you only have three, drop "ROI you can actually see" only if the page already shows a worked-example payback chart elsewhere; otherwise keep it.

---

## 13. Reference landing page

**File:** [`Retrofit_CVP_Landing_BatteryROI.html`](../Retrofit_CVP_Landing_BatteryROI.html)

What's already working:

- The H1 *("Your battery cost a fortune. It should be earning one.")* is the cleanest sunk-cost hook in the suite. Keep verbatim.
- The **news ticker** (rotating real-world claims like "Average home battery uses only 30% of capacity for self-consumption" and "Wholesale spot prices hit $14,500/MWh during peak") is the most persona-fit element on the entire site. It functions as ambient credibility — this buyer trusts a page that links to AEMO-style facts.
- The "Battery utilisation / 100% / Trading 24/7, not just when you're home" outcome tile is the single highest-converting tile across the suite for this persona.
- The closing reassurance *"No new hardware — we add a small controller, that's it"* preempts the persona's biggest mid-funnel objection.

What to keep an eye on:

- The ticker copy must remain factually defensible. If wholesale markets shift, the figures need updating — outdated facts repel this persona faster than no facts.
- The "Bill spike" callout on the chart — keep it but don't oversize. They want to see the trace, not the marketing label.

---

## 14. Conversion psychology summary

The Battery ROI buyer is not converting on optimism — they are converting on **the legitimacy of your numbers**. Your job in every artifact is to make the gap between current and possible utilisation visceral, then make the mechanism for closing that gap auditable. Show your working: dispatch curves, utilisation percentages, payback deltas, revenue splits. Avoid every register of marketing they've spent the last year skipping past — "amazing", "unlock", "transform", "revolution" — and replace it with the only thing they actually trust, which is a specific number tied to a specific assumption. The CTA is not "join us"; it is *"put my battery to work"* — the buyer's own next thought after they've finally accepted that self-consumption was the floor, not the ceiling. Win this persona on substance, lose them on theatre.

---

## 15. Email rules: pre-offer constraint

The **offer-ready email is sent before the offer is calculated** (immediately after the Solax/Sigenstor OAuth completes). At email-send time we do not yet know this user's modelled annual return, fixed monthly fee, payback delta, or offer-expiry timestamp. The **only personalised element in the email body** is the deep link to *their* offer page (`{{ contact.retro_offer }}`).

For full token rules see [`README.md` → Email rule: nothing personalised except the URL](README.md#email-rule-nothing-personalised-except-the-url).

This persona is the **most sensitive to placeholder figures** in the entire suite. A wrong number in the email body — or a number with a wobbly assumption — is irrecoverable for this buyer. The constraint actually helps: if you can't put a number in the email, you can't put a *wrong* one in the email.

### What this persona's email body must contain

| Element | Generic-true copy | Why it works for Battery ROI |
|---|---|---|
| **Hero comparison tile** | *Battery utilisation: **30% → 100%*** + *Payback: **Years faster*** | Both figures are generic industry-standard claims (30% self-consumption ceiling, multi-year payback delta). They are *true on average*, defensibly attributable to AEMO/CSIRO/industry literature, and **not** calculated for this user. |
| **"Show the working" sub-line** | *"Modelled on your last 12 months of dispatch data. Full revenue split, assumptions, and AEMO price references inside. We've shown the working."* | This persona will not click on a number; they will click on the *promise of an audit trail*. Tease the working, not the result. |
| **Validity** | *"Pricing held for **24 hours** from when your offer is generated. After that we re-run the calc against the latest market data."* | "We re-run against the latest market data" is the one phrase that confirms to this buyer that you are running real numbers, not a sales spreadsheet. |
| **Footer date** | *"...because you recently connected your battery to Reposit."* | No specific timestamp. The OAuth flow is recent enough; specificity adds nothing. |
| **CTA** | *"Put My Battery to Work →"* on a single green button → `{{ contact.retro_offer }}`. | The buyer's own next thought after seeing the utilisation gap. |

### Email anti-patterns specific to Battery ROI

- **No specific year-count payback claims in the email body.** "11 years → 7 years" or "~4 years off your payback" must live on the offer page where the assumptions can be footnoted. The email may say *"years faster"* (directional, true) but never a specific number.
- **No placeholder ROI dollar figures.** "Modelled return: $X" with X to be computed reads as bait. Drop the number entirely; promise the working.
- **No AEMO price quotes without full citation.** This persona will check. If you reference wholesale prices, the offer page must show the AEMO data set, the time window, and the methodology. Don't tease a specific spot-price figure in the email at all.
- **No CSIRO / AEMO / industry citations without dates.** A 2019 utilisation stat is worse than no stat for this persona.
- **No "transform" or "unlock" language.** They've been skipping over those words for two years.
- **No marketing-style hero charts in the email.** Save real charts for the offer page where they can be sized correctly. The email is the door, not the data room.

### Tease-the-click lines that work for this persona

These are the only acceptable ways to refer to the personalised numbers *without* showing them:

- "Modelled on your last 12 months of dispatch data."
- "Full revenue split, assumptions, and AEMO price references inside."
- "We've shown the working. Tap to see your modelled return."
- "Your battery's missing 70%. The fix, with the maths attached, is one tap away."
- "From idle to 24/7 trading. Same hardware. The numbers are inside."

The actual calculated figures, the AEMO references, and the full revenue split live on the offer page the user clicks through to. **The email is the door, not the data room.**
