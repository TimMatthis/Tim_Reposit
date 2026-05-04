# Reposit Retrofit — Persona Guides

This folder is the source of truth for the **six retrofit buyer personas** Reposit Power markets the No Bill™ retrofit offer to. Use these guides whenever you write a landing page, an email, an ad, a chatbot script, or a sales call opener for the retrofit funnel.

Each persona guide is structured the same way so you can copy a section in isolation (e.g. "give me the hero formula for Battery ROI") without having to read the whole document.

---

## The six personas

| # | Persona | One-line driver | Landing page |
|---|---|---|---|
| 1 | **Bill Fairness** *(master / default)* | "I have a battery. I shouldn't still have a bill." | [Retrofit_CVP_Landing.html](../Retrofit_CVP_Landing.html) |
| 2 | **Fixed Income** | "My income is fixed. My energy bill should be too." | [Retrofit_CVP_Landing_FixedIncome.html](../Retrofit_CVP_Landing_FixedIncome.html) |
| 3 | **Battery ROI** | "My battery cost a fortune. It should be earning one." | [Retrofit_CVP_Landing_BatteryROI.html](../Retrofit_CVP_Landing_BatteryROI.html) |
| 4 | **Green Impact** | "My battery should power more than my home." | [Retrofit_CVP_Landing_GreenImpact.html](../Retrofit_CVP_Landing_GreenImpact.html) |
| 5 | **Energy Uncertainty** | "I can't control the market. I can control my bill." | [Retrofit_CVP_Landing_EnergyUncertainty.html](../Retrofit_CVP_Landing_EnergyUncertainty.html) |
| 6 | **Set And Forget** | "I have better things to do than manage electricity." | [Retrofit_CVP_Landing_SetAndForget.html](../Retrofit_CVP_Landing_SetAndForget.html) |

---

## Common ground (true for all six)

Before you reach for a persona-specific lever, remember what these buyers all share:

- They **already own a solar/battery system**. The retrofit pitch never asks them to buy hardware. The "small Reposit controller, that's it" framing is universal.
- They are **already mildly disappointed** with what their battery is delivering. Self-consumption alone has not solved the bill problem they hoped it would solve.
- They are **price-aware but not exclusively price-driven**. They responded to a battery purchase that ran into five figures, so they're capable of evaluating value, not just cost.
- They prefer **calm, plain Australian English** over either corporate jargon or hype. Reposit's voice is "boring on purpose, beautiful by consequence" — that's a brand-wide rule.
- They mistrust **electricity retailers** specifically. Any copy that even faintly resembles incumbent retailer marketing erodes trust fast.
- They expect **transparency about commitment**: 60 seconds to connect data, ~15 minutes to a personalised offer, no payment today, 24-hour offer hold, cancel any time before they confirm.

Every persona-specific lever in the rest of these guides is layered **on top of** that common ground, never instead of it.

---

## How to read a persona guide

Each guide has the same 15 sections:

1. **Snapshot** — who they are in one paragraph
2. **Core driver** — the single emotional engine
3. **Inner monologue** — quotes the buyer would actually say to themselves
4. **Tone principles** — five directives for voice
5. **Vocabulary** — words to use, words to avoid
6. **Hooks** — 3–5 winning angles
7. **Trust requirements** — what must be true before they click
8. **Anti-patterns** — what this persona will reject and why
9. **Subject + preheader patterns** — examples with rationale
10. **Hero formula** — eyebrow → H1 → sub structure
11. **CTA verbs** — words that match their drive
12. **Trust chips** — 3–4 persona-specific reassurances
13. **Reference landing page** — what's already working in the live page
14. **Conversion psychology summary** — one paragraph that ties it all together
15. **Email rules: pre-offer constraint** — what the offer-ready email may and may not contain for this persona, given that the email is sent before the offer is calculated

Treat the guides as **prescriptive, not descriptive**. The vocabulary and hook lists are not just "things you might say" — they are the things this persona reliably converts on, plus the things that cause measurable drop-off.

---

## When in doubt, default to Bill Fairness

If a campaign needs to span multiple personas (e.g. a paid social ad that gets shown to all of them), use **Persona 1: Bill Fairness**. It is the master tone Reposit was built on, and it will not actively repel any of the other five. The other five guides exist to *amplify conversion* against a known segment — not to overwrite the brand voice.

---

## Maintenance notes

- These guides are versioned with the landing pages. If you significantly change a landing page's hero, validity panel, or trust chips, update the corresponding persona guide too.
- Don't add a 7th persona without first checking whether an existing persona can be sharpened. Five well-defined segments outperform seven fuzzy ones in A/B testing.
- The persona names (Bill Fairness, Fixed Income, etc.) are **internal codenames**. Never use them as customer-facing copy.

---

## Email rule: nothing personalised except the URL

The "offer ready" emails are sent **before the offer is calculated** (immediately after the Solax/Sigenstor OAuth completes). At email-send time we do not yet know the customer's annual saving, fixed monthly fee, or offer-expiry timestamp — those are produced when they click through.

That changes one thing for every persona: the email body must read as **generic to every recipient**. The single personalised element is the deep link to *their* offer page.

### Tokens permitted in the email body

| Token | Purpose | Personalised? |
|---|---|---|
| `{{ contact.retro_offer }}` | Deep link to this user's calculated offer page. Used on every CTA, "View offer" footer link, and any in-body link. | **Yes — required.** |
| `{{ unsubscribe_link }}` | HubSpot-native unsubscribe/preferences URL. Must be used in an anchor `href`, ideally with `data-unsubscribe="true"`. | HubSpot-managed compliance link. |

### Tokens **not** available at email-send time (do not use)

- ~~`{{annual_saving}}`~~ — not yet calculated.
- ~~`{{offer_expires_iso}}`~~ — not yet calculated. Use the generic phrase **"locked for 24 hours from when your offer is generated"** in validity panels instead.
- ~~`{{connected_at_iso}}`~~ — replaced by the generic phrase **"recently connected your battery to Reposit"** in the footer.
- ~~`{{contact.firstname}}`~~ — never used. The brand position is anonymous-safe.

### What this means for hero tiles

Every "look how much you'd save" hero tile in these emails is **generic-true** rather than personally calculated:

- **Bill Fairness:** *"Your electricity bill / $0"* — the No Bill™ brand promise, true for every retrofit customer.
- **Fixed Income:** *"What you'd pay / One fixed fee"* — the structural promise, true for every retrofit customer.
- **Battery ROI:** Two-column comparison (utilisation 30% → 100%, payback "Years faster") — both are generic industry-standard claims, not calculated per-user.
- **Green Impact:** Fleet tile (2,000+) + "Fixed monthly fee" tile — both true for every recipient.
- **Energy Uncertainty:** *"What you'd pay / One fixed fee / Same. Every. Month."* — structural, persona-aligned.
- **Set And Forget:** *"Decisions required / Zero"* — generic and persona-true.

The actual dollar figure lives **on the offer page they click through to**, not in the email. Each email's hero sub-line ends with a variant of *"Your personalised number is inside"* to make the click feel like a reveal, not a re-pitch.

### What this means for persona guides

The guides still describe the **persona's psychology** — what hooks, what tone, what to avoid. Where a guide cites an illustrative dollar figure (e.g. *"$346/year is roughly four weeks of groceries"*), treat that as a **landing-page or post-click reference**, not as something the email body shows. Emails reach for those promises **structurally** (one fixed fee, $0 bill, fleet of 2,000+) without naming a specific number.

Each persona file now ends with a **Section 15: Email rules** that spells out, for that persona specifically:

- the exact generic-true hero / validity / footer copy
- the persona-specific email anti-patterns (the placeholder-figure traps, hype upgrades, and tone wobbles that look fine in the abstract but cost clicks for *this* buyer)
- the persona-specific tease-the-click lines (the only acceptable ways to refer to the personalised numbers without showing them)

When writing or reviewing a retrofit email, **start with that persona's Section 15** — it is the operational layer for everything in the rest of the guide.

If you ever need to put a personal figure in an email body, you'll have to move email send to **after** offer calculation — at which point all four currently-blocked tokens become available again, and Section 15 of every persona will need to be revisited.
