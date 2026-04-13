# Retrofit End-to-End Journey

## Current Logic Review

### `superPower_YourBatter_dev_Guide.html`
- Purpose: internal wireframe and technical blueprint of the full retrofit funnel.
- Strength: clearly maps consent, OEM data retrieval, optimizer, utility calculator, top-3 offers, payment, and provisioning.
- Gap: front-end behaviors are still mock behavior (email validation only on regex, consent code accepted by length, timer-based loading, simulated payment progression).
- Gap: many production notes exist as comments, but execution logic is not connected to real API calls.

### `Product_Utility_Calculator.html`
- Purpose: conjoint utility workbench and API validation surface.
- Strength: full utility model is implemented; supports slider utilities, CSV survey import, cohort filtering, and API simulation.
- Strength: `top-packages` request path is already live-capable (`GET /api/top-packages?monthlyFee=...`) with clear response rendering.
- Gap: mode selector previously defaulted to fallback/local behavior; now default set to live-first to encourage real API testing.

### `Retrofit_CVP_Landing.html`
- Purpose: customer-facing narrative plus embedded interactive flow.
- Prior gap: journey was presented as simulation (static code behavior and manual profile-first assumptions).
- Prior gap: no explicit UX pattern for missing profile data from backend; user had to rely on simulated inputs.

---

## Rebuilt User Journey (Simplicity + Clarity)

### 1) Consent Start
- User enters battery-app email.
- Page performs a real API call: `POST /api/consent/request-code`.
- UX message confirms that code request was sent.

### 2) Consent Verification
- User enters received code.
- Page performs a real API call: `POST /api/consent/verify-code`.
- If successful, user moves forward automatically.

### 3) Energy Profile Retrieval
- Page attempts real profile retrieval via `GET /api/energy/profile?email=...`.
- If profile is complete, flow proceeds with profile-derived monthly fee anchor.
- If profile is missing/incomplete, a dedicated **manual input modal** opens so this missing-data state is explicit and controlled.

### 4) Conjoint Offer Build
- Monthly fee anchor is derived from profile (`70%` of average monthly spend, rounded).
- Page performs a real conjoint API call to `GET /api/top-packages?monthlyFee=...`.
- If shared utility snapshot exists from calculator session, the page sends a real `POST` payload with custom utilities.
- User receives ranked top-3 package options and can confirm preferred package.

### 5) Downstream Handoff
- Selection confirmation remains in-page.
- Next production step is agreement + billing + provisioning.

---

## UX Principles Applied

- Real API-first journey language (removed simulation framing from the main flow).
- Clear progression (consent -> profile -> offers) with explicit status messaging.
- Distinct fallback UX for missing energy data (modal, not hidden assumptions).
- Keep customer-facing copy focused on one decision at a time.
- Preserve compatibility with the calculator utility structure so messaging and math stay consistent.
