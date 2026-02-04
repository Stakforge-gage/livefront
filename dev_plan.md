# Carton Caps — Analytics Pipeline (SQLite + DuckDB + dbt + Streamlit)
Updated: 2026-02-02
Owner: (You) — Acting Livefront Director of Data

## 1) Context
Carton Caps enables users to scan receipts for qualifying purchases to raise money for schools. Users can refer friends via code/link.
We are building analytics infrastructure to track engagement, referral effectiveness, and school fundraising performance.

Key program rules:
- Referred users must be new; users are US residents. :contentReference[oaicite:17]{index=17}
- Referral code/link must be used at signup or within ~48 hours of install. :contentReference[oaicite:18]{index=18}
- Rewards are earned when the referred user completes onboarding and performs a qualifying action (first eligible scan OR links to a school). :contentReference[oaicite:19]{index=19}
- Self-referrals (same device/email/payment method) are disallowed; rewards can be withheld. :contentReference[oaicite:20]{index=20}
FAQ clarifies attribution is strict (no retro-credit without link/code) and >48h delays matter. :contentReference[oaicite:21]{index=21}

## 2) Stakeholders & Core Questions (design inputs)

### Client Product Lead (decision-maker for experience)
- Where do users drop in onboarding and receipt scanning?
- What features most strongly drive activation and retention?
- Time-to-value: how fast do users perform their first qualifying action?
- What product changes would move scan completion and repeat usage?

### Client Marketing Lead (acquisition & virality)
- What is the referral funnel: sent → clicked → converted (reward-eligible)?
- What conversion rate do we see by channel, cohort, school, user_type?
- What is the network effect / viral coefficient proxy and where does it break?
- Which “super spreaders” or schools generate compounding growth?

### Client Technical Lead (client-server integration)
- How should events be emitted (install, referral applied, onboarding complete, school linked, scan complete)?
- How do we handle incremental loads, idempotency, late-arriving events?
- What data contracts + validation rules prevent broken analytics?

### Finance / BizOps (added stakeholder)
- What is expected reward liability next 30/60/90 days?
- What are expected redemptions vs awards (cashflow timing)?
- Cost per acquired active user (referral vs organic), and scenario modeling:
  - If conversion rate increases by X%, what happens to reward cost?
  - If bonus amount changes, what is the budget impact?

### (Implicit demographic coverage)
- End-user segments: parent/teacher/supporter are first-class attributes from generator. :contentReference[oaicite:22]{index=22}
- Geography: state/city exists on schools; US residency is tracked as an assumption aligned to rules. :contentReference[oaicite:23]{index=23}:contentReference[oaicite:24]{index=24}

## 3) Data Sources
Provided:
- Carton Caps Data.sqlite (base schema / minimal sample)
- data_generator.py (must implement generate_purchases and generate_referrals) :contentReference[oaicite:25]{index=25}
- Referral FAQs + Program Rules PDFs

Generated:
- schools.csv, users.csv, products.csv (already)
- purchases.csv, referrals.csv (to implement)
- events.csv (new, required to fully model rules: install/apply/onboarding/school_linked/etc.)

## 4) Canonical Schema

### Source (Generator-produced)
- schools(school_id, name, address, city, state, zip_code, created_at)
- users(user_id, first_name, last_name, email, school_id, created_at, user_type)
- products(product_id, name, category, price, points_per_dollar, created_at)
- purchases(purchase_id, user_id, product_id, quantity, price_paid, points_earned, purchased_at, day_of_week, hour_of_day)
- referrals(referral_id, referrer_user_id, referred_email, referred_user_id, referral_code, sent_at, converted_at, status)

### New Events (Generator extension)
events(event_id, user_id, event_type, event_at, referral_id, metadata_json)
Event types include: install, referral_applied, onboarding_complete, school_linked, reward_awarded, reward_redeemed, app_open, incentive_viewed, etc.

## 5) Transformations (dbt on DuckDB)
Layers:
- Bronze: raw tables loaded via DuckDB from SQLite/CSVs
- Silver: stg_* typed + standardized
- Gold: dims/facts for product, marketing, finance, trust & safety

Gold tables:
- dim_user, dim_school, dim_product
- fct_purchase
- fct_user_day (engagement/retention)
- fct_referral (rules-compliant lifecycle + 48h window + qualifying action)
- fct_rewards (award/redemption ledger)
- fct_reward_forecast_day (finance forecasts)
- fct_network_effects_user / fct_network_effects_school
- fct_trust_safety_signals (fraud/abuse heuristics)

## 6) Data Quality & Testing
dbt tests:
- PK uniqueness/not-null on ids
- FK integrity across purchases/users/products/schools
- Referral rules checks:
  - referred_user must be new (no conversion to pre-existing user)
  - within_48h_window computed from install/apply events :contentReference[oaicite:26]{index=26}
  - reward_awarded requires onboarding_complete + qualifying_action :contentReference[oaicite:27]{index=27}
Anomaly checks (simple):
- daily volume deltas for purchases/referrals/events
- pending rewards >48h (ops metric) :contentReference[oaicite:28]{index=28}

## 7) Incremental Strategy (prototype-accurate)
- dbt incremental for fct_user_day, fct_school_day, fct_referral, fct_rewards
- reprocessing window: last 7 days to capture late-arriving events

## 8) Streamlit App (Stakeholder Views)
Pages:
1) Executive Overview (KPIs, schools, growth)
2) Product Dashboard (funnels, feature adoption, retention drivers)
3) Referral Program (funnel, 48h window compliance, latency, cohorts)
4) Finance Forecast (expected awards/redemptions/cost, scenarios)
5) Network Effects (graph + super spreaders + school network health)
6) Trust & Safety (fraud flags, suspicious clusters, delayed rewards)

## 9) Delivery Artifacts
- dbt docs site (catalog + lineage)
- dev_plan.md (this)
- assumptions.md (generator distributions + simplifications)
- Streamlit app + screenshots
- runbook.md (how to run generator → dbt build → streamlit)

## 10) Implementation Order
1) Extend generator with events.csv + implement purchases/referrals methods
2) DuckDB project scaffold + dbt sources/staging
3) Gold marts + dbt tests
4) Streamlit pages + visuals
5) Finance forecast + network effects add-ons
6) Polish docs + meeting narrative
