# Carton Caps Analytics Platform  
## Production Migration Guide (Cloud-Ready)

This document outlines how to evolve the current local prototype (SQLite + DuckDB + dbt + Streamlit) into a production-grade, cloud-native analytics platform suitable for real users, SLAs, governance, and scale.

---

## 1. Current Prototype Summary

**Today (local):**
- Synthetic data generator writes CSV + SQLite
- `load_raw.py` ingests CSVs into a per-run DuckDB file (`duckdb/carton_caps_<run_id>.duckdb`)
- dbt builds staging + marts in the same DuckDB file and runs tests
- `LATEST_DB.txt` points Streamlit to the latest “blessed” snapshot
- Streamlit provides ops visibility, contracts, and analytics pages
- Logs/manifests are written locally to `logs/`

**Production goals:**
- Ingest real app + partner data continuously
- Support incremental loads and late-arriving data
- Observability, alerting, and incident response
- Governance, privacy, and access controls
- Repeatable deployments via CI/CD
- Scale compute and storage independently

---

## 2. Target Production Architecture

### 2.1 Data Flow (Recommended)
```
App/Backend events + Receipt OCR + Product catalog + Referrals
        ↓
Landing (Object storage)  [Bronze files]
        ↓
Raw tables (Warehouse)    [Bronze tables]
        ↓
dbt transforms            [Silver/Gold]
        ↓
BI + Product analytics + Finance reporting
        ↓
Monitoring + Data quality + Contracts + Lineage
```

### 2.2 Recommended Cloud Components (generic)
- **Object storage:** S3 / Azure Blob / GCS (landing zone)
- **Orchestration:** Dagster / Airflow / Prefect
- **Warehouse:** Snowflake (or Databricks/Spark if desired)
- **Transformation:** dbt Core + CI
- **Observability:** OpenLineage + dbt artifacts + logs + alerts (Slack/email)
- **Secrets:** Vault / cloud secrets manager
- **BI:** Looker / Tableau / Power BI
- **App analytics:** RudderStack/Segment (optional)

---

## 3. Productionizing Each Stage

### 3.1 Replace Generator with Real Sources
The synthetic generator becomes:
- A **test harness** (dev/staging)
- A **seed dataset** for local demos
- A **data contract validation tool** for CI

Real production sources typically include:
- App event tracking (clicks, views, scans, registrations)
- Receipt scan outcomes (OCR parse confidence, matches)
- Purchase ledger (normalized items, qualifying products)
- Referral code lifecycle
- School metadata and fundraising payouts

**Action:**
- Define each upstream as a **contracted source** with schema + SLA.
- Establish source-of-truth for each domain.

---

### 3.2 Replace CSV/SQLite Landing with Object Storage
Instead of writing CSV locally:
- Write raw files to an object store partitioned by ingestion date/run:
  - `s3://carton-caps/landing/events/dt=YYYY-MM-DD/…`
  - `…/purchases/dt=YYYY-MM-DD/…`

**Best practice:**
- Use immutable files and append-only semantics in landing.
- Capture ingestion metadata: `_ingested_at`, `_source`, `_run_id`, `_file_name`.

---

### 3.3 Raw Ingestion → Warehouse “Bronze”
Replace DuckDB raw tables with warehouse raw tables:
- Create `raw.events`, `raw.purchases`, etc.
- Load from object storage continuously or by schedule.
- Preserve raw payload columns as-is where possible.

**Key decisions:**
- CDC vs append-only ingestion
- Dedup strategy using stable event ids / receipt ids
- Late arriving handling (replays / watermark windows)

---

### 3.4 dbt Transforms → Production Standards
dbt already fits production well:
- Keep `stg_*`, `dim_*`, `fct_*`
- Add **incremental models** where needed:
  - `fct_purchase` incremental by `purchase_ts` + dedup by `purchase_id`
  - `fct_referral` incremental by `referral_id` updates
- Add **snapshots** for slowly changing dimensions (schools, products)
- Add **exposures** tied to BI dashboards

**CI/CD:**
- Use `dbt build` in PRs against a staging schema
- Promote to prod with tagged releases

---

### 3.5 Observability and Alerting
Production requires proactive detection:
- Pipeline step durations (regression detection)
- Rowcount deltas and freshness checks
- Schema drift / contract violations
- dbt test failures → alerting

**Recommended:**
- Store pipeline logs/manifests in a central place (object store or log system)
- Push key metrics to a monitoring system (Datadog/CloudWatch/etc.)
- Alert on SLA breaches (freshness, completeness, error rate)

---

### 3.6 Governance, Security, and Privacy
Key considerations for Carton Caps:
- User data (PII): email, phone, address (if present)
- Referral identifiers
- Receipts may contain sensitive data

**Actions:**
- Data classification and masking policies
- Role-based access controls:
  - Product analytics vs finance vs ops
- Encryption at rest + in transit
- Audit logs and lineage

---

## 4. Operational Runbooks (Production)
In addition to the local RUNBOOK.md, production should include:
- Incident response playbook
- Backfill/replay procedure
- On-call ownership map
- Data correctness signoff for releases

---

## 5. Environment Strategy
Maintain 3 environments:
- **Dev:** fast iteration (small data)
- **Staging:** production-like schemas + test loads
- **Prod:** audited, monitored, SLA-driven

---

## 6. Scalability Strategy
As volume grows:
- Move from batch to micro-batch / streaming ingestion
- Partition raw tables by date and key dimensions (school_id)
- Use incremental transforms and clustering
- Add aggregates / semantic models for BI

---

## 7. Cutover Plan (Recommended)
1. Stand up landing + raw ingestion in parallel
2. Build dbt models against real data in staging
3. Validate contracts and outputs with stakeholders
4. Run parallel reporting for a short window
5. Flip BI and downstream consumers
6. Decommission prototype components (DuckDB local)

---

## 8. What Stays the Same
Even in production, the core concepts remain:
- Contracts
- Testable transformations
- Observability
- Stakeholder-driven marts
- Run manifests and auditability

---

## Appendix: Production Checklist
- [ ] Upstream schemas + SLAs agreed
- [ ] Landing zone configured + encryption
- [ ] Raw ingestion automated + monitored
- [ ] dbt CI/CD pipelines
- [ ] Access control model implemented
- [ ] Alerts configured (failures + freshness)
- [ ] Backfill and replay procedures
- [ ] Documentation + ownership published
