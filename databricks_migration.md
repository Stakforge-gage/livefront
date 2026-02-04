# Carton Caps Analytics Platform  
## Databricks Migration Guide (Step-by-Step)

This document maps the current prototype (SQLite + DuckDB + dbt + Streamlit) to a **Databricks Lakehouse** implementation, keeping the same analytical outputs while upgrading to production-scale ingestion, governance, and observability.

---

## 1. Component Mapping (Prototype → Databricks)

| Prototype Component | Databricks Equivalent | Notes |
|---|---|---|
| Local CSV landing (`data/*.csv`) | Cloud object storage (S3/ADLS/GCS) + Unity Catalog volumes/external locations | Use partitioned folders + immutable files |
| SQLite “source simulation” | Bronze Delta tables (or Auto Loader landing) | SQLite stays as local test harness only |
| DuckDB raw tables (`raw.*`) | Bronze Delta tables in Unity Catalog | Append-only + ingestion metadata columns |
| `_ingested_at` column | Ingestion metadata (`_ingested_at`, `_run_id`, `_source_file`) | Enforced in Bronze contracts |
| dbt DuckDB adapter | dbt-databricks adapter **or** Databricks SQL + DLT | dbt still great for marts; DLT great for streaming & expectations |
| dbt tests + custom SQL tests | dbt tests **and/or** DLT Expectations + Lakehouse Monitoring | Prefer DLT for pipeline quality gates; dbt for model logic tests |
| Per-run DuckDB snapshots | Delta time travel + table versioning, optionally clone/copy into “release” schemas | Use Unity Catalog schemas + naming conventions |
| `LATEST_DB.txt` pointer | “Current” views, tagged releases, or schema promotion | Use `marts_current` views or UC tags |
| Local logs/manifests | Databricks Jobs run logs + MLflow/Delta tables for run metadata | Store run manifests in `ops.pipeline_runs` |
| Streamlit dashboards | Databricks SQL Dashboards / Lakeview, or external Streamlit connecting to Databricks SQL Warehouse | Optional: host Streamlit separately |
| `run_pipeline.py` orchestrator | Databricks Workflows (Jobs) | Tasks: ingest → transform → test → publish |

---

## 2. Target Databricks Architecture

### 2.1 Lakehouse Flow (Bronze → Silver → Gold)
```
App events + Receipt outcomes + Referrals + Catalog + Schools/Users
        ↓
Bronze (Delta, UC): raw append-only + ingestion metadata
        ↓
Silver (Delta, UC): cleaned/typed/deduped + keys
        ↓
Gold (Delta, UC): marts (dim/fct), aggregates, serving views
        ↓
Databricks SQL / BI / Product dashboards / Finance reporting
        ↓
Monitoring + DQ expectations + Alerts
```

### 2.2 Recommended Databricks Services
- **Unity Catalog**: governance, access control, lineage
- **Auto Loader**: scalable file ingestion from cloud storage
- **Delta Lake**: ACID tables, time travel, schema evolution
- **Delta Live Tables (DLT)**: managed pipelines + Expectations
- **Databricks Workflows (Jobs)**: orchestration and scheduling
- **Databricks SQL Warehouse**: BI-grade query serving
- **Lakehouse Monitoring**: data quality + drift monitoring (optional/if enabled)
- **MLflow**: experiment tracking + model registry (for forecasting work)

---

## 3. Ingestion Strategy (Replacing CSV→DuckDB Load)

### 3.1 Landing Zone in Cloud Storage
Move the “raw file” concept from local disk to cloud storage:
- `s3://carton-caps/landing/events/dt=YYYY-MM-DD/…`
- `…/purchases/dt=YYYY-MM-DD/…`

Keep **immutable, append-only** files. Add run metadata at write time when possible:
- `_run_id`, `_source_file`, `_ingested_at`

### 3.2 Auto Loader → Bronze Delta
Use Auto Loader to ingest continuously or micro-batch.

Example concept (not code-specific):
- Auto Loader reads from landing path
- Writes to `uc_catalog.bronze.events` as Delta
- Adds metadata columns:
  - `_ingested_at = current_timestamp()`
  - `_source_file = input_file_name()`
  - `_run_id` if provided

**Why:** Auto Loader scales, handles schema evolution, and supports exactly-once semantics.

---

## 4. Bronze Layer (Databricks equivalent of `raw.*`)

Create Bronze tables in Unity Catalog:
- `carton_caps.bronze.events`
- `carton_caps.bronze.purchases`
- `carton_caps.bronze.referrals`
- `carton_caps.bronze.users`
- `carton_caps.bronze.schools`
- `carton_caps.bronze.products`

Bronze principles:
- Minimal transformation
- Preserve raw fields + ingestion metadata
- Append-only
- Schema evolution allowed but monitored

---

## 5. Silver Layer (Cleaning + Keys + Dedup)

Silver is the place to:
- Cast types reliably
- Standardize timestamps/timezones
- Deduplicate by stable keys (e.g., `event_id`, `purchase_id`)
- Enforce referential integrity where feasible
- Implement “late arriving” correction windows if needed

Typical patterns:
- **Dedup**: keep latest record by `_ingested_at` per key
- **Late arriving**: watermark by event time but accept delayed inserts
- **Soft deletes**: if upstream supports CDC

Tables:
- `carton_caps.silver.stg_events`
- `carton_caps.silver.stg_purchases`
- `carton_caps.silver.stg_referrals`
- etc.

---

## 6. Gold Layer (Marts: dim/fct)

Gold matches your current dbt outputs:
- `carton_caps.gold.dim_user`
- `carton_caps.gold.dim_school`
- `carton_caps.gold.dim_product`
- `carton_caps.gold.fct_purchase`
- `carton_caps.gold.fct_referral`
- `carton_caps.gold.fct_rewards`

Two implementation options:

### Option A: dbt on Databricks (closest to prototype)
- Use `dbt-databricks` adapter
- Materialize models as Delta tables/views
- Run `dbt build` in a Databricks Job task

**Pros:** Reuses your dbt project directly  
**Cons:** DQ expectations live outside dbt unless you add them

### Option B: DLT for Bronze/Silver + dbt for Gold (best of both)
- DLT pipelines handle ingestion + Silver cleaning + expectations
- dbt handles Gold marts and analytics semantics

**Pros:** Strong operational controls + clear medallion separation  
**Cons:** Slightly more moving parts (but very “real”)

---

## 7. Data Quality & Contracts in Databricks

### 7.1 DLT Expectations (Quality Gates)
Convert your “Data Contract Panel” checks into DLT expectations:
- Not null constraints
- Uniqueness constraints (approx via aggregations)
- Freshness checks using `_ingested_at`
- Row count minimums / anomaly detection

**Behavior choices:**
- `expect_or_drop` (drop bad rows)
- `expect_or_fail` (fail pipeline run)
- `expect` (log metrics only)

### 7.2 dbt Tests (Model-Level Guarantees)
Keep dbt tests for:
- unique + not_null
- relationships
- custom tests (like your event window checks)

### 7.3 Monitoring
- DLT event log tables for pipeline health
- Job run status + alerts
- Optional Lakehouse Monitoring for drift/anomalies

---

## 8. Orchestration (Replacing `run_pipeline.py`)

Use **Databricks Workflows** with tasks:

1. **Ingest task**  
   - Auto Loader / DLT pipeline update (Bronze/Silver)

2. **Transform task**  
   - dbt build (Gold) OR DLT Gold if you choose all-DLT

3. **Test/Validate task**  
   - dbt test
   - Contract queries written to `ops.contract_results`

4. **Publish task**  
   - Promote “current” views or tag the release
   - Update `ops.latest_release` table

5. **Notify task**  
   - Slack/email on failure or SLA breach

---

## 9. Replacing `LATEST_DB.txt` (How “Current” Works)

In production you don’t want a text pointer; you want a governed “current” layer:

### Pattern A: “Current Views”
- Create views like:
  - `carton_caps.gold_current.fct_purchase` → points to latest validated table version

### Pattern B: Schema Promotion
- `gold_staging` built and validated
- swap to `gold_prod` via controlled promotion

### Pattern C: Delta Version Pinning
- Store a “release manifest” with table versions:
  - `ops.releases(table_name, delta_version, run_id)`

This mirrors your current run manifest concept but is fully queryable.

---

## 10. Serving & Visualization

### Option A: Databricks SQL Dashboards / Lakeview
- Create dashboards directly in Databricks SQL Warehouse
- Great for ops + stakeholder views

### Option B: External BI (Power BI/Tableau/Looker)
- Connect to Databricks SQL Warehouse
- Governed access via Unity Catalog

### Option C: Keep Streamlit
- Run Streamlit externally (or on a VM/container) and point it to Databricks SQL Warehouse
- Replace DuckDB connector with Databricks SQL connector

---

## 11. Forecasting & Network Effects in Databricks (Finance/Product)

Databricks is ideal for modeling:
- Referral conversion forecasting
- Reward liability forecasting (cash-out exposure)
- Churn/retention models
- Network effect metrics (k-factor, depth distribution)

Recommended stack:
- Feature tables (optional)
- MLflow tracking
- Model registry
- Batch inference jobs writing predictions to `gold.pred_*`

---

## 12. Migration Plan (Practical Cutover)

1. **Stand up Unity Catalog** (catalog/schemas/roles)
2. **Create landing zone** in cloud storage
3. **Implement Auto Loader → Bronze**
4. **Implement Silver cleaning + DQ expectations (DLT)**
5. **Port dbt project to Databricks** (dbt-databricks adapter)
6. **Create Gold marts and run dbt tests**
7. **Operationalize Workflows** (schedule + alerts)
8. **Publish dashboards** (Databricks SQL or external BI)
9. **Parallel-run validation** against DuckDB outputs (short window)
10. **Cut over consumers** to Databricks-hosted marts

---

## 13. Production Checklist (Databricks)
- [ ] Unity Catalog roles + permissions
- [ ] External locations + encryption
- [ ] Auto Loader checkpoints stored securely
- [ ] DLT expectations defined and monitored
- [ ] dbt CI job + prod job in Workflows
- [ ] Run metadata stored in `ops.pipeline_runs`
- [ ] Alerts for failures and freshness SLA breaches
- [ ] Release/promotion strategy documented
- [ ] Data retention + GDPR/PII handling plan

---

## Summary
A Databricks migration preserves your core design (contracts + marts + observability) while upgrading the runtime to a scalable, governed Lakehouse architecture using **Delta Lake + Unity Catalog + Workflows**, optionally **DLT** for managed ingestion and quality gates.
