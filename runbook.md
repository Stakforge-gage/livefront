# Carton Caps Analytics Platform  
## Data Engineering Runbook

**Owner:** Data Engineering (Consulting)  
**Audience:** Product, Marketing, Finance, Technical Stakeholders  
**Purpose:** How to run, monitor, and reason about the Carton Caps analytics pipeline end-to-end

---

## 1. Overview

This repository implements a **full end-to-end analytics pipeline** for the Carton Caps application, designed to demonstrate production-grade data engineering practices in a consulting environment.

The platform supports:
- Realistic synthetic data generation
- Repeatable pipeline runs with isolation per run
- Data quality testing and contracts
- Analytics-ready marts
- Operational observability via Streamlit dashboards

Each pipeline execution produces a **new, immutable analytics snapshot**, enabling safe iteration, debugging, and replay.

---

## 2. Architecture Summary

```
data_generator.py
        ↓
CSV + SQLite (source simulation)
        ↓
duckdb/load_raw.py
        ↓
DuckDB (raw layer + ingestion metadata)
        ↓
dbt (staging + marts + tests)
        ↓
DuckDB (analytics-ready schema)
        ↓
Streamlit (Pipeline Ops, Contracts, Analytics)
```

### Key Design Principles
- Separation of concerns (generation, ingestion, transformation, presentation)
- Append-only, run-isolated pipeline executions
- Ingestion-time SLAs (not event-time)
- Strong data contracts and explicit ownership
- Consulting-friendly transparency and debuggability

---

## 3. Repository Structure

```
carton_caps_analytics/
├── data/                     # Generated CSV + SQLite artifacts
├── generator/
│   └── data_generator.py     # Realistic synthetic data generator
├── duckdb/
│   ├── load_raw.py           # Raw ingestion into DuckDB (+ _ingested_at)
│   ├── carton_caps_*.duckdb  # Per-run DuckDB files
│   └── LATEST_DB.txt         # Pointer to active analytics DB
├── dbt_carton_caps/
│   ├── models/               # Staging + marts
│   ├── tests/                # dbt tests
│   ├── exposures.yml         # Streamlit dashboard dependencies
│   └── profiles.yml          # Auto-written per run
├── pipeline/
│   ├── run_pipeline.py       # Orchestrates full pipeline
│   └── schema_snapshot.py    # Schema contract capture
├── logs/
│   ├── pipeline_*.log        # Pipeline execution logs
│   ├── pipeline_*.json       # Run manifests
│   └── schema_*.json         # Schema snapshots
└── streamlit_app/
    ├── app.py
    └── pages/                # Ops, contracts, analytics dashboards
```

---

## 4. How to Run the Pipeline

### 4.1 Activate Environment

```bash
source .venv/Scripts/activate
```

### 4.2 Run the Full Pipeline

```bash
python pipeline/run_pipeline.py
```

This performs:
1. Synthetic data generation
2. Raw ingestion into a new DuckDB file
3. dbt build + tests
4. Schema snapshot capture
5. Pipeline manifest + logs
6. Updates `duckdb/LATEST_DB.txt`

Each run is fully isolated and safe to repeat.

---

## 5. Pipeline Outputs

Each successful run produces:
- New DuckDB file: `duckdb/carton_caps_<RUN_ID>.duckdb`
- Updated pointer: `duckdb/LATEST_DB.txt`
- Logs and manifests in `logs/`

Streamlit dashboards always read from `LATEST_DB.txt`.

---

## 6. Data Layers & Contracts

### Raw Layer
- Source-like data from CSVs
- Includes `_ingested_at` timestamp
- No business logic applied

### Staging Layer
- Cleaned, typed, constrained
- Mirrors dbt best practices

### Marts Layer
- Analytics-ready fact and dimension tables
- Owned by Analytics / Finance personas
- Backed by tests and contracts

---

## 7. Data Contracts & SLAs

Contracts are enforced in the **Data Contract Panel**.

Dimensions:
- Row count minimums
- Primary key uniqueness
- Not-null constraints
- Freshness SLAs (ingestion-based)

Freshness uses:
```
_ingested_at (preferred)
↓ fallback
event_time
```

---

## 8. Monitoring & Observability

Streamlit pages:
- Pipeline Ops
- Data Contract Panel
- dbt Test Results
- Product, Marketing, Finance dashboards

---

## 9. Common Issues & Resolutions

- **Stale contracts:** Expected if using event-time; fixed via ingestion-time freshness
- **Missing tables in Streamlit:** Restart Streamlit / clear cache
- **DuckDB file lock errors:** Ensure per-run DB isolation

---

## 10. Stakeholder Use Cases

**Product:** Engagement funnels, feature adoption  
**Marketing:** Referral conversion, viral coefficient  
**Finance:** Reward liability, redemption forecasting  
**Engineering:** Pipeline health, schema drift, contracts

---

## 11. Design Decisions

- Run-isolated DuckDB files
- Ingestion-based SLAs
- Explicit ownership & contracts
- Streamlit operational transparency

---

## 12. Scope Boundary

This take-home intentionally stops at:
- Local DuckDB
- Streamlit dashboards
- Single-node execution

Production extensions:
- Cloud storage
- Orchestration
- CI/CD
- Warehouse scaling

---

## 13. Final Notes

This system demonstrates data engineering as **craft, product, and collaboration** — aligned with Livefront’s philosophy.
