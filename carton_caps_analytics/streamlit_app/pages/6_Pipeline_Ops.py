import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df, get_con, get_latest_db_path

st.set_page_config(page_title="Pipeline Ops", layout="wide")

LOG_DIR = ROOT / "logs"
PIPELINE_SCRIPT = ROOT / "pipeline" / "run_pipeline.py"
CONTRACTS_PATH = ROOT / "contracts" / "data_contracts.json"

LOG_DIR.mkdir(exist_ok=True)

st.title("Pipeline Ops")
st.caption("Operational view: pipeline DAG, run status, runtime, row counts, and data quality signals.")

# -----------------------------
# DAG visual
# -----------------------------
st.subheader("Pipeline DAG")
mermaid = r"""
graph TD
  A[generator/data_generator.py<br/>Generate realistic CSV + SQLite] --> B[duckdb/load_raw.py<br/>Load raw schema]
  B --> C[dbt build<br/>staging (views)]
  C --> D[dbt build<br/>marts (tables)]
  D --> E[dbt test<br/>quality gates]
  E --> F[Streamlit dashboards<br/>Product/Marketing/Finance/Ops]
"""
st.markdown(f"```mermaid\n{mermaid}\n```")

st.divider()

# -----------------------------
# Run pipeline now
# -----------------------------
st.subheader("Run Pipeline Now (Local)")
st.caption("Runs the full pipeline and writes logs/manifests to ./logs. Snapshot mode: overwrites data/*.csv each run.")

run_col1, run_col2 = st.columns([1, 2])
with run_col1:
    run_clicked = st.button("▶ Run pipeline now", type="primary")
with run_col2:
    st.info("Tip: Keep this open — output will stream below.")

output_box = st.empty()

def stream_process(cmd):
    p = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    buffer = []
    for line in p.stdout:
        buffer.append(line.rstrip("\n"))
        if len(buffer) > 300:
            buffer = buffer[-300:]
        output_box.code("\n".join(buffer), language="text")
    rc = p.wait()
    return rc, buffer

if run_clicked:
    try:
        get_con.clear()
    except Exception:
        pass
    if not PIPELINE_SCRIPT.exists():
        st.error("pipeline/run_pipeline.py not found. Create it first.")
    else:
        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        st.write(f"Running pipeline run_id={run_id} ...")
        cmd = [sys.executable, str(PIPELINE_SCRIPT), run_id]
        rc, _buf = stream_process(cmd)
        if rc == 0:
            st.success("Pipeline run completed successfully.")
        else:
            st.error("Pipeline run failed. Check the log viewer below for details.")
        time.sleep(0.5)
        st.rerun()

st.divider()

# -----------------------------
# Load run manifests
# -----------------------------
st.subheader("Recent Pipeline Runs")

manifests = sorted(LOG_DIR.glob("pipeline_*.json"), reverse=True)
if not manifests:
    st.warning("No run manifests found yet. Click 'Run pipeline now' or run: python pipeline/run_pipeline.py")
    st.stop()

choices = [m.name for m in manifests[:25]]
selected = st.selectbox("Select a run", choices)

manifest_path = LOG_DIR / selected
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

status = manifest.get("status", "unknown")
run_id = manifest.get("run_id")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Run ID", run_id)
c2.metric("Status", status)
c3.metric("Duration (s)", manifest.get("duration_seconds", None))
c4.metric("Failed step", manifest.get("failed_step", "-") if status != "success" else "-")

st.write("Step breakdown:")
steps_df = pd.DataFrame(manifest.get("steps", []))
st.dataframe(steps_df, use_container_width=True)

log_path = manifest.get("log_path")
if log_path:
    st.caption(f"Log file: {log_path}")

st.divider()

# -----------------------------
# DuckDB helpers
# -----------------------------
def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

# -----------------------------
# Warehouse health stats
# -----------------------------
st.subheader("Warehouse Health (DuckDB)")

row_counts, db_path = q("""
select 'raw.schools' as table_name, count(*) cnt from raw.schools
union all select 'raw.users', count(*) from raw.users
union all select 'raw.products', count(*) from raw.products
union all select 'raw.referrals', count(*) from raw.referrals
union all select 'raw.purchases', count(*) from raw.purchases
union all select 'raw.events', count(*) from raw.events
union all select 'dim_user', count(*) from dim_user
union all select 'fct_purchase', count(*) from fct_purchase
union all select 'fct_referral', count(*) from fct_referral
union all select 'fct_rewards', count(*) from fct_rewards
order by table_name
""")

st.caption(f"Warehouse: {db_path}")

st.dataframe(row_counts, use_container_width=True)

st.divider()

st.subheader("Quality Signals")

signals_df, _ = q("""
with r as (
  select
    avg(case when status='converted' then 1 else 0 end) as referral_conversion_rate,
    avg(case when eligible_referral then 1 else 0 end) as eligible_rate,
    avg(case when within_48h_window then 1 else 0 end) as within_48h_rate
  from fct_referral
),
e as (
  select
    sum(case when event_type='receipt_scan_started' then 1 else 0 end) as scan_started,
    sum(case when event_type='receipt_scan_completed' then 1 else 0 end) as scan_completed
  from stg_events
),
p as (
  select sum(price_paid) as purchase_spend, sum(points_earned) as points_earned from fct_purchase
)
select
  referral_conversion_rate,
  eligible_rate,
  within_48h_rate,
  scan_started,
  scan_completed,
  scan_completed * 1.0 / nullif(scan_started,0) as scan_completion_rate,
  purchase_spend,
  points_earned
from r, e, p
""")
signals = signals_df.iloc[0]

a, b, c, d = st.columns(4)
a.metric("Referral conversion", f"{float(signals['referral_conversion_rate']):.1%}")
b.metric("Eligible referrals", f"{float(signals['eligible_rate']):.1%}")
c.metric("Within 48h", f"{float(signals['within_48h_rate']):.1%}")
d.metric("Scan completion", f"{float(signals['scan_completion_rate']):.1%}")

st.divider()

# -----------------------------
# Data Contract Panel
# -----------------------------
st.subheader("Data Contract Panel")
st.caption("A lightweight, explicit contract: datasets, owners, SLAs, and automated status checks.")

if not CONTRACTS_PATH.exists():
    st.warning("contracts/data_contracts.json not found. Create it to enable contract panel.")
    st.stop()

contracts = json.loads(CONTRACTS_PATH.read_text(encoding="utf-8")).get("contracts", [])

def get_table_stats(table_name: str, fallback_field: str | None):
    # row count
    cnt_df, _ = q(f"select count(*) as cnt from {table_name}")
    cnt = int(cnt_df.iloc[0]["cnt"]) if len(cnt_df) else 0

    max_ts = None
    freshness_col = None
    freshness_error = None

    def fetch_max(col: str):
        ts_df, _ = q(f"select max({col}) as max_ts from {table_name}")
        return ts_df.iloc[0]["max_ts"] if len(ts_df) else None

    try:
        max_ts = fetch_max("_ingested_at")
        freshness_col = "_ingested_at"
    except Exception:
        if fallback_field:
            try:
                max_ts = fetch_max(fallback_field)
                freshness_col = fallback_field
            except Exception:
                freshness_error = f"FRESHNESS_COL_MISSING=_ingested_at,{fallback_field}"
        else:
            freshness_error = "FRESHNESS_COL_MISSING=_ingested_at"

    return cnt, max_ts, freshness_col, freshness_error

rows = []
now = datetime.utcnow()

for cdef in contracts:
    name = cdef["name"]
    freshness_field = cdef.get("freshness_field")
    sla = cdef.get("sla", {})
    min_rows = int(sla.get("min_rows", 0))
    fresh_within_days = int(sla.get("fresh_within_days", 9999))

    try:
        cnt, max_ts, freshness_col, freshness_error = get_table_stats(name, freshness_field)
        # Determine status
        status_parts = []
        ok = True

        if cnt < min_rows:
            ok = False
            status_parts.append(f"ROWCOUNT<{min_rows}")
        else:
            status_parts.append("ROWCOUNT_OK")

        if freshness_col:
            status_parts.append(f"FRESHNESS_COL={freshness_col}")
        elif freshness_error:
            ok = False
            status_parts.append(freshness_error)

        freshness_ok = None
        age_days = None
        if max_ts is not None:
            # duckdb returns python datetime sometimes; safe convert
            try:
                age_days = (now - max_ts).total_seconds() / 86400.0
                freshness_ok = age_days <= fresh_within_days
            except Exception:
                freshness_ok = None

        if freshness_ok is True:
            status_parts.append("FRESH_OK")
        elif freshness_ok is False:
            ok = False
            status_parts.append(f"STALE>{fresh_within_days}d")
        else:
            status_parts.append("FRESH_NA")

        if freshness_error:
            contract_status = "⚠️ ERROR"
        else:
            contract_status = "✅ PASS" if ok else "❌ FAIL"
        rows.append({
            "dataset": name,
            "layer": cdef.get("layer"),
            "owner": cdef.get("owner"),
            "primary_key": cdef.get("primary_key"),
            "freshness_field": freshness_col,
            "max_freshness_ts": str(max_ts) if max_ts is not None else None,
            "age_days": round(age_days, 2) if age_days is not None else None,
            "sla_fresh_within_days": fresh_within_days,
            "row_count": cnt,
            "sla_min_rows": min_rows,
            "status": contract_status,
            "checks": ", ".join(status_parts),
            "description": cdef.get("description"),
            "critical_tests": "; ".join(cdef.get("critical_tests", []))
        })
    except Exception as e:
        rows.append({
            "dataset": name,
            "layer": cdef.get("layer"),
            "owner": cdef.get("owner"),
            "primary_key": cdef.get("primary_key"),
            "freshness_field": freshness_field,
            "max_freshness_ts": None,
            "age_days": None,
            "sla_fresh_within_days": fresh_within_days,
            "row_count": None,
            "sla_min_rows": min_rows,
            "status": "⚠️ ERROR",
            "checks": str(e),
            "description": cdef.get("description"),
            "critical_tests": "; ".join(cdef.get("critical_tests", []))
        })

df = pd.DataFrame(rows)

# quick summary badges
pass_cnt = int((df["status"] == "✅ PASS").sum())
fail_cnt = int((df["status"] == "❌ FAIL").sum())
err_cnt = int((df["status"] == "⚠️ ERROR").sum())
x1, x2, x3 = st.columns(3)
x1.metric("Contracts PASS", pass_cnt)
x2.metric("Contracts FAIL", fail_cnt)
x3.metric("Contracts ERROR", err_cnt)

# show table
st.dataframe(
    df[[
        "dataset","layer","owner","status","row_count","sla_min_rows",
        "max_freshness_ts","age_days","sla_fresh_within_days","checks","primary_key"
    ]],
    use_container_width=True
)

with st.expander("Contract details"):
    st.dataframe(df[["dataset","description","critical_tests"]], use_container_width=True)

# -----------------------------
# Schema Drift Detector
# -----------------------------
st.divider()
st.subheader("Schema Drift Detector")
st.caption("Compares schema snapshots between runs and highlights added/removed/changed columns.")

def load_schema(run_id: str):
    p = LOG_DIR / f"schema_{run_id}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def flatten_schema(snap):
    # returns dict: full_table -> list of (col, type, nullable)
    out = {}
    schemas = snap.get("schemas", {})
    for sch, tables in schemas.items():
        for tbl, cols in tables.items():
            full = f"{sch}.{tbl}"
            out[full] = [(c["column"], c["type"], c["nullable"]) for c in cols]
    return out

def diff_schemas(old, new):
    diffs = []
    old_map = flatten_schema(old) if old else {}
    new_map = flatten_schema(new) if new else {}

    all_tables = sorted(set(old_map.keys()) | set(new_map.keys()))
    for t in all_tables:
        ocols = old_map.get(t, [])
        ncols = new_map.get(t, [])

        oset = set(ocols)
        nset = set(ncols)

        added = sorted(list(nset - oset))
        removed = sorted(list(oset - nset))

        # detect type changes by column name
        o_by_name = {c[0]: c for c in ocols}
        n_by_name = {c[0]: c for c in ncols}
        common = set(o_by_name.keys()) & set(n_by_name.keys())
        changed = []
        for col in sorted(common):
            if o_by_name[col] != n_by_name[col]:
                changed.append((o_by_name[col], n_by_name[col]))

        if added or removed or changed:
            diffs.append({
                "table": t,
                "added_cols": added,
                "removed_cols": removed,
                "changed_cols": changed,
            })
    return diffs

# determine current + previous run_id based on manifest selection
current_run_id = run_id
prev_run_id = None
# manifests is already sorted newest-first; selected may not be index 0, so locate it
try:
    idx = choices.index(selected)
    if idx + 1 < len(choices):
        # next item is older (previous run)
        prev_manifest_name = choices[idx + 1]
        prev_run_id = prev_manifest_name.replace("pipeline_", "").replace(".json", "")
except Exception:
    prev_run_id = None

colA, colB = st.columns(2)
with colA:
    st.write(f"Current run: `{current_run_id}`")
with colB:
    st.write(f"Previous run: `{prev_run_id}`" if prev_run_id else "Previous run: (none)")

if not prev_run_id:
    st.info("Run the pipeline at least twice to enable drift comparison.")
else:
    cur_snap = load_schema(current_run_id)
    prev_snap = load_schema(prev_run_id)

    if not cur_snap or not prev_snap:
        st.warning("Schema snapshots missing for one of the runs. Re-run pipeline to generate schema_<run_id>.json.")
    else:
        diffs = diff_schemas(prev_snap, cur_snap)
        if not diffs:
            st.success("No schema drift detected between the selected runs.")
        else:
            # summarize
            drift_rows = []
            for d in diffs:
                drift_rows.append({
                    "table": d["table"],
                    "added": len(d["added_cols"]),
                    "removed": len(d["removed_cols"]),
                    "changed": len(d["changed_cols"]),
                })
            st.dataframe(pd.DataFrame(drift_rows), use_container_width=True)

            with st.expander("Drift details"):
                for d in diffs:
                    st.markdown(f"**{d['table']}**")
                    if d["added_cols"]:
                        st.write("Added:", [f"{c[0]} ({c[1]}, nullable={c[2]})" for c in d["added_cols"]])
                    if d["removed_cols"]:
                        st.write("Removed:", [f"{c[0]} ({c[1]}, nullable={c[2]})" for c in d["removed_cols"]])
                    if d["changed_cols"]:
                        st.write("Changed:")
                        for before, after in d["changed_cols"]:
                            st.write(f"- {before[0]}: {before[1]}->{after[1]}, nullable {before[2]}->{after[2]}")
