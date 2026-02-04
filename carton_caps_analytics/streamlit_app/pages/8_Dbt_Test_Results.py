import json
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="dbt Test Results", layout="wide")

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"

st.title("dbt Test Results Viewer")
st.caption("Reads run_results_<run_id>.json saved from the pipeline and surfaces failures/warnings fast.")

runs = sorted(LOG_DIR.glob("run_results_*.json"), reverse=True)
if not runs:
    st.warning("No run_results artifacts found. Run the pipeline to generate logs/run_results_<run_id>.json.")
    st.stop()

choices = [p.name for p in runs[:25]]
selected = st.selectbox("Select run_results file", choices)
path = LOG_DIR / selected

data = json.loads(path.read_text(encoding="utf-8"))
results = data.get("results", [])

if not results:
    st.info("No results in this run_results file.")
    st.stop()

# Build a flat table
rows = []
for r in results:
    node = r.get("node", {})
    rows.append({
        "unique_id": node.get("unique_id"),
        "name": node.get("name"),
        "resource_type": node.get("resource_type"),
        "status": r.get("status"),
        "execution_time_s": r.get("execution_time"),
        "message": (r.get("message") or "")[:300],
        "failures": r.get("failures"),
        "thread_id": r.get("thread_id"),
    })

df = pd.DataFrame(rows)

# Summary
status_counts = df["status"].value_counts().reset_index()
status_counts.columns = ["status", "count"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total nodes", len(df))
c2.metric("PASS", int((df["status"] == "success").sum()))
c3.metric("WARN", int((df["status"] == "warn").sum()))
c4.metric("FAIL/ERROR", int(((df["status"] == "fail") | (df["status"] == "error")).sum()))

st.subheader("Status summary")
st.dataframe(status_counts, use_container_width=True)

st.divider()

# Filters
st.subheader("Filter")
options = sorted(df["resource_type"].dropna().unique().tolist())
preferred_defaults = [x for x in ["test","data_test","schema_test"] if x in options]
default = preferred_defaults if preferred_defaults else (options[:1] if options else [])
ftype = st.multiselect("Resource type", options, default=default)
status_options = sorted(df["status"].dropna().unique().tolist())
preferred = ["fail", "error", "warn", "skip", "pass", "success"]
status_default = [x for x in preferred if x in status_options]
if not status_default:
    status_default = status_options
fstatus = st.multiselect("Status", status_options, default=status_default)

filtered = df.copy()
if ftype:
    filtered = filtered[filtered["resource_type"].isin(ftype)]
if fstatus:
    filtered = filtered[filtered["status"].isin(fstatus)]

st.subheader("Results")
st.dataframe(
    filtered.sort_values(["status", "execution_time_s"], ascending=[True, False]),
    use_container_width=True
)

st.divider()

st.subheader("Details")
sel = st.selectbox("Pick a node to inspect", filtered["unique_id"].dropna().tolist()[:100] if len(filtered) else df["unique_id"].dropna().tolist()[:100])
if sel:
    rec = df[df["unique_id"] == sel].iloc[0].to_dict()
    st.json(rec)
