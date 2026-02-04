from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Data Log Viewer", layout="wide")

ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT / "logs"

st.title("Data Log Viewer")
st.caption("Browse pipeline logs for troubleshooting and operational transparency.")

logs = sorted(LOG_DIR.glob("pipeline_*.log"), reverse=True)
if not logs:
    st.warning("No logs found. Run: python pipeline/run_pipeline.py")
    st.stop()

log_names = [p.name for p in logs[:50]]
selected = st.selectbox("Select a log", log_names)
log_path = LOG_DIR / selected

text = log_path.read_text(encoding="utf-8", errors="replace").splitlines()

st.sidebar.header("Filters")
contains = st.sidebar.text_input("Contains text (case-insensitive)", value="")
max_lines = st.sidebar.slider("Max lines to show", min_value=200, max_value=5000, value=1200, step=200)

filtered = []
needle = contains.lower().strip()
for line in text:
    if needle and needle not in line.lower():
        continue
    filtered.append(line)
    if len(filtered) >= max_lines:
        break

st.write(f"Showing {len(filtered)} lines from `{selected}`")
st.code("\n".join(filtered), language="text")
