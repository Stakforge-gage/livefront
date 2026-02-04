from __future__ import annotations

from pathlib import Path
import duckdb
import streamlit as st

def get_latest_db_path(root: Path) -> Path:
    """
    Resolves DuckDB path using duckdb/LATEST_DB.txt if present.
    Falls back to duckdb/carton_caps.duckdb for backwards compatibility.
    """
    duck_dir = root / "duckdb"
    ptr = duck_dir / "LATEST_DB.txt"
    if ptr.exists():
        name = ptr.read_text(encoding="utf-8").strip()
        if name:
            return duck_dir / name
    return duck_dir / "carton_caps.duckdb"

@st.cache_resource
def get_con(db_path: str):
    """
    Cache a connection per db_path. Read-only to reduce contention.
    """
    return duckdb.connect(db_path, read_only=True)

def query_df(root: Path, sql: str, params=None):
    db_path = str(get_latest_db_path(root))
    con = get_con(db_path)
    if params:
        return con.execute(sql, params).df(), db_path
    return con.execute(sql).df(), db_path
