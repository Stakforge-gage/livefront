from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import duckdb


def get_latest_db_path(root: Path) -> Path:
    duck_dir = root / "duckdb"
    latest_ptr = duck_dir / "LATEST_DB.txt"
    default_db = duck_dir / "carton_caps.duckdb"

    if latest_ptr.exists():
        name = latest_ptr.read_text(encoding="utf-8").strip()
        if name:
            p = Path(name)
            if p.is_absolute():
                return p
            return duck_dir / name

    if default_db.exists():
        return default_db

    raise FileNotFoundError(
        f"No DuckDB found. Expected {latest_ptr} or {default_db} to exist."
    )


@lru_cache(maxsize=4)
def get_con(db_path: str):
    return duckdb.connect(db_path, read_only=True)


def query_df(root: Path, sql: str, params=None):
    db_path = get_latest_db_path(root)
    con = get_con(str(db_path))
    if params:
        df = con.execute(sql, params).df()
    else:
        df = con.execute(sql).df()
    return df, db_path
