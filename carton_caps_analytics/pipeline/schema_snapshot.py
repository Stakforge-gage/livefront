import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import sys

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "duckdb" / "carton_caps.duckdb"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# capture these schemas (raw + main for dbt outputs)
SCHEMAS = ["raw", "main"]

def snapshot():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    tables = con.execute("""
      select table_schema, table_name
      from information_schema.tables
      where table_schema in ('raw','main')
      order by table_schema, table_name
    """).fetchall()

    snap = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),
        "db_path": str(DB_PATH),
        "schemas": {},
    }

    for schema, table in tables:
        full = f"{schema}.{table}"
        cols = con.execute(f"""
          select
            column_name,
            data_type,
            is_nullable
          from information_schema.columns
          where table_schema = '{schema}'
            and table_name = '{table}'
          order by ordinal_position
        """).fetchall()

        snap["schemas"].setdefault(schema, {})
        snap["schemas"][schema][table] = [
            {"column": c[0], "type": c[1], "nullable": c[2]} for c in cols
        ]

    con.close()
    return snap

def main():
    run_id = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = LOG_DIR / f"schema_{run_id}.json"
    snap = snapshot()
    out_path.write_text(json.dumps(snap, indent=2), encoding="utf-8")
    print(f"Wrote schema snapshot: {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
