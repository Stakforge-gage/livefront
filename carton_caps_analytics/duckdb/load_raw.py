import os
import duckdb
from pathlib import Path
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
DUCK_DIR = ROOT / "duckdb"
DUCK_DIR.mkdir(exist_ok=True)

DATA = ROOT / "data"
LATEST_PTR = DUCK_DIR / "LATEST_DB.txt"

def main():
    # If run_id passed, use it; otherwise timestamp
    run_id = sys.argv[1].strip() if len(sys.argv) > 1 else datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    db_path = DUCK_DIR / f"carton_caps_{run_id}.duckdb"

    print(f"Using Python interpreter: {sys.executable}")
    print(f"Writing DuckDB to: {db_path}")

    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4;")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    for t in ["schools","users","products","referrals","purchases","events"]:
        con.execute(f"DROP TABLE IF EXISTS raw.{t}")

    con.execute(f"CREATE TABLE raw.schools   AS SELECT *, current_timestamp AS _ingested_at FROM read_csv_auto('{DATA / 'schools.csv'}', header=true)")
    con.execute(f"CREATE TABLE raw.users     AS SELECT *, current_timestamp AS _ingested_at FROM read_csv_auto('{DATA / 'users.csv'}', header=true)")
    con.execute(f"CREATE TABLE raw.products  AS SELECT *, current_timestamp AS _ingested_at FROM read_csv_auto('{DATA / 'products.csv'}', header=true)")
    con.execute(f"CREATE TABLE raw.referrals AS SELECT *, current_timestamp AS _ingested_at FROM read_csv_auto('{DATA / 'referrals.csv'}', header=true)")
    con.execute(f"CREATE TABLE raw.purchases AS SELECT *, current_timestamp AS _ingested_at FROM read_csv_auto('{DATA / 'purchases.csv'}', header=true)")
    con.execute(f"CREATE TABLE raw.events    AS SELECT *, current_timestamp AS _ingested_at FROM read_csv_auto('{DATA / 'events.csv'}', header=true)")

    counts = con.execute("""
    SELECT 'schools' AS table_name, COUNT(*) cnt FROM raw.schools
    UNION ALL SELECT 'users', COUNT(*) FROM raw.users
    UNION ALL SELECT 'products', COUNT(*) FROM raw.products
    UNION ALL SELECT 'referrals', COUNT(*) FROM raw.referrals
    UNION ALL SELECT 'purchases', COUNT(*) FROM raw.purchases
    UNION ALL SELECT 'events', COUNT(*) FROM raw.events
    ORDER BY table_name;
    """).fetchall()

    for name, cnt in counts:
        print(f"{name:10s} {cnt}")

    con.close()

    # Write pointer for Streamlit / dbt

if __name__ == "__main__":
    main()
