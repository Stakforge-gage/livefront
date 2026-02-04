import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

DUCK_DIR = ROOT / "duckdb"
DUCK_DIR.mkdir(exist_ok=True)
LATEST_PTR = DUCK_DIR / "LATEST_DB.txt"

DBT_PROJECT_DIR = ROOT / "dbt_carton_caps"
DBT_PROFILES_DIR = DBT_PROJECT_DIR          # profiles.yml is stored here
DBT_PROFILES_YML = DBT_PROFILES_DIR / "profiles.yml"
DBT_TARGET = DBT_PROJECT_DIR / "target"
DBT_RUN_RESULTS = DBT_TARGET / "run_results.json"
DBT_MANIFEST = DBT_TARGET / "manifest.json"

SCHEMA_SNAPSHOT_SCRIPT = ROOT / "pipeline" / "schema_snapshot.py"

PYTHON = sys.executable or "python"


def utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(cmd, log_fp):
    log_fp.write(f"\n$ {' '.join(map(str, cmd))}\n")
    log_fp.flush()
    p = subprocess.Popen(
        list(map(str, cmd)),
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines = []
    for line in p.stdout:
        lines.append(line)
        log_fp.write(line)
        log_fp.flush()
    rc = p.wait()
    return rc, "".join(lines)


def safe_copy(src: Path, dst: Path):
    try:
        if src.exists():
            shutil.copyfile(src, dst)
            return True
    except Exception:
        pass
    return False


def write_profiles_for_db(db_path_abs: Path):
    """
    Write dbt profiles.yml using an ABSOLUTE path (prevents Windows path resolution issues).
    """
    db_path = db_path_abs.as_posix()
    content = f"""dbt_carton_caps:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      threads: 4
"""
    DBT_PROFILES_YML.write_text(content, encoding="utf-8")


def main():
    run_id = sys.argv[1].strip() if len(sys.argv) >= 2 and sys.argv[1].strip() else utc_run_id()

    log_path = LOG_DIR / f"pipeline_{run_id}.log"
    manifest_path = LOG_DIR / f"pipeline_{run_id}.json"

    db_filename = f"carton_caps_{run_id}.duckdb"
    db_path_abs = DUCK_DIR / db_filename

    steps = [
        {"name": "generate_data", "cmd": [PYTHON, "generator/data_generator.py"]},
        {"name": "load_duckdb_raw", "cmd": [PYTHON, "duckdb/load_raw.py", run_id]},
        {
            "name": "dbt_build",
            "cmd": ["dbt", "build", "--profiles-dir", str(DBT_PROFILES_DIR), "--project-dir", str(DBT_PROJECT_DIR)],
        },
        {
            "name": "dbt_test",
            "cmd": ["dbt", "test", "--profiles-dir", str(DBT_PROFILES_DIR), "--project-dir", str(DBT_PROJECT_DIR)],
        },
    ]

    manifest = {
        "run_id": run_id,
        "python_executable": PYTHON,
        "duckdb_file": db_filename,
        "duckdb_path": str(db_path_abs),
        "started_at_utc": utc_iso(),
        "steps": [],
        "status": "running",
    }

    t0 = time.time()

    with open(log_path, "w", encoding="utf-8") as log_fp:
        log_fp.write(f"Pipeline run_id={run_id}\n")
        log_fp.write(f"Using PYTHON={PYTHON}\n")
        log_fp.write(f"Target DuckDB file={db_path_abs}\n")

        for step in steps:
            s0 = time.time()
            rc, _out = run(step["cmd"], log_fp)
            dur = round(time.time() - s0, 3)

            manifest["steps"].append({
                "name": step["name"],
                "cmd": list(map(str, step["cmd"])),
                "return_code": rc,
                "duration_seconds": dur,
            })

            if rc != 0:
                manifest["status"] = "failed"
                manifest["failed_step"] = step["name"]
                break

            # After load completes, rewrite dbt profiles to THIS run's DB
            if step["name"] == "load_duckdb_raw":
                if not db_path_abs.exists():
                    manifest["status"] = "failed"
                    manifest["failed_step"] = "load_duckdb_raw"
                    log_fp.write(f"\nERROR: expected DuckDB file not found at {db_path_abs}\n")
                    log_fp.flush()
                    break

                write_profiles_for_db(db_path_abs)
                log_fp.write("\nWrote dbt profiles.yml:\n")
                log_fp.write(DBT_PROFILES_YML.read_text(encoding="utf-8") + "\n")
                log_fp.flush()

        if manifest["status"] != "failed":
            manifest["status"] = "success"

    # Post-run artifacts (best effort)
    if SCHEMA_SNAPSHOT_SCRIPT.exists():
        try:
            subprocess.check_call([PYTHON, str(SCHEMA_SNAPSHOT_SCRIPT), run_id], cwd=str(ROOT))
            manifest["schema_snapshot"] = str(LOG_DIR / f"schema_{run_id}.json")
        except Exception as e:
            manifest["schema_snapshot_error"] = str(e)

    copied = {}
    copied["run_results"] = safe_copy(DBT_RUN_RESULTS, LOG_DIR / f"run_results_{run_id}.json")
    copied["dbt_manifest"] = safe_copy(DBT_MANIFEST, LOG_DIR / f"dbt_manifest_{run_id}.json")
    manifest["copied_artifacts"] = copied

    manifest["duration_seconds"] = round(time.time() - t0, 3)
    manifest["ended_at_utc"] = utc_iso()
    manifest["log_path"] = str(log_path)

    # Publish pointer ONLY on success
    if manifest["status"] == "success":
        LATEST_PTR.write_text(db_filename, encoding="utf-8")

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"Wrote log: {log_path}")
    print(f"Wrote manifest: {manifest_path}")
    return 0 if manifest["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
