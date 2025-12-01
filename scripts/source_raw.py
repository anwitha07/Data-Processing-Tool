import os
import glob
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, List
from datetime import datetime
import re

from scripts.audit import log_job_start, log_job_end
from scripts.validate_input import _fetch_config, _fetch_metadata
from scripts.load_type import _get_incremental_tracker, _update_incremental_tracker
from scripts.send_log import send_log

DATE_PATTERN = re.compile(r"(\d{4})_(\d{2})_(\d{2})")

# --- Helpers ---
def _read_source_files(files: List[str], source_type: str) -> pd.DataFrame:
    dfs = []
    for f in files:
        if os.path.getsize(f) == 0:
            print(f"⚠️ Skipping empty file: {f}")
            continue
        if source_type == "csv":
            dfs.append(pd.read_csv(f, dtype=str))   # ✅ force all columns to string
        elif source_type == "json":
            try:
                dfs.append(pd.read_json(f, dtype=str))
            except ValueError as e:
                raise ValueError(f"Invalid JSON in file {f}: {e}")
    if not dfs:
        raise ValueError("No valid dataframes to concatenate")
    return pd.concat(dfs, ignore_index=True)


def _insert_raw(engine: Engine, df: pd.DataFrame, schema: str, table: str):
    """Insert DataFrame into raw table using pyodbc executemany."""
    if df.empty:
        raise ValueError("No data to insert into raw table.")

    cols = df.columns.tolist()
    col_clause = ', '.join(f"[{col}]" for col in cols)
    placeholders = ', '.join(['?'] * len(cols))
    query = f"INSERT INTO [{schema}].[{table}] ({col_clause}) VALUES ({placeholders})"

    # ensure all values are strings before insert
    data = df.astype(str).values.tolist()

    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        cursor.executemany(query, data)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Insert failed: {e}")
    finally:
        cursor.close()
        conn.close()


# --- Main function ---
def load_source_to_raw(engine: Engine, job_name: str) -> Dict[str, Any]:
    log_job_start(engine, job_name, stage="source_to_raw")

    try:
        config_rows = _fetch_config(engine, job_name)
        if not config_rows:
            msg = f"No Config found for job '{job_name}'"
            log_job_end(engine, job_name, stage="source_to_raw", row_count=0, status="failed", message=msg)
            return {"status": "error", "message": msg}
        config = config_rows[0]

        metadata = _fetch_metadata(engine, job_name)
        if not metadata:
            msg = f"No Metadata found for job '{job_name}'"
            log_job_end(engine, job_name, stage="source_to_raw", row_count=0, status="failed", message=msg)
            return {"status": "error", "message": msg}

        target_schema = config["TargetSchema"]
        target_table = config["TargetTable"]
        load_type = str(config["LoadType"]).lower()
        source_path = config["SourcePath"]
        source_type = str(config["SourceType"]).lower()

        # --- Collect files ---
        if os.path.isdir(source_path):
            pattern = "*.csv" if source_type == "csv" else "*.json"
            files = glob.glob(os.path.join(source_path, pattern))
        else:
            files = [source_path]

        if not files:
            raise FileNotFoundError(f"No {source_type.upper()} files found in {source_path}")

        # --- Load logic ---
        def process_files(file_list: List[str]) -> pd.DataFrame:
            df = _read_source_files(file_list, source_type)

            # Ensure all expected source columns exist
            source_cols = [m["SourceColumnName"] for m in metadata]
            missing = set(source_cols) - set(df.columns)
            if missing:
                raise ValueError(f"Missing columns in source file: {missing}")

            # Select only the source columns
            df = df[source_cols]

            # Rename to target column names
            rename_map = {m["SourceColumnName"]: m["TargetColumnName"] for m in metadata}
            df = df.rename(columns=rename_map)

            # Reorder to match metadata
            target_cols = [m["TargetColumnName"] for m in metadata]
            df = df[target_cols]

            return df

        if load_type == "full":
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE [{target_schema}].[{target_table}]"))

            df = process_files(files)
            _insert_raw(engine, df, target_schema, target_table)

            result = {"status": "success", "rows": len(df),
                      "message": f"Full load completed: {len(df)} rows"}

        elif load_type == "incremental":
            last_load_time = _get_incremental_tracker(engine, job_name, stage="source_raw") or datetime(1900, 1, 1)

            new_files = []
            for f in files:
                match = DATE_PATTERN.search(os.path.basename(f))
                if match:
                    year, month, day = map(int, match.groups())
                    file_date = datetime(year, month, day)
                    if file_date > last_load_time:
                        new_files.append((f, file_date))

            if not new_files:
                result = {"status": "success", "rows": 0,
                          "message": "No new files to process"}
            else:
                new_files.sort(key=lambda x: x[1])
                file_paths = [f for f, _ in new_files]

                df = process_files(file_paths)
                _insert_raw(engine, df, target_schema, target_table)

                max_date = max(d for _, d in new_files)
                _update_incremental_tracker(engine, job_name, max_date, stage="source_raw")

                result = {"status": "success", "rows": len(df),
                          "message": f"Incremental load completed: {len(df)} rows"}

        else:
            msg = f"Unsupported LoadType: {load_type}"
            result = {"status": "error", "message": msg}

        log_job_end(engine, job_name, stage="source_to_raw",
                    row_count=result.get("rows", 0),
                    status=result.get("status"), message=result.get("message"))
        send_log("raw_curated", result.get("message"), status=result.get("status"))
        return result

    except Exception as exc:
        log_job_end(engine, job_name, stage="source_to_raw",
                    row_count=0, status="failed", message=str(exc))
        send_log("raw_curated", str(exc), status="failed", exception=exc)
        return {"status": "error", "message": str(exc)}
