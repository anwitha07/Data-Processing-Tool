"""
raw_curated.py
--------------
ETL step: Raw → Curated
- Reads from raw tables (SourceType=table)
- Applies generic data cleaning
- Casts columns to target datatypes from metadata
- Enforces PK/FK constraints (rejects bad rows)
- Loads into curated tables (full or incremental with MERGE)
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict, Any, List
from datetime import datetime

from scripts.audit import log_job_start, log_job_end
from scripts.validate_input import _fetch_config, _fetch_metadata,_table_exists
from scripts.load_type import full_load, incremental_load,_update_incremental_tracker
from scripts.create_ddl import create_target_tables
from scripts.send_log import send_log


# --- Generic Data Cleaning ---
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning: strip whitespace, normalize nulls, drop duplicates."""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": None, "nan": None, "None": None})
    return df.drop_duplicates()


# --- Type Casting based on Metadata ---
def cast_dataframe_types(df: pd.DataFrame, metadata: List[Dict[str, Any]]) -> pd.DataFrame:
    """Cast DataFrame columns to target datatypes defined in metadata."""
    for m in metadata:
        col = m["TargetColumnName"]
        dtype = str(m["TargetDataType"]).upper()
        if col not in df.columns:
            continue
        try:
            if dtype in ("INT", "BIGINT"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype in ("DECIMAL", "NUMERIC", "FLOAT", "REAL"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dtype in ("DATETIME", "TIMESTAMP"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype.startswith(("NVARCHAR", "VARCHAR", "TEXT")):
                df[col] = df[col].astype(str).str.strip()
        except Exception:
            pass
    return df


def enforce_pk_fk(engine: Engine, df: pd.DataFrame, metadata: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Enforce PK, NOT NULL, and FK constraints based on metadata.
    Logs structured audit info for each rule.
    """
    audit_log = []

    # --- Primary Key enforcement ---
    pk_cols = [m["TargetColumnName"] for m in metadata if m.get("IsPK") == 1]
    if pk_cols:
        before = len(df)
        df = df.dropna(subset=pk_cols)
        df = df.drop_duplicates(subset=pk_cols, keep="last")
        after = len(df)
        if before != after:
            audit_log.append({
                "rule": "PK",
                "columns": pk_cols,
                "dropped": before - after
            })

    # --- NOT NULL enforcement ---
    not_null_cols = [m["TargetColumnName"] for m in metadata if m.get("IsNullable") == 0]
    for col in not_null_cols:
        if col in df.columns:
            before = len(df)
            df = df.dropna(subset=[col])
            after = len(df)
            if before != after:
                audit_log.append({
                    "rule": "NOT NULL",
                    "columns": [col],
                    "dropped": before - after
                })

    # --- Foreign Key enforcement ---
    fk_rules = [
        m for m in metadata
        if m.get("IsFK") == 1 and m.get("ReferenceTable") not in (None, "NULL", "")
    ]
    for rule in fk_rules:
        col = rule["TargetColumnName"]
        ref_table = rule["ReferenceTable"]
        ref_schema = rule.get("ReferenceSchema", "curated")

        try:
            with engine.connect() as conn:
                ref_df = pd.read_sql(
                    text(f"SELECT DISTINCT [{col}] FROM [{ref_schema}].[{ref_table}]"),
                    conn
                )
            valid_values = set(ref_df[col].dropna().tolist())
            before = len(df)
            df = df[df[col].isin(valid_values)]
            after = len(df)
            if before != after:
                audit_log.append({
                    "rule": "FK",
                    "columns": [col],
                    "reference": f"{ref_schema}.{ref_table}",
                    "dropped": before - after
                })
        except Exception as e:
            audit_log.append({
                "rule": "FK",
                "columns": [col],
                "reference": f"{ref_schema}.{ref_table}",
                "skipped": True,
                "reason": str(e)
            })
            continue

    # --- Print audit summary ---
    if audit_log:
        print("Constraint Audit Summary:")
        for entry in audit_log:
            print(entry)

    return df


def enforce_no_negative(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce rule: no numeric column may contain values < 0.
    Drops offending rows and logs how many were removed.
    """
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if not numeric_cols.empty:
        before = len(df)
        df = df[~(df[numeric_cols] < 0).any(axis=1)]
        after = len(df)
        if before != after:
            print(f" Dropped {before - after} rows due to negative values in numeric columns {list(numeric_cols)}")
    return df



# --- Main Function: Raw → Curated ---
def load_raw_to_curated(engine: Engine, job_name: str) -> Dict[str, Any]:
    """
    ETL step: Raw → Curated
    - Reads data from raw tables
    - Renames columns based on metadata
    - Cleans and type-casts values
    - Enforces PK/FK/NOT NULL constraints
    - Loads into curated tables (full or incremental)
    """

    log_job_start(engine, job_name, stage="raw_to_curated")

    try:
        # --- Fetch Config & Metadata ---
        config_rows = _fetch_config(engine, job_name)
        if not config_rows:
            return {"status": "error", "message": f"No Config for {job_name}"}
        config = config_rows[0]

        metadata = _fetch_metadata(engine, job_name)
        if not metadata:
            return {"status": "error", "message": f"No Metadata for {job_name}"}

        source_schema, source_table = config["SourceSchema"], config["SourceTable"]
        target_schema, target_table = config["TargetSchema"], config["TargetTable"]
        load_type = str(config["LoadType"]).lower()

        # --- Ensure curated table exists ---
        if not _table_exists(engine, target_schema, target_table):
            print(f"Curated table {target_schema}.{target_table} not found. Creating from metadata...")
            create_target_tables(engine, job_name)
            print(f"Created curated table {target_schema}.{target_table} from metadata")

        # --- Step 1: Read from raw table ---
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM [{source_schema}].[{source_table}]"), conn)
        print(f" Read {len(df)} rows from {source_schema}.{source_table}")

        # --- Step 2: Rename columns using metadata mapping ---
        rename_map = {m["SourceColumnName"]: m["TargetColumnName"] for m in metadata}
        df = df.rename(columns=rename_map)

        target_cols = [m["TargetColumnName"] for m in metadata]
        df = df.reindex(columns=target_cols)
        print(f" After rename/reindex: {len(df)} rows, cols={df.columns.tolist()}")

        # --- Step 3: Clean + Cast ---
        df = clean_dataframe(df)
        df = cast_dataframe_types(df, metadata)
        df = enforce_no_negative(df)

        # --- Step 4: Enforce PK/FK/NOT NULL constraints ---
        df = enforce_pk_fk(engine, df, metadata)
        print(f" After PK/FK enforcement: {len(df)} rows")

        # --- Step 5: Handle empty DataFrame ---
        if df.empty:
            msg = "No rows to load into curated table."
            log_job_end(engine, job_name, stage="raw_to_curated",
                        row_count=0, status="success", message=msg)
            return {"status": "success", "rows": 0, "message": msg}

        # --- Step 6: Load into curated table ---
        if load_type == "full":
            # ✅ Ensure DataFrame columns match metadata exactly
            target_cols = [m["TargetColumnName"] for m in metadata]
            df = df[target_cols]  # ✅ Align columns
            result = full_load(engine, df, target_schema, target_table)
        elif load_type == "incremental":
            key_cols = [m["TargetColumnName"] for m in metadata if m.get("IsPK") == 1]
            # ✅ Ensure DataFrame columns match metadata exactly
            target_cols = [m["TargetColumnName"] for m in metadata]
            df = df[target_cols]  # ✅ Align columns
            result = incremental_load(
                engine,
                job_name,
                target_schema,
                target_table,
                metadata,
                df,
                stage="raw_curated",
                key_columns=key_cols,
            )
            # Optional: update tracker with run timestamp
            _update_incremental_tracker(engine, job_name, datetime.now(), stage="raw_curated")
        else:
            return {"status": "error", "message": f"Unsupported LoadType: {load_type}"}

        # --- Step 7: Log success ---
        log_job_end(engine, job_name, stage="raw_to_curated",
                    row_count=result.get("rows", 0),
                    status=result.get("status"), message=result.get("message"))
        send_log("raw_curated", "Raw to Curated completed successfully", status="success")
        return result

    except Exception as exc:
        # --- Step 8: Log failure ---
        log_job_end(engine, job_name, stage="raw_to_curated",
                    row_count=0, status="failed", message=str(exc))
        send_log("raw_curated", str(exc), status="failed", exception=exc)      
        return {"status": "error", "message": str(exc)}
