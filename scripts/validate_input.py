"""
Validation utilities for Config and Metadata Excel files.
Ensures required columns exist and values are within allowed ranges.
"""
from typing import Dict, List, Any
from typing import List
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

# --- Config validation rules ---
REQUIRED_CONFIG_COLS = [
    "JobName", "SourceType", "SourcePath", "SourceSchema", "SourceTable",
    "TargetSchema", "TargetTable", "LoadType", "SCDType"
]
ALLOWED_SOURCE_TYPES = {"csv", "json","table"}
ALLOWED_LOAD_TYPES = {"full", "incremental"}
ALLOWED_SCD_TYPES = {None, "1", "2"}


# -- Metadata validation rules --- 
REQUIRED_METADATA_COLS = [
    "JobName", "SourceColumnName", "TargetColumnName", "TargetDataType",
    "Length", "IsPK", "IsFK", "IsNullable", "ReferenceTable"
]
ALLOWED_SQL_TYPES = {
    "INT", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "BIT",
    "DATETIME", "DATE", "NVARCHAR", "VARCHAR"
}

def validate_config_df(df: pd.DataFrame) -> List[str]:
    """validate Config Excel DataFrame. Returns list of error messages."""
    errors: List[str] = []

    # Check required columns
    missing = [c for c in REQUIRED_CONFIG_COLS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns in Config: {missing}")

    # Row-level checks
    for idx, row in df.iterrows():
        job = row.get("JobName")
        stype = str(row.get("SourceType") or "").lower()
        ltype = str(row.get("LoadType") or "").lower()
        scd = str(row.get("SCDType") or "").lower() if row.get("SCDType") else None
        tgt_schema = row.get("TargetSchema")
        tgt_table = row.get("TargetTable")
        spath = row.get("SourcePath")

        if not job:
            errors.append(f"Row {idx}: JobName is required.")
        if stype and stype not in ALLOWED_SOURCE_TYPES:
            errors.append(f"Row {idx} ({job}): SourceType must be csv or json or table.")
        if ltype not in ALLOWED_LOAD_TYPES:
            errors.append(f"Row {idx} ({job}): LoadType must be full or incremental.")
        if scd not in ALLOWED_SCD_TYPES:
            errors.append(f"Row {idx} ({job}): SCDType must be 1, 2, or NULL.")
        if not tgt_schema or not tgt_table:
            errors.append(f"Row {idx} ({job}): TargetSchema and TargetTable are required.")
        if stype in ALLOWED_SOURCE_TYPES and not spath:
            errors.append(f"Row {idx} ({job}): SourcePath is required for file sources.")

    return errors

def validate_metadata_df(df: pd.DataFrame) -> List[str]:
    """Validate Metadata Excel DataFrame. Returns list of error messages."""
    errors: List[str] = []

    # Check required columns
    missing = [c for c in REQUIRED_METADATA_COLS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns in Metadata: {missing}")

    # Row-level checks
    for idx, row in df.iterrows():
        job = row.get("JobName")
        dtype = str(row.get("TargetDataType") or "").upper()
        length = row.get("Length")

        base_type = dtype.split("(")[0]
        if base_type not in ALLOWED_SQL_TYPES:
            errors.append(f"Row {idx} ({job}): TargetDataType '{dtype}' not allowed.")

        if pd.isna(length) or int(length) < 0:
            errors.append(f"Row {idx} ({job}): Length must be a non-negative integer.")

        if base_type in {"NVARCHAR", "VARCHAR"} and (pd.isna(length) or int(length) == 0):
            errors.append(f"Row {idx} ({job}): NVARCHAR/VARCHAR require positive Length.")

    return errors

# validate table exists
def _table_exists(engine: Engine, schema: str, table: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE LOWER(s.name) = LOWER(:schema)
                  AND LOWER(t.name) = LOWER(:table)
            """),
            {"schema": schema, "table": table}
        ).scalar()
    return result is not None


# --- Fetch config ---
def _fetch_config(engine: Engine, job_name: str) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM Config WHERE JobName = :job"),
            {"job": job_name}
        ).mappings().all()
    return rows

# --- Fetch metadata ---
def _fetch_metadata(engine: Engine, job_name: str) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM Metadata WHERE JobName = :job"),
            {"job": job_name}
        ).mappings().all()
    return rows