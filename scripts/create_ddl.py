from typing import Dict, List, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime
from collections import defaultdict
from scripts.validate_input import _table_exists
from scripts.audit import log_job_start, log_job_end
from scripts.send_log import send_log

# --- Allowed SQL types ---
VARLEN_TYPES = {"NVARCHAR", "VARCHAR"}

ALLOWED_TYPES = VARLEN_TYPES | {
    "INT", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "BIT",
    "DATETIME", "DATE"
}

# --- Build column definition ---
def _build_column_def(meta_row: Dict[str, Any],schema : str,scd_type:int) -> str:
    col = str(meta_row["TargetColumnName"])
    dtype = str(meta_row["TargetDataType"]).upper()
    length = int(meta_row.get("Length") or 0)

    # If raw schema, just land as NVARCHAR(MAX)
    if schema.lower() == "raw":
        return f"[{col}] NVARCHAR(MAX) NULL"

    if dtype not in ALLOWED_TYPES:
        raise ValueError(f"Unsupported data type: {dtype}")

    # Base type
    if dtype in VARLEN_TYPES:
        if length <= 0:
            raise ValueError(f"{dtype} requires positive Length for column '{col}'")
        base = f"[{col}] {dtype}({length})"
    else:
        base = f"[{col}] {dtype}"

    if dtype in {"DECIMAL", "NUMERIC"}:
        precision = int(meta_row.get("Precision") or 18)
        scale = int(meta_row.get("Scale") or 0)
        base = f"[{col}] {dtype}({precision},{scale})"


    # PK handling
    if schema.lower() == "curated":
        if int(meta_row.get("IsPK") or 0) == 1:
            return f"{base} PRIMARY KEY NOT NULL"

    if schema.lower() == "processed":
        # Never inline PKs from metadata
        nullable = "NULL" if int(meta_row.get("IsNullable") or 1) == 1 else "NOT NULL"
        return f"{base} {nullable}"

    # Default: respect PK flag
    if int(meta_row.get("IsPK") or 0) == 1:
        return f"{base} PRIMARY KEY NOT NULL"

    # Otherwise respect IsNullable
    nullable = "NULL" if int(meta_row.get("IsNullable") or 1) == 1 else "NOT NULL"
    return f"{base} {nullable}"

# --- Foreign key clauses ---
def _foreign_key_clauses(engine: Engine, metadata_rows: List[Dict[str, Any]]) -> List[str]:
    """
    Build foreign key clauses from metadata.
    - Deduplicates identical FKs
    - Ensures proper [schema].[table]([col]) syntax
    - Skips FK if referenced table does not exist
    """
    fk_clauses = []
    seen_fks = set()

    for r in metadata_rows:
        if int(r.get("IsFK") or 0) == 1 and r.get("ReferenceTable"):
            col = r["TargetColumnName"]
            ref = r["ReferenceTable"]

            # Parse schema.table if provided, else default to dbo
            if "." in ref:
                ref_schema, ref_table = ref.split(".", 1)
            else:
                ref_schema, ref_table = "dbo", ref

            # Deduplicate by (column, schema, table)
            key = (col, ref_schema, ref_table)
            if key in seen_fks:
                continue
            seen_fks.add(key)

            # Ensure referenced table exists before adding FK
            if not _table_exists(engine, ref_schema, ref_table):
                # Optionally log a warning instead of failing
                continue

            # Build FK clause with explicit column reference
            fk_clauses.append(
                f"FOREIGN KEY ([{col}]) REFERENCES [{ref_schema}].[{ref_table}]([{col}])"
            )

    return fk_clauses



#table_name 
def _derive_table_name(job_name: str, target_schema: str) -> str:
    # JOB_EMP_Raw -> emp
    parts = job_name.split("_")
    if len(parts) >= 2:
        middle = parts[1].lower()
    else:
        middle = job_name.lower()
    return f"{target_schema}_{middle}"



# condition for the pk=1 for two
def _validate_primary_keys(columns: List[Dict[str, Any]], schema: str, table: str, job_name: str) -> None:
    """Ensure only one PK column is defined for this job/table."""
    if schema.lower() == "raw":
        return
    
    pk_cols = {c["TargetColumnName"] for c in columns if int(c.get("IsPK") or 0) == 1}
    if len(pk_cols) > 1:
        raise ValueError(
            f"Job '{job_name}' for table {schema}.{table} has multiple PK columns: {pk_cols}. "
            "Only one PRIMARY KEY is allowed."
        )

# --- Main function ---
def create_target_tables(engine: Engine, job_name: str) -> Dict[str, Any]:
    """
    Creates target tables for the given job by joining Config + Metadata.
    Config supplies schema/table, Metadata supplies column definitions.
    Deduplicates columns and constraints.
    """
    try:        
        log_job_start(engine, job_name, stage="ddl")
        
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT c.TargetSchema, c.TargetTable,c.LoadType,c.SCDType,
                       m.TargetColumnName, m.TargetDataType, m.Length,
                       m.IsPK, m.IsFK, m.IsNullable, m.ReferenceTable
                FROM Metadata m
                JOIN Config c ON m.JobName = c.JobName
                WHERE m.JobName = :job
            """), {"job": job_name}).mappings().all()

        if not rows:
            msg = f"No metadata/config found for job '{job_name}'"
            log_job_end(engine, job_name, stage="ddl",
                        row_count=0, status="failed", message=msg)
            return {"status": "error", "errors": [msg]}
        
        # Group by schema+table
        tables = defaultdict(list)
        for row in rows:
            tables[(row["TargetSchema"], row["TargetTable"])].append(row)

        ddl_errors = []

        for (schema, table), columns in tables.items():
            col_defs, seen_cols = [], set()
            load_type=rows[0]["LoadType"]
            scd_type=int(rows[0].get("SCDType",1))

            # derive table name from job_name and schema
            if schema.lower() in ("raw", "curated", "processed"):
                table_name = f"{schema.lower()}_{job_name.split('_')[1].lower()}"
            else:
                table_name = table

            if _table_exists(engine, schema, table_name):
                continue
            
            #  validate PKs
            try:
                _validate_primary_keys(columns, schema, table, job_name)
            except ValueError as ve:
                ddl_errors.append(str(ve))
                continue


            for col in columns:
                key = (col["TargetColumnName"], col["TargetDataType"], col["Length"])
                if key in seen_cols:
                    ddl_errors.append(f"Duplicate column '{col['TargetColumnName']}' in {schema}.{table}")
                    continue
                seen_cols.add(key)

                try:
                    col_defs.append(_build_column_def(col,schema,scd_type))
                except Exception as e:
                    ddl_errors.append(str(e))

            # If SCD2, add history columns
            if schema.lower() == "processed" and scd_type == 2:
                col_defs.insert(0, "[RowID] INT IDENTITY(1,1) PRIMARY KEY")
                col_defs.append("[StartTime] DATETIME NOT NULL DEFAULT (GETDATE())")
                col_defs.append("[EndTime] DATETIME NULL")
                col_defs.append("[IsCurrent] BIT NOT NULL DEFAULT (1)")

            
            fk_clauses = _foreign_key_clauses(engine,columns)

            ddl_parts = [f"CREATE TABLE [{schema}].[{table_name}] ("]
            ddl_parts += [f"    {c}," for c in col_defs]

            # Add FK constraints
            ddl_parts += [f"    {fk}," for fk in fk_clauses]

            # Remove trailing comma
            if ddl_parts[-1].endswith(","):
                ddl_parts[-1] = ddl_parts[-1][:-1]

            ddl_parts.append(");")
            ddl_sql = "\n".join(ddl_parts)

            # Execute
            with engine.begin() as conn:
                try:
                    conn.execute(text(ddl_sql))
                except Exception as ddl_exc:
                    ddl_errors.append(str(ddl_exc))

        if ddl_errors:
            log_job_end(engine, job_name, stage="ddl",
                        row_count=0, status="failed", message="; ".join(ddl_errors))
            return {"status": "error", "errors": ddl_errors}

        # --- SUCCESSFUL END ---
        log_job_end(engine, job_name, stage="ddl",
                    row_count=None, status="success", message="DDL completed")
        send_log("ddl", "DDL creation successful", status="success")
        
        return {"status": "success"}

    except Exception as exc:
        log_job_end(engine, job_name, stage="ddl",
                    row_count=0, status="failed", message=str(exc))
        send_log("ddl", str(exc), status="failed", exception=exc)
        return {"status": "error", "errors": [str(exc)]}
