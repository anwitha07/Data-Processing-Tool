"""
curated_processed.py
--------------------
ETL step: Curated → Processed
- Reads from curated tables
- Renames columns to target schema
- Loads into processed tables
- Supports Full / Incremental load
- Supports SCD1 / SCD2
"""
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict, Any
from datetime import datetime

from scripts.audit import log_job_start, log_job_end
from scripts.validate_input import _fetch_config, _fetch_metadata
from scripts.load_type import full_load,_update_incremental_tracker,incremental_load,_get_incremental_tracker
from scripts.scd_type import scd1_merge, scd2_merge
from scripts.create_ddl import create_target_tables  # metadata-driven DDL
from scripts.send_log  import send_log

# --- Case-insensitive schema/table existence check ---
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

def load_curated_to_processed(engine: Engine, job_name: str) -> Dict[str, Any]:
    """Load data from Curated layer into Processed layer for the given job."""
    log_job_start(engine, job_name, stage="curated_to_processed")

    try:
        # --- Fetch config + metadata ---
        config_rows = _fetch_config(engine, job_name)
        if not config_rows:
            return {"status": "error", "message": f"No Config for {job_name}"}
        config = config_rows[0]

        metadata = _fetch_metadata(engine, job_name)
        if not metadata:
            return {"status": "error", "message": f"No Metadata for {job_name}"}

        source_schema, source_table = config["SourceSchema"], config["SourceTable"]
        target_schema = str(config["TargetSchema"]).strip()
        target_table  = str(config["TargetTable"]).strip()
        load_type = str(config["LoadType"]).lower()
        scd_type = int(config.get("SCDType", 1))  # 1 = SCD1, 2 = SCD2

        print(f"Target resolved to: {target_schema}.{target_table}")

        # --- Ensure target table exists ---
        if not _table_exists(engine, target_schema, target_table):
            print(f"Target table {target_schema}.{target_table} not found. Creating from metadata...")
            create_target_tables(engine, job_name)
            print(f"Created table {target_schema}.{target_table} from metadata")

        # --- Read from curated table ---
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM [{source_schema}].[{source_table}]"), conn)
        print(f"Read {len(df)} rows from {source_schema}.{source_table}")

        # --- Rename + align ---
        rename_map = {m["SourceColumnName"]: m["TargetColumnName"] for m in metadata}
        df = df.rename(columns=rename_map)
        target_cols = [m["TargetColumnName"] for m in metadata]
        df = df.reindex(columns=target_cols)

        if df.empty:
            msg = "No rows to load into processed table."
            log_job_end(engine, job_name, stage="curated_to_processed",
                        row_count=0, status="success", message=msg)
            return {"status": "success", "rows": 0, "message": msg}

        # --- Load based on type ---
        if load_type == "full":
            result = full_load(engine, df, target_schema, target_table)
        elif load_type == "incremental":
            key_cols = [m["TargetColumnName"] for m in metadata if m.get("IsPK") == 1]
            if scd_type == 1:
                scd1_merge(engine, target_schema, target_table, df, key_cols, target_cols)
                result = {"status": "success", "rows": len(df), "message": "Incremental SCD1 merge completed"}
            elif scd_type == 2:
                scd2_merge(engine, target_schema, target_table, df, key_cols, target_cols)
                result = {"status": "success", "rows": len(df), "message": "Incremental SCD2 merge completed"}
            else:
                result = incremental_load(
                    engine, job_name, target_schema, target_table,
                    metadata, df, stage="curated_processed", key_columns=key_cols
                )

            # --- Update tracker with run timestamp ---
            _update_incremental_tracker(engine, job_name, datetime.now(), stage="curated_processed")
        else:
            return {"status": "error", "message": f"Unsupported LoadType: {load_type}"}

        # --- SUCCESS ---
        log_job_end(engine, job_name, stage="curated_to_processed",
                    row_count=result.get("rows", 0),
                    status=result.get("status"), message=result.get("message"))
        send_log("curated_to_processed", "Curated to Processed completed successfully", status="success")
        return result
    
    except Exception as exc:
        log_job_end(engine, job_name, stage="curated_to_processed",
                    row_count=0, status="failed", message=str(exc))
        send_log("curated_to_processed", str(exc), status="failed", exception=exc)
        return {"status":"error","rows":0,"message":str(exc)}