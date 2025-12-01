from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from typing import Dict, Any, List
from datetime import datetime
import pandas as pd

# --- Incremental Tracker ---

def _get_incremental_tracker(engine: Engine, job_name: str, stage: str):
    """Get the last load time for a job+stage. Returns None if no record exists."""
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT LastLoadTime 
                FROM dbo.IncrementalTracker 
                WHERE JobName = :job AND Stage = :stage
            """),
            {"job": job_name, "stage": stage}
        ).first()
    return row[0] if row else None

# ----Update Incremental Tracker---
def _update_incremental_tracker(engine: Engine, job_name: str, new_value, stage: str) -> None:
    """Update the IncrementalTracker table with the latest load time for a job+stage."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                MERGE dbo.IncrementalTracker AS t
                USING (SELECT :job AS JobName, :stage AS Stage, :val AS LastLoadTime) AS s
                ON t.JobName = s.JobName AND t.Stage = s.Stage
                WHEN MATCHED THEN 
                    UPDATE SET LastLoadTime = s.LastLoadTime
                WHEN NOT MATCHED THEN 
                    INSERT (JobName, Stage, LastLoadTime) 
                    VALUES (s.JobName, s.Stage, s.LastLoadTime);
            """),
            {"job": job_name, "stage": stage, "val": new_value}
        )

# --- Load Functions for Raw→Curated and Curated→Processed ---

def full_load(engine: Engine, df: pd.DataFrame, schema: str, table: str) -> Dict[str, Any]:
    """
    Perform a full load: truncate target table, then insert all rows.
    Tracker is NOT updated here (only used for incremental).
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE [{schema}].[{table}]"))

        
        if not df.empty:
            df.to_sql(table, engine, schema=schema, if_exists="append", index=False,
               chunksize=1000)
        return {
            "status": "success",
            "rows": len(df),
            "message": f"Full load completed: {len(df)} rows"
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

# --- Incremental Traker -----
def incremental_load(engine: Engine, job_name: str, schema: str, table: str,
                     metadata: List[Dict[str, Any]], df: pd.DataFrame,
                     stage: str, key_columns: List[str]) -> Dict[str, Any]:
    """
    Perform an incremental load with MERGE (upsert) based only on PK columns.
    - Creates a staging table
    - MERGEs into target on key_columns
    - Updates tracker with current timestamp
    """
    try:
        # --- Ensure only target columns are included ---
        target_cols = [m["TargetColumnName"] for m in metadata if m["TargetColumnName"] in df.columns]
        df = df[target_cols]

        if df.empty:
            return {"status": "success", "rows": 0, "message": "No rows to process."}

        # --- Create staging table ---
        staging_table = f"{table}_staging"
        with engine.begin() as conn:
            conn.execute(text(f"IF OBJECT_ID('{schema}.{staging_table}', 'U') IS NOT NULL DROP TABLE [{schema}].[{staging_table}]"))
            col_defs = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in target_cols])
            conn.execute(text(f"CREATE TABLE [{schema}].[{staging_table}] ({col_defs});"))

        # --- Bulk insert into staging ---
        MAX_PARAMS = 32000
        cols_per_row = len(df.columns)
        safe_chunksize = MAX_PARAMS // cols_per_row
        
        print(f"Staging {len(df)} rows into {schema}.{staging_table}")
        if not df.empty:
            df.to_sql(staging_table, engine, schema=schema, if_exists="append", index=False,
              chunksize=safe_chunksize)

        # --- Build MERGE ---
        on_clause = " AND ".join([f"t.[{col}] = s.[{col}]" for col in key_columns])
        update_clause = ", ".join([f"t.[{col}] = s.[{col}]" for col in target_cols if col not in key_columns])
        insert_cols = ", ".join([f"[{col}]" for col in target_cols])
        insert_vals = ", ".join([f"s.[{col}]" for col in target_cols])

        merge_sql = f"""
        MERGE [{schema}].[{table}] AS t
        USING [{schema}].[{staging_table}] AS s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        """

        with engine.begin() as conn:
            conn.execute(text(merge_sql))

        # --- Update tracker with current timestamp ---
        _update_incremental_tracker(engine, job_name, datetime.now(), stage)

        return {
            "status": "success",
            "rows": len(df),
            "message": f"Incremental MERGE completed: {len(df)} rows processed"
        }

    except Exception as exc:
        return {"status": "error", "message": str(exc)}
