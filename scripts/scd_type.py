# """
# scd_type.py
# -----------
# Slowly Changing Dimension (SCD) logic for Curated → Processed loads.
# Supports:
# - SCD1: Overwrite changed attributes
# - SCD2: Maintain history with effective dates and current flag
# """

from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
from datetime import datetime


# ------------------------------MY logic--------------------------------------------

from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
from datetime import datetime


def scd1_merge(engine: Engine, schema: str, table: str,
               df: pd.DataFrame, key_columns: list, target_cols: list) -> None:
    """
    SCD1: Overwrite existing rows with new values, insert new rows if not found.
    No history columns are used.
    """
    on_clause = " AND ".join([f"t.[{col}] = s.[{col}]" for col in key_columns])
    update_clause = ", ".join([f"t.[{col}] = s.[{col}]" for col in target_cols if col not in key_columns])
    insert_cols = ", ".join([f"[{col}]" for col in target_cols])
    insert_vals = ", ".join([f"s.[{col}]" for col in target_cols])

    # Build VALUES clause from DataFrame
    values_clause = ",".join(
        "(" + ",".join([f"'{row[col]}'" if pd.notna(row[col]) else "NULL" for col in target_cols]) + ")"
        for _, row in df.iterrows()
    )

    df = df.drop_duplicates(subset=key_columns, keep="last")
    
    merge_sql = f"""
    MERGE [{schema}].[{table}] AS t
    USING (VALUES {values_clause}) AS s({",".join(target_cols)})
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))


# SCD 2 logic 
def scd2_merge(engine: Engine, schema: str, table: str,
               df: pd.DataFrame, key_columns: list, target_cols: list,
               start_col: str = "StartTime",
               end_col: str = "EndTime",
               current_col: str = "IsCurrent") -> None:
    """
    Implements Slowly Changing Dimension Type 2 (SCD2) with rolling Start/End dates.

    - New rows: inserted with StartTime=now, EndTime=NULL, IsCurrent=1
    - Changed rows: old record closed (IsCurrent=0, EndTime=now),
                    new record inserted with StartTime=now, EndTime=NULL, IsCurrent=1
    - Unchanged rows: ignored
    """

    # Build VALUES clause from DataFrame
    values_clause = ",".join(
        "(" + ",".join([f"'{row[col]}'" if pd.notna(row[col]) else "NULL"
                        for col in target_cols]) + ")"
        for _, row in df.iterrows()
    )

    source_cols = ",".join(target_cols)

    # Build change detection clause for non-key columns
    change_checks = " OR ".join([
        f"(t.[{col}] <> s.[{col}] "
        f"OR (t.[{col}] IS NULL AND s.[{col}] IS NOT NULL) "
        f"OR (t.[{col}] IS NOT NULL AND s.[{col}] IS NULL))"
        for col in target_cols if col not in key_columns
    ])

    # Step 1: Close out old records (set EndTime, IsCurrent=0)
    update_sql = f"""
    UPDATE t
    SET t.{current_col} = 0,
        t.{end_col} = GETDATE()
    FROM [{schema}].[{table}] t
    JOIN (VALUES {values_clause}) AS s({source_cols})
      ON {" AND ".join([f"t.[{col}] = s.[{col}]" for col in key_columns])}
    WHERE t.{current_col} = 1
      AND ({change_checks});
    """

    # Step 2: Insert new versions (new keys or changed rows)
    insert_sql = f"""
    INSERT INTO [{schema}].[{table}]
    ({",".join([f"[{c}]" for c in target_cols])}, [{start_col}], [{end_col}], [{current_col}])
    SELECT {",".join([f"s.[{c}]" for c in target_cols])}, GETDATE(), NULL, 1
    FROM (VALUES {values_clause}) AS s({source_cols})
    LEFT JOIN [{schema}].[{table}] t
      ON {" AND ".join([f"t.[{col}] = s.[{col}]" for col in key_columns])}
     AND t.{current_col} = 1
    WHERE t.[{key_columns[0]}] IS NULL  -- brand new key
       OR ({change_checks});
    """

    # Execute both steps in a transaction
    with engine.begin() as conn:
        conn.execute(text(update_sql))
        conn.execute(text(insert_sql))
