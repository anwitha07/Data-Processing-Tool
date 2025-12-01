import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import Dict, Any

CONFIG_INSERT_SQL = """
INSERT INTO Config (
    JobName, SourceType, SourcePath, SourceSchema, SourceTable,
    TargetSchema, TargetTable, LoadType, SCDType
)
VALUES (
    :JobName, :SourceType, :SourcePath, :SourceSchema, :SourceTable,
    :TargetSchema, :TargetTable, :LoadType, :SCDType
)
"""

METADATA_INSERT_SQL = """
INSERT INTO Metadata (
    JobName, SourceColumnName, TargetColumnName, TargetDataType,
    [Length], IsPK, IsFK, IsNullable, ReferenceTable
)
VALUES (
    :JobName, :SourceColumnName, :TargetColumnName, :TargetDataType,
    :Length, :IsPK, :IsFK, :IsNullable, :ReferenceTable
)
"""

def load_config_df(engine: Engine, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Append Config DataFrame into SQL Server.
    No filtering or deletion — just inserts rows as-is.
    """
    try:
        df = df.copy()
        df = df.where(pd.notnull(df), None)

        inserted = 0
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(text(CONFIG_INSERT_SQL), {
                    "JobName": row.get("JobName"),
                    "SourceType": row.get("SourceType"),
                    "SourcePath": row.get("SourcePath"),
                    "SourceSchema": row.get("SourceSchema"),
                    "SourceTable": row.get("SourceTable"),
                    "TargetSchema": row.get("TargetSchema"),
                    "TargetTable": row.get("TargetTable"),
                    "LoadType": row.get("LoadType"),
                    "SCDType": row.get("SCDType")
                })
                inserted += 1

        return {"status": "success", "inserted": inserted}

    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def load_metadata_df(engine: Engine, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Append Metadata DataFrame into SQL Server.
    No filtering or deletion — just inserts rows as-is.
    """
    try:
        df = df.copy()
        df = df.where(pd.notnull(df), None)

        inserted = 0
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(text(METADATA_INSERT_SQL), {
                    "JobName": row.get("JobName"),
                    "SourceColumnName": row.get("SourceColumnName"),
                    "TargetColumnName": row.get("TargetColumnName"),
                    "TargetDataType": row.get("TargetDataType"),
                    "Length": row.get("Length"),
                    "IsPK": row.get("IsPK"),
                    "IsFK": row.get("IsFK"),
                    "IsNullable": row.get("IsNullable"),
                    "ReferenceTable": row.get("ReferenceTable"),
                })
                inserted += 1

        return {"status": "success", "inserted": inserted}

    except Exception as exc:
        return {"status": "error", "message": str(exc)}
