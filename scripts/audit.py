
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime
from typing import Optional


def log_job_start(engine: Engine, job_name: str, stage: str) -> None:
    """Insert a new audit row when a stage starts."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO JobAudit (JobName, Stage, StartTime, Status)
            VALUES (:job, :stage, :start, 'Running')
        """), {"job": job_name, "stage": stage, "start": datetime.now()})


def log_job_end(engine: Engine, job_name: str, stage: str,
                row_count: Optional[int], status: str, message: Optional[str] = None) -> None:
    """Update the audit row when a stage ends."""
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE JobAudit
            SET EndTime = :end, [RowCount] = :rows, [Status] = :status, [Message] = :msg
            WHERE JobName = :job AND Stage = :stage AND EndTime IS NULL
        """), {
            "end": datetime.now(),
            "rows": row_count,
            "status": status,
            "msg": message,
            "job": job_name,
            "stage": stage
        })
