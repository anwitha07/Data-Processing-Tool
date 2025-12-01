from typing import Dict, Generator
from sqlalchemy.engine import Engine
from sqlalchemy import text

from scripts.create_ddl import create_target_tables
from scripts.source_raw import load_source_to_raw
from scripts.raw_curated import load_raw_to_curated
from scripts.curated_processed import load_curated_to_processed
from scripts.audit import log_job_start, log_job_end


def run_job(engine: Engine, job_name: str) -> Generator[Dict, None, None]:
    """
    Orchestrates the ETL job for a given job_name.
    Yields dict events with keys: step, message, status.
    """

    def event(step: str, message: str, status: str = "running") -> Dict:
        return {"step": step, "message": message, "status": status}

    # --- Start Audit ---
    log_job_start(engine, job_name, stage="init")
    yield event("init", f"Starting job '{job_name}'")

    # --- Look up job config ---
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT SourceType, SourceSchema, SourceTable, TargetSchema
                FROM Config
                WHERE JobName = :job
                """
            ),
            {"job": job_name},
        ).mappings().first()

    if not row:
        yield event("init", f"No config found for job {job_name}", status="error")
        log_job_end(engine, job_name, stage="init", row_count=0,
                    status="failed", message="No config found")
        return

    source_type = row["SourceType"]
    source_schema = row["SourceSchema"]
    source_table = row["SourceTable"]
    target_schema = row["TargetSchema"]

    # --- Create DDL ---
    try:
        ddl_result = create_target_tables(engine, job_name)
        if ddl_result["status"] == "error":
            yield event("ddl", f"DDL creation failed: {ddl_result['errors']}", status="error")
            log_job_end(engine, job_name, stage="ddl", row_count=0,
                        status="failed", message=ddl_result.get("errors"))
            return
        yield event("ddl", "Target tables created/verified.", status="done")
        log_job_end(engine, job_name, stage="ddl", row_count=None,
                    status="success", message="DDL completed")
    except Exception as exc:
        yield event("ddl", f"DDL creation exception: {exc}", status="error")
        log_job_end(engine, job_name, stage="ddl", row_count=0,
                    status="failed", message=str(exc))
        return

    # --- Branch by TargetSchema ---
    if target_schema == "raw":
        # Source → Raw
        try:
            result = load_source_to_raw(engine, job_name)
            if result["status"] == "error":
                yield event("source_to_raw", result["message"], status="error")
                log_job_end(engine, job_name, stage="source_to_raw", row_count=0,
                            status="failed", message=result.get("message"))
                return
            yield event("source_to_raw", result["message"], status="done")
            log_job_end(engine, job_name, stage="source_to_raw",
                        row_count=result.get("rows", 0),
                        status="success", message=result.get("message"))
        except Exception as exc:
            yield event("source_to_raw", f"Exception: {exc}", status="error")
            log_job_end(engine, job_name, stage="source_to_raw", row_count=0,
                        status="failed", message=str(exc))
            return

    elif target_schema == "curated":
        # Raw → Curated
        try:
            if not source_schema or not source_table:
                raise ValueError(f"Missing SourceSchema/SourceTable for job {job_name}")
            result = load_raw_to_curated(engine, job_name)
            if result["status"] == "error":
                yield event("raw_to_curated", result["message"], status="error")
                log_job_end(engine, job_name, stage="raw_to_curated", row_count=0,
                            status="failed", message=result.get("message"))
                return
            yield event("raw_to_curated", result["message"], status="done")
            log_job_end(engine, job_name, stage="raw_to_curated",
                        row_count=result.get("rows", 0),
                        status="success", message=result.get("message"))
        except Exception as exc:
            yield event("raw_to_curated", f"Exception: {exc}", status="error")
            log_job_end(engine, job_name, stage="raw_to_curated", row_count=0,
                        status="failed", message=str(exc))
            return

    elif target_schema == "processed":
        # Curated → Processed
        try:
            if not source_schema or not source_table:
                raise ValueError(f"Missing SourceSchema/SourceTable for job {job_name}")
            result = load_curated_to_processed(engine, job_name)
            if result["status"] == "error":
                yield event("curated_to_processed", result["message"], status="error")
                log_job_end(engine, job_name, stage="curated_to_processed", row_count=0,
                            status="failed", message=result.get("message"))
                return
            yield event("curated_to_processed", result["message"], status="done")
            log_job_end(engine, job_name, stage="curated_to_processed",
                        row_count=result.get("rows", 0),
                        status="success", message=result.get("message"))
        except Exception as exc:
            yield event("curated_to_processed", f"Exception: {exc}", status="error")
            log_job_end(engine, job_name, stage="curated_to_processed", row_count=0,
                        status="failed", message=str(exc))
            return

    else:
        yield event("init", f"Unknown target schema '{target_schema}' for job {job_name}", status="error")
        log_job_end(engine, job_name, stage="init", row_count=0,
                    status="failed", message="Unknown target schema")
        return

    # --- End Audit ---
    log_job_end(engine, job_name, stage="finalize", row_count=None,
                status="success", message="Job completed successfully")
    yield event("finalize", f"Job '{job_name}' completed successfully.", status="done")
