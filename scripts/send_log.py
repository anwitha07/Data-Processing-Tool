import uuid
import datetime
import requests

ENDPOINT = "http://10.112.141.172:8001/logs"

# Map ETL stages to severity levels for failures
SEVERITY_MAP = {
    "source_raw": "ERROR",
    "raw_curated": "CRITICAL",
    "curated_to_processed": "SERVER",
    "init": "ERROR",
    "ddl": "ERROR"
}

def send_log(stage: str,
             message: str,
             status: str = "success",   # "success" or "failed"
             exception: Exception = None,
             user_id: str = "system"):
    """
    Send a structured log entry to the endpoint for both success and failure.
    - On success: level=INFO, severity=INFO
    - On failure: level=ERROR, severity depends on stage
    """

    if status == "success":
        level = "INFO"
        severity = "INFO"
    else:
        level = "ERROR"
        severity = SEVERITY_MAP.get(stage, "ERROR")

    log_entry = [{
        # "log_id": str(uuid.uuid4()),
        "application": "ETL",
        "app_id": "etl-aff72bb8",
        "level": level,
        "severity": severity,
        "message": message,
        "timestamp": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "path": stage,
        "exception_type": type(exception).__name__ if exception else None,
        "status_code": 200 if status == "success" else 500,
        "user_id":"6900934556bc8e47e7a464fd",
        "tags": [stage, "etl", status]
    }]

    try:
        resp = requests.post(ENDPOINT, json=log_entry, timeout=5)
        resp.raise_for_status()
        print(f"✅ Sent {status} log for {stage}: {log_entry}")
    except Exception as e:
        print(f"⚠️ Could not send log for {stage}: {e}")
        print("Log entry:", log_entry)
