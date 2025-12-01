import argparse
import pandas as pd
from scripts.db_connection import get_engine
from scripts.validate_input import validate_config_df, validate_metadata_df
from scripts.load_config_metadata import load_config_df, load_metadata_df
from scripts.orchestration import run_job


def main():
    parser = argparse.ArgumentParser(description="ETL Job Runner")
    parser.add_argument("--job", required=True, help="Job name to run")
    parser.add_argument("--config", help="Path to Config Excel (required on first run)")
    parser.add_argument("--metadata", help="Path to Metadata Excel (required on first run)")
    args = parser.parse_args()

    job_name = args.job
    config_path = args.config
    metadata_path = args.metadata

    print(f"[INFO] Starting ETL job: {job_name}")
    engine = get_engine()

    try:
        # --- Load Config if provided ---
        if config_path:
            print(f"[INFO] Reading Config Excel: {config_path}")
            config_df = pd.read_excel(config_path)
            errors = validate_config_df(config_df)
            if errors:
                print("[ERROR] Config validation failed:")
                for err in errors:
                    print(f"  - {err}")
                return
            result = load_config_df(engine, config_df, job_name)
            if result["status"] == "success":
                print(f"[SUCCESS] Config loaded: {result['inserted']} rows (replaced {result['deleted']})")
            else:
                print(f"[ERROR] Config load failed: {result['message']}")
                return

        # --- Load Metadata if provided ---
        if metadata_path:
            print(f"[INFO] Reading Metadata Excel: {metadata_path}")
            metadata_df = pd.read_excel(metadata_path)
            errors = validate_metadata_df(metadata_df)
            if errors:
                print("[ERROR] Metadata validation failed:")
                for err in errors:
                    print(f"  - {err}")
                return
            result = load_metadata_df(engine, metadata_df, job_name)
            if result["status"] == "success":
                print(f"[SUCCESS] Metadata loaded: {result['inserted']} rows")
            else:
                print(f"[ERROR] Metadata load failed: {result['message']}")
                return

        # --- Run orchestration ---
        print("[INFO] Running orchestration...")
        for event in run_job(engine, job_name):
            if event["status"] == "running":
                print(f"[RUNNING] {event['step']}: {event['message']}")
            elif event["status"] == "done":
                print(f"[DONE] {event['step']}: {event['message']}")
            elif event["status"] == "error":
                print(f"[FAILED] {event['step']}: {event['message']}")
                return

        print(f"[SUCCESS] Job {job_name} completed successfully.")

    except Exception as exc:
        print(f"[FATAL] Unexpected error: {exc}")


if __name__ == "__main__":
    main()
