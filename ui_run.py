import io
import sys
import os
import re
import pandas as pd
import streamlit as st
from sqlalchemy import text
from scripts.db_connection import get_engine
from scripts.validate_input import validate_config_df, validate_metadata_df
from scripts.load_config_metadata import load_config_df, load_metadata_df
from scripts.orchestration import run_job
from scripts.scd_type import scd1_merge, scd2_merge


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- Page setup ---
st.set_page_config(page_title="Data Processing Tool", layout="wide")
st.title("🚀Data Processing Tool")

# --- Instructions ---
with st.expander("📖 How to use this app", expanded=True):
    st.markdown("""
    1. Enter a **Job Name** in the box below.  
    2. On the **first run**, upload both **Config_YYYY_MM_DD.xlsx** and **Metadata_YYYY_MM_DD.xlsx**.  
    3. On later runs, just enter the Job Name and click **Run Job**.  
    4. You can also upload new Config/Metadata at any time using the optional uploader section.  
    5. After completion, view the **Raw, Curated, and Processed tables** in the Lineage view.  
    6. Use the **History tab** to see past runs from the JobAudit table.  
    """)

# --- Directories ---
CONFIG_DIR = r"C:\Users\ANayak4\OneDrive - Rockwell Automation, Inc\Desktop\ETL Job Runner\input_files\config"
METADATA_DIR = r"C:\Users\ANayak4\OneDrive - Rockwell Automation, Inc\Desktop\ETL Job Runner\input_files\metadata"
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)

engine = get_engine()

# --- Helpers ---
def job_exists(engine, job: str) -> bool:
    if not job:
        return False
    with engine.connect() as conn:
        res = conn.execute(text("SELECT 1 FROM Config WHERE JobName = :job"), {"job": job})
        return res.first() is not None

def valid_filename(filename: str, prefix: str) -> bool:
    """Check if filename matches required pattern e.g. Config_YYYY_MM_DD.xlsx"""
    pattern = rf"^{prefix}_[0-9]{{4}}_[0-9]{{2}}_[0-9]{{1,2}}\.xlsx$"
    return re.match(pattern, filename, re.IGNORECASE) is not None

# --- Job Name ---
job_name = st.text_input("🔑 Enter Job Name", placeholder="Eg. JOB_EMP_RAW")

# --- Upload Section ---
config_file = None
metadata_file = None
with st.expander("📂 Upload Config/Metadata files (required for first run, optional otherwise)"):
    config_file = st.file_uploader(
        "Upload Config Excel (Config_YYYY_MM_DD.xlsx)",
        type=["xlsx"],
        key="config_upload"
    )
    metadata_file = st.file_uploader(
        "Upload Metadata Excel (Metadata_YYYY_MM_DD.xlsx)",
        type=["xlsx"],
        key="metadata_upload"
    )

# --- Handle Config upload ---
if config_file is not None:
    config_path = os.path.join(CONFIG_DIR, config_file.name)
    if os.path.exists(config_path):
        st.error(f"❌ Config file '{config_file.name}' already exists. Upload blocked.")
         # prevent saving and stop further execution
    else:
        with open(config_path, "wb") as f:
            f.write(config_file.getbuffer())
        st.success(f"✅ Config file '{config_file.name}' saved.")

# --- Handle Metadata upload ---
if metadata_file is not None:
    metadata_path = os.path.join(METADATA_DIR, metadata_file.name)
    if os.path.exists(metadata_path):
        st.error(f"❌ Metadata file '{metadata_file.name}' already exists. Upload blocked.")
        
    else:
        with open(metadata_path, "wb") as f:
            f.write(metadata_file.getbuffer())
        st.success(f"✅ Metadata file '{metadata_file.name}' saved.")


# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["▶️ Run Job", "📜 History", "📊 Lineage View"])

with tab1:
    if st.button("Run Job"):
        if not job_name:
            st.error("❌ Job name is required.")
            st.stop()

        job_in_db = job_exists(engine, job_name)
        first_run = not job_in_db

        if first_run and (config_file is None or metadata_file is None):
            st.error("❌ First run requires both Config and Metadata Excel files.")
            st.stop()

        # --- Save + Load Config ---
        if config_file:
            if not valid_filename(config_file.name, "Config"):
                st.error("❌ Config file must be named like Config_YYYY_MM_DD.xlsx")
                st.stop()
            config_path = os.path.join(CONFIG_DIR, config_file.name)
            with open(config_path, "wb") as f:
                f.write(config_file.getbuffer())
            st.info(f"📂 Config file saved to {config_path}")

            df = pd.read_excel(io.BytesIO(config_file.getvalue()))
            errors = validate_config_df(df)
            if errors:
                st.error("⚠️ Config validation failed:")
                for err in errors:
                    st.write(f"- {err}")
                st.stop()
            result = load_config_df(engine, df)
            if result["status"] == "success":
                inserted = result.get("inserted", 0)
                st.success(f"✅ Config loaded: {inserted} rows")
            else:
                st.error(f"❌ Config load failed: {result['message']}")
                st.stop()

        # --- Save + Load Metadata ---
        if metadata_file:
            if not valid_filename(metadata_file.name, "Metadata"):
                st.error("❌ Metadata file must be named like Metadata_YYYY_MM_DD.xlsx")
                st.stop()
            metadata_path = os.path.join(METADATA_DIR, metadata_file.name)
            with open(metadata_path, "wb") as f:
                f.write(metadata_file.getbuffer())
            st.info(f"📂 Metadata file saved to {metadata_path}")

            df = pd.read_excel(io.BytesIO(metadata_file.getvalue()))
            errors = validate_metadata_df(df)
            if errors:
                st.error("⚠️ Metadata validation failed:")
                for err in errors:
                    st.write(f"- {err}")
                st.stop()
            result = load_metadata_df(engine, df)
            if result["status"] == "success":
                inserted = result.get("inserted", 0)
                st.success(f"✅ Metadata loaded: {inserted} rows")
            else:
                st.error(f"❌ Metadata load failed: {result['message']}")
                st.stop()

        # --- Run orchestration ---
        st.info(f"🚦 Starting ETL job: {job_name}")
        log_box = st.empty()
        for event in run_job(engine, job_name):
            if event["status"] == "running":
                log_box.write(f"➡️ {event['step']}: {event['message']}")
            elif event["status"] == "done":
                st.success(f"✅ {event['step']}: {event['message']}")
            elif event["status"] == "error":
                st.error(f"❌ {event['step']}: {event['message']}")
                st.stop()
        st.success("🎉 Job finished. Now check the Lineage View tab.")

with tab2:
    st.subheader("📜 Job History (Audit Log)")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT TOP 50 * FROM JobAudit ORDER BY StartTime DESC"), conn)
            st.dataframe(df, use_container_width=True)
    except Exception as exc:
        st.warning(f"Could not fetch JobAudit: {exc}")


with tab3:
    st.subheader("📊 Lineage View")
    if not job_name:
        st.info("Enter a Job Name above and run it to see lineage.")
    else:
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT TargetSchema FROM Config WHERE JobName = :job"),
                    {"job": job_name}
                ).mappings().first()

            if not row:
                st.warning(f"No Config found for job '{job_name}'")
            else:
                target_schema = row["TargetSchema"]

                # Decide lineage chain
                if target_schema == "raw":
                    schemas_to_show = ["raw"]
                elif target_schema == "curated":
                    schemas_to_show = ["raw", "curated"]
                elif target_schema == "processed":
                    schemas_to_show = ["raw", "curated", "processed"]
                else:
                    schemas_to_show = [target_schema]

                tabs = st.tabs([s.capitalize() for s in schemas_to_show])

                # Derive job family prefix: e.g. JOB_EMP, JOB_MANAGER
                family_prefix = "_".join(job_name.split("_")[:2])

                with engine.connect() as conn:
                    for idx, schema in enumerate(schemas_to_show):
                        with tabs[idx]:
                            config_row = conn.execute(
                                text("SELECT TargetTable FROM Config WHERE JobName LIKE :prefix AND TargetSchema = :schema"),
                                {"prefix": f"{family_prefix}%", "schema": schema}
                            ).mappings().first()

                            if config_row:
                                table = config_row["TargetTable"]
                                st.markdown(f"**{schema}.{table}**")
                                try:
                                    df = pd.read_sql(text(f"SELECT TOP 50 * FROM [{schema}].[{table}]"), conn)
                                    st.dataframe(df, use_container_width=True)
                                except Exception as exc:
                                    st.warning(f"No table found: {schema}.{table} ({exc})")
                            else:
                                st.warning(f"No table mapping found for schema {schema}")
        except Exception as exc:
            st.error(f"💥 Unexpected error while showing lineage: {exc}")
