📊 Data Processing Tool
Overview
Data Processing Tool is a metadata-driven ETL pipeline built in Python with SQL Server as the backend. It automates ingestion of source files (CSV/JSON) into a structured data warehouse organized into three layers:

Raw Layer → Stores raw ingested data
Curated Layer → Cleansed and standardized data
Processed Layer → Final analytical tables with Slowly Changing Dimensions (SCD1/SCD2)

The pipeline supports full and incremental loads, dynamic DDL creation, audit logging, and includes a Streamlit UI for orchestration.

✨ Features
Metadata-driven schema creation
Full & incremental load support
SCD1 and SCD2 handling
Audit trail with external logging
Streamlit UI for job execution and monitoring


📂 Project Structure
ETL-Job-Runner/
│
├── input_files/
│   ├── config/        # Config Excel files
│   ├── metadata/      # Metadata Excel files
│   └── data/          # Source data files
│
├── scripts/
│   ├── audit.py               # Job audit logging
│   ├── create_ddl.py          # Dynamic DDL creation
│   ├── curated_processed.py   # Curated → Processed ETL
│   ├── db_connection.py       # SQLAlchemy engine setup
│   ├── load_config_metadata.py# Load Config & Metadata
│   ├── load_type.py           # Full & Incremental load logic
│   ├── main.py                # CLI entry point
│   ├── orchestration.py       # Job orchestration
│   ├── raw_curated.py         # Raw → Curated ETL
│   ├── scd_type.py            # SCD1 & SCD2 merge logic
│   ├── send_log.py            # External logging
│   ├── source_raw.py          # Source → Raw ETL
│   └── validate_input.py      # Config & Metadata validation
│
├── ui_run.py                  # Streamlit UI
├── ETLJobRunner.sql           # Database schema & control tables
└── README.md


🗄️ Database Setup
Run ETLJobRunner.sql in SQL Server to create:

Schemas: raw, curated, processed
Control tables: Config, Metadata, JobAudit, IncrementalTracker


⚙️ How It Works
Config & Metadata Upload

Config defines job settings (source type, path, schema, load type, SCD type).
Metadata defines column mappings, data types, PK/FK constraints.

ETL Flow

Source → Raw: Reads CSV/JSON, validates columns, inserts into raw tables.
Raw → Curated: Cleans data, enforces PK/FK, casts types.
Curated → Processed: Applies SCD logic (SCD1 overwrite, SCD2 history).

Audit & Logging

Logs job start/end with status and row counts.
Sends structured logs to external endpoint.


▶️ Run Options
Command Line
python scripts/main.py --job JOB_EMP_RAW --config path/to/config.xlsx --metadata 
path/to/metadata.xlsx

Streamlit UI
streamlit run ui_run.py

🛠️ Tech Stack
Python: Pandas, SQLAlchemy, Streamlit
SQL Server: Storage & MERGE operations