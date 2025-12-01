from sqlalchemy import create_engine, text

# Constants (UPPER_CASE for configuration)
SERVER_NAME = "2M1Z6D3\\SQLEXPRESS"
DATABASE_NAME = "ETLJobRunner"
DRIVER_NAME = "{ODBC Driver 17 for SQL Server}"


def get_engine():
    """Create and return a SQLAlchemy engine for SQL Server."""
    print("[INFO] Building connection string...")
    connection_string = (
        f"DRIVER={DRIVER_NAME};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        "Trusted_Connection=yes;"
    )
    print(f"[DEBUG] Connection string built for server: {SERVER_NAME}, database: {DATABASE_NAME}")
    return create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")


def test_connection(engine):
    """Test the database connection by querying the server name."""
    print("[INFO] Testing database connection...")
    try:
        with engine.connect() as connection:
            print("[INFO] Connection established, executing test query...")
            result = connection.execute(text("SELECT @@SERVERNAME AS ServerName"))
            for row in result:
                print(f"[SUCCESS] Successfully connected to server: {row.ServerName}")
    except Exception as exc:
        print(f"[ERROR] An error occurred while testing connection: {exc}")


# if __name__ == "__main__":
#     print("[INFO] Starting DB connection script...")
#     engine = get_engine()
#     test_connection(engine)
#     # print("[INFO] Script finished.")
