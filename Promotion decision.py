echo "🚀 Promotion decision approved."
echo "✅ Proceeding to promote dataset."

python - <<'PYCODE'
import os
import snowflake.connector

print("🔗 Logging promotion decision to Snowflake...")

# -------------------------------
# Environment variables
# -------------------------------
sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = os.getenv("SNOWFLAKE_DATABASE")
sf_schema = os.getenv("SNOWFLAKE_SCHEMA")

missing = [v for v in [
    "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"
] if not os.getenv(v)]

if missing:
    raise RuntimeError(f"Missing Snowflake env vars: {missing}")

# -------------------------------
# Connect
# -------------------------------
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
)
cur = conn.cursor()

# -------------------------------
# Set execution context (THIS FIXES THE ERROR)
# -------------------------------
cur.execute(f"USE WAREHOUSE {sf_warehouse}")
cur.execute(f"USE DATABASE {sf_database}")
cur.execute(f"USE SCHEMA {sf_schema}")

# -------------------------------
# Insert promotion decision
# -------------------------------
insert_sql = """
INSERT INTO VALIDATION_LOG
(
    FILE_NAME,
    FILE_PATH,
    STAGE_NAME,
    VALIDATION_STATUS,
    DOMAIN,
    SOURCE,
    TRIGGERED_BY,
    LOG_TS
)
VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
"""

cur.execute(insert_sql, (
    os.path.basename(os.getenv("BLOB_FILE_NAME", "unknown.csv")),
    os.getenv("DOWNLOAD_DIR", "/home/vsts/work/_temp/downloads"),
    "PromotionDecision",
    "PROMOTED",
    "Claims",
    "ADO_PIPELINE",
    "ADO_ReleasePipeline"
))

conn.commit()
cur.close()
conn.close()

print("✅ Promotion decision successfully logged to Snowflake.")
PYCODE
