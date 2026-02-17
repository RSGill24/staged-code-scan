echo "🚀 Starting Snowflake load stage..."
echo "📦 Installing required Python packages..."
pip install --user snowflake-connector-python azure-storage-blob -q

python <<'PYCODE'
import os, re, sys
from azure.storage.blob import BlobServiceClient
import snowflake.connector

DOWNLOAD_DIR = "/home/vsts/work/_temp/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# -------------------------------
# Azure Blob download
# -------------------------------
conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_url = os.getenv("BLOB_FILE_URL")
blob_name = os.getenv("BLOB_FILE_NAME")

if not all([conn_str, blob_url, blob_name]):
    raise Exception("❌ Missing Azure Blob environment variables")

match = re.match(r"https://[^/]+/([^/]+)/(.+)", blob_url)
if not match:
    raise Exception(f"Invalid blob URL: {blob_url}")

container_name, blob_path = match.groups()

blob_service = BlobServiceClient.from_connection_string(conn_str)
blob_client = blob_service.get_blob_client(container_name, blob_path)

file_path = os.path.join(DOWNLOAD_DIR, blob_name)
with open(file_path, "wb") as f:
    f.write(blob_client.download_blob().readall())

print(f"✅ Downloaded file: {file_path}")

# -------------------------------
# Read header row
# -------------------------------
with open(file_path, "r", encoding="utf-8") as f:
    header = f.readline().strip()

if not header:
    raise Exception("❌ File header is empty")

columns = [re.sub(r"[^\w]", "_", c.strip()).upper() for c in header.split(",")]
column_sql = ",\n".join([f'"{c}" VARCHAR' for c in columns])

# -------------------------------
# Snowflake connection
# -------------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role="ADO_EDL_ROLE"
)
cur = conn.cursor()

# -------------------------------
# Database & schema (VALIDATE + QUOTE)
# -------------------------------
DB_NAME = os.getenv("SNOWFLAKE_FILE_DB")
SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA")

print(f"🔍 DB_NAME     = {DB_NAME}")
print(f"🔍 SCHEMA_NAME = {SCHEMA_NAME}")

if not DB_NAME or not SCHEMA_NAME:
    print("❌ Database or Schema env variable is missing")
    sys.exit(1)

cur.execute(f'USE DATABASE "{DB_NAME}"')
cur.execute(f'USE SCHEMA "{SCHEMA_NAME}"')

print(f"✅ Using {DB_NAME}.{SCHEMA_NAME}")

# -------------------------------
# Table name
# -------------------------------
table_name = re.sub(r"[^\w]", "_", os.path.splitext(blob_name)[0].upper())
print(f"📊 Target table: {table_name}")

# -------------------------------
# CREATE TABLE
# -------------------------------
cur.execute(f'''
CREATE TABLE IF NOT EXISTS "{table_name}" (
    {column_sql}
)
''')
print("✅ Table created")

# -------------------------------
# PUT
# -------------------------------
cur.execute(f'PUT file://{file_path} @%"{table_name}" OVERWRITE = TRUE')
print("📤 File uploaded to stage")

# -------------------------------
# COPY
# -------------------------------
cur.execute(f'''
COPY INTO "{table_name}"
FROM @%"{table_name}"
FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = CONTINUE
''')

conn.commit()
print("✅ Data loaded successfully")

cur.close()
conn.close()
PYCODE
