import os
import pandas as pd
from pandasql import sqldf
from azure.storage.blob import BlobServiceClient

# -------------------------------
# Environment Variables
# -------------------------------
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')
BLOB_FILE_NAME = os.getenv('BLOB_FILE_NAME')          # e.g., organizations-100.csv
SQLQUERY = os.getenv('SQLQUERY')                      # e.g., SELECT * FROM {table}
OUTPUT_CSV_NAME = os.getenv('OUTPUT_CSV_NAME')

DEST_AZURE_STORAGE_CONNECTION_STRING = os.getenv('DEST_AZURE_STORAGE_CONNECTION_STRING')
DEST_CONTAINER_NAME = os.getenv('DEST_CONTAINER_NAME')

# -------------------------------
# Validation for required vars
# -------------------------------
required_vars = {
    "AZURE_STORAGE_CONNECTION_STRING": AZURE_STORAGE_CONNECTION_STRING,
    "CONTAINER_NAME": CONTAINER_NAME,
    "BLOB_FILE_NAME": BLOB_FILE_NAME,
    "DEST_AZURE_STORAGE_CONNECTION_STRING": DEST_AZURE_STORAGE_CONNECTION_STRING,
    "DEST_CONTAINER_NAME": DEST_CONTAINER_NAME,
    "SQLQUERY": SQLQUERY,
    "OUTPUT_CSV_NAME": OUTPUT_CSV_NAME
}

for name, value in required_vars.items():
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

# -------------------------------
# Step 1: Download source blob
# -------------------------------
print(f"⬇️ Downloading '{BLOB_FILE_NAME}' from container '{CONTAINER_NAME}'...")
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_FILE_NAME)

download_file_path = f"/tmp/{BLOB_FILE_NAME}"
with open(download_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

print(f"✅ Downloaded blob to '{download_file_path}'")

# -------------------------------
# Step 2: Read CSV into DataFrame
# -------------------------------
df = pd.read_csv(download_file_path)

# -------------------------------
# Step 3: Execute SQL query (FIXED)
# -------------------------------
raw_table_name = os.path.splitext(BLOB_FILE_NAME)[0]

# 🔧 Make table name SQL-safe
table_name = raw_table_name.replace("-", "_").replace(" ", "_")

SQLQUERY = SQLQUERY.format(table=table_name)
print(f"🧠 Executing query: {SQLQUERY}")

pysqldf = lambda q: sqldf(q, {table_name: df})
result_df = pysqldf(SQLQUERY)

# -------------------------------
# Step 4: Save result locally
# -------------------------------
output_file_path = f"/tmp/{OUTPUT_CSV_NAME}"
result_df.to_csv(output_file_path, index=False)
print(f"✅ Query result saved locally as '{output_file_path}'")

# -------------------------------
# Step 5: Upload results to destination storage
# -------------------------------
print(f"⬆️ Uploading '{OUTPUT_CSV_NAME}' to destination container '{DEST_CONTAINER_NAME}'...")
dest_blob_service = BlobServiceClient.from_connection_string(DEST_AZURE_STORAGE_CONNECTION_STRING)
dest_blob_client = dest_blob_service.get_blob_client(
    container=DEST_CONTAINER_NAME,
    blob=OUTPUT_CSV_NAME
)

with open(output_file_path, "rb") as data:
    dest_blob_client.upload_blob(data, overwrite=True)

print(f"✅ Uploaded result CSV '{OUTPUT_CSV_NAME}' successfully.")
