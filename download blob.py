echo "⬇️ Downloading file $(BLOB_FILE_NAME) from Azure Blob Storage..."
pip install azure-storage-blob -q

python - <<'PYCODE'
import os, re, sys
from azure.storage.blob import BlobServiceClient

# === Environment Variables ===
conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_url = os.getenv("BLOB_FILE_URL")
blob_name = os.getenv("BLOB_FILE_NAME", "downloaded_file")
file_types_var = os.getenv("FILE_TYPE", "").strip()  # e.g., ".csv,.xlsx,.parquet"

# === Validate Inputs ===
if not conn_str:
    print("❌ ERROR: AZURE_STORAGE_CONNECTION_STRING is missing.")
    sys.exit(1)
if not blob_url:
    print("❌ ERROR: BLOB_FILE_URL is missing.")
    sys.exit(1)
if not file_types_var:
    print("❌ ERROR: FILE_TYPE variable is missing (expected one or more of .csv, .xlsx, .parquet).")
    sys.exit(1)

# === Prepare list of acceptable file types ===
expected_formats = [f.strip().lower() for f in file_types_var.split(",") if f.strip()]
print(f"📄 Allowed file formats: {expected_formats}")

# === Parse Container and Blob Path ===
match = re.match(r"https://[^/]+/([^/]+)/(.+)", blob_url)
if not match:
    print(f"❌ ERROR: Invalid blob URL format: {blob_url}")
    sys.exit(1)

container_name, blob_path = match.groups()
print(f"🔗 Connecting to container: {container_name}")

# === Connect to Blob Storage ===
service_client = BlobServiceClient.from_connection_string(conn_str)
blob_client = service_client.get_blob_client(container=container_name, blob=blob_path)

# === Download Blob ===
output_path = os.path.join(os.getcwd(), blob_name)
print(f"⬇️ Downloading blob '{blob_path}' ...")

with open(output_path, "wb") as f:
    f.write(blob_client.download_blob().readall())

print(f"✅ File downloaded successfully to: {output_path}")

# === Determine Actual File Format ===
_, ext = os.path.splitext(blob_name)
ext = ext.lower()

if ext not in expected_formats:
    print(f"❌ ERROR: File format mismatch. Expected one of {expected_formats}, but got '{ext}'")
    sys.exit(1)

print(f"✅ File format check passed: {ext}")
PYCODE