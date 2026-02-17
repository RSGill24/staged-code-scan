echo "⬇️ Downloading file $(BLOB_FILE_NAME) from Azure Blob Storage..."
pip install azure-storage-blob -q

python - <<'PYCODE'
import os, re
from azure.storage.blob import BlobServiceClient

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_url = os.getenv("BLOB_FILE_URL")
blob_name = os.getenv("BLOB_FILE_NAME", "downloaded_file.csv")

if not conn_str:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING is missing")
if not blob_url:
    raise ValueError("BLOB_FILE_URL is missing")

# Parse container and blob path from blob URL
match = re.match(r"https://[^/]+/([^/]+)/(.+)", blob_url)
if not match:
    raise Exception(f"Invalid blob URL format: {blob_url}")
container_name, blob_path = match.groups()

print(f"🔗 Connecting to container: {container_name}")
service_client = BlobServiceClient.from_connection_string(conn_str)
blob_client = service_client.get_blob_client(container=container_name, blob=blob_path)

output_path = os.path.join(os.getcwd(), blob_name)
print(f"⬇️ Downloading blob '{blob_path}' ...")
with open(output_path, "wb") as f:
    f.write(blob_client.download_blob().readall())

print(f"✅ File downloaded successfully to: {output_path}")
PYCODE