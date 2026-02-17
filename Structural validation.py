# Choose a predictable, writable directory
DOWNLOAD_DIR="/home/vsts/work/_temp/downloads"
mkdir -p "$DOWNLOAD_DIR"

echo "⬇️ Downloading file $(BLOB_FILE_NAME) from Azure Blob Storage into $DOWNLOAD_DIR..."
pip install azure-storage-blob pandas -q

python - <<'PYCODE'
import os, re
from azure.storage.blob import BlobServiceClient

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_url = os.getenv("BLOB_FILE_URL")
blob_name = os.getenv("BLOB_FILE_NAME", "downloaded_file.csv")
download_dir = "/home/vsts/work/_temp/downloads"

if not conn_str:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING is missing")
if not blob_url:
    raise ValueError("BLOB_FILE_URL is missing")

# Parse container name and blob path
match = re.match(r"https://[^/]+/([^/]+)/(.+)", blob_url)
if not match:
    raise Exception(f"Invalid blob URL format: {blob_url}")
container_name, blob_path = match.groups()

print(f"🔗 Connecting to container: {container_name}")
service_client = BlobServiceClient.from_connection_string(conn_str)
blob_client = service_client.get_blob_client(container=container_name, blob=blob_path)

output_path = os.path.join(download_dir, blob_name)
os.makedirs(download_dir, exist_ok=True)

print(f"⬇️ Downloading blob '{blob_path}' ...")
with open(output_path, "wb") as f:
    f.write(blob_client.download_blob().readall())

print(f"✅ File downloaded successfully to: {output_path}")
PYCODE

# ✅ Now run validation on that same file
echo "🔍 Validating file format..."
echo "📂 File path is: /home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)"

python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/validate_structure.py" \
  --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)"