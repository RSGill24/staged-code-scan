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

INIT_RESULT=$(python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/validate_format.py" \
  --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)")
echo "$INIT_RESULT"

STRUCT_RESULT=$(python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/validate_structure.py" \
  --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)")

READINESS_RESULT=$(python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/integration_readiness.py" --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)" --domain Claims)
echo "$READINESS_RESULT"

FORMAT_STATUS=$(echo "$INIT_RESULT" | grep "^{.*}$" | jq -r '.status')
FORMAT_ERROR=$(echo "$INIT_RESULT" | grep "^{.*}$" | jq -r '.error_message')

STRUCTURE_STATUS=$(echo "$STRUCT_RESULT" | grep "^{.*}$" | jq -r '.status')
STRUCTURE_ERROR=$(echo "$STRUCT_RESULT" | grep "^{.*}$" | jq -r '.error_message')
ROW_COUNT=$(echo "$STRUCT_RESULT" | grep "^{.*}$" | jq -r '.row_count // empty')
COLUMN_COUNT=$(echo "$STRUCT_RESULT" | grep "^{.*}$" | jq -r '.col_count // empty')
NULL_PCT=$(echo "$STRUCT_RESULT" | grep "^{.*}$" | jq -r '.null_pct // empty')

READINESS_STATUS=$(echo "$READINESS_RESULT" | grep "^{.*}$" | jq -r '.status')
READINESS_ERROR=$(echo "$READINESS_RESULT" | grep "^{.*}$" | jq -r '.error_message')
MISSING_KEYS=$(echo "$READINESS_RESULT" | grep "^{.*}$" | jq -r '.missing_keys // empty')

echo "📊 Extracted summary:"
echo "  - FORMAT_STATUS=$FORMAT_STATUS"
echo "  - STRUCTURE_STATUS=$STRUCTURE_STATUS"
echo "  - READINESS_STATUS=$READINESS_STATUS"


# ============================================================
# 5️⃣ LOG VALIDATION RESULTS TO SQL
# ============================================================

echo " Installing Snowflake Python connector..."
pip install --upgrade pip
pip install snowflake-connector-python -q

# 1️⃣ Initial Validation
echo "➡️ Logging Initial Validation..."
python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/log_results.py" \
  --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)" \
  --status "$FORMAT_STATUS" \
  --domain "Claims" \
  --stage "InitialValidation" \
  --source "AzureBlob" \
  --errormsg "$FORMAT_ERROR"

# 2️⃣ Structural Validation
echo "➡️ Logging Structural Validation..."
python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/log_results.py" \
  --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)" \
  --status "$STRUCTURE_STATUS" \
  --domain "Claims" \
  --stage "StructuralValidation" \
  --source "AzureBlob" \
  --rowcount "$ROW_COUNT" \
  --colcount "$COLUMN_COUNT" \
  --nullpct "$NULL_PCT" \
  --errormsg "$STRUCTURE_ERROR"

# 3️⃣ Integration Readiness
echo "➡️ Logging Integration Readiness..."
python "$(System.DefaultWorkingDirectory)/_Landing a file in EDL in less than 24 hours/scripts/log_results.py" \
  --file "/home/vsts/work/_temp/downloads/$(BLOB_FILE_NAME)" \
  --status "$READINESS_STATUS" \
  --domain "Claims" \
  --stage "IntegrationReadiness" \
  --source "AzureBlob" \
  --missingkeys "$MISSING_KEYS" \
  --errormsg "$READINESS_ERROR"

echo "✅ All validation results logged successfully!"


echo "##vso[task.setvariable variable=FORMAT_STATUS]$FORMAT_STATUS"
echo "##vso[task.setvariable variable=STRUCTURE_STATUS]$STRUCTURE_STATUS"
echo "##vso[task.setvariable variable=READINESS_STATUS]$READINESS_STATUS"