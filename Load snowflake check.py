echo "🔍 Checking validation results before Snowflake load..."

echo "FORMAT_STATUS=$(FORMAT_STATUS)"
echo "STRUCTURE_STATUS=$(STRUCTURE_STATUS)"
echo "READINESS_STATUS=$(READINESS_STATUS)"

if [[ "$(FORMAT_STATUS)" == "FAIL" || "$(STRUCTURE_STATUS)" == "FAIL" || "$(READINESS_STATUS)" == "FAIL" ]]; then
  echo "🚫 One or more validations did NOT pass."
  echo "❌ Skipping Snowflake load."
  exit 0
fi

echo "✅ All validations PASSED. Proceeding to Snowflake load..."
