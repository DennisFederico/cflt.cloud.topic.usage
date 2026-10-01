#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Bucket Cleanup Script for Confluent Cloud Topic Usage Dashboard
# -----------------------------------------------------------------------------
# Cleans up older versions of deployment zip bundles in the GCS bucket.
# Keeps the latest N versions for rollback safety and deletes older ones.
#
# Usage:
#   ./scripts/clean_bucket.sh             # Keeps latest 2 versions
#   ./scripts/clean_bucket.sh --keep 1    # Keeps only the latest version
#   ./scripts/clean_bucket.sh --dry-run   # Preview what would be deleted
# -----------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="${ROOT_DIR}/terraform"

KEEP_COUNT=2
DRY_RUN=false
PROJECT_ID="solutionsarchitect-01"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep)
      KEEP_COUNT="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--keep <number>] [--dry-run]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Resolve bucket name
BUCKET_NAME=$(cd "${TERRAFORM_DIR}" && terraform output -raw deploy_bucket_name 2>/dev/null || true)
if [[ -z "${BUCKET_NAME}" || "${BUCKET_NAME}" == "null" ]]; then
  BUCKET_NAME=$(gsutil ls -p "${PROJECT_ID}" 2>/dev/null | grep "cflt-usage-dashboard" | head -n 1 | sed 's|gs://||;s|/||' || true)
fi

if [[ -z "${BUCKET_NAME}" ]]; then
  echo "Error: Could not resolve deployment bucket name."
  exit 1
fi

echo "=== GCS Deployment Bucket: gs://${BUCKET_NAME} ==="
echo "Policy: Keep latest ${KEEP_COUNT} version(s)"
if [[ "${DRY_RUN}" == "true" ]]; then
  echo "Mode:   DRY RUN (no objects will be deleted)"
fi
echo ""

# Query objects and sort by creation timestamp
PYTHON_SCRIPT='
import json, sys

keep_count = int(sys.argv[1])
dry_run = sys.argv[2] == "true"
bucket = sys.argv[3]

raw = sys.stdin.read().strip()
if not raw:
    print("No deployment bundles found in bucket.")
    sys.exit(0)

try:
    items = json.loads(raw)
except Exception as e:
    print(f"Error parsing JSON: {e}")
    sys.exit(1)

# Only target versioned deployment archives: app_deploy-*.zip
versioned = [
    item for item in items 
    if item.get("metadata", {}).get("name", "").startswith("app_deploy-") 
    and item.get("metadata", {}).get("name", "").endswith(".zip")
]

if not versioned:
    print("No versioned bundles (app_deploy-*.zip) found to clean.")
    sys.exit(0)

# Sort newest first
versioned.sort(key=lambda x: x.get("metadata", {}).get("timeCreated", ""), reverse=True)

to_keep = versioned[:keep_count]
to_delete = versioned[keep_count:]

print(f"Total versioned bundles found: {len(versioned)}")
print(f"\nKeeping ({len(to_keep)}):")
for item in to_keep:
    meta = item.get("metadata", {})
    name = meta.get("name", "")
    created = meta.get("timeCreated", "")
    print(f"  [KEEP]   {name} (Created: {created})")

if not to_delete:
    print("\nNo older versions need deletion.")
    sys.exit(0)

print(f"\nDeleting ({len(to_delete)}):")
for item in to_delete:
    meta = item.get("metadata", {})
    name = meta.get("name", "")
    created = meta.get("timeCreated", "")
    print(f"  [DELETE] {name} (Created: {created})")

# Print objects to delete for bash consumption
if not dry_run:
    with open("/tmp/gcs_objects_to_delete.txt", "w") as f:
        for item in to_delete:
            f.write(f"gs://{bucket}/{item.get('metadata', {}).get('name', '')}\n")
'

# Clear any previous deletion list
rm -f /tmp/gcs_objects_to_delete.txt

gcloud storage ls --json "gs://${BUCKET_NAME}/app_deploy-*.zip" 2>/dev/null | \
  python3 -c "${PYTHON_SCRIPT}" "${KEEP_COUNT}" "${DRY_RUN}" "${BUCKET_NAME}"

if [[ -f /tmp/gcs_objects_to_delete.txt && "${DRY_RUN}" == "false" ]]; then
  echo ""
  echo "=== Removing old bundles from GCS ==="
  while IFS= read -r obj; do
    echo "Deleting ${obj}..."
    gcloud storage rm "${obj}"
  done < /tmp/gcs_objects_to_delete.txt
  rm -f /tmp/gcs_objects_to_delete.txt
  echo "Cleanup complete."
fi
