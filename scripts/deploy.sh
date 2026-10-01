#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Quick Application Deploy Script for Confluent Cloud Topic Usage Dashboard
# -----------------------------------------------------------------------------
# Re-packages the application, uploads the archive to GCS, and updates the
# running Docker Compose application on the VM in-place WITHOUT destroying
# the VM or wiping Prometheus historical metrics.
# -----------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="${ROOT_DIR}/terraform"

echo "=== [1/4] Preparing Deployment Package ==="
cd "${ROOT_DIR}"
rm -f terraform/app_deploy.zip
zip -r terraform/app_deploy.zip app docker-compose.yml \
  -x "app/.venv/*" "app/__pycache__/*" "app/.DS_Store" "app/clear/*" \
     "app/**/.venv/*" "app/**/__pycache__/*" "app/**/.DS_Store" "app/**/clear/*"

echo "=== [2/4] Resolving Infrastructure Targets ==="
# Attempt to read targets from Terraform outputs, falling back to defaults
BUCKET_NAME=$(cd "${TERRAFORM_DIR}" && terraform output -raw deploy_bucket_name 2>/dev/null || true)
VM_NAME=$(cd "${TERRAFORM_DIR}" && terraform output -raw vm_name 2>/dev/null || true)
PROJECT_ID="solutionsarchitect-01"
ZONE="europe-west2-a"

if [[ -z "${BUCKET_NAME}" || "${BUCKET_NAME}" == "null" ]]; then
  BUCKET_NAME=$(gsutil ls -p "${PROJECT_ID}" | grep "cflt-usage-dashboard" | head -n 1 | sed 's|gs://||;s|/||')
fi

if [[ -z "${VM_NAME}" || "${VM_NAME}" == "null" ]]; then
  VM_NAME="cflt-topic-usage-dashboard"
fi

echo "Target Bucket: gs://${BUCKET_NAME}"
echo "Target VM:     ${VM_NAME} (${ZONE})"

echo "=== [3/4] Uploading Archive to GCS ==="
gsutil cp terraform/app_deploy.zip "gs://${BUCKET_NAME}/app_deploy.zip"

echo "=== [4/4] In-Place Redeployment on VM ==="
gcloud compute ssh "${VM_NAME}" --zone "${ZONE}" --project "${PROJECT_ID}" --command "
  set -euo pipefail
  echo '--> Downloading new package from GCS...'
  sudo gsutil cp gs://${BUCKET_NAME}/app_deploy.zip /tmp/app_deploy.zip
  
  echo '--> Extracting package...'
  sudo unzip -o /tmp/app_deploy.zip -d /opt/cflt-app
  
  echo '--> Rebuilding and restarting containers...'
  cd /opt/cflt-app
  sudo docker compose up --build -d
  
  echo '--> Current container status:'
  sudo docker ps --filter name=cflt
"

echo "=== Deployment completed successfully! ==="
DASHBOARD_URL=$(cd "${TERRAFORM_DIR}" && terraform output -raw dashboard_url 2>/dev/null || true)
if [[ -n "${DASHBOARD_URL}" ]]; then
  echo "Dashboard URL: ${DASHBOARD_URL}"
fi
