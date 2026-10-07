# Confluent Cloud Topic Usage Dashboard (Multi-Org Prometheus Dynamic Scraper)

This project dynamically monitors Confluent Cloud Kafka cluster/topic activity across **multiple organizations and environments**. It uses **Prometheus** configured with a **dynamic multi-org scraper scheduler** and a **FastAPI Web UI dashboard**.

It automatically discovers Confluent Cloud clusters/environments across all configured organizations and pulls:

1. **Topic Telemetry Metrics:** Throughput statistics (bytes in, bytes out, and retained bytes) via the Confluent Cloud Telemetry API (`api.telemetry.confluent.cloud`).
2. **Topic Inventory:** Active topic metadata via the Kafka REST API.
3. **Multi-Org Hierarchical View:** Distinct organization groupings with segregated environments and friendly display names.

---

## 1. Multi-Organization Configuration

You can configure one or more Confluent Cloud organizations using any of the following methods:

### Method A: Single Org (Default / Legacy Compatibility)
In your `.env` file:
```env
CLOUD_ORG_NAME="Santander Global Cards Production"
CLOUD_API_KEY="your_api_key"
CLOUD_API_SECRET="your_api_secret"
```

### Method B: Multiple Orgs via JSON (`CONFLUENT_ORGS`)
Specify all organizations in a structured JSON string in `.env`:
```env
CONFLUENT_ORGS='[
  {
    "id": "santander-prod",
    "name": "Santander Global Cards (Production)",
    "api_key": "PROD_API_KEY",
    "api_secret": "PROD_API_SECRET"
  },
  {
    "id": "santander-nonprod",
    "name": "Santander Global Cards (Non-Prod / QA)",
    "api_key": "NONPROD_API_KEY",
    "api_secret": "NONPROD_API_SECRET"
  }
]'
```

### Method C: Multiple Orgs via Prefixed Environment Variables
Define organizations using indexed or named environment variables in `.env`:
```env
CLOUD_ORG_1_NAME="Santander Cards Production"
CLOUD_ORG_1_KEY="PROD_KEY"
CLOUD_ORG_1_SECRET="PROD_SECRET"

CLOUD_ORG_2_NAME="Santander Cards QA"
CLOUD_ORG_2_KEY="QA_KEY"
CLOUD_ORG_2_SECRET="QA_SECRET"
```

### Method D: Manage via the Web UI
Click the **"🏢 Organizations"** button in the dashboard header to add, edit, or delete organizations and their API credentials live without restarting the services.

---

## 2. Metrics Scraping Rate Limits & Throttling Analysis

> [!IMPORTANT]
> **Origin IP vs. Organization Rate Limits:**
> The Confluent Cloud Metrics API (`api.telemetry.confluent.cloud/v2/metrics/cloud/export`) enforces rate limits at the **origin IP address level, NOT per organization**.

* **Global IP Rate Limit:** **300 requests per IP address, per minute** (~5 requests/second).
* **Per-Resource Throttle:** **160 requests per resource (cluster), per hour** on the `/export` endpoint (maximum ~1 request every 22.5 seconds per cluster).
* **Data Point Volume Limit:** Up to 60,000 data points per resource type per metric per request.

### What this means for Multi-Org Scraping:
1. **Shared IP Pool:** Because all Prometheus scrape requests originate from the same host / container VM, **all organizations and clusters share the 300 requests/minute IP quota**.
2. **Safe Fleet Capacity:**
   * At default `PROM_SCRAPE_INTERVAL=1m` (1 scrape per minute per cluster): Each cluster generates 60 requests/hour (well below the 160 req/h resource limit).
   * A single host can comfortably scrape **up to ~250–280 clusters** across all organizations without hitting the 300 req/min IP ceiling.
   * If your multi-org fleet exceeds 250 clusters, adjust the scrape interval in `.env`:
     ```env
     PROM_SCRAPE_INTERVAL=2m
     PROM_SCRAPE_TIMEOUT=1m45s
     ```
     At `2m` scrape intervals, capacity doubles to ~500 clusters per origin IP.
3. **Live Monitor in UI:** Click the **"⏱️ Scrape Quotas"** button in the dashboard header to view live request rates, quota utilization percentage, and health status for your instance.

---

## 3. Directory Structure

All dynamic and hot-reloaded state folders are located inside a single hidden directory `.cflt-local/` to keep the workspace clean:

* `.cflt-local/data/clusters_config.json`: Local cache database of Confluent Cloud organizations, clusters, environments, and configured Kafka REST API credentials.
* `.cflt-local/prometheus/prometheus.yml`: Dynamically generated Prometheus configuration containing jobs for each cluster, authenticating with its specific organization credentials and labeled with `cflt_org_id`, `cflt_org_name`, `cflt_env_id`, and `cflt_env_name`.

---

## 4. Core Architecture

* **FastAPI Backend (`app/main.py`)**: Runs a background multi-org scheduler loop (`CLUSTER_CHECK_INTERVAL=3h`) that scans each organization's Confluent Cloud Org API and Telemetry discovery endpoints, associates clusters with their respective org and environment, and updates Prometheus.
* **Config Manager (`app/config_manager.py`)**: Manages multi-org configurations and renders custom `prometheus.yml` files mapping each cluster to its organization's API credentials.
* **Web UI Frontend (`app/static/index.html`)**: Glassmorphism dashboard featuring:
  * Quick filter pills by organization.
  * Hierarchical accordion grouping: **Organization ➔ Environments ➔ Clusters**.
  * Cluster drilldown displaying friendly Organization and Environment badges.
  * Live Organizations Management and Scrape Rate Quotas modals.

---

## 5. Run Instructions

### Option A: Fully Containerized Mode (Docker Compose - Recommended)

1. Make sure you have a `.env` file in the project root containing your Confluent Cloud metrics credentials:

   ```env
   CLOUD_API_KEY=your_telemetry_api_key
   CLOUD_API_SECRET=your_telemetry_api_secret
   ```

2. Build and start all services (Prometheus and the FastAPI dashboard):

   ```bash
   docker compose up --build -d
   ```

3. Access the interfaces:
   * **Dashboard App UI:** [http://localhost:8000](http://localhost:8000)
   * **Prometheus UI:** [http://localhost:9090](http://localhost:9090)

---

### Option B: Local CLI Mode (Local Dev / Running on Host)

In this mode, Prometheus runs in Docker while the FastAPI application runs directly on your host machine.

1. **Start Prometheus in Docker:**

   ```bash
   docker compose up -d prometheus
   ```

2. **Setup Python Virtual Environment:**

   ```bash
   cd app
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables:**
   Create or edit the `.env` file in the project root with your credentials.

4. **Start the FastAPI server:**

   ```bash
   uvicorn main:app --port 8000 --reload
   ```

5. **Open Dashboard:** Go to [http://localhost:8000](http://localhost:8000).

---

## 4. Cloud Deployment (GCP Compute Engine)

### Fast Updates / Test Deployments

Use the dedicated deployment script to bundle and redeploy in ~5 seconds without invoking Terraform:

```bash
./scripts/deploy.sh
```

This packages the application, uploads it to GCS, and updates Docker Compose on the VM in-place while keeping the `prometheus_data` volume intact.

### Infrastructure & Repeatable Deployments (Terraform)

Run Terraform from the `terraform/` directory:

```bash
cd terraform
terraform apply
```

* Computes a deterministic SHA-256 hash across all application files.
* Only triggers when code or configs change.
* Uploads the versioned archive to GCS.
* Connects via `gcloud compute ssh` to update the application live on the VM.
* Protected by `lifecycle { prevent_destroy = true }` so your VM and Prometheus historical data are never destroyed.

### Cleaning Up Old GCS Bundles

To purge older deployment bundles from your storage bucket while keeping recent ones for rollback safety:

```bash
./scripts/clean_bucket.sh            # Keeps the latest 2 versions
./scripts/clean_bucket.sh --keep 1   # Keeps only the latest version
./scripts/clean_bucket.sh --dry-run  # Preview without deleting
```

Additionally, Terraform configures an automatic GCS Lifecycle policy to purge artifacts older than 14 days.
