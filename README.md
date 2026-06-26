# Confluent Cloud Topic Usage Dashboard (Prometheus Dynamic Scraper)

This project dynamically monitors Confluent Cloud Kafka cluster/topic activity. It uses **Prometheus** configured with a **dynamic scraper scheduler** and a **FastAPI Web UI dashboard**.

It automatically discovers Confluent Cloud clusters/environments and pulls:
1. **Topic Telemetry Metrics:** Throughput statistics (bytes in, bytes out, and retained bytes) via the Confluent Cloud Telemetry API.
2. **Topic Inventory:** Active topic metadata via the Kafka REST API.

---

## 1. Directory Structure

All dynamic and hot-reloaded state folders are located inside a single hidden directory `.cflt-local/` to keep the workspace clean:

* `.cflt-local/data/clusters_config.json`: Local cache database of Confluent Cloud clusters, environments, and configured Kafka REST API credentials.
* `.cflt-local/prometheus/prometheus.yml`: Dynamically generated Prometheus configuration containing targets for all active clusters.
* `.cflt-local/prometheus/prometheus.yml.tmpl`: Prometheus config template used by the FastAPI config manager.

---

## 2. Core Architecture

* **FastAPI Backend (`app/main.py`)**: Launches a background scheduler loop that periodically runs Confluent Cloud Org API discovery. If credential authorization fails, it falls back to Telemetry discovery endpoints to scan for cluster IDs.
* **Config Manager (`app/config_manager.py`)**: Renders custom `prometheus.yml` files whenever new clusters are discovered and sends an HTTP POST request to the Prometheus `/-/reload` endpoint to apply configuration changes hot.
* **Web UI Frontend (`app/static/index.html`)**: A modern Single Page Application dashboard featuring glassmorphism CSS, search and sorting filters, dynamic interval selector (1h to 30d), and credentials setup dialogs.

---

## 3. Run Instructions

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
