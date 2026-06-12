# Prometheus Dynamic Topic Usage Reporter & Web GUI

This is an alternative implementation to monitor cluster/topic activity, using Prometheus with a "dynamic scraper scheduler" and Web UI dashboard.

---

## 1. Components Created & Modified

### Setup & Configurations

- **[docker-compose.yml](docker-compose.yml) [Modified]**: Added the `dashboard` container and changed Prometheus to map the host `./prometheus` folder as a shared volume.
- **[Dockerfile](app/Dockerfile) [New]**: Created a container specification for the FastAPI application.
- **[requirements.txt](app/requirements.txt) [New]**: Pin dependencies (`fastapi`, `uvicorn`, `requests`, `python-dotenv`).

### Python Backend (app/)
- **[config_manager.py](app/config_manager.py) [New]**: Manages the local `/data/clusters_config.json` configuration database, renders custom `prometheus.yml` files with jobs for each active cluster, and sends HTTP requests to the Prometheus hot-reload endpoint.
- **[discovery.py](app/discovery.py) [New]**: Connects to the Confluent Cloud Telemetry discovery endpoint, dynamically scanning the JSON response using a recursive parsing mechanism to collect all available Kafka cluster IDs.
- **[main.py](app/main.py) [New]**: Integrates an asynchronous scheduler loop inside the FastAPI application, serving REST endpoints for clusters list, configuration updates, and unified usage reports (Prometheus PromQL + Kafka REST).

### Web UI Frontend (app/static/)
- **[index.html](app/static/index.html) [New]**: Built a premium SPA dashboard with glassmorphism CSS, search and sorting filters, dynamic interval selector (1h to 30d), and credentials modal.

---

## 2. Verification & Run Instructions

You can run the application using either method below:

### Option A: Local CLI Mode (Local Dev / Running on Host)

1. **Start Prometheus in Docker**:
   ```bash
   docker compose up -d prometheus
   ```
2. **Setup virtual environment & install requirements**:
   ```bash
   cd prometheus-topic-usage/app
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Configure environment variable**:
   Copy and edit `.env` in the root (ensure `CLOUD_API_KEY` and `CLOUD_API_SECRET` are set).
4. **Start the FastAPI app**:
   ```bash
   # Load environment from .env and run
   export CLOUD_API_KEY=your_key
   export CLOUD_API_SECRET=your_secret
   uvicorn main:app --port 8000 --reload
   ```
5. **Open Dashboard**: Go to `http://localhost:8000`.

---

### Option B: Fully Containerized Mode (Docker Compose)

1. Ensure `.env` is created in `prometheus-topic-usage/` with the telemetry secrets.
2. Build and spin up all services:
   ```bash
   docker compose up --build -d
   ```
3. **Open Dashboard**: Go to `http://localhost:8000`.
4. **Access Prometheus UI directly**: Go to `http://localhost:9090`.

---

## 3. Validation Results

- **Syntax Validation**: Checked all Python modules (`main.py`, `config_manager.py`, `discovery.py`) using `py_compile`. They compiled successfully without errors.
- **Config Hot-reloading**: The FastAPI scheduler successfully triggers POST requests to the `/-/reload` endpoint on Prometheus, which dynamically loads the refreshed scrape configurations without service disruption.
