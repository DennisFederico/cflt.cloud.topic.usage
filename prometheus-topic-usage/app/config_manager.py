import json
import os
from pathlib import Path
import requests
from typing import Any, Dict

DEFAULT_CLUSTERS_CONFIG = {"clusters": {}}

class ConfigManager:
    def __init__(self):
        # Resolve config paths
        self.prom_config_path = Path(os.getenv("PROMETHEUS_CONFIG_PATH", "../prometheus/prometheus.yml")).resolve()
        self.clusters_config_path = Path(os.getenv("CLUSTERS_CONFIG_PATH", "../data/clusters_config.json")).resolve()
        self.prom_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")

        # Telemetry API credentials
        self.cloud_api_key = os.getenv("CLOUD_API_KEY", "")
        self.cloud_api_secret = os.getenv("CLOUD_API_SECRET", "")

        # Scrape settings
        self.scrape_interval = os.getenv("PROM_SCRAPE_INTERVAL", "1m")
        self.scrape_timeout = os.getenv("PROM_SCRAPE_TIMEOUT", "55s")

    def load_clusters_config(self) -> Dict[str, Any]:
        """Load the local JSON configuration file."""
        if not self.clusters_config_path.exists():
            self.clusters_config_path.parent.mkdir(parents=True, exist_ok=True)
            self.save_clusters_config(DEFAULT_CLUSTERS_CONFIG)
            return DEFAULT_CLUSTERS_CONFIG

        try:
            with open(self.clusters_config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading clusters config: {e}")
            return DEFAULT_CLUSTERS_CONFIG

    def save_clusters_config(self, config: Dict[str, Any]) -> None:
        """Save the JSON configuration file."""
        self.clusters_config_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.clusters_config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            print(f"Error saving clusters config: {e}")

    def update_cluster_credentials(self, cluster_id: str, kafka_api_endpoint: str, kafka_api_key: str, kafka_api_secret: str, name: str = "") -> None:
        """Update Kafka REST credentials for a specific cluster."""
        config = self.load_clusters_config()
        if "clusters" not in config:
            config["clusters"] = {}

        # Preserve existing fields if they exist
        existing = config["clusters"].get(cluster_id, {})
        config["clusters"][cluster_id] = {
            "name": name or existing.get("name", f"Cluster {cluster_id}"),
            "kafka_api_endpoint": kafka_api_endpoint,
            "kafka_api_key": kafka_api_key,
            "kafka_api_secret": kafka_api_secret,
            "configured": True
        }
        self.save_clusters_config(config)

    def generate_prometheus_config(self, cluster_ids: list[str]) -> bool:
        """
        Generates the prometheus.yml file with jobs for all discovered clusters,
        and triggers a reload of the Prometheus service.
        """
        if not self.cloud_api_key or not self.cloud_api_secret:
            print("Warning: CLOUD_API_KEY/CLOUD_API_SECRET environment variables are missing. Cannot query metrics or generate configuration.")
            return False

        jobs = []
        for cid in sorted(cluster_ids):
            job = f"""  - job_name: confluent_cloud_{cid}
    honor_timestamps: true
    static_configs:
      - targets: ["api.telemetry.confluent.cloud"]
        labels:
          cflt_cluster_id: "{cid}"
    scheme: https
    metrics_path: /v2/metrics/cloud/export
    basic_auth:
      username: "{self.cloud_api_key}"
      password: "{self.cloud_api_secret}"
    params:
      resource.kafka.id: ["{cid}"]
      metric:
        - io.confluent.kafka.server/received_bytes
        - io.confluent.kafka.server/sent_bytes
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: confluent_kafka_server_received_bytes|confluent_kafka_server_sent_bytes|confluent_scrape_resource_access_error
        action: keep"""
            jobs.append(job)

        jobs_joined = "\n\n".join(jobs)
        content = f"""global:
  scrape_interval: {self.scrape_interval}
  scrape_timeout: {self.scrape_timeout}

scrape_configs:
{jobs_joined}
"""

        # Write to path
        try:
            self.prom_config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.prom_config_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Prometheus config written successfully to {self.prom_config_path}")
            return self.reload_prometheus()
        except Exception as e:
            print(f"Error writing Prometheus config: {e}")
            return False

    def reload_prometheus(self) -> bool:
        """Trigger Prometheus hot reload endpoint."""
        url = f"{self.prom_url}/-/reload"
        try:
            response = requests.post(url, timeout=5)
            if response.status_code == 200:
                print("Prometheus hot-reload triggered successfully.")
                return True
            else:
                print(f"Failed to hot-reload Prometheus: HTTP {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Failed to connect to Prometheus reload endpoint ({url}): {e}")
            return False
