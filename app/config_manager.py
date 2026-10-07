import json
import os
from pathlib import Path
import re
import requests
from typing import Any, Dict, List, Optional
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

DEFAULT_CLUSTERS_CONFIG = {"organizations": {}, "clusters": {}}

class ConfigManager:
    def __init__(self):
        # Load environment variables
        if load_dotenv:
            try:
                load_dotenv()
            except Exception:
                pass
        ROOT = Path(__file__).resolve().parent.parent
        if load_dotenv:
            try:
                load_dotenv(ROOT / ".env")
                load_dotenv(ROOT.parent / ".env")
            except Exception:
                pass

        # Fallback manual loader if dotenv didn't populate variables
        self._load_env_file_manually(ROOT / ".env")
        self._load_env_file_manually(ROOT.parent / ".env")

        # Resolve config paths
        self.prom_config_path = Path(os.getenv("PROMETHEUS_CONFIG_PATH", ROOT / ".cflt-local/prometheus/prometheus.yml")).resolve()
        self.clusters_config_path = Path(os.getenv("CLUSTERS_CONFIG_PATH", ROOT / ".cflt-local/data/clusters_config.json")).resolve()
        self.prom_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")

        # Legacy / fallback Telemetry API credentials
        self.cloud_api_key = os.getenv("CLOUD_API_KEY", "")
        self.cloud_api_secret = os.getenv("CLOUD_API_SECRET", "")

        # Scrape settings
        self.scrape_interval = os.getenv("PROM_SCRAPE_INTERVAL", "1m")
        self.scrape_timeout = os.getenv("PROM_SCRAPE_TIMEOUT", "55s")

    def _load_env_file_manually(self, path: Path):
        """Fallback to read key=value pairs directly from .env file if missing in os.environ."""
        if not path.exists():
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k not in os.environ or not os.environ[k]:
                        os.environ[k] = v
        except Exception:
            pass

    def _extract_env_organizations(self) -> Dict[str, Dict[str, str]]:
        """
        Parses organizations from environment variables:
        1. CONFLUENT_ORGS (JSON string)
        2. CLOUD_ORG_<KEY>_* / CFLT_ORG_<KEY>_*
        3. Legacy CLOUD_API_KEY / CLOUD_API_SECRET
        """
        orgs: Dict[str, Dict[str, str]] = {}

        # 1. Parse JSON list or dict in CONFLUENT_ORGS / CFLT_ORGS
        raw_json = os.getenv("CONFLUENT_ORGS") or os.getenv("CFLT_ORGS") or os.getenv("ORGS_CONFIG")
        if raw_json:
            try:
                parsed = json.loads(raw_json)
                if isinstance(parsed, list):
                    for item in parsed:
                        oid = item.get("id") or item.get("org_id") or re.sub(r'[^a-zA-Z0-9_-]', '-', (item.get("name") or "org").lower())
                        orgs[oid] = {
                            "id": oid,
                            "name": item.get("name") or f"Organization {oid}",
                            "api_key": item.get("api_key") or "",
                            "api_secret": item.get("api_secret") or ""
                        }
                elif isinstance(parsed, dict):
                    for oid, item in parsed.items():
                        orgs[oid] = {
                            "id": oid,
                            "name": item.get("name") or f"Organization {oid}",
                            "api_key": item.get("api_key") or "",
                            "api_secret": item.get("api_secret") or ""
                        }
            except Exception as e:
                print(f"Error parsing CONFLUENT_ORGS JSON: {e}")

        # 2. Parse indexed/keyed env vars e.g. CLOUD_ORG_1_NAME, CLOUD_ORG_1_KEY, CLOUD_ORG_1_SECRET
        pattern = re.compile(r'^(?:CLOUD|CFLT)_ORG_([A-Za-z0-9_-]+)_(NAME|KEY|SECRET|ID)$', re.IGNORECASE)
        indexed: Dict[str, Dict[str, str]] = {}
        for k, v in os.environ.items():
            m = pattern.match(k)
            if m:
                key_id = m.group(1).lower()
                field = m.group(2).lower()
                if key_id not in indexed:
                    indexed[key_id] = {}
                indexed[key_id][field] = v

        for key_id, fields in indexed.items():
            oid = fields.get("id") or f"org-{key_id}"
            if oid not in orgs:
                orgs[oid] = {
                    "id": oid,
                    "name": fields.get("name") or f"Organization {key_id.upper()}",
                    "api_key": fields.get("key") or "",
                    "api_secret": fields.get("secret") or ""
                }
            else:
                if fields.get("name"): orgs[oid]["name"] = fields["name"]
                if fields.get("key"): orgs[oid]["api_key"] = fields["key"]
                if fields.get("secret"): orgs[oid]["api_secret"] = fields["secret"]

        # 3. Legacy single org fallback
        if self.cloud_api_key and self.cloud_api_secret:
            legacy_id = os.getenv("CLOUD_ORG_ID", "default-org")
            legacy_name = os.getenv("CLOUD_ORG_NAME", "Default Organization")
            if legacy_id not in orgs:
                orgs[legacy_id] = {
                    "id": legacy_id,
                    "name": legacy_name,
                    "api_key": self.cloud_api_key,
                    "api_secret": self.cloud_api_secret
                }

        return orgs

    def load_clusters_config(self) -> Dict[str, Any]:
        """Load the local JSON configuration file and sync environment organizations."""
        config = DEFAULT_CLUSTERS_CONFIG.copy()
        if self.clusters_config_path.exists():
            try:
                with open(self.clusters_config_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        config = loaded
            except Exception as e:
                print(f"Error loading clusters config: {e}")

        if "organizations" not in config or not isinstance(config["organizations"], dict):
            config["organizations"] = {}
        if "clusters" not in config or not isinstance(config["clusters"], dict):
            config["clusters"] = {}

        # Merge or override environment organizations into config["organizations"]
        env_orgs = self._extract_env_organizations()
        overwrite_gui = os.getenv("OVERWRITE_ORGS_ON_DEPLOY", "false").lower() in ("true", "1", "yes")

        if overwrite_gui and env_orgs:
            # Strict mode: If deployment provides orgs and OVERWRITE_ORGS_ON_DEPLOY is enabled,
            # enforce deployment orgs as the sole source of truth
            config["organizations"] = env_orgs
        else:
            # Default smart merge: deployment values take precedence for matching orgs,
            # while GUI-added orgs are preserved
            for oid, odata in env_orgs.items():
                if oid not in config["organizations"]:
                    config["organizations"][oid] = odata
                else:
                    # Update credentials or name if env provided non-empty values
                    existing = config["organizations"][oid]
                    if odata.get("name") and (not existing.get("name") or existing.get("name") == f"Organization {oid}"):
                        existing["name"] = odata["name"]
                    if odata.get("api_key"):
                        existing["api_key"] = odata["api_key"]
                    if odata.get("api_secret"):
                        existing["api_secret"] = odata["api_secret"]

        # If clusters exist without org_id, associate them with the primary organization
        primary_org_id = next(iter(config["organizations"].keys()), "default-org")
        primary_org_name = config["organizations"].get(primary_org_id, {}).get("name", "Default Organization")

        for cid, cdata in config["clusters"].items():
            if not cdata.get("org_id"):
                cdata["org_id"] = primary_org_id
            if not cdata.get("org_name"):
                cdata["org_name"] = config["organizations"].get(cdata["org_id"], {}).get("name", primary_org_name)

        return config

    def save_clusters_config(self, config: Dict[str, Any]) -> None:
        """Save the JSON configuration file."""
        self.clusters_config_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.clusters_config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            print(f"Error saving clusters config: {e}")

    def get_organizations(self, include_secrets: bool = False) -> Dict[str, Dict[str, Any]]:
        """Retrieve all organizations with or without sensitive secrets."""
        config = self.load_clusters_config()
        orgs = config.get("organizations", {})
        result = {}
        for oid, odata in orgs.items():
            item = {
                "id": oid,
                "name": odata.get("name") or f"Organization {oid}",
                "has_credentials": bool(odata.get("api_key") and odata.get("api_secret"))
            }
            if include_secrets:
                item["api_key"] = odata.get("api_key", "")
                item["api_secret"] = odata.get("api_secret", "")
            else:
                key = odata.get("api_key", "")
                item["api_key_masked"] = (key[:4] + "..." + key[-4:]) if len(key) >= 8 else ("***" if key else "")
            result[oid] = item
        return result

    def get_organizations_list(self, include_secrets: bool = False) -> List[Dict[str, Any]]:
        """Retrieve organizations as a list."""
        orgs = self.get_organizations(include_secrets=include_secrets)
        return list(orgs.values())

    def save_organization(self, org_id: Optional[str], name: str, api_key: str, api_secret: str) -> Dict[str, Any]:
        """Create or update an organization."""
        config = self.load_clusters_config()
        if "organizations" not in config:
            config["organizations"] = {}

        if not org_id:
            # Generate slug from name
            clean_name = re.sub(r'[^a-zA-Z0-9_-]', '-', name.strip().lower()).strip('-')
            org_id = f"org-{clean_name}" if clean_name else f"org-{len(config['organizations']) + 1}"

        existing = config["organizations"].get(org_id, {})
        config["organizations"][org_id] = {
            "id": org_id,
            "name": name.strip() or existing.get("name", f"Organization {org_id}"),
            "api_key": api_key.strip() if api_key.strip() else existing.get("api_key", ""),
            "api_secret": api_secret.strip() if api_secret.strip() else existing.get("api_secret", "")
        }

        # Update org_name in clusters assigned to this org
        if "clusters" in config:
            for cid, cdata in config["clusters"].items():
                if cdata.get("org_id") == org_id:
                    cdata["org_name"] = config["organizations"][org_id]["name"]

        self.save_clusters_config(config)
        return config["organizations"][org_id]

    def delete_organization(self, org_id: str) -> bool:
        """Delete an organization and unassign associated clusters."""
        config = self.load_clusters_config()
        if "organizations" in config and org_id in config["organizations"]:
            del config["organizations"][org_id]
            # Remove or unassign clusters
            if "clusters" in config:
                for cid, cdata in config["clusters"].items():
                    if cdata.get("org_id") == org_id:
                        cdata["org_id"] = "unassigned"
                        cdata["org_name"] = "Unassigned"
            self.save_clusters_config(config)
            return True
        return False

    def update_cluster_credentials(self, cluster_id: str, kafka_api_endpoint: str, kafka_api_key: str, kafka_api_secret: str, name: str = "", org_id: str = "") -> None:
        """Update Kafka REST credentials for a specific cluster."""
        config = self.load_clusters_config()
        if "clusters" not in config:
            config["clusters"] = {}

        existing = config["clusters"].get(cluster_id, {})
        target_org_id = org_id or existing.get("org_id") or next(iter(config.get("organizations", {}).keys()), "default-org")
        target_org_name = config.get("organizations", {}).get(target_org_id, {}).get("name", "Default Organization")

        config["clusters"][cluster_id] = {
            "name": name or existing.get("name", f"Cluster {cluster_id}"),
            "org_id": target_org_id,
            "org_name": target_org_name,
            "environment_id": existing.get("environment_id", ""),
            "environment_name": existing.get("environment_name", ""),
            "kafka_api_endpoint": kafka_api_endpoint,
            "kafka_api_key": kafka_api_key,
            "kafka_api_secret": kafka_api_secret,
            "configured": True
        }
        self.save_clusters_config(config)

    def generate_prometheus_config(self, clusters: Optional[Dict[str, Any]] = None) -> bool:
        """
        Generates the prometheus.yml file with jobs for all discovered clusters,
        using each cluster's respective organization credentials, and reloads Prometheus.
        """
        config = self.load_clusters_config()
        orgs = config.get("organizations", {})
        
        if clusters is None:
            clusters = config.get("clusters", {})
        elif isinstance(clusters, list):
            # If a list of IDs was passed, map them from config
            all_clusters = config.get("clusters", {})
            clusters = {cid: all_clusters.get(cid, {}) for cid in clusters}

        # Fallback credentials if an org doesn't have explicit credentials
        fallback_org = next(iter(orgs.values()), {})
        default_key = fallback_org.get("api_key") or self.cloud_api_key
        default_secret = fallback_org.get("api_secret") or self.cloud_api_secret

        jobs = []
        for cid in sorted(clusters.keys()):
            cdata = clusters[cid]
            org_id = cdata.get("org_id") or "default-org"
            org_info = orgs.get(org_id, {})
            org_name = org_info.get("name") or cdata.get("org_name") or f"Organization {org_id}"
            
            env_id = cdata.get("environment_id") or "unassigned"
            env_name = cdata.get("environment_name") or "Unassigned"

            key = org_info.get("api_key") or default_key
            secret = org_info.get("api_secret") or default_secret

            if not key or not secret:
                print(f"Skipping cluster {cid}: No API key/secret found for organization '{org_id}'")
                continue

            job = f"""  - job_name: confluent_cloud_{cid}
    honor_timestamps: true
    static_configs:
      - targets: ["api.telemetry.confluent.cloud"]
        labels:
          cflt_cluster_id: "{cid}"
          cflt_org_id: "{org_id}"
          cflt_org_name: "{org_name}"
          cflt_env_id: "{env_id}"
          cflt_env_name: "{env_name}"
    scheme: https
    metrics_path: /v2/metrics/cloud/export
    basic_auth:
      username: "{key}"
      password: "{secret}"
    params:
      resource.kafka.id: ["{cid}"]
      metric:
        - io.confluent.kafka.server/received_bytes
        - io.confluent.kafka.server/sent_bytes
        - io.confluent.kafka.server/retained_bytes
        - io.confluent.kafka.server/partition_count
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: confluent_kafka_server_received_bytes|confluent_kafka_server_sent_bytes|confluent_kafka_server_retained_bytes|confluent_kafka_server_partition_count|confluent_scrape_resource_access_error
        action: keep"""
            jobs.append(job)

        jobs_joined = "\n\n".join(jobs)
        content = f"""global:
  scrape_interval: {self.scrape_interval}
  scrape_timeout: {self.scrape_timeout}

scrape_configs:
{jobs_joined}
"""

        # Write to path only if content has changed
        try:
            self.prom_config_path.parent.mkdir(parents=True, exist_ok=True)
            if self.prom_config_path.exists():
                try:
                    with open(self.prom_config_path, "r", encoding="utf-8") as f:
                        if f.read() == content:
                            return True
                except Exception:
                    pass

            with open(self.prom_config_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Prometheus config updated and written successfully to {self.prom_config_path} ({len(jobs)} jobs)")
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
