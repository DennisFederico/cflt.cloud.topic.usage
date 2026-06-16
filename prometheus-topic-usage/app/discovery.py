import re
import requests
from typing import Set, Any

class ClusterDiscovery:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.telemetry.confluent.cloud/v2/metrics/cloud"

    def discover_clusters(self) -> dict[str, dict[str, str]]:
        """
        Query Confluent Cloud REST endpoints to find all Kafka Clusters and their environments.
        Primary: Org API (/org/v2/environments -> /cmk/v2/clusters)
        Fallback: Telemetry discovery & descriptors
        """
        if self.api_key == "MOCK" or self.api_secret == "MOCK":
            print("Discovery: Using mock cluster list (lkc-mock-prod, lkc-mock-staging)")
            return {
                "lkc-mock-prod": {
                    "name": "Mock Production Cluster",
                    "environment_id": "env-mock-prod",
                    "environment_name": "Mock Production",
                    "kafka_api_endpoint": ""
                },
                "lkc-mock-staging": {
                    "name": "Mock Staging Cluster",
                    "environment_id": "env-mock-staging",
                    "environment_name": "Mock Staging",
                    "kafka_api_endpoint": ""
                }
            }

        if not self.api_key or not self.api_secret:
            print("Discovery skipped: Credentials CLOUD_API_KEY/CLOUD_API_SECRET not set.")
            return {}

        clusters_metadata = {}
        try:
            # 1. Discover environments
            environments = []
            env_url = "https://api.confluent.cloud/org/v2/environments"
            env_params = {"page_size": 100}
            
            while env_url:
                response = requests.get(
                    env_url,
                    auth=(self.api_key, self.api_secret),
                    headers={"Content-Type": "application/json"},
                    params=env_params,
                    timeout=15
                )
                response.raise_for_status()
                data = response.json()
                
                env_list = data.get("data") or []
                for env in env_list:
                    env_id = env.get("id")
                    env_name = env.get("display_name")
                    if env_id:
                        environments.append((env_id, env_name))
                
                env_url = data.get("metadata", {}).get("next")
                env_params = None  # query parameters are in the next url already
            
            # 2. Discover clusters per environment
            for env_id, env_name in environments:
                cluster_url = "https://api.confluent.cloud/cmk/v2/clusters"
                cluster_params = {"environment": env_id, "page_size": 100}
                
                while cluster_url:
                    response = requests.get(
                        cluster_url,
                        auth=(self.api_key, self.api_secret),
                        headers={"Content-Type": "application/json"},
                        params=cluster_params,
                        timeout=15
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    cluster_list = data.get("data") or []
                    for cluster in cluster_list:
                        cluster_id = cluster.get("id")
                        if not cluster_id:
                            continue
                        
                        spec = cluster.get("spec", {})
                        cluster_name = spec.get("display_name")
                        endpoint = spec.get("http_endpoint") or spec.get("api_endpoint") or ""
                        
                        clusters_metadata[cluster_id] = {
                            "name": cluster_name or f"Cluster {cluster_id}",
                            "environment_id": env_id,
                            "environment_name": env_name or "",
                            "kafka_api_endpoint": endpoint
                        }
                    
                    cluster_url = data.get("metadata", {}).get("next")
                    cluster_params = None
            
            print(f"Org-based discovery found clusters: {list(clusters_metadata.keys())}")
            return clusters_metadata

        except Exception as e:
            print(f"Error querying Confluent Cloud Org API: {e}. Falling back to telemetry endpoints.")
            return self._discover_fallback()

    def _discover_fallback(self) -> dict[str, dict[str, str]]:
        """Fallback to telemetry endpoints to discover cluster IDs."""
        cluster_ids = self._discover_telemetry_primary()
        if not cluster_ids:
            cluster_ids = self._discover_telemetry_secondary()
            
        return {
            cid: {
                "name": f"Cluster {cid}",
                "environment_id": "",
                "environment_name": "",
                "kafka_api_endpoint": ""
            }
            for cid in cluster_ids
        }

    def _discover_telemetry_primary(self) -> Set[str]:
        """Query Confluent Cloud telemetry discovery endpoint to find Kafka Cluster IDs."""
        url = f"{self.base_url}/discovery"
        try:
            response = requests.get(
                url,
                auth=(self.api_key, self.api_secret),
                headers={"Content-Type": "application/json"},
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            return self._recursive_extract_cids(data)
        except Exception as e:
            print(f"Error querying telemetry discovery endpoint: {e}")
            return set()

    def _discover_telemetry_secondary(self) -> Set[str]:
        """Fallback querying the telemetry descriptors/resources endpoint."""
        url = f"{self.base_url}/descriptors/resources"
        try:
            response = requests.get(
                url,
                auth=(self.api_key, self.api_secret),
                headers={"Content-Type": "application/json"},
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            return self._recursive_extract_cids(data)
        except Exception as e:
            print(f"Error in telemetry secondary discovery: {e}")
            return set()

    def _recursive_extract_cids(self, data: Any) -> Set[str]:
        """Recursively scan JSON payload for any strings containing lkc-xxxxx cluster IDs."""
        cids = set()
        pattern = re.compile(r"lkc-[a-zA-Z0-9]+")

        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, str):
                    matches = pattern.findall(v)
                    for match in matches:
                        cids.add(match)
                else:
                    cids.update(self._recursive_extract_cids(v))
        elif isinstance(data, list):
            for item in data:
                cids.update(self._recursive_extract_cids(item))

        return cids
