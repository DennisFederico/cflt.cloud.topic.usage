import re
import requests
from typing import Set, Any

class ClusterDiscovery:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.telemetry.confluent.cloud/v2/metrics/cloud"

    def discover_clusters(self) -> Set[str]:
        """
        Query Confluent Cloud telemetry endpoints to find all Kafka Cluster IDs.
        Primary: /discovery
        Fallback: /descriptors/resources (schema descriptors)
        """
        if self.api_key == "MOCK" or self.api_secret == "MOCK":
            print("Discovery: Using mock cluster list (lkc-mock-prod, lkc-mock-staging)")
            return {"lkc-mock-prod", "lkc-mock-staging"}

        if not self.api_key or not self.api_secret:
            print("Discovery skipped: Credentials CLOUD_API_KEY/CLOUD_API_SECRET not set.")
            return set()

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
            cluster_ids = self._recursive_extract_cids(data)
            print(f"Discovery found clusters: {list(cluster_ids)}")
            return cluster_ids
        except Exception as e:
            print(f"Error querying telemetry discovery endpoint: {e}")
            return self._discover_fallback()

    def _discover_fallback(self) -> Set[str]:
        """Fallback querying the descriptors/resources endpoint."""
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
            cluster_ids = self._recursive_extract_cids(data)
            print(f"Fallback discovery found clusters: {list(cluster_ids)}")
            return cluster_ids
        except Exception as e:
            print(f"Error in fallback discovery: {e}")
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
