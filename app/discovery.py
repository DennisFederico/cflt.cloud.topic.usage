import re
import requests
from typing import Set, Any, Dict, List, Tuple

class ClusterDiscovery:
    def __init__(self, api_key: str, api_secret: str, org_id: str = "default-org", org_name: str = "Default Organization"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.org_id = org_id or "default-org"
        self.org_name = org_name or "Default Organization"
        self.base_url = "https://api.telemetry.confluent.cloud/v2/metrics/cloud"

    def discover_clusters(self) -> dict[str, dict[str, str]]:
        """
        Query Confluent Cloud REST endpoints to find all Kafka Clusters and their environments.
        Primary: Org API (/org/v2/organizations -> /org/v2/environments -> /cmk/v2/clusters)
        Fallback: Telemetry discovery & descriptors
        """
        if self.api_key == "MOCK" or self.api_secret == "MOCK":
            print(f"Discovery ({self.org_name}): Using mock cluster list")
            prefix = re.sub(r'[^a-zA-Z0-9]', '', self.org_id.lower())[:6] or "mock"
            return {
                f"lkc-{prefix}-prod": {
                    "name": f"{self.org_name} - Production Cluster",
                    "org_id": self.org_id,
                    "org_name": self.org_name,
                    "environment_id": f"env-{prefix}-prod",
                    "environment_name": f"{self.org_name} Production",
                    "kafka_api_endpoint": ""
                },
                f"lkc-{prefix}-staging": {
                    "name": f"{self.org_name} - Staging Cluster",
                    "org_id": self.org_id,
                    "org_name": self.org_name,
                    "environment_id": f"env-{prefix}-staging",
                    "environment_name": f"{self.org_name} Staging",
                    "kafka_api_endpoint": ""
                }
            }

        if not self.api_key or not self.api_secret:
            print(f"Discovery skipped for {self.org_name}: Credentials not set.")
            return {}

        clusters_metadata = {}
        try:
            # 0. Try to discover organization display name if default or generic
            try:
                org_url = "https://api.confluent.cloud/org/v2/organizations"
                org_resp = requests.get(
                    org_url,
                    auth=(self.api_key, self.api_secret),
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                if org_resp.status_code == 200:
                    org_list = org_resp.json().get("data") or []
                    if org_list:
                        disp_name = org_list[0].get("display_name")
                        remote_id = org_list[0].get("id")
                        if disp_name and (self.org_name == "Default Organization" or self.org_name.startswith("Organization ")):
                            self.org_name = disp_name
                        if remote_id and self.org_id in ("default-org", ""):
                            self.org_id = remote_id
            except Exception as e:
                # Telemetry-only credentials might not have access to orgs endpoint, ignore
                pass

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
                            "org_id": self.org_id,
                            "org_name": self.org_name,
                            "environment_id": env_id,
                            "environment_name": env_name or "",
                            "kafka_api_endpoint": endpoint
                        }
                    
                    cluster_url = data.get("metadata", {}).get("next")
                    cluster_params = None
            
            print(f"Org-based discovery for '{self.org_name}' found clusters: {list(clusters_metadata.keys())}")
            return clusters_metadata

        except Exception as e:
            print(f"Error querying Confluent Cloud Org API for '{self.org_name}': {e}. Falling back to telemetry endpoints.")
            return self._discover_fallback()

    def _discover_fallback(self) -> dict[str, dict[str, str]]:
        """Fallback to telemetry endpoints to discover cluster IDs."""
        cluster_ids = self._discover_telemetry_primary()
        if not cluster_ids:
            cluster_ids = self._discover_telemetry_secondary()
            
        return {
            cid: {
                "name": f"Cluster {cid}",
                "org_id": self.org_id,
                "org_name": self.org_name,
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
            print(f"Error querying telemetry discovery endpoint for '{self.org_name}': {e}")
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
            print(f"Error in telemetry secondary discovery for '{self.org_name}': {e}")
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


class MultiOrgClusterDiscovery:
    def __init__(self, organizations: List[Dict[str, Any]]):
        self.organizations = organizations

    def discover_all_clusters(self) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
        """
        Discovers clusters across all configured organizations.
        Returns:
            all_clusters: dict of cluster_id -> cluster metadata
            updated_org_names: dict of org_id -> discovered org display name
        """
        all_clusters: Dict[str, Dict[str, str]] = {}
        updated_org_names: Dict[str, str] = {}

        # If in Mock mode and only default org exists, provide rich multi-org mock dataset
        mock_org = next((o for o in self.organizations if o.get("api_key") == "MOCK"), None)
        if mock_org and len(self.organizations) <= 1:
            print("MultiOrgDiscovery: Injecting multi-org mock demonstration clusters")
            mock_clusters = {
                "lkc-santander-prod-emea": {
                    "name": "Santander Cards Core EMEA",
                    "org_id": "org-santander-prod",
                    "org_name": "Santander Global Cards (Production)",
                    "environment_id": "env-prod-emea",
                    "environment_name": "EMEA Production",
                    "kafka_api_endpoint": ""
                },
                "lkc-santander-prod-latam": {
                    "name": "Santander Cards LATAM Gateway",
                    "org_id": "org-santander-prod",
                    "org_name": "Santander Global Cards (Production)",
                    "environment_id": "env-prod-latam",
                    "environment_name": "LATAM Production",
                    "kafka_api_endpoint": ""
                },
                "lkc-santander-qa-pci": {
                    "name": "Santander Cards QA PCI-DSS",
                    "org_id": "org-santander-qa",
                    "org_name": "Santander Global Cards (Non-Prod)",
                    "environment_id": "env-qa-pci",
                    "environment_name": "QA PCI Environment",
                    "kafka_api_endpoint": ""
                },
                "lkc-santander-dev-sandbox": {
                    "name": "Santander Cards Dev Playground",
                    "org_id": "org-santander-qa",
                    "org_name": "Santander Global Cards (Non-Prod)",
                    "environment_id": "env-dev-sandbox",
                    "environment_name": "Dev Sandbox",
                    "kafka_api_endpoint": ""
                }
            }
            return mock_clusters, {
                "org-santander-prod": "Santander Global Cards (Production)",
                "org-santander-qa": "Santander Global Cards (Non-Prod)"
            }

        for org in self.organizations:
            org_id = org.get("id") or "default-org"
            org_name = org.get("name") or f"Organization {org_id}"
            api_key = org.get("api_key") or ""
            api_secret = org.get("api_secret") or ""

            if not api_key or not api_secret:
                print(f"Skipping discovery for organization '{org_name}' ({org_id}): missing API credentials")
                continue

            try:
                discoverer = ClusterDiscovery(api_key, api_secret, org_id=org_id, org_name=org_name)
                clusters = discoverer.discover_clusters()
                all_clusters.update(clusters)
                if discoverer.org_name and discoverer.org_name != org_name:
                    updated_org_names[org_id] = discoverer.org_name
            except Exception as e:
                print(f"Failed discovery for organization '{org_name}' ({org_id}): {e}")

        print(f"Multi-org discovery completed. Total discovered clusters: {len(all_clusters)}")
        return all_clusters, updated_org_names
