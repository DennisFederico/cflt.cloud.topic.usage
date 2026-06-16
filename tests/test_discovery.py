import sys
from pathlib import Path
import pytest
from unittest.mock import patch, Mock

# Add prometheus-topic-usage/app/ to path so we can import discovery
sys.path.append(str(Path(__file__).parent.parent / "prometheus-topic-usage" / "app"))

from discovery import ClusterDiscovery

def test_mock_discovery():
    discovery = ClusterDiscovery("MOCK", "MOCK")
    clusters = discovery.discover_clusters()
    assert clusters == {
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

def test_missing_credentials():
    discovery = ClusterDiscovery("", "")
    clusters = discovery.discover_clusters()
    assert clusters == {}

@patch("requests.get")
def test_org_discovery_success(mock_get):
    # Setup mock responses for Org API discovery
    env_response = Mock()
    env_response.json.return_value = {
        "data": [
            {"id": "env-prod", "display_name": "Production"},
            {"id": "env-dev", "display_name": "Development"}
        ],
        "metadata": {}
    }
    env_response.status_code = 200

    prod_clusters_response = Mock()
    prod_clusters_response.json.return_value = {
        "data": [
            {
                "id": "lkc-prod1",
                "spec": {
                    "display_name": "Prod Cluster 1",
                    "http_endpoint": "https://pkc-prod1.aws.confluent.cloud:443"
                }
            }
        ],
        "metadata": {}
    }
    prod_clusters_response.status_code = 200

    dev_clusters_response = Mock()
    dev_clusters_response.json.return_value = {
        "data": [
            {
                "id": "lkc-dev1",
                "spec": {
                    "display_name": "Dev Cluster 1",
                    "api_endpoint": "https://pkc-dev1.aws.confluent.cloud:443"
                }
            }
        ],
        "metadata": {}
    }
    dev_clusters_response.status_code = 200

    mock_get.side_effect = [env_response, prod_clusters_response, dev_clusters_response]

    discovery = ClusterDiscovery("real_key", "real_secret")
    clusters = discovery.discover_clusters()

    assert mock_get.call_count == 3
    assert clusters == {
        "lkc-prod1": {
            "name": "Prod Cluster 1",
            "environment_id": "env-prod",
            "environment_name": "Production",
            "kafka_api_endpoint": "https://pkc-prod1.aws.confluent.cloud:443"
        },
        "lkc-dev1": {
            "name": "Dev Cluster 1",
            "environment_id": "env-dev",
            "environment_name": "Development",
            "kafka_api_endpoint": "https://pkc-dev1.aws.confluent.cloud:443"
        }
    }

@patch("requests.get")
def test_org_discovery_paginated(mock_get):
    # Page 1 environments pointing to Page 2
    env_page1 = Mock()
    env_page1.json.return_value = {
        "data": [{"id": "env-prod", "display_name": "Production"}],
        "metadata": {"next": "https://api.confluent.cloud/org/v2/environments?page_token=page2"}
    }
    env_page1.status_code = 200

    # Page 2 environments
    env_page2 = Mock()
    env_page2.json.return_value = {
        "data": [{"id": "env-dev", "display_name": "Development"}],
        "metadata": {}
    }
    env_page2.status_code = 200

    # Clusters for env-prod (paginated: page 1 points to page 2)
    prod_clusters_page1 = Mock()
    prod_clusters_page1.json.return_value = {
        "data": [
            {
                "id": "lkc-prod1",
                "spec": {"display_name": "Prod Cluster 1"}
            }
        ],
        "metadata": {"next": "https://api.confluent.cloud/cmk/v2/clusters?environment=env-prod&page_token=prod_page2"}
    }
    prod_clusters_page1.status_code = 200

    prod_clusters_page2 = Mock()
    prod_clusters_page2.json.return_value = {
        "data": [
            {
                "id": "lkc-prod2",
                "spec": {"display_name": "Prod Cluster 2"}
            }
        ],
        "metadata": {}
    }
    prod_clusters_page2.status_code = 200

    # Clusters for env-dev (single page)
    dev_clusters = Mock()
    dev_clusters.json.return_value = {
        "data": [
            {
                "id": "lkc-dev1",
                "spec": {"display_name": "Dev Cluster 1"}
            }
        ],
        "metadata": {}
    }
    dev_clusters.status_code = 200

    mock_get.side_effect = [env_page1, env_page2, prod_clusters_page1, prod_clusters_page2, dev_clusters]

    discovery = ClusterDiscovery("real_key", "real_secret")
    clusters = discovery.discover_clusters()

    assert mock_get.call_count == 5
    assert clusters == {
        "lkc-prod1": {
            "name": "Prod Cluster 1",
            "environment_id": "env-prod",
            "environment_name": "Production",
            "kafka_api_endpoint": ""
        },
        "lkc-prod2": {
            "name": "Prod Cluster 2",
            "environment_id": "env-prod",
            "environment_name": "Production",
            "kafka_api_endpoint": ""
        },
        "lkc-dev1": {
            "name": "Dev Cluster 1",
            "environment_id": "env-dev",
            "environment_name": "Development",
            "kafka_api_endpoint": ""
        }
    }

@patch("requests.get")
def test_org_discovery_fallback_to_telemetry(mock_get):
    # Org API call fails
    org_fail_response = Mock()
    org_fail_response.raise_for_status.side_effect = Exception("HTTP 403 Forbidden")
    
    # Fallback telemetry primary succeeds with flat data
    telemetry_response = Mock()
    telemetry_response.json.return_value = [
        {"resource_id": "lkc-telemetry1"},
        {"resource_id": "lkc-telemetry2"}
    ]
    telemetry_response.status_code = 200

    mock_get.side_effect = [org_fail_response, telemetry_response]

    discovery = ClusterDiscovery("real_key", "real_secret")
    clusters = discovery.discover_clusters()

    assert mock_get.call_count == 2
    assert clusters == {
        "lkc-telemetry1": {
            "name": "Cluster lkc-telemetry1",
            "environment_id": "",
            "environment_name": "",
            "kafka_api_endpoint": ""
        },
        "lkc-telemetry2": {
            "name": "Cluster lkc-telemetry2",
            "environment_id": "",
            "environment_name": "",
            "kafka_api_endpoint": ""
        }
    }
