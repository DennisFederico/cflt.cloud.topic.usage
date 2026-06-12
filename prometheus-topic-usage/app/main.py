import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import re
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import requests

from config_manager import ConfigManager
from discovery import ClusterDiscovery

# Config
config_mgr = ConfigManager()
discovery = ClusterDiscovery(config_mgr.cloud_api_key, config_mgr.cloud_api_secret)

# Parse interval
interval_str = os.getenv("CLUSTER_CHECK_INTERVAL", "3h")
def parse_interval_to_seconds(val: str) -> int:
    match = re.match(r"^(\d+)([smh])$", val.strip().lower())
    if not match:
        return 3 * 3600  # Default to 3 hours
    amount, unit = int(match.group(1)), match.group(2)
    if unit == "s": return amount
    if unit == "m": return amount * 60
    if unit == "h": return amount * 3600
    return 3 * 3600

check_interval_seconds = parse_interval_to_seconds(interval_str)

async def check_and_update_clusters():
    """Discover clusters, save new ones, and update Prometheus config."""
    print("Running cluster discovery loop...")
    try:
        discovered_cids = discovery.discover_clusters()
        if not discovered_cids:
            print("No clusters discovered.")
            return

        config = config_mgr.load_clusters_config()
        if "clusters" not in config:
            config["clusters"] = {}

        changed = False
        for cid in discovered_cids:
            if cid not in config["clusters"]:
                config["clusters"][cid] = {
                    "name": f"Cluster {cid}",
                    "kafka_api_endpoint": "",
                    "kafka_api_key": "",
                    "kafka_api_secret": "",
                    "configured": False
                }
                changed = True

        # Clean up config if a cluster is no longer returned (optional, but keep for consistency)
        # We only remove it if it was never configured
        for cid in list(config["clusters"].keys()):
            if cid not in discovered_cids and not config["clusters"][cid].get("configured", False):
                del config["clusters"][cid]
                changed = True

        if changed or not config_mgr.prom_config_path.exists():
            print("Cluster list changed or config missing. Generating new prometheus.yml...")
            config_mgr.save_clusters_config(config)
            config_mgr.generate_prometheus_config(list(discovered_cids))
        else:
            # Generate config anyway if file doesn't exist, just in case
            if not config_mgr.prom_config_path.exists():
                config_mgr.generate_prometheus_config(list(discovered_cids))
            else:
                print("No changes in cluster list. Configuration is up to date.")
    except Exception as e:
        print(f"Error in check_and_update_clusters background loop: {e}")

async def run_scheduler():
    """Background polling loop for cluster discovery."""
    print(f"Starting discovery scheduler with polling interval: {interval_str} ({check_interval_seconds}s)")
    # Wait 5 seconds on startup for containers/network to stabilize
    await asyncio.sleep(5)
    while True:
        await check_and_update_clusters()
        try:
            await asyncio.sleep(check_interval_seconds)
        except asyncio.CancelledError:
            break

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: run check once immediately, then start background loop
    asyncio.create_task(check_and_update_clusters())
    task = asyncio.create_task(run_scheduler())
    yield
    # Shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    title="Confluent Cloud Topic Usage Dashboard",
    description="Monitors Kafka topics and traffic status via Prometheus and Kafka REST.",
    version="1.0.0",
    lifespan=lifespan
)

class ClusterConfigModel(BaseModel):
    name: str
    kafka_api_endpoint: str
    kafka_api_key: str
    kafka_api_secret: str

def resolve_cluster_name_from_prometheus(prom_url: str, cluster_id: str) -> str | None:
    """Query Prometheus series endpoint for the kafka_name label associated with the cluster ID."""
    url = f"{prom_url}/api/v1/series"
    # Try cflt_cluster_id
    try:
        response = requests.get(url, params={"match[]": f'{{cflt_cluster_id="{cluster_id}"}}'}, timeout=5)
        if response.status_code == 200:
            data = response.json().get("data", [])
            for item in data:
                name = item.get("kafka_name")
                if name:
                    return name
    except Exception as e:
        print(f"Error querying series by cflt_cluster_id: {e}")

    # Fallback to kafka_id
    try:
        response = requests.get(url, params={"match[]": f'{{kafka_id="{cluster_id}"}}'}, timeout=5)
        if response.status_code == 200:
            data = response.json().get("data", [])
            for item in data:
                name = item.get("kafka_name")
                if name:
                    return name
    except Exception as e:
        print(f"Error querying series by kafka_id: {e}")
    return None

@app.get("/api/clusters")
def get_clusters():
    """Retrieve the list of discovered and configured clusters."""
    config = config_mgr.load_clusters_config()
    clusters = config.get("clusters", {})
    
    # Try to dynamically resolve names from Prometheus for default-named clusters
    updated = False
    for cid, cluster in clusters.items():
        if cluster.get("name", "").startswith("Cluster lkc-"):
            resolved_name = resolve_cluster_name_from_prometheus(config_mgr.prom_url, cid)
            if resolved_name:
                cluster["name"] = resolved_name
                updated = True
                
    if updated:
        config_mgr.save_clusters_config(config)
        
    return clusters

@app.post("/api/clusters/{cluster_id}/config")
def configure_cluster(cluster_id: str, payload: ClusterConfigModel):
    """Save/update Kafka REST API credentials for a cluster."""
    config = config_mgr.load_clusters_config()
    if "clusters" not in config:
        config["clusters"] = {}

    config["clusters"][cluster_id] = {
        "name": payload.name.strip() or f"Cluster {cluster_id}",
        "kafka_api_endpoint": payload.kafka_api_endpoint.strip().rstrip("/"),
        "kafka_api_key": payload.kafka_api_key.strip(),
        "kafka_api_secret": payload.kafka_api_secret.strip(),
        "configured": bool(payload.kafka_api_endpoint.strip() and payload.kafka_api_key.strip())
    }
    config_mgr.save_clusters_config(config)
    return {"status": "success", "cluster": config["clusters"][cluster_id]}

@app.post("/api/clusters/trigger-discovery")
async def trigger_discovery(background_tasks: BackgroundTasks):
    """Trigger cluster discovery on demand."""
    background_tasks.add_task(check_and_update_clusters)
    return {"status": "Discovery triggered in background"}

def query_prom_bytes(prom_url: str, query: str) -> Dict[str, float]:
    """Execute PromQL instant query and return a map of topic -> sum value."""
    url = f"{prom_url}/api/v1/query"
    try:
        response = requests.get(url, params={"query": query}, timeout=10)
        response.raise_for_status()
        res_json = response.json()
        if res_json.get("status") != "success":
            return {}
        
        result_list = res_json.get("data", {}).get("result", [])
        totals = {}
        for item in result_list:
            metric = item.get("metric", {})
            topic = metric.get("topic")
            value = item.get("value")
            if topic and isinstance(value, list) and len(value) == 2:
                try:
                    totals[topic] = float(value[1])
                except (ValueError, TypeError):
                    continue
        return totals
    except Exception as e:
        print(f"Error querying Prometheus: {e}")
        return {}

def list_kafka_topics(endpoint: str, cluster_id: str, key: str, secret: str) -> List[str]:
    """Retrieve the inventory of active topics from Kafka REST API."""
    topics = []
    next_url = f"{endpoint}/kafka/v3/clusters/{cluster_id}/topics"
    params = {"page_size": 100}

    try:
        while next_url:
            response = requests.get(next_url, params=params, auth=(key, secret), timeout=10)
            response.raise_for_status()
            payload = response.json()
            params = None  # query params only on first page

            items = payload.get("data", [])
            for item in items:
                name = item.get("topic_name")
                if name:
                    topics.append(name)
            
            next_url = payload.get("metadata", {}).get("next") or payload.get("links", {}).get("next")
        return sorted(list(set(topics)))
    except Exception as e:
        print(f"Error fetching Kafka topics from REST API: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Could not connect to Kafka REST endpoint: {str(e)}"
        )

@app.get("/api/clusters/{cluster_id}/usage")
def get_cluster_usage(cluster_id: str, period: str = "30d"):
    """
    Returns the dynamic list of topics and their Prometheus traffic stats.
    Cross-references with Kafka REST API if credentials exist.
    """
    if not re.match(r"^\d+[smhdw]$", period):
        raise HTTPException(status_code=400, detail="Invalid period. Examples: 30d, 7d, 24h.")

    config = config_mgr.load_clusters_config()
    cluster = config.get("clusters", {}).get(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found.")

    # Simulator Mode Interceptor
    if cluster_id.startswith("lkc-mock-") or config_mgr.cloud_api_key == "MOCK":
        is_configured = cluster.get("configured", False)
        inventory = ["orders-v1", "customers-v2", "payments-v1", "billing-events", "analytics-raw", "temp-debug-topic", "schema-changes"]
        mock_topics = []
        for i, topic in enumerate(inventory):
            if topic == "temp-debug-topic" or (topic == "schema-changes" and is_configured):
                mock_topics.append({
                    "topic": topic,
                    "bytes_in": 0,
                    "bytes_out": 0,
                    "status": "unused" if is_configured else "active"
                })
            else:
                bytes_in_val = (i + 1) * 1024 * 500
                bytes_out_val = (i + 1) * 1024 * 900
                mock_topics.append({
                    "topic": topic,
                    "bytes_in": bytes_in_val,
                    "bytes_out": bytes_out_val,
                    "status": "active"
                })
        
        if not is_configured:
            mock_topics = [t for t in mock_topics if t["bytes_in"] > 0]

        total = len(mock_topics)
        unused = sum(1 for t in mock_topics if t["status"] == "unused")
        active = total - unused

        return {
            "cluster_id": cluster_id,
            "name": cluster.get("name", f"Cluster {cluster_id}"),
            "configured": is_configured,
            "total_topics_count": total,
            "active_topics_count": active,
            "unused_topics_count": unused,
            "topics": mock_topics
        }

    # 1. Query Prometheus for bytes in and out
    # Try cflt_cluster_id first, then fallback to kafka_id
    query_in = f'sum by (topic) (sum_over_time(confluent_kafka_server_received_bytes{{cflt_cluster_id="{cluster_id}"}}[{period}]))'
    query_out = f'sum by (topic) (sum_over_time(confluent_kafka_server_sent_bytes{{cflt_cluster_id="{cluster_id}"}}[{period}]))'
    
    bytes_in = query_prom_bytes(config_mgr.prom_url, query_in)
    bytes_out = query_prom_bytes(config_mgr.prom_url, query_out)

    # Fallback to kafka_id if we got 0 metrics
    if not bytes_in and not bytes_out:
        query_in_fb = f'sum by (topic) (sum_over_time(confluent_kafka_server_received_bytes{{kafka_id="{cluster_id}"}}[{period}]))'
        query_out_fb = f'sum by (topic) (sum_over_time(confluent_kafka_server_sent_bytes{{kafka_id="{cluster_id}"}}[{period}]))'
        bytes_in = query_prom_bytes(config_mgr.prom_url, query_in_fb)
        bytes_out = query_prom_bytes(config_mgr.prom_url, query_out_fb)

    topics_list = []
    is_configured = cluster.get("configured", False)

    if is_configured:
        # Get actual inventory from Kafka REST API
        try:
            inventory = list_kafka_topics(
                cluster["kafka_api_endpoint"],
                cluster_id,
                cluster["kafka_api_key"],
                cluster["kafka_api_secret"]
            )
        except HTTPException as he:
            raise he
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Kafka REST API call failed: {e}")

        # Combine inventory and Prometheus stats
        for topic in inventory:
            bin_val = bytes_in.get(topic, 0.0)
            bout_val = bytes_out.get(topic, 0.0)
            is_unused = (bin_val == 0.0) and (bout_val == 0.0)
            topics_list.append({
                "topic": topic,
                "bytes_in": int(bin_val) if bin_val.is_integer() else bin_val,
                "bytes_out": int(bout_val) if bout_val.is_integer() else bout_val,
                "status": "unused" if is_unused else "active"
            })
    else:
        # Unconfigured - only return topics that have telemetry in Prometheus
        all_metric_topics = set(bytes_in.keys()).union(set(bytes_out.keys()))
        for topic in sorted(list(all_metric_topics)):
            bin_val = bytes_in.get(topic, 0.0)
            bout_val = bytes_out.get(topic, 0.0)
            topics_list.append({
                "topic": topic,
                "bytes_in": int(bin_val) if bin_val.is_integer() else bin_val,
                "bytes_out": int(bout_val) if bout_val.is_integer() else bout_val,
                "status": "active"  # Since they have timeseries, we assume active
            })

    # Count stats
    total = len(topics_list)
    unused = sum(1 for t in topics_list if t["status"] == "unused")
    active = total - unused

    return {
        "cluster_id": cluster_id,
        "name": cluster.get("name", f"Cluster {cluster_id}"),
        "configured": is_configured,
        "total_topics_count": total,
        "active_topics_count": active,
        "unused_topics_count": unused,
        "topics": topics_list
    }

# Serve static dashboard
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/", StaticFiles(directory=str(static_path), html=True), name="static")
