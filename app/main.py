import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import requests

from config_manager import ConfigManager
from discovery import MultiOrgClusterDiscovery

# Config
config_mgr = ConfigManager()

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
    """Discover clusters across all organizations, save new ones, and update Prometheus config."""
    print("Running multi-org cluster discovery loop...")
    try:
        orgs = config_mgr.get_organizations_list(include_secrets=True)
        if not orgs:
            print("Discovery skipped: No organizations configured.")
            return

        discovery = MultiOrgClusterDiscovery(orgs)
        discovered_clusters, updated_org_names = discovery.discover_all_clusters()
        if not discovered_clusters:
            print("No clusters discovered.")
            return

        config = config_mgr.load_clusters_config()
        if "clusters" not in config:
            config["clusters"] = {}
        if "organizations" not in config:
            config["organizations"] = {}

        changed = False

        # Update discovered org display names
        for oid, oname in updated_org_names.items():
            if oid in config["organizations"] and config["organizations"][oid].get("name") != oname:
                config["organizations"][oid]["name"] = oname
                changed = True

        for cid, meta in discovered_clusters.items():
            if cid not in config["clusters"]:
                config["clusters"][cid] = {
                    "name": meta.get("name") or f"Cluster {cid}",
                    "org_id": meta.get("org_id") or "default-org",
                    "org_name": meta.get("org_name") or "Default Organization",
                    "environment_id": meta.get("environment_id") or "",
                    "environment_name": meta.get("environment_name") or "",
                    "kafka_api_endpoint": meta.get("kafka_api_endpoint") or "",
                    "kafka_api_key": "",
                    "kafka_api_secret": "",
                    "configured": False
                }
                changed = True
            else:
                cluster_entry = config["clusters"][cid]
                # Update organization and environment metadata if present
                for key in ["org_id", "org_name", "environment_id", "environment_name"]:
                    if key in meta and meta[key] and cluster_entry.get(key) != meta[key]:
                        cluster_entry[key] = meta[key]
                        changed = True

                # If name is default, update to display name
                if meta.get("name") and (cluster_entry.get("name") == f"Cluster {cid}" or not cluster_entry.get("name")):
                    cluster_entry["name"] = meta["name"]
                    changed = True

                # Update endpoint if not currently set
                if meta.get("kafka_api_endpoint") and not cluster_entry.get("kafka_api_endpoint"):
                    cluster_entry["kafka_api_endpoint"] = meta["kafka_api_endpoint"]
                    changed = True

        # Clean up unconfigured clusters that are no longer returned
        for cid in list(config["clusters"].keys()):
            if cid not in discovered_clusters and not config["clusters"][cid].get("configured", False):
                del config["clusters"][cid]
                changed = True

        if changed:
            print("Cluster/Org list changed. Saving updated clusters config...")
            config_mgr.save_clusters_config(config)

        # Check and ensure prometheus.yml is in sync with latest generator logic
        config_mgr.generate_prometheus_config(config["clusters"])
    except Exception as e:
        print(f"Error in check_and_update_clusters background loop: {e}")

async def run_scheduler():
    """Background polling loop for cluster discovery."""
    print(f"Starting discovery scheduler with polling interval: {interval_str} ({check_interval_seconds}s)")
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

class OrganizationModel(BaseModel):
    id: Optional[str] = None
    name: str
    api_key: str
    api_secret: str

class ClusterConfigModel(BaseModel):
    name: str
    kafka_api_endpoint: str
    kafka_api_key: str
    kafka_api_secret: str
    org_id: Optional[str] = None

@app.get("/api/organizations")
def get_organizations():
    """Retrieve all configured organizations with metadata and counts."""
    config = config_mgr.load_clusters_config()
    orgs = config_mgr.get_organizations(include_secrets=False)
    clusters = config.get("clusters", {})

    for oid, odata in orgs.items():
        org_clusters = [c for c in clusters.values() if c.get("org_id") == oid]
        org_envs = {c.get("environment_id") for c in org_clusters if c.get("environment_id")}
        odata["clusters_count"] = len(org_clusters)
        odata["environments_count"] = len(org_envs)

    return list(orgs.values())

@app.post("/api/organizations")
async def create_or_update_organization(payload: OrganizationModel, background_tasks: BackgroundTasks):
    """Add or update an organization and trigger discovery scan."""
    saved = config_mgr.save_organization(payload.id, payload.name, payload.api_key, payload.api_secret)
    background_tasks.add_task(check_and_update_clusters)
    return {"status": "success", "organization": saved}

@app.delete("/api/organizations/{org_id}")
def delete_organization(org_id: str):
    """Delete an organization and refresh Prometheus."""
    success = config_mgr.delete_organization(org_id)
    if not success:
        raise HTTPException(status_code=404, detail="Organization not found.")
    config = config_mgr.load_clusters_config()
    config_mgr.generate_prometheus_config(config.get("clusters", {}))
    return {"status": "success", "deleted_org_id": org_id}

@app.get("/api/rate-limits")
def get_rate_limits():
    """
    Returns telemetry scrape rate metrics, Confluent Cloud API quotas,
    and explanation of origin IP vs. organization rate limits.
    """
    config = config_mgr.load_clusters_config()
    clusters_count = len(config.get("clusters", {}))
    interval = config_mgr.scrape_interval.strip().lower()
    
    # Calculate interval in seconds
    interval_sec = 60
    if interval.endswith("m"):
        interval_sec = int(interval[:-1]) * 60
    elif interval.endswith("s"):
        interval_sec = int(interval[:-1])
    elif interval.endswith("h"):
        interval_sec = int(interval[:-1]) * 3600

    requests_per_minute = round((clusters_count * 60) / max(interval_sec, 1), 2)
    requests_per_second = round(requests_per_minute / 60, 2)
    hourly_scrapes_per_cluster = round(3600 / max(interval_sec, 1), 1)

    return {
        "global_ip_rate_limit_per_minute": 300,
        "global_ip_rate_limit_per_second": 5,
        "endpoint_resource_rate_limit_per_hour": 160,
        "configured_scrape_interval": config_mgr.scrape_interval,
        "configured_scrape_timeout": config_mgr.scrape_timeout,
        "total_monitored_clusters": clusters_count,
        "current_requests_per_minute": requests_per_minute,
        "current_requests_per_second": requests_per_second,
        "hourly_scrapes_per_cluster": hourly_scrapes_per_cluster,
        "limit_scope": "origin_ip",
        "status": "warning" if requests_per_minute >= 250 else "healthy",
        "explanation": (
            "Confluent Cloud Metrics API (api.telemetry.confluent.cloud/v2/metrics/cloud/export) "
            "enforces a global rate limit of 300 requests per IP address per minute (not per organization). "
            "Because this scraper runs on a single host/VM, all scrape jobs across all organizations share "
            "the 300 req/min origin quota. The export endpoint also enforces a limit of 160 requests per resource per hour."
        )
    }

def resolve_cluster_name_from_prometheus(prom_url: str, cluster_id: str) -> str | None:
    """Query Prometheus series endpoint for the kafka_name label associated with the cluster ID."""
    url = f"{prom_url}/api/v1/series"
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

def query_prom_instant_clusters(prom_url: str, query: str) -> Dict[str, float]:
    """Execute PromQL instant query and return a map of cluster_id -> value."""
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
            cid = metric.get("cflt_cluster_id") or metric.get("kafka_id")
            value = item.get("value")
            if cid and isinstance(value, list) and len(value) == 2:
                try:
                    totals[cid] = float(value[1])
                except (ValueError, TypeError):
                    continue
        return totals
    except Exception as e:
        print(f"Error querying Prometheus instant clusters ({query}): {e}")
        return {}

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

    # Query cluster partition counts and topic counts from Prometheus
    partition_map = query_prom_instant_clusters(config_mgr.prom_url, "confluent_kafka_server_partition_count")
    topic_map = query_prom_instant_clusters(config_mgr.prom_url, "count by (cflt_cluster_id) (confluent_kafka_server_retained_bytes)")
    if not topic_map:
        topic_map = query_prom_instant_clusters(config_mgr.prom_url, "count by (kafka_id) (confluent_kafka_server_retained_bytes)")

    for cid, cluster in clusters.items():
        cluster["partitions_count"] = int(partition_map.get(cid, 0))
        cluster["topics_count"] = int(topic_map.get(cid, 0))
        
        # Mock fallback if in mock mode
        if (cid.startswith("lkc-mock-") or cid.startswith("lkc-santander-") or config_mgr.cloud_api_key == "MOCK") and cluster["partitions_count"] == 0:
            cluster["partitions_count"] = 24
            cluster["topics_count"] = 7
        
    return clusters

@app.post("/api/clusters/{cluster_id}/config")
def configure_cluster(cluster_id: str, payload: ClusterConfigModel):
    """Save/update Kafka REST API credentials for a cluster."""
    config = config_mgr.load_clusters_config()
    if "clusters" not in config:
        config["clusters"] = {}

    existing = config["clusters"].get(cluster_id, {})
    org_id = payload.org_id or existing.get("org_id") or "default-org"
    org_name = config.get("organizations", {}).get(org_id, {}).get("name", existing.get("org_name", "Default Organization"))

    config["clusters"][cluster_id] = {
        "name": payload.name.strip() or f"Cluster {cluster_id}",
        "org_id": org_id,
        "org_name": org_name,
        "environment_id": existing.get("environment_id", ""),
        "environment_name": existing.get("environment_name", ""),
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

def classify_topic(bytes_in: float, bytes_out: float, retained_bytes: float) -> str:
    """
    Classify Kafka topic usage based on traffic metrics and storage:
    - active: bytes_in > 0 and bytes_out > 0 (or bytes_in == 0 and bytes_out > 0 for active consumer)
    - inactive: bytes_in > 0 and bytes_out == 0 (receiving data, but no consumers)
    - unused: bytes_in == 0 and bytes_out == 0 and retained_bytes > 0 (dormant with stored data)
    - empty: bytes_in == 0 and bytes_out == 0 and retained_bytes == 0 (no traffic, no data)
    """
    if bytes_in > 0 and bytes_out > 0:
        return "active"
    elif bytes_in > 0 and bytes_out == 0:
        return "inactive"
    elif bytes_in == 0 and bytes_out > 0:
        return "active"  # Active consumer draining backlog
    elif bytes_in == 0 and bytes_out == 0 and retained_bytes > 0:
        return "unused"
    else:
        return "empty"

@app.get("/api/clusters/{cluster_id}/usage")
def get_cluster_usage(cluster_id: str, period: str = "30d"):
    """
    Returns the dynamic list of topics and their Prometheus traffic stats.
    Cross-references with Kafka REST API if credentials exist.
    Classifies topics into: ACTIVE, INACTIVE, UNUSED, and EMPTY.
    """
    if not re.match(r"^\d+[smhdw]$", period):
        raise HTTPException(status_code=400, detail="Invalid period. Examples: 30d, 7d, 24h.")

    config = config_mgr.load_clusters_config()
    cluster = config.get("clusters", {}).get(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found.")

    # Simulator Mode Interceptor
    if cluster_id.startswith("lkc-mock-") or cluster_id.startswith("lkc-santander-") or config_mgr.cloud_api_key == "MOCK":
        is_configured = cluster.get("configured", False)
        inventory = [
            ("orders-v1", 1024 * 500, 1024 * 900, 1024 * 1200),
            ("payments-v1", 2048 * 500, 2048 * 900, 2048 * 1200),
            ("analytics-raw", 512 * 500, 0, 512 * 800),
            ("clickstream-feed", 256 * 500, 0, 256 * 600),
            ("billing-events", 0, 0, 1024 * 350),
            ("schema-changes", 0, 0, 1024 * 150),
            ("temp-debug-topic", 0, 0, 0),
            ("dead-letter-queue", 0, 0, 0)
        ]
        mock_topics = []
        for name, bin_val, bout_val, ret_val in inventory:
            status = classify_topic(bin_val, bout_val, ret_val)
            mock_topics.append({
                "topic": name,
                "bytes_in": bin_val,
                "bytes_out": bout_val,
                "retained_bytes": ret_val,
                "status": status
            })
        
        if not is_configured:
            # Telemetry-only: topics with active metrics or retained data
            mock_topics = [t for t in mock_topics if t["bytes_in"] > 0 or t["bytes_out"] > 0 or t["retained_bytes"] > 0]

        total = len(mock_topics)
        active = sum(1 for t in mock_topics if t["status"] == "active")
        inactive = sum(1 for t in mock_topics if t["status"] == "inactive")
        unused = sum(1 for t in mock_topics if t["status"] == "unused")
        empty = sum(1 for t in mock_topics if t["status"] == "empty")

        return {
            "cluster_id": cluster_id,
            "name": cluster.get("name", f"Cluster {cluster_id}"),
            "org_id": cluster.get("org_id", ""),
            "org_name": cluster.get("org_name", ""),
            "environment_id": cluster.get("environment_id", ""),
            "environment_name": cluster.get("environment_name", ""),
            "configured": is_configured,
            "total_topics_count": total,
            "active_topics_count": active,
            "inactive_topics_count": inactive,
            "unused_topics_count": unused,
            "empty_topics_count": empty,
            "total_partitions_count": 48,
            "topics": mock_topics
        }

    # 1. Query Prometheus for bytes in, bytes out, and retained bytes
    query_in = f'sum by (topic) (sum_over_time(confluent_kafka_server_received_bytes{{cflt_cluster_id="{cluster_id}"}}[{period}]))'
    query_out = f'sum by (topic) (sum_over_time(confluent_kafka_server_sent_bytes{{cflt_cluster_id="{cluster_id}"}}[{period}]))'
    query_retained_period = f'sum by (topic) (max_over_time(confluent_kafka_server_retained_bytes{{cflt_cluster_id="{cluster_id}"}}[{period}]))'
    query_retained_instant = f'sum by (topic) (confluent_kafka_server_retained_bytes{{cflt_cluster_id="{cluster_id}"}})'
    
    bytes_in = query_prom_bytes(config_mgr.prom_url, query_in)
    bytes_out = query_prom_bytes(config_mgr.prom_url, query_out)
    bytes_retained_period = query_prom_bytes(config_mgr.prom_url, query_retained_period)
    bytes_retained_instant = query_prom_bytes(config_mgr.prom_url, query_retained_instant)

    # Fallback to kafka_id if we got 0 metrics
    if not bytes_in and not bytes_out and not bytes_retained_period and not bytes_retained_instant:
        query_in_fb = f'sum by (topic) (sum_over_time(confluent_kafka_server_received_bytes{{kafka_id="{cluster_id}"}}[{period}]))'
        query_out_fb = f'sum by (topic) (sum_over_time(confluent_kafka_server_sent_bytes{{kafka_id="{cluster_id}"}}[{period}]))'
        query_retained_period_fb = f'sum by (topic) (max_over_time(confluent_kafka_server_retained_bytes{{kafka_id="{cluster_id}"}}[{period}]))'
        query_retained_instant_fb = f'sum by (topic) (confluent_kafka_server_retained_bytes{{kafka_id="{cluster_id}"}})'
        bytes_in = query_prom_bytes(config_mgr.prom_url, query_in_fb)
        bytes_out = query_prom_bytes(config_mgr.prom_url, query_out_fb)
        bytes_retained_period = query_prom_bytes(config_mgr.prom_url, query_retained_period_fb)
        bytes_retained_instant = query_prom_bytes(config_mgr.prom_url, query_retained_instant_fb)

    # Combine retained bytes across period and instant query to capture any stored data
    all_retained_topics = set(bytes_retained_period.keys()).union(set(bytes_retained_instant.keys()))
    retained_combined = {}
    for t in all_retained_topics:
        retained_combined[t] = max(bytes_retained_period.get(t, 0.0), bytes_retained_instant.get(t, 0.0))

    # Query cluster total partitions
    query_partitions = f'confluent_kafka_server_partition_count{{cflt_cluster_id="{cluster_id}"}}'
    partitions_data = query_prom_instant_clusters(config_mgr.prom_url, query_partitions)
    if not partitions_data:
        query_partitions_fb = f'confluent_kafka_server_partition_count{{kafka_id="{cluster_id}"}}'
        partitions_data = query_prom_instant_clusters(config_mgr.prom_url, query_partitions_fb)
    total_partitions = int(partitions_data[cluster_id]) if (partitions_data and cluster_id in partitions_data) else None

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
            bret_val = retained_combined.get(topic, 0.0)
            status = classify_topic(bin_val, bout_val, bret_val)
            topics_list.append({
                "topic": topic,
                "bytes_in": int(bin_val) if bin_val.is_integer() else bin_val,
                "bytes_out": int(bout_val) if bout_val.is_integer() else bout_val,
                "retained_bytes": int(bret_val) if bret_val.is_integer() else bret_val,
                "status": status
            })
    else:
        # Unconfigured - return topics that have any telemetry in Prometheus
        all_metric_topics = set(bytes_in.keys()).union(set(bytes_out.keys())).union(set(retained_combined.keys()))
        for topic in sorted(list(all_metric_topics)):
            bin_val = bytes_in.get(topic, 0.0)
            bout_val = bytes_out.get(topic, 0.0)
            bret_val = retained_combined.get(topic, 0.0)
            status = classify_topic(bin_val, bout_val, bret_val)
            topics_list.append({
                "topic": topic,
                "bytes_in": int(bin_val) if bin_val.is_integer() else bin_val,
                "bytes_out": int(bout_val) if bout_val.is_integer() else bout_val,
                "retained_bytes": int(bret_val) if bret_val.is_integer() else bret_val,
                "status": status
            })

    # Count stats
    total = len(topics_list)
    active = sum(1 for t in topics_list if t["status"] == "active")
    inactive = sum(1 for t in topics_list if t["status"] == "inactive")
    unused = sum(1 for t in topics_list if t["status"] == "unused")
    empty = sum(1 for t in topics_list if t["status"] == "empty")

    return {
        "cluster_id": cluster_id,
        "name": cluster.get("name", f"Cluster {cluster_id}"),
        "org_id": cluster.get("org_id", ""),
        "org_name": cluster.get("org_name", ""),
        "environment_id": cluster.get("environment_id", ""),
        "environment_name": cluster.get("environment_name", ""),
        "configured": is_configured,
        "total_topics_count": total,
        "active_topics_count": active,
        "inactive_topics_count": inactive,
        "unused_topics_count": unused,
        "empty_topics_count": empty,
        "total_partitions_count": total_partitions,
        "topics": topics_list
    }

def period_to_timedelta(period: str) -> timedelta:
    """Parse Prometheus period string (e.g. 30d, 7d, 24h) to datetime timedelta."""
    match = re.match(r"^(\d+)([smhdw])$", period.strip().lower())
    if not match:
        return timedelta(days=30)
    amount, unit = int(match.group(1)), match.group(2)
    if unit == "s": return timedelta(seconds=amount)
    if unit == "m": return timedelta(minutes=amount)
    if unit == "h": return timedelta(hours=amount)
    if unit == "d": return timedelta(days=amount)
    if unit == "w": return timedelta(weeks=amount)
    return timedelta(days=30)

def to_pascal_case(name: str) -> str:
    """Convert a name string to PascalCase for clean export filenames."""
    words = re.findall(r"[a-zA-Z0-9]+", name or "")
    if not words:
        return "Cluster"
    return "".join(w[0].upper() + w[1:] for w in words)

@app.get("/api/clusters/{cluster_id}/export-csv")
def export_cluster_topics_csv(cluster_id: str, period: str = "30d"):
    """
    Export all topics for the specified cluster as a CSV file.
    Includes metadata comments identifying environment, cluster, time window with start/end dates,
    and status legend, with columns topic_name,partitions,status.
    Filename format: "ClusterName_clusterId.csv"
    """
    if not re.match(r"^\d+[smhdw]$", period):
        raise HTTPException(status_code=400, detail="Invalid period. Examples: 30d, 7d, 24h.")

    usage = get_cluster_usage(cluster_id, period=period)
    cluster_name = usage.get("name") or f"Cluster_{cluster_id}"
    env_name = usage.get("environment_name") or usage.get("environment_id") or "Unassigned"
    env_id = usage.get("environment_id") or "unassigned"
    org_name = usage.get("org_name") or "Default Organization"
    org_id = usage.get("org_id") or ""

    pascal_name = to_pascal_case(cluster_name)
    filename = f"{pascal_name}_{cluster_id}.csv"

    # Calculate time window dates
    now_utc = datetime.now(timezone.utc)
    delta = period_to_timedelta(period)
    start_utc = now_utc - delta
    start_str = start_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    end_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    start_iso = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Header comments identifying environment, cluster, time window with start/end date, and status definitions
    lines = [
        f"# Organization: {org_name} ({org_id})" if org_id else f"# Organization: {org_name}",
        f"# Environment: {env_name} ({env_id})" if env_id != "unassigned" else f"# Environment: {env_name}",
        f"# Cluster: {cluster_name} ({cluster_id})",
        f"# Time Window: {period}",
        f"# Start Date: {start_iso}",
        f"# End Date: {end_iso}",
        "# Status Definitions:",
        "#   ACTIVE   : Ingress and egress observed in time window (or active consumer)",
        "#   INACTIVE : Ingress observed, but 0 egress in time window (unconsumed data)",
        "#   UNUSED   : No traffic in time window, but stored data retained on broker",
        "#   EMPTY    : No traffic and 0 bytes stored",
        "",
        "topic_name,partitions,status"
    ]

    topics = usage.get("topics", [])
    for t in topics:
        topic_name = t.get("topic", "")
        status = (t.get("status") or "empty").upper()
        if topic_name:
            escaped_topic = f'"{topic_name.replace(chr(34), chr(34)+chr(34))}"' if ("," in topic_name or '"' in topic_name) else topic_name
            lines.append(f"{escaped_topic},-1,{status}")

    csv_content = "\n".join(lines) + "\n"

    return Response(
        content=csv_content,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )

# Serve static dashboard
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/", StaticFiles(directory=str(static_path), html=True), name="static")
