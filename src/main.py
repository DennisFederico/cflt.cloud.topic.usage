import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

from clients.catalog_api import CatalogApiClient
from clients.kafka_v3_api import KafkaV3Client
from clients.metrics_api import MetricsApiClient, QUERY_INTERVAL_LAST_7_DAYS
from config import ConfigError, from_env
from http_client import ApiError, HttpClient
from interval_utils import resolve_interval_window
from transform.topic_usage import build_topic_usage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Confluent Cloud topic usage (bytes in/out, partitions, owner) as JSON."
    )
    parser.add_argument("--cluster-id", help="Confluent Kafka cluster ID (e.g., lkc-abc123)")
    parser.add_argument(
        "--include-internal-topics",
        action="store_true",
        help="Include internal topics (default: false)",
    )
    parser.add_argument(
        "--interval",
        default=QUERY_INTERVAL_LAST_7_DAYS,
        help=(
            "Metrics query interval in Confluent interval format "
            "(e.g. 'now-7d/now-5m'). Default: last 7 days."
        ),
    )
    parser.add_argument(
        "--output-path",
        help=(
            "Directory path to also write the JSON output file. "
            "Useful with Docker volume mounts."
        ),
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    execution_time = datetime.now(timezone.utc)

    config = from_env(
        cluster_id_override=args.cluster_id,
        include_internal_override=True if args.include_internal_topics else None,
    )

    metrics_client = MetricsApiClient(
        HttpClient(
            base_url=config.metrics_api_endpoint,
            api_key=config.metrics_api_key,
            api_secret=config.metrics_api_secret,
            timeout_seconds=config.request_timeout_seconds,
            max_retries=config.max_retries,
        )
    )
    kafka_client = KafkaV3Client(
        HttpClient(
            base_url=config.kafka_api_endpoint,
            api_key=config.kafka_api_key,
            api_secret=config.kafka_api_secret,
            timeout_seconds=config.request_timeout_seconds,
            max_retries=config.max_retries,
        )
    )
    catalog_client = CatalogApiClient(
        HttpClient(
            base_url=config.catalog_api_endpoint,
            api_key=config.catalog_api_key,
            api_secret=config.catalog_api_secret,
            timeout_seconds=config.request_timeout_seconds,
            max_retries=config.max_retries,
        )
    )

    topic_partitions = kafka_client.list_topics_with_partitions(
        cluster_id=config.cluster_id,
        include_internal_topics=config.include_internal_topics,
    )

    query_interval = args.interval
    try:
        interval_start, interval_end = resolve_interval_window(query_interval, now=execution_time)
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    bytes_in, bytes_out = metrics_client.get_topic_bytes(
        cluster_id=config.cluster_id,
        interval=query_interval,
    )
    owners, owner_emails = catalog_client.get_topic_owners(config.cluster_id, list(topic_partitions.keys()))

    output = build_topic_usage(
        cluster_id=config.cluster_id,
        query_interval=query_interval,
        topic_partitions=topic_partitions,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
        owners=owners,
        owner_emails=owner_emails,
    )
    output_json = json.dumps(output, indent=2, sort_keys=False)
    print(output_json)

    if args.output_path:
        output_file = _write_output_file(
            output_path=args.output_path,
            cluster_id=config.cluster_id,
            interval_start=interval_start,
            interval_end=interval_end,
            payload_json=output_json,
        )
        print(f"Wrote JSON output file: {output_file}", file=sys.stderr)

    return 0


def _write_output_file(
    output_path: str,
    cluster_id: str,
    interval_start: datetime,
    interval_end: datetime,
    payload_json: str,
) -> Path:
    target_dir = Path(output_path)
    target_dir.mkdir(parents=True, exist_ok=True)

    file_name = _build_output_filename(cluster_id, interval_start, interval_end)
    file_path = target_dir / file_name
    file_path.write_text(payload_json + "\n", encoding="utf-8")
    return file_path


def _build_output_filename(cluster_id: str, interval_start: datetime, interval_end: datetime) -> str:
    safe_cluster_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", cluster_id).strip("-") or "cluster"
    start_str = interval_start.strftime("%Y%m%dT%H%M%SZ")
    end_str = interval_end.strftime("%Y%m%dT%H%M%SZ")
    return f"topic-usage_{safe_cluster_id}_{start_str}_{end_str}.json"


def main() -> None:
    try:
        exit_code = run()
    except (ConfigError, ApiError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
