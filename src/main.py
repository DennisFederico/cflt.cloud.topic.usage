import argparse
import json
import sys

from clients.catalog_api import CatalogApiClient
from clients.kafka_v3_api import KafkaV3Client
from clients.metrics_api import MetricsApiClient
from config import ConfigError, from_env
from http_client import ApiError, HttpClient
from transform.topic_usage import build_topic_usage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Confluent Cloud topic usage (30d bytes in/out, partitions, owner) as JSON."
    )
    parser.add_argument("--cluster-id", help="Confluent Kafka cluster ID (e.g., lkc-abc123)")
    parser.add_argument(
        "--include-internal-topics",
        action="store_true",
        help="Include internal topics (default: false)",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()

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

    bytes_in, bytes_out = metrics_client.get_topic_bytes_for_last_30_days(cluster_id=config.cluster_id)
    owners = catalog_client.get_topic_owners(config.cluster_id, list(topic_partitions.keys()))

    output = build_topic_usage(
        cluster_id=config.cluster_id,
        topic_partitions=topic_partitions,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
        owners=owners,
    )
    print(json.dumps(output, indent=2, sort_keys=False))
    return 0


def main() -> None:
    try:
        exit_code = run()
    except (ConfigError, ApiError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
