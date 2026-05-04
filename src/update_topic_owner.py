"""CLI tool to update Kafka topic owner and ownerEmail in Confluent Catalog."""

import argparse
import json
import sys

from clients.catalog_api import CatalogApiClient
from clients.kafka_v3_api import KafkaV3Client
from config import from_env, ConfigError
from http_client import HttpClient


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update owner and ownerEmail for a Kafka topic in Confluent Catalog.",
    )
    parser.add_argument("--cluster-id", required=False, help="Kafka cluster ID (overrides env var)")
    parser.add_argument("--catalog-api-endpoint", required=False, help="Catalog/Schema Registry endpoint (overrides env var)")
    parser.add_argument("--catalog-api-key", required=False, help="Catalog API key (overrides env var)")
    parser.add_argument("--catalog-api-secret", required=False, help="Catalog API secret (overrides env var)")
    parser.add_argument("--kafka-api-endpoint", required=False, help="Kafka REST API endpoint (overrides env var)")
    parser.add_argument("--kafka-api-key", required=False, help="Kafka API key (overrides env var)")
    parser.add_argument("--kafka-api-secret", required=False, help="Kafka API secret (overrides env var)")
    parser.add_argument("--request-timeout-seconds", type=int, required=False, help="HTTP request timeout in seconds (overrides env var)")
    parser.add_argument("--max-retries", type=int, required=False, help="Max HTTP retries (overrides env var)")
    parser.add_argument("--topic", required=True, help="Topic name to update")
    parser.add_argument("--owner", required=True, help="New owner name")
    parser.add_argument("--owner-email", required=True, help="New owner email address")

    args = parser.parse_args()

    try:
        config = from_env(cluster_id_override=args.cluster_id)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # Apply CLI overrides
    catalog_api_endpoint = args.catalog_api_endpoint or config.catalog_api_endpoint
    catalog_api_key = args.catalog_api_key or config.catalog_api_key
    catalog_api_secret = args.catalog_api_secret or config.catalog_api_secret
    kafka_api_endpoint = args.kafka_api_endpoint or config.kafka_api_endpoint
    kafka_api_key = args.kafka_api_key or config.kafka_api_key
    kafka_api_secret = args.kafka_api_secret or config.kafka_api_secret
    request_timeout_seconds = args.request_timeout_seconds or config.request_timeout_seconds
    max_retries = args.max_retries or config.max_retries

    # Validate topic exists in cluster
    try:
        kafka_client = KafkaV3Client(
            HttpClient(
                base_url=kafka_api_endpoint,
                api_key=kafka_api_key,
                api_secret=kafka_api_secret,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
            ),
        )
        topics_response = kafka_client.list_topics_with_partitions(config.cluster_id)
        existing_topics = set(topics_response.keys())

        if args.topic not in existing_topics:
            print(f"error: topic '{args.topic}' not found in cluster {config.cluster_id}", file=sys.stderr)
            return 1
    except Exception as exc:
        print(f"error: failed to validate topic: {exc}", file=sys.stderr)
        return 1

    # Update owner via Catalog API
    try:
        catalog_client = CatalogApiClient(
            HttpClient(
                base_url=catalog_api_endpoint,
                api_key=catalog_api_key,
                api_secret=catalog_api_secret,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
            ),
        )
        catalog_client.update_topic_owner(
            cluster_id=config.cluster_id,
            topic_name=args.topic,
            owner=args.owner,
            owner_email=args.owner_email,
        )
        output = {
            "cluster_id": config.cluster_id,
            "topic": args.topic,
            "owner": args.owner,
            "owner_email": args.owner_email,
            "status": "updated",
        }
        print(json.dumps(output, indent=2))
        return 0
    except Exception as exc:
        print(f"error: failed to update topic owner: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
