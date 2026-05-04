import argparse
import json
import os
import sys
from pathlib import Path

from clients.kafka_v3_api import KafkaV3Client
from http_client import ApiError, HttpClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate previous topic usage reports and print existing topics with "
            "zero bytes_in and/or zero bytes_out."
        )
    )
    parser.add_argument(
        "--reports-path",
        required=True,
        help="Directory containing JSON report files (for example, mounted Docker volume path).",
    )
    parser.add_argument("--cluster-id", default=os.getenv("CLUSTER_ID", ""), help="Kafka cluster ID.")
    parser.add_argument(
        "--kafka-api-endpoint",
        default=os.getenv("KAFKA_API_ENDPOINT", ""),
        help="Kafka REST endpoint.",
    )
    parser.add_argument("--kafka-api-key", default=os.getenv("KAFKA_API_KEY", ""), help="Kafka API key.")
    parser.add_argument(
        "--kafka-api-secret",
        default=os.getenv("KAFKA_API_SECRET", ""),
        help="Kafka API secret.",
    )
    parser.add_argument(
        "--include-internal-topics",
        action="store_true",
        help="Include internal topics when checking existing topics.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.getenv("MAX_RETRIES", "3")),
        help="Maximum HTTP retries.",
    )
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    _validate_args(args)

    report_files = _list_report_files(args.reports_path)
    totals, reports_used = _aggregate_topic_bytes(report_files, cluster_id=args.cluster_id)

    kafka_client = KafkaV3Client(
        HttpClient(
            base_url=args.kafka_api_endpoint,
            api_key=args.kafka_api_key,
            api_secret=args.kafka_api_secret,
            timeout_seconds=args.request_timeout_seconds,
            max_retries=args.max_retries,
        )
    )
    existing_topics = kafka_client.list_topics_with_partitions(
        cluster_id=args.cluster_id,
        include_internal_topics=args.include_internal_topics,
    )

    zero_topics = _find_zero_topics(existing_topics.keys(), totals)
    non_zero_topics = _find_non_zero_topics(existing_topics.keys(), totals)

    output = {
        "cluster_id": args.cluster_id,
        "reports_path": str(Path(args.reports_path).resolve()),
        "reports_processed": reports_used,
        "existing_topics_count": len(existing_topics),
        "zero_topics": zero_topics,
        "non_zero_topics": non_zero_topics,
    }
    print(json.dumps(output, indent=2, sort_keys=False))
    return 0


def _validate_args(args: argparse.Namespace) -> None:
    if not args.cluster_id.strip():
        raise ValueError("Missing required cluster id. Use --cluster-id or set CLUSTER_ID.")
    if not args.kafka_api_endpoint.strip():
        raise ValueError("Missing required Kafka endpoint. Use --kafka-api-endpoint or set KAFKA_API_ENDPOINT.")
    if not args.kafka_api_key.strip():
        raise ValueError("Missing required Kafka API key. Use --kafka-api-key or set KAFKA_API_KEY.")
    if not args.kafka_api_secret.strip():
        raise ValueError("Missing required Kafka API secret. Use --kafka-api-secret or set KAFKA_API_SECRET.")
    if args.request_timeout_seconds < 1:
        raise ValueError("request timeout must be >= 1")
    if args.max_retries < 0:
        raise ValueError("max retries must be >= 0")


def _list_report_files(reports_path: str) -> list[Path]:
    directory = Path(reports_path)
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"reports path does not exist or is not a directory: {reports_path}")
    return sorted(p for p in directory.glob("*.json") if p.is_file())


def _aggregate_topic_bytes(report_files: list[Path], cluster_id: str) -> tuple[dict[str, dict[str, float]], int]:
    totals: dict[str, dict[str, float]] = {}
    reports_used = 0

    for file_path in report_files:
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        if payload.get("cluster_id") != cluster_id:
            continue

        topics = payload.get("topics")
        if not isinstance(topics, list):
            continue

        reports_used += 1
        for topic_entry in topics:
            if not isinstance(topic_entry, dict):
                continue

            topic_name = topic_entry.get("topic")
            if not isinstance(topic_name, str) or not topic_name:
                continue

            bytes_in = _coerce_number(topic_entry.get("bytes_in", topic_entry.get("bytes_in_7d", 0)))
            bytes_out = _coerce_number(topic_entry.get("bytes_out", topic_entry.get("bytes_out_7d", 0)))

            if topic_name not in totals:
                totals[topic_name] = {"bytes_in": 0.0, "bytes_out": 0.0}

            totals[topic_name]["bytes_in"] += bytes_in
            totals[topic_name]["bytes_out"] += bytes_out

    return totals, reports_used


def _find_zero_topics(existing_topics: list[str] | tuple[str, ...] | set[str], totals: dict[str, dict[str, float]]) -> list[dict]:
    result: list[dict] = []

    for topic_name in sorted(existing_topics):
        topic_totals = totals.get(topic_name, {"bytes_in": 0.0, "bytes_out": 0.0})
        bytes_in = float(topic_totals.get("bytes_in", 0.0))
        bytes_out = float(topic_totals.get("bytes_out", 0.0))

        zero_in = bytes_in == 0.0
        zero_out = bytes_out == 0.0
        if zero_in or zero_out:
            result.append(
                {
                    "topic": topic_name,
                    "zero_bytes_in": zero_in,
                    "zero_bytes_out": zero_out,
                }
            )

    return result


def _find_non_zero_topics(existing_topics: list[str] | tuple[str, ...] | set[str], totals: dict[str, dict[str, float]]) -> list[str]:
    result: list[str] = []

    for topic_name in sorted(existing_topics):
        topic_totals = totals.get(topic_name, {"bytes_in": 0.0, "bytes_out": 0.0})
        bytes_in = float(topic_totals.get("bytes_in", 0.0))
        bytes_out = float(topic_totals.get("bytes_out", 0.0))

        if bytes_in != 0.0 and bytes_out != 0.0:
            result.append(topic_name)

    return result


def _coerce_number(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    try:
        code = run()
    except (ValueError, ApiError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc

    raise SystemExit(code)


if __name__ == "__main__":
    main()
