import argparse
import csv
from datetime import datetime
import json
import os
import sys
from io import StringIO
from pathlib import Path

from clients.catalog_api import CatalogApiClient
from clients.kafka_v3_api import KafkaV3Client
from http_client import ApiError, HttpClient


DEFAULT_CRITICALITY_THRESHOLD_BYTES = 1024 * 1024


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
        "--topic-source",
        choices=("catalog", "kafka"),
        default=os.getenv("TOPIC_SOURCE", "catalog").strip().lower() or "catalog",
        help="Source for listing topics and partition counts (catalog or kafka). Default: catalog.",
    )
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
        "--catalog-api-endpoint",
        default=os.getenv("CATALOG_API_ENDPOINT", "https://api.confluent.cloud"),
        help="Catalog/Schema Registry endpoint.",
    )
    parser.add_argument(
        "--catalog-api-key",
        default=os.getenv("CATALOG_API_KEY", os.getenv("METRICS_API_KEY", "")),
        help="Catalog API key (defaults to METRICS_API_KEY when not set).",
    )
    parser.add_argument(
        "--catalog-api-secret",
        default=os.getenv("CATALOG_API_SECRET", os.getenv("METRICS_API_SECRET", "")),
        help="Catalog API secret (defaults to METRICS_API_SECRET when not set).",
    )
    parser.add_argument(
        "--catalog-page-limit",
        type=int,
        default=int(os.getenv("CATALOG_TOPICS_PAGE_LIMIT", "500")),
        help="Page size for Catalog topic listing pagination. Default: 500.",
    )
    parser.add_argument(
        "--criticality-threshold-bytes",
        type=int,
        default=int(os.getenv("CRITICALITY_THRESHOLD_BYTES", str(DEFAULT_CRITICALITY_THRESHOLD_BYTES))),
        help="Threshold in bytes used to classify criticality levels 1/2. Default: 1048576 (1 MiB).",
    )
    parser.add_argument(
        "--output-format",
        choices=("json", "csv"),
        default=os.getenv("FIND_ZERO_TOPICS_OUTPUT_FORMAT", "json").strip().lower() or "json",
        help="Output format for results. Default: json.",
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
    totals, latest_topic_snapshot, reports_used = _aggregate_topic_bytes(report_files, cluster_id=args.cluster_id)

    if args.topic_source == "catalog":
        topic_client = CatalogApiClient(
            HttpClient(
                base_url=args.catalog_api_endpoint,
                api_key=args.catalog_api_key,
                api_secret=args.catalog_api_secret,
                timeout_seconds=args.request_timeout_seconds,
                max_retries=args.max_retries,
            )
        )
        existing_topics = topic_client.list_topics_with_partitions(
            cluster_id=args.cluster_id,
            include_internal_topics=True,
            page_limit=args.catalog_page_limit,
        )
    else:
        topic_client = KafkaV3Client(
            HttpClient(
                base_url=args.kafka_api_endpoint,
                api_key=args.kafka_api_key,
                api_secret=args.kafka_api_secret,
                timeout_seconds=args.request_timeout_seconds,
                max_retries=args.max_retries,
            )
        )
        existing_topics = topic_client.list_topics_with_partitions(
            cluster_id=args.cluster_id,
            include_internal_topics=True,
        )

    topics = _build_topics_output(
        existing_topics=existing_topics,
        totals=totals,
        latest_topic_snapshot=latest_topic_snapshot,
        criticality_threshold_bytes=args.criticality_threshold_bytes,
    )
    summary = _build_summary(topics)

    output = {
        "cluster_id": args.cluster_id,
        "reports_path": str(Path(args.reports_path).resolve()),
        "reports_processed": reports_used,
        "existing_topics_count": len(existing_topics),
        "criticality_threshold_bytes": args.criticality_threshold_bytes,
        "topics": topics,
        "summary": summary,
    }
    if args.output_format == "csv":
        print(_render_csv_output(topics, summary))
    else:
        print(json.dumps(output, indent=2, sort_keys=False))
    return 0


def _validate_args(args: argparse.Namespace) -> None:
    if not args.cluster_id.strip():
        raise ValueError("Missing required cluster id. Use --cluster-id or set CLUSTER_ID.")

    if args.topic_source == "kafka":
        if not args.kafka_api_endpoint.strip():
            raise ValueError("Missing required Kafka endpoint. Use --kafka-api-endpoint or set KAFKA_API_ENDPOINT.")
        if not args.kafka_api_key.strip():
            raise ValueError("Missing required Kafka API key. Use --kafka-api-key or set KAFKA_API_KEY.")
        if not args.kafka_api_secret.strip():
            raise ValueError("Missing required Kafka API secret. Use --kafka-api-secret or set KAFKA_API_SECRET.")
    else:
        if not args.catalog_api_endpoint.strip():
            raise ValueError("Missing required Catalog endpoint. Use --catalog-api-endpoint or set CATALOG_API_ENDPOINT.")
        if not args.catalog_api_key.strip():
            raise ValueError("Missing required Catalog API key. Use --catalog-api-key or set CATALOG_API_KEY.")
        if not args.catalog_api_secret.strip():
            raise ValueError("Missing required Catalog API secret. Use --catalog-api-secret or set CATALOG_API_SECRET.")
        if args.catalog_page_limit < 1:
            raise ValueError("catalog page limit must be >= 1")

    if args.criticality_threshold_bytes < 1:
        raise ValueError("criticality threshold bytes must be >= 1")

    if args.request_timeout_seconds < 1:
        raise ValueError("request timeout must be >= 1")
    if args.max_retries < 0:
        raise ValueError("max retries must be >= 0")


def _list_report_files(reports_path: str) -> list[Path]:
    directory = Path(reports_path)
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"reports path does not exist or is not a directory: {reports_path}")
    return sorted(p for p in directory.glob("*.json") if p.is_file())


def _aggregate_topic_bytes(
    report_files: list[Path],
    cluster_id: str,
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, object]], int]:
    totals: dict[str, dict[str, float]] = {}
    latest_topic_snapshot: dict[str, dict[str, object]] = {}
    reports_used = 0
    latest_report_time: datetime | None = None

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
        report_time = _parse_generated_at(payload)
        use_as_latest_snapshot = False
        if latest_report_time is None:
            use_as_latest_snapshot = True
        elif report_time is not None and report_time >= latest_report_time:
            use_as_latest_snapshot = True

        if use_as_latest_snapshot:
            latest_topic_snapshot = {}

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

            if use_as_latest_snapshot:
                latest_topic_snapshot[topic_name] = {
                    "retained_bytes": _coerce_number(topic_entry.get("retained_bytes", 0.0)),
                    "partitions": _coerce_int(topic_entry.get("partitions", 0), default=0),
                    "owner": _coerce_owner(topic_entry.get("owner")),
                    "owner_email": _coerce_owner_email(topic_entry.get("owner_email")),
                }

        if use_as_latest_snapshot:
            latest_report_time = report_time

    return totals, latest_topic_snapshot, reports_used


def _build_topics_output(
    existing_topics: dict[str, int],
    totals: dict[str, dict[str, float]],
    latest_topic_snapshot: dict[str, dict[str, object]],
    criticality_threshold_bytes: int,
) -> list[dict]:
    topics: list[dict] = []

    for topic_name in sorted(existing_topics.keys()):
        topic_totals = totals.get(topic_name, {"bytes_in": 0.0, "bytes_out": 0.0})
        bytes_in = float(topic_totals.get("bytes_in", 0.0))
        bytes_out = float(topic_totals.get("bytes_out", 0.0))
        zero_in = bytes_in == 0.0
        zero_out = bytes_out == 0.0

        snapshot = latest_topic_snapshot.get(topic_name, {})
        retained_bytes = _coerce_number(snapshot.get("retained_bytes", 0.0))
        partitions = _coerce_int(snapshot.get("partitions", existing_topics.get(topic_name, 0)), default=0)
        owner = _coerce_owner(snapshot.get("owner"))
        owner_email = _coerce_owner_email(snapshot.get("owner_email"))
        criticality_level = _classify_criticality_level(
            bytes_in=bytes_in,
            bytes_out=bytes_out,
            threshold_bytes=criticality_threshold_bytes,
        )

        topics.append(
            {
                "topic": topic_name,
                "bytes_in": _normalize_number(bytes_in),
                "bytes_out": _normalize_number(bytes_out),
                "retained_bytes": _normalize_number(retained_bytes),
                "partitions": partitions,
                "owner": owner,
                "owner_email": owner_email,
                "criticality_level": criticality_level,
                "zero_bytes_in": zero_in,
                "zero_bytes_out": zero_out,
            }
        )

    topics.sort(key=lambda entry: (int(entry["criticality_level"]), str(entry["topic"])))
    return topics


def _build_summary(topics: list[dict]) -> dict:
    partitions_per_criticality: dict[str, int] = {str(level): 0 for level in range(1, 6)}
    for topic in topics:
        level = str(int(topic.get("criticality_level", 5)))
        partitions_per_criticality[level] = partitions_per_criticality.get(level, 0) + _coerce_int(
            topic.get("partitions", 0),
            default=0,
        )

    return {
        "partitions_per_criticality": partitions_per_criticality,
    }


def _render_csv_output(topics: list[dict], summary: dict) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)

    writer.writerow(
        [
            "topic",
            "bytes_in",
            "bytes_out",
            "retained_bytes",
            "partitions",
            "owner",
            "owner_email",
            "criticality_level",
            "zero_bytes_in",
            "zero_bytes_out",
        ]
    )

    for topic in topics:
        writer.writerow(
            [
                topic.get("topic", ""),
                topic.get("bytes_in", 0),
                topic.get("bytes_out", 0),
                topic.get("retained_bytes", 0),
                topic.get("partitions", 0),
                topic.get("owner", "unknown"),
                topic.get("owner_email", ""),
                topic.get("criticality_level", 5),
                str(bool(topic.get("zero_bytes_in", False))).lower(),
                str(bool(topic.get("zero_bytes_out", False))).lower(),
            ]
        )

    writer.writerow([])
    writer.writerow(["summary", "criticality_level", "partitions"])

    partitions_per_criticality = summary.get("partitions_per_criticality", {})
    for level in ("1", "2", "3", "4", "5"):
        writer.writerow(["partitions_per_criticality", level, partitions_per_criticality.get(level, 0)])

    return buffer.getvalue().rstrip("\n")


def _coerce_number(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_int(value: object, default: int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return default
    return default


def _coerce_owner(value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "unknown"


def _coerce_owner_email(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _normalize_number(value: float | int) -> int | float:
    numeric = float(value)
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _classify_criticality_level(bytes_in: float, bytes_out: float, threshold_bytes: int) -> int:
    if bytes_in == 0.0 and bytes_out == 0.0:
        return 5
    if bytes_in == 0.0 and bytes_out > 0.0:
        return 3
    if bytes_in > 0.0 and bytes_out == 0.0:
        return 4

    average_flow = (bytes_in + bytes_out) / 2.0
    if average_flow > float(threshold_bytes):
        return 1
    return 2


def _parse_generated_at(payload: dict) -> datetime | None:
    value = payload.get("generated_at")
    if not isinstance(value, str) or not value.strip():
        return None

    candidate = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def main() -> None:
    try:
        code = run()
    except (ValueError, ApiError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc

    raise SystemExit(code)


if __name__ == "__main__":
    main()
