#!/usr/bin/env python3
"""Generate topic usage report from Prometheus and Kafka REST API."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
from typing import Any

import requests

PROM_RECEIVED_METRIC = "confluent_kafka_server_received_bytes"
PROM_SENT_METRIC = "confluent_kafka_server_sent_bytes"


def _load_dotenv(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a topic usage report by combining Prometheus time-series totals "
            "with current Kafka topic inventory."
        )
    )
    parser.add_argument("--cluster-id", required=True, help="Kafka cluster ID (e.g., lkc-abc123).")
    parser.add_argument(
        "--prometheus-url",
        default=os.getenv("PROMETHEUS_URL", "http://localhost:9090"),
        help="Prometheus base URL.",
    )
    parser.add_argument(
        "--period",
        default="30d",
        help="PromQL range duration (examples: 30d, 7d, 12h, 90m).",
    )
    parser.add_argument(
        "--time",
        default=None,
        help="Evaluation time in RFC3339 UTC format (default: now).",
    )
    parser.add_argument(
        "--kafka-api-endpoint",
        default=os.getenv("KAFKA_API_ENDPOINT", ""),
        help="Kafka REST endpoint (for topic inventory).",
    )
    parser.add_argument(
        "--kafka-api-key",
        default=os.getenv("KAFKA_API_KEY", ""),
        help="Kafka API key.",
    )
    parser.add_argument(
        "--kafka-api-secret",
        default=os.getenv("KAFKA_API_SECRET", ""),
        help="Kafka API secret.",
    )
    parser.add_argument(
        "--include-internal-topics",
        action="store_true",
        help="Include internal topics in the report.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output JSON file path.",
    )
    return parser.parse_args()


def _parse_rfc3339_utc(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)

    candidate = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"Invalid --time value: {value}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _parse_prom_duration(value: str) -> timedelta:
    pattern = re.compile(r"^(?P<num>\d+)(?P<unit>[smhdw])$")
    match = pattern.match(value.strip())
    if not match:
        raise ValueError("Invalid --period. Use forms like 30d, 7d, 12h, 90m, 45s, 2w.")

    amount = int(match.group("num"))
    unit = match.group("unit")

    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    if unit == "w":
        return timedelta(days=amount * 7)

    raise ValueError(f"Unsupported period unit: {unit}")


def _request_json(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    auth: tuple[str, str] | None = None,
) -> dict[str, Any]:
    response = requests.request(
        method=method,
        url=url,
        params=params,
        auth=auth,
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return body


def _query_prometheus_totals(
    prometheus_url: str,
    cluster_id: str,
    metric_name: str,
    period: str,
    eval_time: datetime,
) -> dict[str, float]:
    prom_url = prometheus_url.rstrip("/") + "/api/v1/query"
    query = (
        f'sum by (topic) (sum_over_time({metric_name}'
        f'{{kafka_id="{cluster_id}"}}[{period}]))'
    )
    payload = _request_json(
        "GET",
        prom_url,
        params={"query": query, "time": eval_time.isoformat()},
    )

    if payload.get("status") != "success":
        raise ValueError(f"Prometheus query failed for {metric_name}: {payload}")

    result = payload.get("data", {}).get("result", [])
    if not isinstance(result, list):
        raise ValueError("Prometheus response has unexpected shape for query results.")

    totals: dict[str, float] = {}
    for item in result:
        metric = item.get("metric", {})
        topic = metric.get("topic")
        value = item.get("value")
        if not isinstance(topic, str) or not topic:
            continue
        if not isinstance(value, list) or len(value) != 2:
            continue
        try:
            totals[topic] = float(value[1])
        except (TypeError, ValueError):
            continue

    return totals


def _list_topics(
    kafka_api_endpoint: str,
    cluster_id: str,
    auth: tuple[str, str],
    include_internal_topics: bool,
) -> list[str]:
    topics: list[str] = []
    next_url = kafka_api_endpoint.rstrip("/") + f"/kafka/v3/clusters/{cluster_id}/topics"
    params: dict[str, Any] | None = {"page_size": 500}

    while next_url:
        payload = _request_json("GET", next_url, params=params, auth=auth)
        params = None

        items = payload.get("data", [])
        if not isinstance(items, list):
            raise ValueError("Kafka topics response has unexpected shape.")

        for item in items:
            if not isinstance(item, dict):
                continue

            name = item.get("topic_name")
            if not isinstance(name, str) or not name:
                continue

            is_internal = bool(item.get("is_internal", name.startswith("_")))
            if is_internal and not include_internal_topics:
                continue

            topics.append(name)

        next_url = payload.get("metadata", {}).get("next")

    return sorted(set(topics))


def _coerce_output_number(value: float) -> int | float:
    if value.is_integer():
        return int(value)
    return value


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    if not args.kafka_api_endpoint.strip():
        raise ValueError("Missing Kafka API endpoint. Set --kafka-api-endpoint or KAFKA_API_ENDPOINT.")
    if not args.kafka_api_key.strip():
        raise ValueError("Missing Kafka API key. Set --kafka-api-key or KAFKA_API_KEY.")
    if not args.kafka_api_secret.strip():
        raise ValueError("Missing Kafka API secret. Set --kafka-api-secret or KAFKA_API_SECRET.")

    eval_time = _parse_rfc3339_utc(args.time)
    period_delta = _parse_prom_duration(args.period)
    start_time = eval_time - period_delta

    bytes_in = _query_prometheus_totals(
        prometheus_url=args.prometheus_url,
        cluster_id=args.cluster_id,
        metric_name=PROM_RECEIVED_METRIC,
        period=args.period,
        eval_time=eval_time,
    )
    bytes_out = _query_prometheus_totals(
        prometheus_url=args.prometheus_url,
        cluster_id=args.cluster_id,
        metric_name=PROM_SENT_METRIC,
        period=args.period,
        eval_time=eval_time,
    )

    topics = _list_topics(
        kafka_api_endpoint=args.kafka_api_endpoint,
        cluster_id=args.cluster_id,
        auth=(args.kafka_api_key, args.kafka_api_secret),
        include_internal_topics=args.include_internal_topics,
    )

    topic_rows: list[dict[str, Any]] = []
    zero_topics: list[dict[str, Any]] = []
    non_zero_topics: list[str] = []

    for topic in topics:
        value_in = float(bytes_in.get(topic, 0.0))
        value_out = float(bytes_out.get(topic, 0.0))
        zero_in = value_in == 0.0
        zero_out = value_out == 0.0
        unused = zero_in and zero_out

        row = {
            "topic": topic,
            "bytes_in": _coerce_output_number(value_in),
            "bytes_out": _coerce_output_number(value_out),
            "zero_bytes_in": zero_in,
            "zero_bytes_out": zero_out,
            "unused": unused,
        }
        topic_rows.append(row)

        if unused:
            zero_topics.append({"topic": topic, "zero_bytes_in": True, "zero_bytes_out": True})
        else:
            non_zero_topics.append(topic)

    report = {
        "cluster_id": args.cluster_id,
        "period": args.period,
        "range_start": start_time.isoformat(),
        "range_end": eval_time.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "existing_topics_count": len(topics),
        "unused_topics_count": len(zero_topics),
        "active_topics_count": len(non_zero_topics),
        "topics": topic_rows,
        "unused_topics": zero_topics,
        "active_topics": non_zero_topics,
    }
    return report


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    _load_dotenv(root / ".env")

    args = parse_args()
    report = build_report(args)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
