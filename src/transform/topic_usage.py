from datetime import datetime, timezone


ONE_MEBIBYTE_BYTES = 1024 * 1024


def build_topic_usage(
    cluster_id: str,
    query_interval: str,
    topic_partitions: dict[str, int],
    bytes_in: dict[str, float],
    bytes_out: dict[str, float],
    retained_bytes: dict[str, float],
    owners: dict[str, str],
    owner_emails: dict[str, str] | None = None,
    criticality_threshold_bytes: int = ONE_MEBIBYTE_BYTES,
) -> dict:
    if criticality_threshold_bytes < 1:
        raise ValueError("criticality_threshold_bytes must be >= 1")

    topics = []
    for topic_name in sorted(topic_partitions.keys()):
        topic_bytes_in = _normalize_number(bytes_in.get(topic_name, 0.0))
        topic_bytes_out = _normalize_number(bytes_out.get(topic_name, 0.0))
        topic_criticality_level = _classify_criticality_level(
            topic_bytes_in,
            topic_bytes_out,
            threshold_bytes=criticality_threshold_bytes,
        )

        entry = {
            "topic": topic_name,
            "bytes_in": topic_bytes_in,
            "bytes_out": topic_bytes_out,
            "retained_bytes": _normalize_number(retained_bytes.get(topic_name, 0.0)),
            "partitions": int(topic_partitions.get(topic_name, 0)),
            "owner": owners.get(topic_name, "unknown") or "unknown",
            "owner_email": (owner_emails or {}).get(topic_name, ""),
            "criticality_level": topic_criticality_level,
        }
        topics.append(entry)

    return {
        "cluster_id": cluster_id,
        "query_interval": query_interval,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "topics": topics,
    }


def _normalize_number(value: float | int) -> int | float:
    numeric = float(value)
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _classify_criticality_level(bytes_in: int | float, bytes_out: int | float, threshold_bytes: int) -> int:
    inbound = float(bytes_in)
    outbound = float(bytes_out)

    if inbound == 0.0 and outbound == 0.0:
        return 5
    if inbound == 0.0 and outbound > 0.0:
        return 3
    if inbound > 0.0 and outbound == 0.0:
        return 4

    average_flow = (inbound + outbound) / 2.0
    if average_flow > float(threshold_bytes):
        return 1
    return 2
