from datetime import datetime, timezone


def build_topic_usage(
    cluster_id: str,
    query_interval: str,
    topic_partitions: dict[str, int],
    bytes_in: dict[str, float],
    bytes_out: dict[str, float],
    owners: dict[str, str],
    owner_emails: dict[str, str] | None = None,
) -> dict:
    topics = []
    for topic_name in sorted(topic_partitions.keys()):
        entry = {
            "topic": topic_name,
            "bytes_in_7d": _normalize_number(bytes_in.get(topic_name, 0.0)),
            "bytes_out_7d": _normalize_number(bytes_out.get(topic_name, 0.0)),
            "partitions": int(topic_partitions.get(topic_name, 0)),
            "owner": owners.get(topic_name, "unknown") or "unknown",
            "owner_email": (owner_emails or {}).get(topic_name, ""),
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
