import os
from dataclasses import dataclass


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class AppConfig:
    cluster_id: str
    metrics_api_endpoint: str
    metrics_api_key: str
    metrics_api_secret: str
    kafka_api_endpoint: str
    kafka_api_key: str
    kafka_api_secret: str
    catalog_api_endpoint: str
    catalog_api_key: str
    catalog_api_secret: str
    include_internal_topics: bool
    request_timeout_seconds: int
    max_retries: int


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"Invalid boolean value: {value}")


def from_env(cluster_id_override: str | None = None, include_internal_override: bool | None = None) -> AppConfig:
    metrics_api_key = _require_env("METRICS_API_KEY")
    metrics_api_secret = _require_env("METRICS_API_SECRET")

    kafka_api_key = _require_env("KAFKA_API_KEY")
    kafka_api_secret = _require_env("KAFKA_API_SECRET")
    kafka_api_endpoint = _require_env("KAFKA_API_ENDPOINT")

    catalog_api_key = os.getenv("CATALOG_API_KEY", "").strip() or metrics_api_key
    catalog_api_secret = os.getenv("CATALOG_API_SECRET", "").strip() or metrics_api_secret

    cluster_id = (cluster_id_override or os.getenv("CLUSTER_ID", "")).strip()
    if not cluster_id:
        raise ConfigError("Cluster id is required. Use --cluster-id or set CLUSTER_ID.")

    include_internal_topics = (
        include_internal_override
        if include_internal_override is not None
        else _parse_bool(os.getenv("INCLUDE_INTERNAL_TOPICS"), default=False)
    )

    request_timeout_seconds = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))

    if request_timeout_seconds < 1:
        raise ConfigError("REQUEST_TIMEOUT_SECONDS must be >= 1")
    if max_retries < 0:
        raise ConfigError("MAX_RETRIES must be >= 0")

    return AppConfig(
        cluster_id=cluster_id,
        metrics_api_endpoint=os.getenv("METRICS_API_ENDPOINT", "https://api.telemetry.confluent.cloud").rstrip("/"),
        metrics_api_key=metrics_api_key,
        metrics_api_secret=metrics_api_secret,
        kafka_api_endpoint=kafka_api_endpoint.rstrip("/"),
        kafka_api_key=kafka_api_key,
        kafka_api_secret=kafka_api_secret,
        catalog_api_endpoint=os.getenv("CATALOG_API_ENDPOINT", "https://api.confluent.cloud").rstrip("/"),
        catalog_api_key=catalog_api_key,
        catalog_api_secret=catalog_api_secret,
        include_internal_topics=include_internal_topics,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
    )
