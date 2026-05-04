# Confluent Topic Usage Exporter

Containerized Python CLI to export topic usage as JSON for a Confluent Cloud Kafka cluster.

## What It Returns

For each topic in a given cluster:

- `query_interval` (metrics query range used for bytes calculation)
- `topic`
- `bytes_in`
- `bytes_out`
- `partitions`
- `owner` (from Catalog API when available, otherwise `unknown`)
- `owner_email` (from Catalog API when available, otherwise `""`)

Internal topics are excluded by default.

## APIs Used

- Metrics API (Confluent Cloud metrics): `POST /v2/metrics/cloud/query`
- Kafka V3 API (Cluster Rest Endpoint):
  - `GET /kafka/v3/clusters/{cluster_id}/topics`
  - `GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/partitions` (fallback)
- Catalog API (Environment Schema Registry):
  - `GET /catalog/v1/entity/type/kafka_topic/name/{lkc-id}:{topic-name}`

## Required Environment Variables

- `CLUSTER_ID` (or pass `--cluster-id`)
- `METRICS_API_KEY`
- `METRICS_API_SECRET`
- `KAFKA_API_ENDPOINT` (for example `https://pkc-xxxxx.region.provider.confluent.cloud`)
- `KAFKA_API_KEY`
- `KAFKA_API_SECRET`

Optional:

- `METRICS_API_ENDPOINT` (default `https://api.telemetry.confluent.cloud`)
- `CATALOG_API_ENDPOINT` — Schema Registry / Catalog endpoint for the environment that owns the cluster. Set this to the Schema Registry URL (e.g. `https://psrc-xxxxx.region.provider.confluent.cloud`). Defaults to `https://api.confluent.cloud`.
- `CATALOG_API_KEY` — API key with access to the Catalog API. Defaults to `METRICS_API_KEY` if not set.
- `CATALOG_API_SECRET` — API secret for `CATALOG_API_KEY`. Defaults to `METRICS_API_SECRET` if not set.
- `INCLUDE_INTERNAL_TOPICS` (`true` or `false`, default `false`)
- `REQUEST_TIMEOUT_SECONDS` (default `30`)
- `MAX_RETRIES` (default `3`)

> **Note:** `owner` and `owner_email` are resolved via the Catalog API using `CATALOG_API_ENDPOINT`, `CATALOG_API_KEY`, and `CATALOG_API_SECRET`. The Catalog API uses different credentials from the Kafka and Metrics API, set all three explicitly.

## CLI Arguments

| Argument | Description | Default |
|---|---|---|
| `--cluster-id` | Kafka cluster ID (e.g. `lkc-abc123`) | `CLUSTER_ID` env var |
| `--interval` | Metrics query interval (see format below) | `now-7d\|d/now-5m\|m` (last 7 days) |
| `--include-internal-topics` | Include internal Kafka topics in output | `false` |

### Interval Format

The `--interval` argument uses the [Confluent Metrics API interval format](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html):

```
now-<duration>|<granularity>/now-<offset>|<granularity>
```

Examples:

| Interval | Meaning |
|---|---|
| `now-7d\|d/now-5m\|m` | Last 7 days (default) |
| `now-1d\|d/now-5m\|m` | Last 24 hours |
| `now-3d\|d/now-5m\|m` | Last 3 days |

> **Note:** The Confluent Metrics API retains data for [7 days](https://docs.confluent.io/cloud/current/monitoring/monitor-faq.html#what-is-the-retention-time-of-metrics-in-the-metrics-api). Intervals beyond 7 days will return incomplete results.

## Build

```bash
docker build -t cflt-topic-usage:latest .
```

## Run

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$CLOUD_API_KEY" \
  -e METRICS_API_SECRET="$CLOUD_API_SECRET" \
  -e KAFKA_API_ENDPOINT="$KAFKA_REST" \
  -e KAFKA_API_KEY="$KAFKA_API_KEY" \
  -e KAFKA_API_SECRET="$KAFKA_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest [--interval now-1d|d/now-5m|m]
```

KAFKA_API_ENDPOINT format is usually `https://pkc-xxxxx.region.provider.confluent.cloud`
CATALOG_API_ENPOINT format is usually `https://psrc-xxxxx.region.provider.confluent.cloud`

## Local Test Run

```bash
python -m pytest -q
```

## Documentation References

- [Confluent Cloud API Reference](https://docs.confluent.io/cloud/current/api.html/)

## Things to take into account

- Pagination:
  - Kafka V3 API list operations do not support pagination
  - All "list" operations follow the same pattern with the following parameters:
    - page_size – client-provided max number of items per page, only valid on the first request.
    - page_token – server-generated token used for traversing through the result set.
- Rate Limit: There are haders that explain rate limit when 429 code is received
  - Note that headers are not returned for a 429 error response with Kafka REST API (v3)
- Retention time of metrics in the Metrics API is [7 days](https://docs.confluent.io/cloud/current/monitoring/monitor-faq.html#what-is-the-retention-time-of-metrics-in-the-metrics-api)

## TODOs

[] Topic ID ... what happens with metrics when topic is deleted and recreated with the same name during?
[] Paginataion ... some endpoint have a default of 100 results, max 500 via request parameter, need to paginate if greater than that
[] Request Rate ... do we need to throttle the requests?
