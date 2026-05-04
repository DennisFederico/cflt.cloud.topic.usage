# Confluent Topic Usage Exporter

Containerized Python CLI to export topic usage as JSON for a Confluent Cloud Kafka cluster.

## What It Returns

For each topic in a given cluster:

- `topic`
- `bytes_in_30d`
- `bytes_out_30d`
- `partitions`
- `owner` (from Catalog API when available, otherwise `unknown`)
- `owner_email` (from Catalog API when available, otherwise `""`)

Internal topics are excluded by default.

## APIs Used

- Metrics API: `POST /v2/metrics/cloud/query`
- Kafka V3 API:
  - `GET /kafka/v3/clusters/{cluster_id}/topics`
  - `GET /kafka/v3/clusters/{cluster_id}/topics/{topic_name}/partitions` (fallback)
- Catalog API:
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

> **Note:** `owner` and `owner_email` are resolved via the Catalog API using `CATALOG_API_ENDPOINT`, `CATALOG_API_KEY`, and `CATALOG_API_SECRET`. If the Catalog API uses different credentials from the Metrics API, set all three explicitly.

## Build

```bash
docker build -t cflt-topic-usage:latest .
```

## Run

```bash
docker run --rm \
  -e CLUSTER_ID="lkc-123" \
  -e METRICS_API_KEY="<cloud_api_key>" \
  -e METRICS_API_SECRET="<cloud_api_secret>" \
  -e KAFKA_API_ENDPOINT="https://pkc-xxxxx.region.provider.confluent.cloud" \
  -e KAFKA_API_KEY="<kafka_api_key>" \
  -e KAFKA_API_SECRET="<kafka_api_secret>" \
  -e CATALOG_API_ENDPOINT="https://psrc-xxxxx.region.provider.confluent.cloud" \
  -e CATALOG_API_KEY="<catalog_api_key>" \
  -e CATALOG_API_SECRET="<catalog_api_secret>" \
  cflt-topic-usage:latest
```

## Local Test Run

```bash
python -m pytest -q
```
