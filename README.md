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
| `--interval` | Metrics query interval (see format below) | `now-7d\|d/now-5m` (last 7 days) |
| `--output-path` | Directory path to also write JSON output file | not set |
| `--include-internal-topics` | Include internal Kafka topics in output | `false` |

### Interval Format

The `--interval` argument uses the [Confluent Metrics API interval format](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html):

`Array of strings <ISO-8601 interval (<start>/<end> | <start>/<duration> | <duration>/<end>)> (Interval) non-empty [ items <ISO-8601 interval (<start>/<end> | <start>/<duration> | <duration>/<end>) > ]`

Note that the code currently forces/uses granularity `ALL`

Examples:

| Interval | Meaning |
|---|---|
| `now-7d/now-5m` | Last 7 days (default) |
| `now-1d/now-5m` | Last 24 hours |
| `now-3d/now-5m` | Last 3 days |

> **Note:** The Confluent Metrics API retains data for [7 days](https://docs.confluent.io/cloud/current/monitoring/monitor-faq.html#what-is-the-retention-time-of-metrics-in-the-metrics-api). Intervals beyond 7 days will return incomplete results.

### Output File Naming

When `--output-path` is provided, the tool still prints JSON to stdout and also writes a file using this pattern:

`topic-usage_<cluster-id>_<start-date>_<end-date>.json`

Where:

- `<cluster-id>` comes from `--cluster-id` or `CLUSTER_ID`
- `<start-date>` and `<end-date>` are computed from `--interval` against current execution time
- Dates are UTC timestamps in `YYYYMMDDTHHMMSSZ` format

## Build

```bash
docker build -t cflt-topic-usage:latest .
```

## Run

KAFKA_API_ENDPOINT format is usually `https://pkc-xxxxx.region.provider.confluent.cloud`
CATALOG_API_ENPOINT format is usually `https://psrc-xxxxx.region.provider.confluent.cloud`

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
  cflt-topic-usage:latest
```

Example with shared volume output:

```bash
docker run --rm \
  -v "$(pwd)/.out:/data/out" \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$CLOUD_API_KEY" \
  -e METRICS_API_SECRET="$CLOUD_API_SECRET" \
  -e KAFKA_API_ENDPOINT="$KAFKA_REST" \
  -e KAFKA_API_KEY="$KAFKA_API_KEY" \
  -e KAFKA_API_SECRET="$KAFKA_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest \
  --output-path /data/out
```

Example running for the last day

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
  cflt-topic-usage:latest --interval "now-15d/now-5m"
```

## Local Test Run

```bash
python -m pytest -q
```

## Zero-Bytes Aggregation Tool

The same Docker image includes an additional CLI tool that:

- reads previous report files from a directory (for example, a mounted Docker volume)
- aggregates `bytes_in` and `bytes_out` by topic across all reports for the same cluster
- queries the Kafka cluster for currently existing topics
- prints to stdout a JSON document with two arrays:
  - `zero_topics` — existing topics where aggregated `bytes_in` and/or `bytes_out` is `0`, each entry contains `topic`, `zero_bytes_in` (bool), and `zero_bytes_out` (bool)
  - `non_zero_topics` — existing topics where **both** `bytes_in > 0` and `bytes_out > 0`, as a flat sorted list of topic name strings

Report interval overlaps or gaps are intentionally ignored for this check.

### Run with Docker Volume

```bash
docker run --rm \
  -v "$(pwd)/.out:/data/reports" \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e KAFKA_API_ENDPOINT="$KAFKA_REST" \
  -e KAFKA_API_KEY="$KAFKA_API_KEY" \
  -e KAFKA_API_SECRET="$KAFKA_API_SECRET" \
  cflt-topic-usage:latest \
  find-zero-topics \
  --reports-path /data/reports
```

Optional flags:

- `--cluster-id`
- `--kafka-api-endpoint`
- `--kafka-api-key`
- `--kafka-api-secret`
- `--include-internal-topics`
- `--request-timeout-seconds`
- `--max-retries`

## Change Topic Owner Tool

The Docker image also includes a third tool to update Kafka topic metadata in Confluent Catalog:

- validates that the topic exists in the target cluster
- updates the topic's `owner` and `ownerEmail` attributes via Catalog API PUT request
- outputs a JSON confirmation with the updated values

### Run with Docker

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$METRICS_API_KEY" \
  -e METRICS_API_SECRET="$METRICS_API_SECRET" \
  -e KAFKA_API_ENDPOINT="$KAFKA_REST" \
  -e KAFKA_API_KEY="$KAFKA_API_KEY" \
  -e KAFKA_API_SECRET="$KAFKA_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest \
  update-topic-owner \
  --topic <topic-name> \
  --owner "Full Name" \
  --owner-email "email@company.com"
```

IMPORTANT NOTE:
Currently, all tools in this image share the same configuration loader. Because of that, the `update-topic-owner` tool still requires all core environment variables to be present, including `METRICS_API_KEY` and `METRICS_API_SECRET`, even though it does not call the Metrics API directly.

Required flags:

- `--topic` — Topic name to update
- `--owner` — New owner name
- `--owner-email` — New owner email address

Optional flags:

- `--cluster-id`
- `--catalog-api-endpoint`
- `--catalog-api-key`
- `--catalog-api-secret`
- `--kafka-api-endpoint`
- `--kafka-api-key`
- `--kafka-api-secret`
- `--request-timeout-seconds`
- `--max-retries`

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
