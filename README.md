# Confluent Topic Usage Exporter

Containerized Python CLI suite for working with Kafka topic usage in Confluent Cloud.

It comprises three flows:

1. Snapshot flow
   - Generates a per-topic usage snapshot combining topic metadata, bytes in/out, retained bytes, and criticality.
2. Aggregation flow
   - Aggregates previous snapshot JSON reports and highlights current topics by criticality and traffic status.
3. Change Topic Owner Tool
   - Updates topic ownership metadata in Confluent Catalog.

## Common Properties

These properties are shared across the tools unless stated otherwise:

- `CLUSTER_ID` or `--cluster-id`
- `METRICS_API_KEY` / `METRICS_API_SECRET`
- `CATALOG_API_ENDPOINT`
- `CATALOG_API_KEY` / `CATALOG_API_SECRET`
- `TOPIC_SOURCE` (`catalog` by default, `kafka` for fallback testing)
- `INCLUDE_INTERNAL_TOPICS` (`true` by default)
- `REQUEST_TIMEOUT_SECONDS` (default `30`)
- `MAX_RETRIES` (default `3`)
- `CRITICALITY_THRESHOLD_BYTES` (default `1048576`, approximately 1 MiB)
- `CATALOG_TOPICS_PAGE_LIMIT` (default `500`, used by snapshot flow and by aggregation when listing existing topics)

Topic inventory is resolved from Catalog by default and includes internal topics. When `TOPIC_SOURCE=kafka`, Kafka REST is used for inventory and Catalog is used for owner lookups.

## Build

Build the image with Docker:

```bash
docker build -t cflt-topic-usage:latest .
```

## Snapshot Flow

This is the default CLI flow when you run the image without a subcommand. It builds a topic snapshot by:

1. Fetching topic metadata from Catalog by default
   - Uses `GET /catalog/v1/search/attribute` with `deleted=false`
   - Collects `partitionsCount`, `owner`, and `ownerEmail`
   - Paginates with `limit` and `offset`
2. Fetching usage totals from Metrics API
   - `received_bytes` for bytes in
   - `sent_bytes` for bytes out
   - `retained_bytes` as a snapshot using `granularity=PT1M` and `interval=now-5m/now-4m`
3. Correlating everything by topic name and emitting JSON
   - Adds `criticality_level` using the configured threshold
   - Fills missing values with defaults

### Output Fields

Each topic includes:

- `topic`
- `bytes_in`
- `bytes_out`
- `retained_bytes`
- `criticality_level`
- `partitions`
- `owner`
- `owner_email`

Criticality rules:

- `1`: bytes in and bytes out are both present and the average is greater than the threshold
- `2`: bytes in and bytes out are both present and the average is less than or equal to the threshold
- `3`: no `bytes_in`, but `bytes_out` is present
- `4`: `bytes_in` is present, but no `bytes_out`
- `5`: no `bytes_in` and no `bytes_out`

### Usage Examples

Default snapshot with Docker:

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$CLOUD_API_KEY" \
  -e METRICS_API_SECRET="$CLOUD_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest \
  --output-path /data/out
```

Default snapshot from the local CLI:

```bash
python src/entrypoint.py \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --output-path ./.out
```

Snapshot with a custom criticality threshold of 2 MiB:

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$CLOUD_API_KEY" \
  -e METRICS_API_SECRET="$CLOUD_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  -e CRITICALITY_THRESHOLD_BYTES=2000000 \
  cflt-topic-usage:latest \
  --output-path /data/out
```

Snapshot excluding internal topics:

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$CLOUD_API_KEY" \
  -e METRICS_API_SECRET="$CLOUD_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  -e INCLUDE_INTERNAL_TOPICS=false \
  cflt-topic-usage:latest \
  --output-path /data/out
```

Snapshot using Kafka REST as the inventory source for comparison:

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
  -e TOPIC_SOURCE=kafka \
  cflt-topic-usage:latest \
  --output-path /data/out \
  --interval "now-15d/now-4d"
```

## Aggregation Flow

This flow reads previous snapshot reports from a directory and emits a unified topic list.

### High-Level Flow

1. Reads all JSON reports in `--reports-path`
2. Keeps only reports matching the requested cluster
3. Lists currently existing topics from Catalog using `CATALOG_TOPICS_PAGE_LIMIT`
4. Aggregates `bytes_in` and `bytes_out` across reports by topic
5. Uses the most recent report snapshot for `retained_bytes`, `partitions`, `owner`, and `owner_email`
6. Recalculates `criticality_level` from the aggregated bytes and the threshold
7. Sorts topics by criticality and emits a single topic list with a summary of partitions per criticality

### Output Formats

- JSON: default
- CSV: optional, first line is the header, then one row per topic, and summary rows at the end

### Usage Examples

Default JSON output with Docker:

```bash
docker run --rm \
  -v "$(pwd)/.out:/data/reports" \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest \
  find-zero-topics \
  --reports-path /data/reports
```

Default JSON output from the local CLI:

```bash
python src/entrypoint.py find-zero-topics \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --reports-path ./.out
```

CSV output with Docker:

```bash
docker run --rm \
  -v "$(pwd)/.out:/data/reports" \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest \
  find-zero-topics \
  --reports-path /data/reports \
  --output-format csv
```

CSV output with a custom criticality threshold:

```bash
docker run --rm \
  -v "$(pwd)/.out:/data/reports" \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  -e CRITICALITY_THRESHOLD_BYTES=2000000 \
  cflt-topic-usage:latest \
  find-zero-topics \
  --reports-path /data/reports \
  --output-format csv
```

CSV output using Kafka inventory as a fallback comparison source:

```bash
docker run --rm \
  -v "$(pwd)/.out:/data/reports" \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e KAFKA_API_ENDPOINT="$KAFKA_REST" \
  -e KAFKA_API_KEY="$KAFKA_API_KEY" \
  -e KAFKA_API_SECRET="$KAFKA_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  -e TOPIC_SOURCE=kafka \
  cflt-topic-usage:latest \
  find-zero-topics \
  --reports-path /data/reports \
  --output-format csv
```

Optional flags:

- `--topic-source`
- `--cluster-id`
- `--catalog-api-endpoint`
- `--catalog-api-key`
- `--catalog-api-secret`
- `--catalog-page-limit`
- `--kafka-api-endpoint`
- `--kafka-api-key`
- `--kafka-api-secret`
- `--criticality-threshold-bytes`
- `--output-format` (`json` or `csv`)
- `--request-timeout-seconds`
- `--max-retries`

## Change Topic Owner Tool

This flow updates topic ownership metadata in Confluent Catalog.

### High-Level Flow

1. Validates that the topic exists in Catalog
2. Sends a Catalog PUT request to update `owner` and `ownerEmail`
3. Returns a JSON confirmation

### Usage Examples

Default Docker usage:

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$METRICS_API_KEY" \
  -e METRICS_API_SECRET="$METRICS_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  cflt-topic-usage:latest \
  update-topic-owner \
  --topic abc \
  --owner "Someone" \
  --owner-email "anyone@company.com"
```

Default local CLI usage:

```bash
python src/entrypoint.py update-topic-owner \
  --cluster-id "$KAFKA_CLUSTER_ID" \
  --topic abc \
  --owner "Someone" \
  --owner-email "anyone@company.com"
```

Variation with explicit timeout/retry settings:

```bash
docker run --rm \
  -e CLUSTER_ID="$KAFKA_CLUSTER_ID" \
  -e METRICS_API_KEY="$METRICS_API_KEY" \
  -e METRICS_API_SECRET="$METRICS_API_SECRET" \
  -e CATALOG_API_ENDPOINT="$CATALOG_ENDPOINT" \
  -e CATALOG_API_KEY="$CATALOG_API_KEY" \
  -e CATALOG_API_SECRET="$CATALOG_API_SECRET" \
  -e REQUEST_TIMEOUT_SECONDS=60 \
  -e MAX_RETRIES=5 \
  cflt-topic-usage:latest \
  update-topic-owner \
  --topic abc \
  --owner "Someone" \
  --owner-email "anyone@company.com"
```

## Documentation References

- [Confluent Cloud API Reference](https://docs.confluent.io/cloud/current/api.html/)
- [Catalog API Usage Limits and Best Practices](https://docs.confluent.io/cloud/current/stream-governance/stream-catalog-rest-apis.html#catalog-api-usage-limitations-and-best-practices)
- [Confluent Metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)

## Things to take into account

- Kafka V3 API inventory is only used when `TOPIC_SOURCE=kafka`
- `find-zero-topics` includes internal topics by default
- `CRITICALITY_THRESHOLD_BYTES` controls classification levels `1` and `2`
