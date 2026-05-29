# Prometheus Topic Usage Reporter

Standalone project that:

1. Scrapes Confluent Cloud metrics via `/v2/metrics/cloud/export` using Prometheus.
2. Stores time-series locally in Prometheus TSDB.
3. Generates JSON reports for topic `bytes_in`/`bytes_out` over a period (default `30d`).
4. Flags unused topics (`bytes_in == 0` and `bytes_out == 0`) among existing Kafka topics.

## Project Layout

- `docker-compose.yml`: Runs Prometheus in a container.
- `prometheus/prometheus.yml.tmpl`: Scrape template for Confluent Cloud export endpoint.
- `scripts/render_prometheus_config.py`: Renders `prometheus.yml` from environment variables.
- `scripts/report_topic_usage.py`: Reporting CLI using Prometheus API + Kafka REST API.
- `.env.example`: Environment variable template.
- `scripts/requirements.txt`: Python dependencies for reporting scripts.

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Confluent **Cloud API Key/Secret** (not Kafka cluster API key/secret) for telemetry scraping
- Kafka REST credentials for topic inventory (`KAFKA_API_KEY`/`KAFKA_API_SECRET`)

## 1) Configure Environment

Copy and edit:

```bash
cp .env.example .env
```

Required values in `.env`:

- `CFLT_CLOUD_API_KEY`
- `CFLT_CLOUD_API_SECRET`
- `CFLT_CLUSTER_ID` (example: `lkc-abc123`)
- `KAFKA_API_ENDPOINT` (example: `https://pkc-xxxxx.region.provider.confluent.cloud`)
- `KAFKA_API_KEY`
- `KAFKA_API_SECRET`

Optional:

- `PROM_RETENTION_TIME` (default `180d`)
- `PROM_SCRAPE_INTERVAL` (default `1m`)
- `PROM_SCRAPE_TIMEOUT` (default `55s`)

## 2) Render Prometheus Config

```bash
python3 scripts/render_prometheus_config.py
```

This creates `prometheus/prometheus.yml` from `prometheus/prometheus.yml.tmpl`.

## 3) Start Prometheus

```bash
docker compose up -d
```

Prometheus UI: `http://localhost:9090`

## 4) Generate a 30-day Report

Install deps once:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

Run report:

```bash
python3 scripts/report_topic_usage.py --cluster-id "$CFLT_CLUSTER_ID" --period 30d --output reports/topic-usage-30d.json
```

The script also prints report JSON to stdout.

## Report Semantics

- Prometheus query uses:
  - `sum by(topic) (sum_over_time(confluent_kafka_server_received_bytes{kafka_id="<cluster>"}[<period>]))`
  - `sum by(topic) (sum_over_time(confluent_kafka_server_sent_bytes{kafka_id="<cluster>"}[<period>]))`
- Existing topics are fetched from Kafka REST API.
- `unused_topics` are existing topics where both sums are zero.
- Topics with no time-series in the selected range are treated as zero.

## Notes

- Confluent export values are per-interval deltas and exposed as Prometheus gauges by Confluent; summing over time gives total bytes in/out.
- Keep `honor_timestamps: true` in scrape config because Confluent export uses a timestamp offset.
- If you scrape many clusters/topics, split scrape jobs to stay under Confluent return/rate limits.
