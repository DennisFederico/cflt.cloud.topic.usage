from collections import defaultdict
from typing import Any

from http_client import HttpClient


QUERY_INTERVAL_LAST_7_DAYS = "now-7d/now-5m"


class MetricsApiClient:
    def __init__(self, http_client: HttpClient):
        self.http = http_client

    def get_topic_bytes(
        self,
        cluster_id: str,
        interval: str = QUERY_INTERVAL_LAST_7_DAYS,
    ) -> tuple[dict[str, float], dict[str, float]]:
        bytes_in = self._query_metric(cluster_id, "io.confluent.kafka.server/received_bytes", interval)
        bytes_out = self._query_metric(cluster_id, "io.confluent.kafka.server/sent_bytes", interval)
        return bytes_in, bytes_out

    def _query_metric(
        self,
        cluster_id: str,
        metric_name: str,
        interval: str = QUERY_INTERVAL_LAST_7_DAYS,
    ) -> dict[str, float]:
        results: dict[str, float] = defaultdict(float)
        page_token: str | None = None

        while True:
            params = {"page_token": page_token} if page_token else None
            payload = {
                "aggregations": [{"metric": metric_name}],
                "group_by": ["metric.topic"],
                "filter": {
                    "field": "resource.kafka.id",
                    "op": "EQ",
                    "value": cluster_id,
                },
                "granularity": "ALL",
                "intervals": [interval],
                "format": "GROUPED",
                "limit": 1000,
            }

            response = self.http.request_json(
                method="POST",
                path_or_url="/v2/metrics/cloud/query",
                params=params,
                json_body=payload,
            )

            data = response.get("data", [])
            if data and "points" in data[0]:
                self._consume_grouped_results(data, results)
            else:
                self._consume_flat_results(data, results)

            page_token = (
                response.get("meta", {})
                .get("pagination", {})
                .get("next_page_token")
            )
            if not page_token:
                break

        return dict(results)

    @staticmethod
    def _consume_grouped_results(data: list[dict[str, Any]], out: dict[str, float]) -> None:
        for group in data:
            topic = group.get("metric.topic")
            if not topic:
                continue
            points = group.get("points", [])
            topic_sum = 0.0
            for point in points:
                try:
                    topic_sum += float(point.get("value", 0.0))
                except (TypeError, ValueError):
                    continue
            out[topic] = out.get(topic, 0.0) + topic_sum

    @staticmethod
    def _consume_flat_results(data: list[dict[str, Any]], out: dict[str, float]) -> None:
        for record in data:
            topic = record.get("metric.topic")
            if not topic:
                continue
            try:
                out[topic] = out.get(topic, 0.0) + float(record.get("value", 0.0))
            except (TypeError, ValueError):
                continue
