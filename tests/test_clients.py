from clients.catalog_api import CatalogApiClient
from clients.metrics_api import MetricsApiClient


class DummyHttpClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def request_json(self, method, path_or_url, params=None, json_body=None, extra_headers=None, allow_404=False):
        self.calls.append(
            {
                "method": method,
                "path_or_url": path_or_url,
                "params": params,
                "json_body": json_body,
                "allow_404": allow_404,
            }
        )
        key = (method, path_or_url, (params or {}).get("page_token"))
        return self.responses.get(key)


def test_metrics_client_grouped_and_pagination():
    responses = {
        ("POST", "/v2/metrics/cloud/query", None): {
            "data": [
                {"metric.topic": "t1", "points": [{"value": 10}]},
                {"metric.topic": "t2", "points": [{"value": 5.5}]},
            ],
            "meta": {"pagination": {"next_page_token": "next"}},
        },
        ("POST", "/v2/metrics/cloud/query", "next"): {
            "data": [{"metric.topic": "t1", "points": [{"value": 2}]}],
            "meta": {"pagination": {}},
        },
    }
    client = MetricsApiClient(DummyHttpClient(responses))

    result = client._query_metric("lkc-1", "io.confluent.kafka.server/received_bytes")
    assert result == {"t1": 12.0, "t2": 5.5}


def test_catalog_owner_extraction_and_fallback():
    response = {
        ("GET", "/catalog/v1/entity/type/kafka_topic/name/lkc-1%3Atopic-1", None): {
            "entity": {"attributes": {"owner": "team-a", "ownerEmail": "team-a@company.com"}}
        },
        ("GET", "/catalog/v1/entity/type/kafka_topic/name/lkc-1%3Atopic-2", None): None,
    }
    client = CatalogApiClient(DummyHttpClient(response))

    owner, owner_email = client.get_topic_owner_info("lkc-1", "topic-1")
    assert owner == "team-a"
    assert owner_email == "team-a@company.com"

    owner2, owner_email2 = client.get_topic_owner_info("lkc-1", "topic-2")
    assert owner2 == "unknown"
    assert owner_email2 == ""
