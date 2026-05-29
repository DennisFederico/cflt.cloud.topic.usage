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
        key = (method, path_or_url, tuple(sorted((params or {}).items())))
        return self.responses.get(key)


def test_metrics_client_grouped_and_pagination():
    responses = {
        ("POST", "/v2/metrics/cloud/query", ()): {
            "data": [
                {"metric.topic": "t1", "points": [{"value": 10}]},
                {"metric.topic": "t2", "points": [{"value": 5.5}]},
            ],
            "meta": {"pagination": {"next_page_token": "next"}},
        },
        ("POST", "/v2/metrics/cloud/query", (("page_token", "next"),)): {
            "data": [{"metric.topic": "t1", "points": [{"value": 2}]}],
            "meta": {"pagination": {}},
        },
    }
    client = MetricsApiClient(DummyHttpClient(responses))

    result = client._query_metric("lkc-1", "io.confluent.kafka.server/received_bytes")
    assert result == {"t1": 12.0, "t2": 5.5}


def test_metrics_client_retained_bytes_snapshot_query_shape():
    responses = {
        ("POST", "/v2/metrics/cloud/query", ()): {
            "data": [
                {"metric.topic": "topic-a", "points": [{"value": 2048}]},
            ],
            "meta": {"pagination": {}},
        }
    }
    http = DummyHttpClient(responses)
    client = MetricsApiClient(http)

    result = client.get_topic_retained_bytes("lkc-1")

    assert result == {"topic-a": 2048.0}
    payload = http.calls[0]["json_body"]
    assert payload["aggregations"][0]["metric"] == "io.confluent.kafka.server/retained_bytes"
    assert payload["intervals"] == ["now-5m/now-4m"]
    assert payload["granularity"] == "PT1M"


def test_catalog_owner_extraction_and_fallback():
    response = {
        ("GET", "/catalog/v1/entity/type/kafka_topic/name/lkc-1%3Atopic-1", ()): {
            "entity": {"attributes": {"owner": "team-a", "ownerEmail": "team-a@company.com"}}
        },
        ("GET", "/catalog/v1/entity/type/kafka_topic/name/lkc-1%3Atopic-2", ()): None,
    }
    client = CatalogApiClient(DummyHttpClient(response))

    owner, owner_email = client.get_topic_owner_info("lkc-1", "topic-1")
    assert owner == "team-a"
    assert owner_email == "team-a@company.com"

    owner2, owner_email2 = client.get_topic_owner_info("lkc-1", "topic-2")
    assert owner2 == "unknown"
    assert owner_email2 == ""


def test_catalog_list_topics_with_partitions_paginated():
    response = {
        (
            "GET",
            "/catalog/v1/search/attribute?type=kafka_topic&attrName=qualifiedName&attrValuePrefix=lkc-1&attr=partitionsCount&attr=owner&attr=ownerEmail&deleted=false&limit=2&offset=0",
            (),
        ): {
            "entities": [
                {
                    "entity": {
                        "attributes": {
                            "name": "topic-1",
                            "qualifiedName": "lkc-1:topic-1",
                            "partitionsCount": 3,
                            "owner": "owner-a",
                            "ownerEmail": "owner-a@example.com",
                        }
                    }
                },
                {
                    "entity": {
                        "attributes": {
                            "qualifiedName": "tenant-a:lkc-1:topic-2",
                            "partitionsCount": "7",
                        }
                    }
                },
            ]
        },
        (
            "GET",
            "/catalog/v1/search/attribute?type=kafka_topic&attrName=qualifiedName&attrValuePrefix=lkc-1&attr=partitionsCount&attr=owner&attr=ownerEmail&deleted=false&limit=2&offset=2",
            (),
        ): {
            "entities": []
        },
    }

    http = DummyHttpClient(response)
    client = CatalogApiClient(http)
    topics = client.list_topics_with_partitions("lkc-1", page_limit=2)

    assert topics == {"topic-1": 3, "topic-2": 7}
    assert "offset=0" in http.calls[0]["path_or_url"]
    assert "offset=2" in http.calls[1]["path_or_url"]


def test_catalog_list_topics_metadata_parses_owner_and_email_from_list_call():
    response = {
        (
            "GET",
            "/catalog/v1/search/attribute?type=kafka_topic&attrName=qualifiedName&attrValuePrefix=lkc-1&attr=partitionsCount&attr=owner&attr=ownerEmail&deleted=false&limit=500&offset=0",
            (),
        ): {
            "entities": [
                {
                    "typeName": "kafka_topic",
                    "attributes": {
                        "name": "test-123",
                        "qualifiedName": "tenant-a:lkc-1:test-123",
                        "partitionsCount": 6,
                        "owner": "testOwner",
                        "ownerEmail": "owner@email.com",
                    },
                },
                {
                    "typeName": "kafka_topic",
                    "attributes": {
                        "name": "topic-without-owner",
                        "qualifiedName": "tenant-a:lkc-1:topic-without-owner",
                        "partitionsCount": 1,
                    },
                },
            ]
        },
        (
            "GET",
            "/catalog/v1/search/attribute?type=kafka_topic&attrName=qualifiedName&attrValuePrefix=lkc-1&attr=partitionsCount&attr=owner&attr=ownerEmail&deleted=false&limit=500&offset=500",
            (),
        ): {
            "entities": []
        },
    }
    client = CatalogApiClient(DummyHttpClient(response))

    partitions, owners, owner_emails = client.list_topics_metadata("lkc-1")

    assert partitions == {"test-123": 6, "topic-without-owner": 1}
    assert owners == {"test-123": "testOwner", "topic-without-owner": "unknown"}
    assert owner_emails == {"test-123": "owner@email.com", "topic-without-owner": ""}


def test_catalog_list_topics_with_partitions_includes_internal_by_default():
    response = {
        (
            "GET",
            "/catalog/v1/search/attribute?type=kafka_topic&attrName=qualifiedName&attrValuePrefix=lkc-1&attr=partitionsCount&attr=owner&attr=ownerEmail&deleted=false&limit=500&offset=0",
            (),
        ): {
            "entities": [
                {
                    "entity": {
                        "attributes": {
                            "name": "_confluent-internal",
                            "qualifiedName": "lkc-1:_confluent-internal",
                            "partitionsCount": 1,
                        }
                    }
                },
                {
                    "entity": {
                        "attributes": {
                            "name": "business-topic",
                            "qualifiedName": "lkc-1:business-topic",
                            "partitionsCount": 2,
                        }
                    }
                },
            ]
        },
    }
    client = CatalogApiClient(DummyHttpClient(response))

    topics = client.list_topics_with_partitions("lkc-1")

    assert topics == {"_confluent-internal": 1, "business-topic": 2}


def test_catalog_list_topics_with_partitions_can_exclude_internal_topics():
    response = {
        (
            "GET",
            "/catalog/v1/search/attribute?type=kafka_topic&attrName=qualifiedName&attrValuePrefix=lkc-1&attr=partitionsCount&attr=owner&attr=ownerEmail&deleted=false&limit=500&offset=0",
            (),
        ): {
            "entities": [
                {
                    "entity": {
                        "attributes": {
                            "name": "_confluent-internal",
                            "qualifiedName": "lkc-1:_confluent-internal",
                            "partitionsCount": 1,
                        }
                    }
                },
                {
                    "entity": {
                        "attributes": {
                            "name": "business-topic",
                            "qualifiedName": "lkc-1:business-topic",
                            "partitionsCount": 2,
                        }
                    }
                },
            ]
        },
    }
    client = CatalogApiClient(DummyHttpClient(response))

    topics = client.list_topics_with_partitions("lkc-1", include_internal_topics=False)

    assert topics == {"business-topic": 2}
