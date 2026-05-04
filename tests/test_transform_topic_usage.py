from transform.topic_usage import build_topic_usage


def test_build_topic_usage_defaults_and_sorting():
    output = build_topic_usage(
        cluster_id="lkc-123",
        query_interval="now-7d|d/now-5m|m",
        topic_partitions={"b": 2, "a": 3},
        bytes_in={"a": 10.0},
        bytes_out={"b": 5.5},
        owners={"a": "team-a"},
        owner_emails={"a": "team-a@company.com"},
    )

    assert output["cluster_id"] == "lkc-123"
    assert output["query_interval"] == "now-7d|d/now-5m|m"
    assert [item["topic"] for item in output["topics"]] == ["a", "b"]

    topic_a = output["topics"][0]
    assert topic_a["bytes_in"] == 10
    assert topic_a["bytes_out"] == 0
    assert topic_a["partitions"] == 3
    assert topic_a["owner"] == "team-a"
    assert topic_a["owner_email"] == "team-a@company.com"

    topic_b = output["topics"][1]
    assert topic_b["bytes_in"] == 0
    assert topic_b["bytes_out"] == 5.5
    assert topic_b["partitions"] == 2
    assert topic_b["owner"] == "unknown"
    assert topic_b["owner_email"] == ""
