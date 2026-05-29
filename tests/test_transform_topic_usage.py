from transform.topic_usage import build_topic_usage


def test_build_topic_usage_defaults_and_sorting():
    output = build_topic_usage(
        cluster_id="lkc-123",
        query_interval="now-7d|d/now-5m|m",
        topic_partitions={"b": 2, "a": 3, "c": 1, "d": 1, "e": 1},
        bytes_in={"a": 10.0},
        bytes_out={"b": 5.5, "d": 1.0, "e": 3_000_000.0},
        retained_bytes={"a": 1024.0},
        owners={"a": "team-a"},
        owner_emails={"a": "team-a@company.com"},
    )

    assert output["cluster_id"] == "lkc-123"
    assert output["query_interval"] == "now-7d|d/now-5m|m"
    assert [item["topic"] for item in output["topics"]] == ["a", "b", "c", "d", "e"]

    topic_a = output["topics"][0]
    assert topic_a["bytes_in"] == 10
    assert topic_a["bytes_out"] == 0
    assert topic_a["retained_bytes"] == 1024
    assert topic_a["partitions"] == 3
    assert topic_a["owner"] == "team-a"
    assert topic_a["owner_email"] == "team-a@company.com"
    assert topic_a["criticality_level"] == 4

    topic_b = output["topics"][1]
    assert topic_b["bytes_in"] == 0
    assert topic_b["bytes_out"] == 5.5
    assert topic_b["retained_bytes"] == 0
    assert topic_b["partitions"] == 2
    assert topic_b["owner"] == "unknown"
    assert topic_b["owner_email"] == ""
    assert topic_b["criticality_level"] == 3

    topic_c = output["topics"][2]
    assert topic_c["bytes_in"] == 0
    assert topic_c["bytes_out"] == 0
    assert topic_c["criticality_level"] == 5

    topic_d = output["topics"][3]
    assert topic_d["bytes_in"] == 0
    assert topic_d["bytes_out"] == 1
    assert topic_d["criticality_level"] == 3

    topic_e = output["topics"][4]
    assert topic_e["bytes_in"] == 0
    assert topic_e["bytes_out"] == 3000000
    assert topic_e["criticality_level"] == 3


def test_build_topic_usage_criticality_level_for_bidirectional_traffic():
    output = build_topic_usage(
        cluster_id="lkc-123",
        query_interval="now-7d/now-5m",
        topic_partitions={"low": 1, "high": 1},
        bytes_in={"low": 200_000.0, "high": 2_000_000.0},
        bytes_out={"low": 300_000.0, "high": 2_000_000.0},
        retained_bytes={},
        owners={},
        owner_emails={},
    )

    low = output["topics"][1]
    high = output["topics"][0]

    assert high["topic"] == "high"
    assert high["criticality_level"] == 1

    assert low["topic"] == "low"
    assert low["criticality_level"] == 2


def test_build_topic_usage_criticality_level_uses_custom_threshold():
    output = build_topic_usage(
        cluster_id="lkc-123",
        query_interval="now-7d/now-5m",
        topic_partitions={"t": 1},
        bytes_in={"t": 700_000.0},
        bytes_out={"t": 700_000.0},
        retained_bytes={},
        owners={},
        owner_emails={},
        criticality_threshold_bytes=600_000,
    )

    topic = output["topics"][0]
    assert topic["criticality_level"] == 1


def test_build_topic_usage_rejects_non_positive_threshold():
    try:
        build_topic_usage(
            cluster_id="lkc-123",
            query_interval="now-7d/now-5m",
            topic_partitions={"t": 1},
            bytes_in={"t": 1.0},
            bytes_out={"t": 1.0},
            retained_bytes={},
            owners={},
            owner_emails={},
            criticality_threshold_bytes=0,
        )
        assert False, "Expected ValueError for non-positive criticality threshold"
    except ValueError as exc:
        assert "criticality_threshold_bytes must be >= 1" in str(exc)
