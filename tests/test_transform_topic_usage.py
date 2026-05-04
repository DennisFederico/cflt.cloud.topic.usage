from transform.topic_usage import build_topic_usage


def test_build_topic_usage_defaults_and_sorting():
    output = build_topic_usage(
        cluster_id="lkc-123",
        topic_partitions={"b": 2, "a": 3},
        bytes_in={"a": 10.0},
        bytes_out={"b": 5.5},
        owners={"a": "team-a"},
    )

    assert output["cluster_id"] == "lkc-123"
    assert [item["topic"] for item in output["topics"]] == ["a", "b"]

    topic_a = output["topics"][0]
    assert topic_a["bytes_in_30d"] == 10
    assert topic_a["bytes_out_30d"] == 0
    assert topic_a["partitions"] == 3
    assert topic_a["owner"] == "team-a"

    topic_b = output["topics"][1]
    assert topic_b["bytes_in_30d"] == 0
    assert topic_b["bytes_out_30d"] == 5.5
    assert topic_b["partitions"] == 2
    assert topic_b["owner"] == "unknown"
