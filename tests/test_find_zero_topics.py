from pathlib import Path

from find_zero_topics import _aggregate_topic_bytes, _find_zero_topics, _find_non_zero_topics


def test_aggregate_topic_bytes_supports_legacy_and_current_fields(tmp_path: Path):
    report_1 = {
        "cluster_id": "lkc-1",
        "topics": [
            {"topic": "a", "bytes_in": 0, "bytes_out": 10},
            {"topic": "b", "bytes_in": 5, "bytes_out": 0},
        ],
    }
    report_2 = {
        "cluster_id": "lkc-1",
        "topics": [
            {"topic": "a", "bytes_in_7d": 0, "bytes_out_7d": 0},
            {"topic": "b", "bytes_in_7d": 2, "bytes_out_7d": 0},
        ],
    }
    report_other_cluster = {
        "cluster_id": "lkc-2",
        "topics": [{"topic": "a", "bytes_in": 100, "bytes_out": 100}],
    }

    file_1 = tmp_path / "report1.json"
    file_1.write_text(__import__("json").dumps(report_1), encoding="utf-8")
    file_2 = tmp_path / "report2.json"
    file_2.write_text(__import__("json").dumps(report_2), encoding="utf-8")
    file_3 = tmp_path / "report3.json"
    file_3.write_text(__import__("json").dumps(report_other_cluster), encoding="utf-8")

    totals, reports_used = _aggregate_topic_bytes([file_1, file_2, file_3], cluster_id="lkc-1")

    assert reports_used == 2
    assert totals["a"] == {"bytes_in": 0.0, "bytes_out": 10.0}
    assert totals["b"] == {"bytes_in": 7.0, "bytes_out": 0.0}


def test_find_zero_topics_only_existing_topics():
    existing_topics = ["a", "b", "c"]
    totals = {
        "a": {"bytes_in": 0.0, "bytes_out": 10.0},
        "b": {"bytes_in": 7.0, "bytes_out": 0.0},
        "z_deleted": {"bytes_in": 0.0, "bytes_out": 0.0},
    }

    zero_topics = _find_zero_topics(existing_topics, totals)

    assert zero_topics == [
        {"topic": "a", "zero_bytes_in": True, "zero_bytes_out": False},
        {"topic": "b", "zero_bytes_in": False, "zero_bytes_out": True},
        {"topic": "c", "zero_bytes_in": True, "zero_bytes_out": True},
    ]


def test_find_non_zero_topics():
    existing_topics = ["a", "b", "c", "d"]
    totals = {
        "a": {"bytes_in": 0.0, "bytes_out": 10.0},
        "b": {"bytes_in": 7.0, "bytes_out": 0.0},
        "c": {"bytes_in": 5.0, "bytes_out": 3.0},
    }

    non_zero = _find_non_zero_topics(existing_topics, totals)

    assert non_zero == ["c"]
