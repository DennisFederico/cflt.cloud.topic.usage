from pathlib import Path

from find_zero_topics import _aggregate_topic_bytes, _build_summary, _build_topics_output, _render_csv_output


def test_aggregate_topic_bytes_supports_legacy_and_current_fields(tmp_path: Path):
    report_1 = {
        "cluster_id": "lkc-1",
        "generated_at": "2026-05-28T10:00:00+00:00",
        "topics": [
            {
                "topic": "a",
                "bytes_in": 0,
                "bytes_out": 10,
                "retained_bytes": 50,
                "partitions": 2,
                "owner": "team-a",
                "owner_email": "team-a@company.com",
            },
            {"topic": "b", "bytes_in": 5, "bytes_out": 0, "partitions": 1},
        ],
    }
    report_2 = {
        "cluster_id": "lkc-1",
        "generated_at": "2026-05-29T10:00:00+00:00",
        "topics": [
            {
                "topic": "a",
                "bytes_in_7d": 0,
                "bytes_out_7d": 0,
                "retained_bytes": 99,
                "partitions": 5,
                "owner": "team-a-new",
                "owner_email": "team-a-new@company.com",
            },
            {"topic": "b", "bytes_in_7d": 2, "bytes_out_7d": 0, "partitions": 3},
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

    totals, latest_topic_snapshot, reports_used = _aggregate_topic_bytes([file_1, file_2, file_3], cluster_id="lkc-1")

    assert reports_used == 2
    assert totals["a"] == {"bytes_in": 0.0, "bytes_out": 10.0}
    assert totals["b"] == {"bytes_in": 7.0, "bytes_out": 0.0}
    assert latest_topic_snapshot["a"]["retained_bytes"] == 99.0
    assert latest_topic_snapshot["a"]["partitions"] == 5
    assert latest_topic_snapshot["a"]["owner"] == "team-a-new"
    assert latest_topic_snapshot["a"]["owner_email"] == "team-a-new@company.com"


def test_build_topics_output_and_summary():
    existing_topics = {"a": 1, "b": 1, "c": 2, "d": 3, "e": 4}
    totals = {
        "a": {"bytes_in": 0.0, "bytes_out": 10.0},
        "b": {"bytes_in": 7.0, "bytes_out": 0.0},
        "d": {"bytes_in": 2_000_000.0, "bytes_out": 2_000_000.0},
        "e": {"bytes_in": 600_000.0, "bytes_out": 600_000.0},
    }
    latest_topic_snapshot = {
        "a": {"retained_bytes": 11.0, "partitions": 5, "owner": "oa", "owner_email": "oa@company.com"},
        "b": {"retained_bytes": 22.0, "partitions": 6, "owner": "ob", "owner_email": "ob@company.com"},
        "c": {"retained_bytes": 33.0, "partitions": 7, "owner": "oc", "owner_email": "oc@company.com"},
        "d": {"retained_bytes": 44.0, "partitions": 8, "owner": "od", "owner_email": "od@company.com"},
        "e": {"retained_bytes": 55.0, "partitions": 9, "owner": "oe", "owner_email": "oe@company.com"},
    }

    topics = _build_topics_output(
        existing_topics=existing_topics,
        totals=totals,
        latest_topic_snapshot=latest_topic_snapshot,
        criticality_threshold_bytes=1_048_576,
    )
    summary = _build_summary(topics)

    # Sorted by criticality level then topic
    assert [topic["topic"] for topic in topics] == ["d", "e", "a", "b", "c"]

    topic_d = topics[0]
    assert topic_d["criticality_level"] == 1
    assert topic_d["zero_bytes_in"] is False
    assert topic_d["zero_bytes_out"] is False

    topic_e = topics[1]
    assert topic_e["criticality_level"] == 2

    topic_a = topics[2]
    assert topic_a["criticality_level"] == 3
    assert topic_a["retained_bytes"] == 11
    assert topic_a["partitions"] == 5

    topic_b = topics[3]
    assert topic_b["criticality_level"] == 4

    topic_c = topics[4]
    assert topic_c["criticality_level"] == 5
    assert topic_c["bytes_in"] == 0
    assert topic_c["bytes_out"] == 0

    assert summary == {
        "partitions_per_criticality": {
            "1": 8,
            "2": 9,
            "3": 5,
            "4": 6,
            "5": 7,
        }
    }


def test_render_csv_output_has_header_topics_and_summary_lines():
    topics = [
        {
            "topic": "a",
            "bytes_in": 0,
            "bytes_out": 10,
            "retained_bytes": 11,
            "partitions": 5,
            "owner": "oa",
            "owner_email": "oa@company.com",
            "criticality_level": 3,
            "zero_bytes_in": True,
            "zero_bytes_out": False,
        },
        {
            "topic": "b",
            "bytes_in": 7,
            "bytes_out": 0,
            "retained_bytes": 22,
            "partitions": 6,
            "owner": "ob",
            "owner_email": "ob@company.com",
            "criticality_level": 4,
            "zero_bytes_in": False,
            "zero_bytes_out": True,
        },
    ]
    summary = {
        "partitions_per_criticality": {
            "1": 0,
            "2": 0,
            "3": 5,
            "4": 6,
            "5": 0,
        }
    }

    csv_text = _render_csv_output(topics, summary)
    lines = csv_text.splitlines()

    assert lines[0] == "topic,bytes_in,bytes_out,retained_bytes,partitions,owner,owner_email,criticality_level,zero_bytes_in,zero_bytes_out"
    assert lines[1] == "a,0,10,11,5,oa,oa@company.com,3,true,false"
    assert lines[2] == "b,7,0,22,6,ob,ob@company.com,4,false,true"
    assert lines[3] == ""
    assert lines[4] == "summary,criticality_level,partitions"
    assert lines[5] == "partitions_per_criticality,1,0"
    assert lines[6] == "partitions_per_criticality,2,0"
    assert lines[7] == "partitions_per_criticality,3,5"
    assert lines[8] == "partitions_per_criticality,4,6"
    assert lines[9] == "partitions_per_criticality,5,0"
