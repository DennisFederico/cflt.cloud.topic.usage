from datetime import datetime, timezone

from main import _build_output_filename


def test_build_output_filename_includes_cluster_and_window():
    file_name = _build_output_filename(
        cluster_id="lkc-00k589",
        interval_start=datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
        interval_end=datetime(2026, 5, 4, 11, 55, 0, tzinfo=timezone.utc),
    )

    assert file_name == "topic-usage_lkc-00k589_20260501T000000Z_20260504T115500Z.json"
