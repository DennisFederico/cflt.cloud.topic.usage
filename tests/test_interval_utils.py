from datetime import datetime, timezone

import pytest

from interval_utils import resolve_interval_window


def test_resolve_relative_interval_against_now():
    now = datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc)

    start, end = resolve_interval_window("now-7d/now-5m", now=now)

    assert start == datetime(2026, 4, 27, 12, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 4, 11, 55, 0, tzinfo=timezone.utc)


def test_resolve_iso_interval():
    start, end = resolve_interval_window("2026-05-01T00:00:00Z/2026-05-04T00:00:00Z")

    assert start == datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 5, 4, 0, 0, 0, tzinfo=timezone.utc)


def test_resolve_invalid_interval_raises():
    with pytest.raises(ValueError):
        resolve_interval_window("now-7d")
