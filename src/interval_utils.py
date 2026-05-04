from datetime import datetime, timedelta, timezone
import re


_RELATIVE_NOW_PATTERN = re.compile(r"^now(?:(?P<sign>[+-])(?P<value>\d+)(?P<unit>[smhdw]))?$")


def resolve_interval_window(interval: str, now: datetime | None = None) -> tuple[datetime, datetime]:
    """Resolve a Confluent-style interval into UTC start/end datetimes.

    Supported forms:
    - now-7d/now-5m
    - now/now
    - 2026-05-01T00:00:00Z/2026-05-04T00:00:00Z
    - mixed absolute and relative endpoints
    """
    if "/" not in interval:
        raise ValueError(f"Invalid interval '{interval}'. Expected '<start>/<end>'.")

    start_expr, end_expr = interval.split("/", 1)
    reference_now = now or datetime.now(timezone.utc)

    start = _resolve_time_expr(start_expr.strip(), reference_now)
    end = _resolve_time_expr(end_expr.strip(), reference_now)

    if start > end:
        raise ValueError(f"Invalid interval '{interval}'. Start is after end.")

    return start, end


def _resolve_time_expr(expr: str, now: datetime) -> datetime:
    match = _RELATIVE_NOW_PATTERN.match(expr)
    if match:
        sign = match.group("sign")
        value = match.group("value")
        unit = match.group("unit")
        if sign and value and unit:
            delta = _to_timedelta(int(value), unit)
            return now - delta if sign == "-" else now + delta
        return now

    return _parse_iso_datetime(expr)


def _to_timedelta(value: int, unit: str) -> timedelta:
    unit_to_kwargs = {
        "s": {"seconds": value},
        "m": {"minutes": value},
        "h": {"hours": value},
        "d": {"days": value},
        "w": {"weeks": value},
    }
    if unit not in unit_to_kwargs:
        raise ValueError(f"Unsupported duration unit: {unit}")
    return timedelta(**unit_to_kwargs[unit])


def _parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
