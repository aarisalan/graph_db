# app/utils.py
# Utilities to normalize datetimes and build Neo4j datetime parameter maps.

from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Tuple, Union

DateLike = Union[datetime, str]

__all__ = [
    "coerce_to_datetime",
    "tz_to_offset",
    "to_neo_datetime_params",
    "ensure_datetime_param",
    "range_to_neo_params",
]

def coerce_to_datetime(any_dt: DateLike) -> datetime:
    # Normalize to datetime; supports ISO date, ISO datetime, or "YYYY-MM-DD HH:MM".
    if isinstance(any_dt, datetime):
        return any_dt
    s = str(any_dt).strip()
    if len(s) == 10:
        return datetime.fromisoformat(s + "T00:00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        except Exception:
            return datetime.fromisoformat(s[:10] + "T00:00:00")

def tz_to_offset(tz: Optional[str]) -> Optional[str]:
    # Minimal IANA/alias → fixed offset mapping.
    if not tz:
        return None
    tzu = tz.upper()
    if tzu in ("UTC", "GMT"):
        return "+00:00"
    if tz in ("Europe/Istanbul", "Turkey", "TRT", "TR"):
        return "+03:00"
    return None

def to_neo_datetime_params(dt: datetime, tz_offset: Optional[str] = None) -> Dict[str, int | str]:
    # Build dict for Neo4j datetime($p).
    params: Dict[str, int | str] = {
        "year": dt.year,
        "month": dt.month,
        "day": dt.day,
        "hour": dt.hour,
        "minute": dt.minute,
        "second": dt.second,
    }
    if tz_offset:
        params["timezone"] = tz_offset
    return params

def ensure_datetime_param(any_dt: DateLike, tz: Optional[str] = None, default_offset: Optional[str] = None) -> Dict[str, int | str]:
    # Coerce + attach offset (from tz or default) → Neo4j param map.
    dt = coerce_to_datetime(any_dt)
    offset = tz_to_offset(tz) or default_offset
    return to_neo_datetime_params(dt, tz_offset=offset)

def range_to_neo_params(start_at: DateLike, end_at: DateLike, tz: Optional[str] = None, default_offset: Optional[str] = None) -> Tuple[Dict[str, int | str], Dict[str, int | str]]:
    # Start/end param maps for range MATCH/WHERE clauses.
    return (
        ensure_datetime_param(start_at, tz=tz, default_offset=default_offset),
        ensure_datetime_param(end_at,   tz=tz, default_offset=default_offset),
    )
