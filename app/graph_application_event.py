# app/graph_application_event.py
# Upserts ApplicationEvent nodes and Field→ApplicationEvent relationships.

from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.agromatiq import get_activities
from app.utils import ensure_datetime_param


def _as_date(v: Any) -> date:
    # Normalize input to date (accepts datetime/ISO-like strings).
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()

def _norm_str(s: Optional[str]) -> Optional[str]:
    # Trim whitespace and convert empty strings to None.
    if s is None:
        return None
    s2 = s.strip()
    return s2 if s2 else None

def _pick_app_type(a: Any) -> str:
    # Pick application type from best-available attribute; fallback to "unknown".
    for k in ("type_code", "type_name", "sub_type_code", "category_code"):
        val = getattr(a, k, None)
        if val:
            return str(val).strip()
    return "unknown"

def _area_unit_to_da_factor(unit: Optional[str]) -> Optional[float]:
    # Convert area unit to "decare" factor (da=1, ha=10, m2=0.01); unknown→None.
    if not unit:
        return None
    u = unit.strip().lower()
    if u in {"da", "decare", "dekar"}:
        return 1.0
    if u == "ha":
        return 10.0
    if u in {"m2", "m²"}:
        return 0.01
    return None

def _to_liters(amount: Optional[float], unit_abbr: Optional[str]) -> Optional[float]:
    # Convert amount to liters (supports L/ml only); unknown units→None.
    if amount is None:
        return None
    u = (unit_abbr or "").strip().lower()
    if u in {"l", "lt", "ltr"}:
        return float(amount)
    if u in {"ml"}:
        return float(amount) / 1000.0
    return None

def _estimate_water_per_da_L(inventories: Optional[List[Dict[str, Any]]]) -> Tuple[Optional[float], Optional[float]]:
    # Heuristic: sum water volume per decare from inventory rows; return (L/da, area_da).
    if not inventories:
        return None, None

    total_per_da = 0.0
    found = False

    for item in inventories:
        try:
            liters = _to_liters(item.get("amount"), item.get("amount_unit_abbr"))
            if liters is None:
                continue
            factor = _area_unit_to_da_factor(item.get("dose_per_unit"))
            if factor is None or factor == 0:
                continue
            total_per_da += liters / factor
            found = True
        except Exception:
            continue

    if not found:
        return None, None
    return total_per_da, 1.0

async def upsert_application_events(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Counters for debugging/telemetry.
    fields_tried = 0
    nodes = 0
    rels = 0

    # Per-(field,date,crop,app_type) incremental index.
    counters: Dict[Tuple[int, str, str, str], int] = {}

    # Iterate days in inclusive window.
    day = start_at.date()
    end_day = end_at.date()

    while day <= end_day:
        # Use midnight timestamp to fetch activities of that day.
        created_at_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            field_name = getattr(f, "name", None)
            if field_id is None:
                continue

            try:
                # Fetch field activities for the day from Postgres.
                acts = await get_activities(pg_pool, field_id=field_id, created_at=created_at_dt)
            except Exception as e:
                # Log and continue on data source errors.
                print(f"[DBG] field_id={field_id}: get_activities error {e!r}, skipping")
                continue

            if not acts:
                continue

            for a in acts:
                # Basic descriptors extracted from activity.
                crop_name   = _norm_str(getattr(a, "crop_name", None)) or ""
                app_type    = _pick_app_type(a)
                start_at_ts = getattr(a, "start_at", None)
                date_for_ev = _as_date(start_at_ts or day)

                # Localized date param for Neo4j temporal fields.
                date_params = ensure_datetime_param(date_for_ev, tz="Europe/Istanbul", default_offset="+03:00")

                # Optional lab/notes metrics.
                ph  = getattr(a, "ph", None)
                ec  = getattr(a, "ec", None)
                note= getattr(a, "notes", None)

                # Estimate water per decare from inventories.
                inventories = getattr(a, "inventories", None) or []
                water_L, area_da = _estimate_water_per_da_L(inventories)

                # Sequential idx per (field, date, crop, app_type).
                key = (field_id, date_for_ev.isoformat(), crop_name, app_type)
                idx = counters.get(key, 0) + 1
                counters[key] = idx

                # Upsert ApplicationEvent node.
                await session.run(
                    """
                    MERGE (ae:ApplicationEvent {
                        field_id: $field_id,
                        date: datetime($date),
                        crop_name: $crop_name,
                        app_type: $app_type,
                        idx: $idx
                    })
                    SET ae.field_name     = $field_name,
                        ae.water_volume_L = $water_L,
                        ae.area_da        = $area_da,
                        ae.pH             = $ph,
                        ae.EC             = $ec,
                        ae.comment        = $comment,
                        ae.start_at       = datetime($start_at)
                    """,
                    field_id=field_id,
                    field_name=field_name,
                    date=date_params,
                    crop_name=crop_name,
                    app_type=app_type,
                    idx=idx,
                    water_L=water_L,
                    area_da=area_da,
                    ph=ph,
                    ec=ec,
                    comment=_norm_str(note),
                    start_at=ensure_datetime_param(
                        start_at_ts or datetime.combine(date_for_ev, datetime.min.time()),
                        tz="Europe/Istanbul",
                        default_offset="+03:00"
                    ),
                )
                nodes += 1

                # Link Field → ApplicationEvent.
                await session.run(
                    """
                    MATCH (fld:Field { field_id: $field_id })
                    MATCH (ae:ApplicationEvent {
                        field_id: $field_id,
                        date: datetime($date),
                        crop_name: $crop_name,
                        app_type: $app_type,
                        idx: $idx
                    })
                    MERGE (fld)-[:HAS_APPLICATION]->(ae)
                    """,
                    field_id=field_id,
                    date=date_params,
                    crop_name=crop_name,
                    app_type=app_type,
                    idx=idx,
                )
                rels += 1

        # Advance to next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final summary log.
    print(f"[DBG] application_events summary → fields_tried:{fields_tried} nodes:{nodes} rels:{rels}")
