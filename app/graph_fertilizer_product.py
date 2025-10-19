# app/graph_fertilizer_product.py
# Upserts FertilizerProduct nodes and links ProductApplication→FertilizerProduct.

from __future__ import annotations

from datetime import datetime, date, time
from typing import Any, Dict, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.agromatiq import get_activities
from app.utils import ensure_datetime_param


# ---------- helpers ----------

def _as_date(v: Any) -> date:
    # Normalize input to date.
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()

def _to_datetime(v: Any, fallback_day: date) -> datetime:
    # Normalize to datetime; accept 'Z' and fallback to given day's midnight.
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, time.min)
    try:
        s = str(v)
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.combine(fallback_day, time.min)

def _norm(s: Optional[str]) -> Optional[str]:
    # Trim and convert empty strings to None.
    if s is None:
        return None
    s2 = s.strip()
    return s2 if s2 else None

def _safe_get(obj: Any, *names: str):
    # Safely read attribute/key from dict/obj/pydantic-like structures.
    if obj is None:
        return None
    if isinstance(obj, dict):
        for n in names:
            if n in obj:
                return obj.get(n)
        return None
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    try:
        d = obj.model_dump()
        for n in names:
            if n in d:
                return d.get(n)
    except Exception:
        pass
    try:
        d = obj.dict()
        for n in names:
            if n in d:
                return d.get(n)
    except Exception:
        pass
    return None

def _pick_app_type(a: Any) -> str:
    # Pick the first available type field as app_type; fallback to "unknown".
    for k in ("type_code", "type_name", "sub_type_code", "category_code"):
        val = getattr(a, k, None)
        if val:
            return str(val).strip()
    return "unknown"

def _make_app_ev_id(field_id: int, date_iso: str, crop_name: str, app_type: str, idx: int) -> str:
    # Stable ApplicationEvent identifier.
    return f"{field_id}:{date_iso}:{crop_name}:{app_type}:{idx}"

# -----------------------------


async def upsert_fertilizer_products(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Telemetry counters.
    fields_tried = 0
    products_written = 0
    rels_written = 0

    # Iterate days in inclusive window.
    day = start_at.date()
    end_day = end_at.date()

    while day <= end_day:
        # Use midnight of current day for activity fetch.
        created_at_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            if field_id is None:
                continue

            try:
                # Fetch activities for field/day.
                acts = await get_activities(pg_pool, field_id=field_id, created_at=created_at_dt)
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_activities error {e!r}, skipping")
                continue

            if not acts:
                continue

            for a in acts:
                # Extract descriptors and localize date.
                crop_name   = _norm(getattr(a, "crop_name", None)) or ""
                app_type    = _pick_app_type(a)
                start_ts    = getattr(a, "start_at", None)
                ev_date     = _as_date(start_ts or day)
                date_params = ensure_datetime_param(ev_date, tz="Europe/Istanbul", default_offset="+03:00")

                # Find the AE idx closest to activity start time.
                rec = await session.run(
                    """
                    MATCH (ae:ApplicationEvent {
                        field_id: $field_id,
                        date: datetime($date),
                        crop_name: $crop_name,
                        app_type: $app_type
                    })
                    RETURN ae.idx AS idx, ae.start_at AS start_at
                    ORDER BY ae.start_at ASC, ae.idx ASC
                    """,
                    field_id=field_id, date=date_params, crop_name=crop_name, app_type=app_type,
                )
                ae_list = await rec.data()
                if not ae_list:
                    print(f"[DBG] Missing ApplicationEvent for field_id={field_id} {ev_date} {crop_name} {app_type}; skip products")
                    continue

                # Convert various temporal shapes to epoch seconds.
                def _ts_to_float(ts) -> float:
                    try:
                        if isinstance(ts, dict):
                            comps = (
                                int(ts.get("year", 1970)),
                                int(ts.get("month", 1)),
                                int(ts.get("day", 1)),
                                int(ts.get("hour", 0)),
                                int(ts.get("minute", 0)),
                                int(ts.get("second", 0)),
                            )
                            return datetime(*comps).timestamp()
                        if isinstance(ts, datetime):
                            return ts.timestamp()
                        if isinstance(ts, date):
                            return datetime.combine(ts, datetime.min.time()).timestamp()
                        return _to_datetime(ts, ev_date).timestamp()
                    except Exception:
                        return 0.0

                # Pick best idx by minimal |ts - act_ts|.
                act_dt  = _to_datetime(start_ts, ev_date)
                act_sec = act_dt.timestamp()
                best_idx = None
                best_diff = None
                for row in ae_list:
                    ts = row.get("start_at")
                    if ts is None:
                        continue
                    sec = _ts_to_float(ts)
                    diff = abs(sec - act_sec)
                    if best_diff is None or diff < best_diff:
                        best_diff = diff
                        best_idx = row.get("idx")
                if best_idx is None:
                    best_idx = ae_list[0].get("idx")

                app_ev_id = _make_app_ev_id(field_id, ev_date.isoformat(), crop_name, app_type, int(best_idx))

                # Iterate inventories and create/attach FertilizerProduct.
                inventories: List[Dict[str, Any]] = getattr(a, "inventories", None) or []
                prod_idx = 0
                for it in inventories:
                    prod_idx += 1

                    name  = _norm(_safe_get(it, "fertilizer_name"))
                    brand = _norm(_safe_get(it, "fertilizer_brand"))

                    # Require both name and brand for a valid product.
                    if not name or not brand:
                        continue

                    # Optional product metadata.
                    nutrients = _safe_get(it, "nutrients", "composition", "elements")
                    ph_prod   = _safe_get(it, "ph", "pH")
                    ec_prod   = _safe_get(it, "ec", "EC")

                    # Upsert FertilizerProduct node; keep existing nutrients unless new provided.
                    await session.run(
                        """
                        MERGE (fp:FertilizerProduct { name: $name, brand: $brand })
                        SET fp.nutrients = CASE WHEN $nutrients IS NULL THEN fp.nutrients ELSE $nutrients END,
                            fp.pH        = COALESCE($ph, fp.pH),
                            fp.EC        = COALESCE($ec, fp.EC)
                        """,
                        name=name, brand=brand, nutrients=nutrients, ph=ph_prod, ec=ec_prod,
                    )
                    products_written += 1

                    # Link ProductApplication → FertilizerProduct by (application_event_id, idx).
                    await session.run(
                        """
                        MATCH (pa:ProductApplication { application_event_id: $app_ev_id, idx: $idx })
                        MATCH (fp:FertilizerProduct { name: $name, brand: $brand })
                        MERGE (pa)-[:USES_PRODUCT]->(fp)
                        """,
                        app_ev_id=app_ev_id, idx=prod_idx, name=name, brand=brand,
                    )
                    rels_written += 1

        # Advance to next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final summary.
    print(f"[DBG] fertilizer_products summary → fields_tried:{fields_tried} products:{products_written} rels:{rels_written}")
