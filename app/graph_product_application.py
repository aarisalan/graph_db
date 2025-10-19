# app/graph_product_application.py
# Upserts ProductApplication nodes and links ApplicationEvent→ProductApplication.

from __future__ import annotations

from datetime import datetime, date, time
from typing import Any, Dict, List, Optional, Tuple

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.agromatiq import get_activities
from app.utils import ensure_datetime_param


# -------- helpers ------------------------------------------------------------

def _as_date(v: Any) -> date:
    # Normalize input to date.
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()


def _to_datetime(v: Any, fallback_day: date) -> datetime:
    # To datetime; handle 'Z' and fallback to day's midnight.
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


def _norm_str(s: Optional[str]) -> Optional[str]:
    # Trim and convert empty strings to None.
    if s is None:
        return None
    s2 = s.strip()
    return s2 if s2 else None


def _to_liters(amount: Optional[float], unit_abbr: Optional[str]) -> Optional[float]:
    # Convert amount to liters (supports L/mL).
    if amount is None:
        return None
    u = (unit_abbr or "").strip().lower()
    if u in {"l", "lt", "ltr"}:
        return float(amount)
    if u == "ml":
        return float(amount) / 1000.0
    return None


def _pick_app_type(a: Any) -> str:
    # Best-available type field; fallback to "unknown".
    for k in ("type_code", "type_name", "sub_type_code", "category_code"):
        val = getattr(a, k, None)
        if val:
            return str(val).strip()
    return "unknown"


def _make_app_ev_id(field_id: int, date_iso: str, crop_name: str, app_type: str, idx: int) -> str:
    # Deterministic id consistent with ApplicationEvent MERGE key.
    return f"{field_id}:{date_iso}:{crop_name}:{app_type}:{idx}"


def _safe_get(obj: Any, *names: str):
    # Safely read from dict/obj/pydantic v1/v2 by alias list.
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


# ---------------------------------------------------------------------------


async def upsert_product_applications(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Telemetry counters.
    fields_tried = 0
    nodes = 0
    rels = 0

    # Iterate days in inclusive window.
    day = start_at.date()
    end_day = end_at.date()

    while day <= end_day:
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
                # Extract descriptors.
                crop_name   = _norm_str(getattr(a, "crop_name", None)) or ""
                app_type    = _pick_app_type(a)
                start_ts    = getattr(a, "start_at", None)
                ev_date     = _as_date(start_ts or day)
                date_params = ensure_datetime_param(ev_date, tz="Europe/Istanbul", default_offset="+03:00")

                ph   = getattr(a, "ph", None)
                ec   = getattr(a, "ec", None)
                note = _norm_str(getattr(a, "notes", None))

                inventories: List[Dict[str, Any]] = getattr(a, "inventories", None) or []

                # Query ApplicationEvent idx list for this (field,date,crop,type).
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
                    field_id=field_id,
                    date=date_params,
                    crop_name=crop_name,
                    app_type=app_type,
                )
                ae_list = await rec.data()
                if not ae_list:
                    print(f"[DBG] Missing ApplicationEvent for field_id={field_id} {ev_date} {crop_name} {app_type}; skip products")
                    continue

                # Pick the AE whose start_at is closest to activity start.
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
                            return datetime.combine(ts, time.min).timestamp()
                        return _to_datetime(ts, ev_date).timestamp()
                    except Exception:
                        return 0.0

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

                # Write ProductApplication rows (idx = 1..N).
                prod_idx = 0
                for it in inventories:
                    prod_idx += 1

                    dose_amount = _safe_get(it, "dose_amount", "inventory_amount")
                    dose_unit   = _safe_get(it, "dose_amount_unit_abbr", "dose_unit_abbr", "inventory_unit_abbr")
                    amt_value   = _safe_get(it, "amount", "dosage_amount")
                    amt_unit    = _safe_get(it, "amount_unit_abbr", "dosage_unit_abbr")
                    water_L     = _to_liters(amt_value, amt_unit)

                    # Optional labels.
                    inv_name   = _norm_str(_safe_get(it, "inventory_name", "name"))
                    inv_brand  = _norm_str(_safe_get(it, "inventory_brand", "brand_name", "brand"))
                    fert_name  = _norm_str(_safe_get(it, "fertilizer_name"))
                    fert_brand = _norm_str(_safe_get(it, "fertilizer_brand"))

                    await session.run(
                        """
                        MERGE (pa:ProductApplication {
                            application_event_id: $app_ev_id,
                            idx: $idx
                        })
                        SET pa.dose_unit       = $dose_unit,
                            pa.dose_amount     = $dose_amount,
                            pa.water_volume_L  = $water_L,
                            pa.pH              = $ph,
                            pa.EC              = $ec,
                            pa.comment         = $comment,
                            pa.product_name    = $product_name,
                            pa.product_brand   = $product_brand,
                            pa.fertilizer_name = $fertilizer_name,
                            pa.fertilizer_brand= $fertilizer_brand
                        """,
                        app_ev_id=app_ev_id,
                        idx=prod_idx,
                        dose_unit=dose_unit,
                        dose_amount=dose_amount,
                        water_L=water_L,
                        ph=ph,
                        ec=ec,
                        comment=note,
                        product_name=inv_name,
                        product_brand=inv_brand,
                        fertilizer_name=fert_name,
                        fertilizer_brand=fert_brand,
                    )
                    nodes += 1

                    # Link ApplicationEvent → ProductApplication.
                    await session.run(
                        """
                        MATCH (ae:ApplicationEvent {
                            field_id: $field_id,
                            date: datetime($date),
                            crop_name: $crop_name,
                            app_type: $app_type,
                            idx: $ae_idx
                        })
                        MATCH (pa:ProductApplication { application_event_id: $app_ev_id, idx: $idx })
                        MERGE (ae)-[:HAS_PRODUCT_APP]->(pa)
                        """,
                        field_id=field_id,
                        date=date_params,
                        crop_name=crop_name,
                        app_type=app_type,
                        ae_idx=int(best_idx),
                        app_ev_id=app_ev_id,
                        idx=prod_idx,
                    )
                    rels += 1

        # Next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final summary.
    print(f"[DBG] product_applications summary → fields_tried:{fields_tried} nodes:{nodes} rels:{rels}")
