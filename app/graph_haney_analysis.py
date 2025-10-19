# app/graph_haney_analysis.py
# Upserts HaneyAnalysis nodes and links Field→HaneyAnalysis for a date window.

from __future__ import annotations

import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.lab_analysis import get_haney_analyses  # single-day fetch

def _iso_day(d: date) -> str:
    # ISO date string (YYYY-MM-DD).
    return d.isoformat()

def _to_float_or_none(v) -> Optional[float]:
    # Safe float conversion; invalid/None → None.
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None

def _jsonable_elements(items) -> Optional[List[Dict[str, Any]]]:
    # Normalize element rows to a JSON-serializable list of dicts.
    if not items:
        return None
    out: List[Dict[str, Any]] = []
    for e in items:
        if isinstance(e, dict):
            out.append({
                "element": e.get("element"),
                "value": e.get("value"),
                "unit" : e.get("unit"),
            })
        else:
            out.append({
                "element": getattr(e, "element", None),
                "value" : getattr(e, "value", None),
                "unit"  : getattr(e, "unit", None),
            })
    return out

async def upsert_haney_analyses(
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
    start_day = start_at.date()
    end_day = end_at.date()
    day = start_day

    while day <= end_day:
        # Use midnight timestamp for single-day fetch.
        day_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            field_name = getattr(f, "name", None)
            if field_id is None:
                continue

            try:
                # Fetch Haney analyses completed on the given day.
                analyses = await get_haney_analyses(pg_pool, field_id=field_id, completed_at=day_dt)
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_haney_analyses error {e!r}, skipping")
                continue

            if not analyses:
                print(f"[DBG] field_id={field_id}: no Haney analyses for {day.isoformat()}")
                continue

            for a in analyses:
                # Extract fields with safe conversions.
                crop_name        = getattr(a, "crop_name", None)
                sample_date      = getattr(a, "sample_date", None) or day
                lab_no           = getattr(a, "lab_no", None)
                beginning_depth  = _to_float_or_none(getattr(a, "beginning_depth", None))
                ending_depth     = _to_float_or_none(getattr(a, "ending_depth", None))
                elements_raw     = getattr(a, "elements", None)

                # Serialize element list as JSON string for Neo4j property.
                elements = _jsonable_elements(elements_raw)
                elements_json = json.dumps(elements) if elements else None

                # Compute sample depth (cm) if both endpoints exist.
                sample_depth_cm: Optional[float] = None
                if beginning_depth is not None and ending_depth is not None:
                    sample_depth_cm = ending_depth - beginning_depth

                # Use sample date as key date.
                date_iso = _iso_day(sample_date)

                # Upsert HaneyAnalysis node.
                await session.run(
                    """
                    MERGE (ha:HaneyAnalysis {
                        field_id: $field_id,
                        date: $date,
                        lab_no: $lab_no
                    })
                    SET ha.field_name       = $field_name,
                        ha.crop_name        = $crop_name,
                        ha.sample_depth_cm  = $sample_depth_cm,
                        ha.beginning_depth  = $beginning_depth,
                        ha.ending_depth     = $ending_depth,
                        ha.elements_json    = $elements_json
                    """,
                    field_id=field_id,
                    field_name=field_name,
                    date=date_iso,
                    lab_no=str(lab_no) if lab_no is not None else None,
                    crop_name=crop_name,
                    sample_depth_cm=sample_depth_cm,
                    beginning_depth=beginning_depth,
                    ending_depth=ending_depth,
                    elements_json=elements_json,
                )
                nodes += 1

                # Link Field → HaneyAnalysis.
                await session.run(
                    """
                    MATCH (fld:Field { field_id: $field_id })
                    MATCH (ha:HaneyAnalysis { field_id: $field_id, date: $date, lab_no: $lab_no })
                    MERGE (fld)-[:HAS_HANEY_ANALYSIS]->(ha)
                    """,
                    field_id=field_id,
                    date=date_iso,
                    lab_no=str(lab_no) if lab_no is not None else None,
                )
                rels += 1

        # Advance to next day.
        day += timedelta(days=1)

    # Final summary.
    print(f"[DBG] haney_analyses summary → fields_tried:{fields_tried} nodes:{nodes} rels:{rels}")
