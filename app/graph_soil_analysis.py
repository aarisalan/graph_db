# app/graph_soil_analysis.py
# Upserts SoilAnalysis nodes and links Field→SoilAnalysis for a date window.

from __future__ import annotations

import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.soil_analysis import get_soil_analyses


def _iso_day(d: date | datetime | str) -> str:
    # To YYYY-MM-DD.
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)


def _to_float_or_none(v) -> Optional[float]:
    # Safe float parse; trims simple inequality markers like "<= 1.2".
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        if isinstance(v, str):
            s = v.strip().lstrip("<>").lstrip("=").strip()
            try:
                return float(s)
            except Exception:
                return None
        return None


def _elements_to_json(elements_obj: Any) -> Optional[str]:
    # SoilAnalysis.results (aka 'elements') → JSON string for Neo4j property.
    if elements_obj is None:
        return None
    try:
        return json.dumps(elements_obj)
    except TypeError:
        return json.dumps(elements_obj, default=str)


async def upsert_soil_analyses(
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
        day_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            field_name = getattr(f, "name", None)
            if field_id is None:
                continue

            try:
                analyses = await get_soil_analyses(pg_pool, field_id=field_id, created_at=day_dt)
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_soil_analyses error {e!r}, skipping")
                continue

            if not analyses:
                continue

            for a in analyses:
                # Expected aliases from SQL: crop_name, lab_id, sample_date, beginning_depth, ending_depth, elements.
                crop_name       = getattr(a, "crop_name", None)
                lab_id          = getattr(a, "lab_id", None)
                sample_date     = getattr(a, "sample_date", None) or day
                beginning_depth = _to_float_or_none(getattr(a, "beginning_depth", None))
                ending_depth    = _to_float_or_none(getattr(a, "ending_depth", None))
                elements_raw    = getattr(a, "elements", None)

                sample_depth_cm: Optional[float] = None
                if beginning_depth is not None and ending_depth is not None:
                    sample_depth_cm = ending_depth - beginning_depth

                elements_json = _elements_to_json(elements_raw)
                date_iso = _iso_day(sample_date)

                # Upsert SoilAnalysis node (idempotent).
                await session.run(
                    """
                    MERGE (sa:SoilAnalysis {
                        field_id: $field_id,
                        date: $date,
                        lab_id: $lab_id
                    })
                    SET sa.field_name      = $field_name,
                        sa.crop_name       = $crop_name,
                        sa.beginning_depth = $beginning_depth,
                        sa.ending_depth    = $ending_depth,
                        sa.sample_depth_cm = $sample_depth_cm,
                        sa.elements_json   = $elements_json
                    """,
                    field_id=field_id,
                    field_name=field_name,
                    date=date_iso,
                    lab_id=str(lab_id) if lab_id is not None else None,
                    crop_name=crop_name,
                    beginning_depth=beginning_depth,
                    ending_depth=ending_depth,
                    sample_depth_cm=sample_depth_cm,
                    elements_json=elements_json,
                )
                nodes += 1

                # Link Field → SoilAnalysis.
                await session.run(
                    """
                    MATCH (fld:Field { field_id: $field_id })
                    MATCH (sa:SoilAnalysis { field_id: $field_id, date: $date, lab_id: $lab_id })
                    MERGE (fld)-[:HAS_SOIL_ANALYSIS]->(sa)
                    """,
                    field_id=field_id,
                    date=date_iso,
                    lab_id=str(lab_id) if lab_id is not None else None,
                )
                rels += 1

        day += timedelta(days=1)

    # Final summary.
    print(f"[DBG] soil_analyses summary → fields_tried:{fields_tried} nodes:{nodes} rels:{rels}")
