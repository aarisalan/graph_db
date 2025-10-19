# app/graph_water_analysis.py
# Upserts WaterAnalysis nodes and links Field→WaterAnalysis for a date window.

from __future__ import annotations

import json
from datetime import datetime, timedelta, date
from typing import Any, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.water_analysis import get_water_analyses
from app.utils import ensure_datetime_param


def _iso_day(d: date | datetime | str) -> str:
    # To YYYY-MM-DD.
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)


def _json_dump(obj: Any) -> Optional[str]:
    # Safe JSON encode (keeps ASCII, tolerates non-serializables).
    if obj is None:
        return None
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps(str(obj), ensure_ascii=False)


async def upsert_water_analyses(
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

    # Inclusive day loop.
    day = start_at.date()
    end_day = end_at.date()

    while day <= end_day:
        created_at_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            field_name = getattr(f, "name", None)
            if field_id is None:
                continue

            # Pull analyses for the given field/day.
            try:
                analyses = await get_water_analyses(
                    pg_pool, field_id=field_id, created_at=created_at_dt
                )
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_water_analyses error {e!r}, skipping")
                continue

            if not analyses:
                continue

            for a in analyses:
                crop_name    = getattr(a, "crop_name", None)
                lab_id       = getattr(a, "lab_id", None)
                sample_date  = getattr(a, "sample_date", None) or day
                sample_src   = getattr(a, "sample_source", None) or ""
                elements_raw = getattr(a, "elements", None)

                # Parameterize date for Neo4j datetime($p).
                date_params   = ensure_datetime_param(sample_date, tz="Europe/Istanbul", default_offset="+03:00")

                # Persist raw results as JSON (future per-parameter graphing).
                elements_json = _json_dump([e.model_dump() for e in (elements_raw or [])])

                # Upsert WaterAnalysis node (unique key: field_id + date + sample_source).
                await session.run(
                    """
                    MERGE (wa:WaterAnalysis {
                        field_id: $field_id,
                        date: datetime($date),
                        sample_source: $sample_source
                    })
                    SET wa.field_name    = $field_name,
                        wa.crop_name     = $crop_name,
                        wa.lab_id        = $lab_id,
                        wa.elements_json = $elements_json
                    """,
                    field_id=field_id,
                    date=date_params,
                    sample_source=sample_src,
                    field_name=field_name,
                    crop_name=crop_name,
                    lab_id=str(lab_id) if lab_id is not None else None,
                    elements_json=elements_json,
                )
                nodes += 1

                # Link Field → WaterAnalysis.
                await session.run(
                    """
                    MATCH (fld:Field { field_id: $field_id })
                    MATCH (wa:WaterAnalysis {
                        field_id: $field_id,
                        date: datetime($date),
                        sample_source: $sample_source
                    })
                    MERGE (fld)-[:HAS_WATER_ANALYSIS]->(wa)
                    """,
                    field_id=field_id,
                    date=date_params,
                    sample_source=sample_src,
                )
                rels += 1

        # Next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final summary.
    print(f"[DBG] water_analyses summary → fields_tried:{fields_tried} nodes:{nodes} rels:{rels}")
