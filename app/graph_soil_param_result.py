# app/graph_soil_param_result.py
# Upserts SoilParamResult nodes per SoilAnalysis and links SoilAnalysis→SoilParamResult.

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.soil_analysis import get_soil_analyses
from app.utils import ensure_datetime_param

def _iso_day(v: Any) -> str:
    # To YYYY-MM-DD.
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)

def _norm_param(name: Optional[str]) -> Optional[str]:
    # Lowercased trimmed parameter name (or None).
    if not name:
        return None
    return name.strip().lower()

def _make_soil_analysis_id(field_id: int, date_iso: str, lab_id: Optional[str]) -> str:
    # Deterministic SoilAnalysis identifier.
    lab_key = str(lab_id) if lab_id is not None else "no-lab"
    return f"{field_id}:{date_iso}:{lab_key}"

def _iter_params(elements: Any) -> List[Dict[str, Any]]:
    # Normalize 'elements' to list[dict] with parameter fields; else [].
    if not elements:
        return []
    if isinstance(elements, list):
        out: List[Dict[str, Any]] = []
        for e in elements:
            if isinstance(e, dict):
                out.append(e)
        return out
    return []

async def upsert_soil_param_results(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Counters.
    fields_tried = 0
    params_written = 0
    rels_written = 0

    # Iterate inclusive date window.
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
                analyses = await get_soil_analyses(pg_pool, field_id=field_id, created_at=created_at_dt)
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_soil_analyses error {e!r}, skipping")
                continue

            if not analyses:
                continue

            for a in analyses:
                # Extract header fields.
                field_name   = getattr(a, "field_name", None)
                lab_id       = getattr(a, "lab_id", None)
                crop_name    = getattr(a, "crop_name", None)
                sample_date  = getattr(a, "sample_date", None) or day
                elements_raw = getattr(a, "elements", None)

                date_iso = _iso_day(sample_date)
                sa_id = _make_soil_analysis_id(field_id, date_iso, str(lab_id) if lab_id is not None else None)

                # Use datetime($p) semantics in MATCH.
                date_params = ensure_datetime_param(sample_date, tz="Europe/Istanbul", default_offset="+03:00")

                for p in _iter_params(elements_raw):
                    param_en = _norm_param(p.get("parameter_english"))
                    if not param_en:
                        continue

                    value     = p.get("value")
                    unit      = p.get("unit")
                    status    = p.get("status")
                    method    = p.get("method")
                    range_min = p.get("range_min")
                    range_max = p.get("range_max")

                    # Upsert SoilParamResult.
                    await session.run(
                        """
                        MERGE (spr:SoilParamResult {
                            soil_analysis_id: $sa_id,
                            parameter_english: $param_en
                        })
                        SET spr.value = $value,
                            spr.unit = $unit,
                            spr.status = $status,
                            spr.method = $method,
                            spr.range_min = $range_min,
                            spr.range_max = $range_max
                        """,
                        sa_id=sa_id,
                        param_en=param_en,
                        value=value,
                        unit=unit,
                        status=status,
                        method=method,
                        range_min=range_min,
                        range_max=range_max,
                    )
                    params_written += 1

                    # Link SoilAnalysis → SoilParamResult.
                    await session.run(
                        """
                        MATCH (sa:SoilAnalysis {
                            field_id: $field_id,
                            date: datetime($date),
                            lab_id: $lab_id
                        })
                        MATCH (spr:SoilParamResult { soil_analysis_id: $sa_id, parameter_english: $param_en })
                        MERGE (sa)-[:HAS_SOIL_PARAM]->(spr)
                        """,
                        field_id=field_id,
                        date=date_params,
                        lab_id=str(lab_id) if lab_id is not None else None,
                        sa_id=sa_id,
                        param_en=param_en,
                    )
                    rels_written += 1

        # Next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final summary.
    print(f"[DBG] soil_param_results summary → fields_tried:{fields_tried} params:{params_written} rels:{rels_written}")
