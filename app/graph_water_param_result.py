# app/graph_water_param_result.py
# Upserts WaterParamResult nodes from WaterAnalysis and links WaterAnalysis→WaterParamResult.

from __future__ import annotations

from datetime import datetime, date
from typing import Any, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.water_analysis import get_water_analyses
from app.utils import ensure_datetime_param


def _iso_day(v: Any) -> str:
    # Normalize to YYYY-MM-DD.
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)

def _norm_param(name: Optional[str]) -> Optional[str]:
    # Lowercased, trimmed parameter key (None if empty).
    if not name:
        return None
    return name.strip().lower()

def _make_wa_id(field_id: int, date_iso: str, sample_source: Optional[str]) -> str:
    # Deterministic WaterAnalysis identifier aligned with its MERGE key.
    src = (sample_source or "unknown").strip()
    return f"{field_id}:{date_iso}:{src}"


async def upsert_water_param_results(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Telemetry counters.
    fields_tried = 0
    params_written = 0
    rels_written = 0

    # Inclusive day loop.
    day = start_at.date()
    end_day = end_at.date()

    while day <= end_day:
        created_at_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            if field_id is None:
                continue

            # Fetch analyses for field/day.
            try:
                analyses = await get_water_analyses(pg_pool, field_id=field_id, created_at=created_at_dt)
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_water_analyses error {e!r}, skipping")
                continue

            if not analyses:
                continue

            for a in analyses:
                sample_date   = getattr(a, "sample_date", None) or day
                sample_source = getattr(a, "sample_source", None)
                elements      = getattr(a, "elements", None) or []  # list[WaterAnalysisElement]
                date_iso      = _iso_day(sample_date)
                date_params   = ensure_datetime_param(sample_date, tz="Europe/Istanbul", default_offset="+03:00")
                wa_id         = _make_wa_id(field_id, date_iso, sample_source)

                # Each element → a WaterParamResult node.
                for e in elements:
                    param_orig = getattr(e, "parameter", None)
                    param_key  = _norm_param(param_orig)
                    if not param_key:
                        continue

                    value     = getattr(e, "value", None)
                    unit      = getattr(e, "unit", None)
                    status    = getattr(e, "evaluation", None)   # map evaluation→status
                    method    = getattr(e, "method", None)
                    range_min = getattr(e, "min", None)
                    range_max = getattr(e, "max", None)

                    # Upsert WaterParamResult (idempotent).
                    await session.run(
                        """
                        MERGE (wpr:WaterParamResult {
                            water_analysis_id: $wa_id,
                            parameter: $parameter
                        })
                        SET wpr.value = $value,
                            wpr.unit = $unit,
                            wpr.status = $status,
                            wpr.method = $method,
                            wpr.range_min = $range_min,
                            wpr.range_max = $range_max,
                            wpr.parameter_label = $parameter_label
                        """,
                        wa_id=wa_id,
                        parameter=param_key,
                        value=value,
                        unit=unit,
                        status=status,
                        method=method,
                        range_min=range_min,
                        range_max=range_max,
                        parameter_label=param_orig,
                    )
                    params_written += 1

                    # Link WaterAnalysis → WaterParamResult.
                    await session.run(
                        """
                        MATCH (wa:WaterAnalysis {
                            field_id: $field_id,
                            date: datetime($date),
                            sample_source: $sample_source
                        })
                        MATCH (wpr:WaterParamResult { water_analysis_id: $wa_id, parameter: $parameter })
                        MERGE (wa)-[:HAS_WATER_PARAM]->(wpr)
                        """,
                        field_id=field_id,
                        date=date_params,
                        sample_source=(sample_source or "unknown").strip(),
                        wa_id=wa_id,
                        parameter=param_key,
                    )
                    rels_written += 1

        # Next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final summary.
    print(f"[DBG] water_param_results summary → fields_tried:{fields_tried} params:{params_written} rels:{rels_written}")
