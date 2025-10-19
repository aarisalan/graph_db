# app/graph_sap_element_result.py
# Upserts SAPElementResult nodes (ppm per nutrient) and links SAPAnalysis→SAPElementResult.

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.sap_analysis import get_sap_analyses

NumberLike = Optional[float]

_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

def _parse_number(s: Optional[str]) -> NumberLike:
    # Extract the first numeric token; accept commas/approx/ineq; else None.
    if not s:
        return None
    m = _NUM_RE.search(str(s))
    if not m:
        return None
    raw = m.group(0).replace(",", ".")
    try:
        return float(raw)
    except Exception:
        return None

def _jsonable_list(items: Optional[Sequence[object]]) -> Optional[List[Dict[str, Any]]]:
    # Normalize pydantic/objects to list[dict] with mineral/young/old/optimum.
    if not items:
        return None
    out: List[Dict[str, Any]] = []
    for e in items:
        if isinstance(e, dict):
            out.append({
                "mineral": e.get("mineral"),
                "young":   e.get("young"),
                "old":     e.get("old"),
                "optimum": e.get("optimum"),
            })
        else:
            out.append({
                "mineral": getattr(e, "mineral", None),
                "young":   getattr(e, "young", None),
                "old":     getattr(e, "old", None),
                "optimum": getattr(e, "optimum", None),
            })
    return out

def _pick_for_leaf(entry: Dict[str, Any], leaf_type: str) -> Optional[str]:
    # Return the string value for 'young' or 'old'.
    key = "young" if leaf_type == "young" else "old"
    v = entry.get(key)
    return None if v is None else str(v)

def _iso_day(v: Any) -> str:
    # datetime/date/str-like → 'YYYY-MM-DD'.
    if isinstance(v, datetime):
        return v.date().isoformat()
    return str(v)

def _make_sa_id(field_id: int, date_iso: str, crop_name: str, leaf_type: str, sample_id: str) -> str:
    # Stable SAPAnalysis identifier.
    return f"{field_id}:{date_iso}:{crop_name}:{leaf_type}:{sample_id}"

async def upsert_sap_element_results(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    completed_at: datetime,
) -> None:
    # Telemetry counters.
    total_fields = 0
    total_elements = 0

    for f in (fields or []):
        total_fields += 1
        field_id = getattr(f, "id", None)
        if field_id is None:
            continue

        try:
            analyses = await get_sap_analyses(pg_pool, field_id=field_id, completed_at=completed_at)
        except Exception as e:
            print(f"[DBG] field_id={field_id}: get_sap_analyses error {e!r}, skipping")
            continue

        if not analyses:
            print(f"[DBG] field_id={field_id}: no sap analyses for {completed_at.date().isoformat()}")
            continue

        for a in analyses:
            # Common keys.
            crop_name    = getattr(a, "crop_name", None) or ""
            sample_date  = getattr(a, "sample_date", None) or getattr(a, "date", None)
            date_iso     = _iso_day(sample_date)
            young_sample = getattr(a, "young_sample", None)
            old_sample   = getattr(a, "old_sample", None)
            elements     = getattr(a, "elements", None)  # ppm-bearing entries only
            elements_list = _jsonable_list(elements) or []

            # Create/link results per leaf type.
            for leaf_type, sample_id in (("young", young_sample), ("old", old_sample)):
                if not sample_id:
                    continue
                sa_id = _make_sa_id(field_id, date_iso, crop_name, leaf_type, str(sample_id))

                for e in elements_list:
                    nutrient = (e.get("mineral") or "").strip()
                    if not nutrient:
                        continue
                    val_str = _pick_for_leaf(e, leaf_type)
                    val_num = _parse_number(val_str)
                    if val_num is None:
                        continue

                    # Upsert SAPElementResult node.
                    await session.run(
                        """
                        MERGE (ser:SAPElementResult {
                            sap_analysis_id: $sa_id,
                            nutrient: $nutrient
                        })
                        SET ser.value_ppm = $value_ppm
                        """,
                        sa_id=sa_id,
                        nutrient=nutrient,
                        value_ppm=val_num,
                    )

                    # Link SAPAnalysis → SAPElementResult.
                    await session.run(
                        """
                        MATCH (sa:SAPAnalysis {
                            field_id: $field_id, date: $date,
                            crop_name: $crop_name, leaf_type: $leaf_type, sample_id: $sample_id
                        })
                        MATCH (ser:SAPElementResult { sap_analysis_id: $sa_id, nutrient: $nutrient })
                        MERGE (sa)-[:MEASURED_ELEMENT]->(ser)
                        """,
                        field_id=field_id,
                        date=date_iso,
                        crop_name=crop_name,
                        leaf_type=leaf_type,
                        sample_id=str(sample_id),
                        sa_id=sa_id,
                        nutrient=nutrient,
                    )

                    total_elements += 1

    # Final summary.
    print(f"[DBG] sap_element_results summary → fields:{total_fields} elements:{total_elements}")
