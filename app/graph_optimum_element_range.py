# app/graph_optimum_element_range.py
# Upserts OptimumElementRange nodes and links them under OptimumSAPRange per crop/date.

from __future__ import annotations

import re
from itertools import chain
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union, Set

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.sap_analysis import get_sap_analyses

# Patterns like "1-4", ">= 2.5", "<=7", or "3".
_NUM = r"[0-9]+(?:\.[0-9]+)?"
RE_RANGE = re.compile(
    rf"^\s*(?:(?P<op>>=|<=|>|<)\s*(?P<val>{_NUM})|(?P<min>{_NUM})\s*-\s*(?P<max>{_NUM})|(?P<single>{_NUM}))\s*$"
)

def _iso_day(v: Any) -> str:
    # ISO day string from datetime/date/str-like.
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)

def _to_float(s: Optional[str]) -> Optional[float]:
    # Safe float parse.
    if s is None:
        return None
    try:
        return float(s)
    except Exception:
        return None

def _parse_optimum(opt: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    # Parse "optimum" text into (min,max); single value → (v,v); inequalities → one-sided.
    if not opt:
        return None, None
    m = RE_RANGE.match(str(opt).strip())
    if not m:
        return None, None

    if m.group("min") and m.group("max"):
        return _to_float(m.group("min")), _to_float(m.group("max"))

    if m.group("single"):
        v = _to_float(m.group("single"))
        return (v, v)

    op = m.group("op")
    v = _to_float(m.group("val"))
    if v is None:
        return None, None
    if op in (">=", ">"):
        return v, None
    if op in ("<=", "<"):
        return None, v
    return None, None

def _iter_entries(items: Optional[Iterable[Union[dict, object]]]) -> Iterable[Dict[str, Any]]:
    # Normalize pydantic/objects into dicts with expected keys.
    if not items:
        return []
    for e in items:
        if isinstance(e, dict):
            yield {
                "mineral": e.get("mineral"),
                "young": e.get("young"),
                "old": e.get("old"),
                "optimum": e.get("optimum"),
            }
        else:
            yield {
                "mineral": getattr(e, "mineral", None),
                "young": getattr(e, "young", None),
                "old": getattr(e, "old", None),
                "optimum": getattr(e, "optimum", None),
            }

async def upsert_optimum_element_ranges(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    completed_at: datetime,
) -> None:
    # Telemetry counters.
    fields_tried = 0
    nodes_written = 0
    rels_written = 0
    headers_touched: Set[Tuple[str, str]] = set()  # (crop_name, date)

    for f in (fields or []):
        fields_tried += 1
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
            crop_name = getattr(a, "crop_name", None)
            if not crop_name:
                continue

            sample_date = getattr(a, "sample_date", None) or getattr(a, "date", None)
            date_iso = _iso_day(sample_date)
            opt_range_id = f"{crop_name}|{date_iso}"

            # Ensure parent OptimumSAPRange and link from Crop (idempotent).
            if (crop_name, date_iso) not in headers_touched:
                await session.run(
                    """
                    MERGE (osr:OptimumSAPRange { crop_name: $crop_name, date: $date })
                    MERGE (c:Crop { name: $crop_name })
                    MERGE (c)-[:HAS_OPTIMUM_RANGE]->(osr)
                    """,
                    crop_name=crop_name, date=date_iso,
                )
                headers_touched.add((crop_name, date_iso))

            elements = getattr(a, "elements", None)
            others = getattr(a, "others", None)

            # Merge element sources into a single stream.
            for entry in chain(_iter_entries(elements), _iter_entries(others)):
                nutrient = (entry.get("mineral") or "").strip()
                if not nutrient:
                    continue
                opt_str = entry.get("optimum")
                if not opt_str:
                    continue

                vmin, vmax = _parse_optimum(opt_str)
                if vmin is None and vmax is None:
                    continue

                # Upsert OptimumElementRange (per crop/date/nutrient).
                await session.run(
                    """
                    MERGE (oer:OptimumElementRange { opt_range_id: $opt_range_id, nutrient: $nutrient })
                    SET oer.min = $min, oer.max = $max
                    """,
                    opt_range_id=opt_range_id, nutrient=nutrient, min=vmin, max=vmax,
                )
                nodes_written += 1

                # Link OptimumSAPRange → OptimumElementRange.
                await session.run(
                    """
                    MATCH (osr:OptimumSAPRange { crop_name: $crop_name, date: $date })
                    MATCH (oer:OptimumElementRange { opt_range_id: $opt_range_id, nutrient: $nutrient })
                    MERGE (osr)-[:HAS_OPTIMUM_ELEMENT]->(oer)
                    """,
                    crop_name=crop_name, date=date_iso,
                    opt_range_id=opt_range_id, nutrient=nutrient,
                )
                rels_written += 1

    # Final summary.
    print(f"[DBG] optimum_element_ranges summary → fields_tried:{fields_tried} nodes:{nodes_written} rels:{rels_written}")
