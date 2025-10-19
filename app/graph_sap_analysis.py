# app/graph_sap_analysis.py
# Upserts SAPAnalysis nodes (young/old leaves) and links Field→SAPAnalysis.

from __future__ import annotations

import json
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.sap_analysis import get_sap_analyses


def _iso_day(v: Any) -> str:
    # Convert date/datetime/str-like to YYYY-MM-DD.
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


# ---------- helpers: work with dicts or pydantic-like objects ----------
def _mineral_name(e: Union[dict, object]) -> str:
    # Lowercased mineral name.
    if isinstance(e, dict):
        return str(e.get("mineral", "")).lower().strip()
    return str(getattr(e, "mineral", "")).lower().strip()


def _value_for_leaf(e: Union[dict, object], leaf_type: str) -> Optional[str]:
    # Pick 'young' or 'old' value as string (or None).
    key = "young" if leaf_type == "young" else "old"
    if isinstance(e, dict):
        v = e.get(key)
        return None if v is None else str(v)
    v = getattr(e, key, None)
    return None if v is None else str(v)


def _find_first(
    elements: Iterable[Union[dict, object]],
    names: Sequence[str]
) -> Optional[Union[dict, object]]:
    # Find first element whose mineral name matches any of 'names'.
    want = {n.lower().strip() for n in names}
    for e in elements:
        if _mineral_name(e) in want:
            return e
    return None
# ------------------------------------------------------------------------


def _extract_scalar_metrics(
    others: Optional[Sequence[Union[dict, object]]],
    leaf_type: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # Extract pH, EC, sugars (as strings) from 'others' for a leaf type.
    if not others:
        return None, None, None

    ph_e  = _find_first(others, ["ph"])
    ec_e  = _find_first(others, ["ec", "electrical_conductivity", "conductivity"])
    sug_e = _find_first(others, ["sugar", "sugars", "brix", "°brix", "brix°"])

    ph  = _value_for_leaf(ph_e, leaf_type)  if ph_e  else None
    ec  = _value_for_leaf(ec_e, leaf_type)  if ec_e  else None
    sug = _value_for_leaf(sug_e, leaf_type) if sug_e else None

    return ph, ec, sug


def _jsonable_list(items: Optional[Sequence[object]]) -> Optional[List[Dict[str, Any]]]:
    # Normalize items into list[dict] suitable for JSON serialization.
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


async def upsert_sap_analyses(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    completed_at: datetime,
) -> None:
    # Debug counters.
    total_fields = 0
    total_nodes = 0

    for f in (fields or []):
        total_fields += 1
        field_id = getattr(f, "id", None)
        field_name = getattr(f, "name", None)
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
            # Extract payload.
            crop_name    = getattr(a, "crop_name", None)
            sample_date  = getattr(a, "sample_date", None) or getattr(a, "date", None)
            young_sample = getattr(a, "young_sample", None)
            old_sample   = getattr(a, "old_sample", None)
            elements     = getattr(a, "elements", None)
            others       = getattr(a, "others", None)

            # Convert to JSON-friendly lists.
            elements_list = _jsonable_list(elements)
            others_list   = _jsonable_list(others)

            # Serialize for Neo4j properties (strings).
            elements_str = json.dumps(elements_list, ensure_ascii=False) if elements_list else None
            others_str   = json.dumps(others_list,   ensure_ascii=False) if others_list   else None

            date_iso = _iso_day(sample_date)

            # Create separate nodes for young and old leaves.
            for leaf_type, sample_id in (("young", young_sample), ("old", old_sample)):
                if not sample_id:
                    continue

                # Scalar metrics derived from 'others'.
                ph, ec, sugars = _extract_scalar_metrics(others_list, leaf_type)

                # Upsert SAPAnalysis node.
                await session.run(
                    """
                    MERGE (sa:SAPAnalysis {
                        field_id: $field_id,
                        date: $date,
                        crop_name: $crop_name,
                        leaf_type: $leaf_type,
                        sample_id: $sample_id
                    })
                    SET sa.field_name = $field_name,
                        sa.ph = $ph,
                        sa.ec = $ec,
                        sa.sugars = $sugars,
                        sa.elements = $elements_str,   // JSON string
                        sa.others   = $others_str      // JSON string
                    """,
                    field_id=field_id,
                    field_name=field_name,
                    date=date_iso,
                    crop_name=crop_name,
                    leaf_type=leaf_type,
                    sample_id=str(sample_id),
                    ph=ph, ec=ec, sugars=sugars,
                    elements_str=elements_str,
                    others_str=others_str,
                )

                # Link Field → SAPAnalysis.
                await session.run(
                    """
                    MATCH (fld:Field { field_id: $field_id })
                    MATCH (sa:SAPAnalysis {
                        field_id: $field_id, date: $date,
                        crop_name: $crop_name, leaf_type: $leaf_type, sample_id: $sample_id
                    })
                    MERGE (fld)-[:HAS_SAP_ANALYSIS]->(sa)
                    """,
                    field_id=field_id,
                    date=date_iso,
                    crop_name=crop_name,
                    leaf_type=leaf_type,
                    sample_id=str(sample_id),
                )

                total_nodes += 1

    # Final summary.
    print(f"[DBG] sap_analyses summary → fields:{total_fields} nodes:{total_nodes}")
