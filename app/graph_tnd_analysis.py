# app/graph_tnd_analysis.py
# Upserts TNDAnalysis nodes and links Field→TNDAnalysis; extracts totals and C/N ratio.

from __future__ import annotations

import json
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterable, List, Optional

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.lab_analysis import get_tnd_analyses  # single-day fetch


def _iso_day(d: date | datetime | str) -> str:
    # To YYYY-MM-DD.
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)


def _to_float_or_none(v) -> Optional[float]:
    # Safe float; tolerate simple inequalities like "<= 5".
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


def _jsonable_elements(items) -> Optional[List[Dict[str, Any]]]:
    # Normalize to list[dict] with element/value/unit.
    if not items:
        return None
    out: List[Dict[str, Any]] = []
    for e in items:
        if isinstance(e, dict):
            out.append({
                "element": e.get("element"),
                "value": e.get("value"),
                "unit":  e.get("unit"),
            })
        else:
            out.append({
                "element": getattr(e, "element", None),
                "value":  getattr(e, "value", None),
                "unit":   getattr(e, "unit", None),
            })
    return out


def _norm(s: Optional[str]) -> str:
    # Lowercased/trimmed (empty → "").
    return (s or "").strip().lower()


def _derive_totals_and_cn(elements_jsonable: Optional[List[Dict[str, Any]]]) -> Dict[str, Optional[float]]:
    # Derive total_* metrics and cn_ratio from free-form element list.
    res: Dict[str, Optional[float]] = {
        "total_c": None, "total_n": None, "total_p": None, "total_k": None,
        "total_s": None, "total_ca": None, "total_mg": None, "total_na": None,
        "total_fe": None, "total_mn": None, "total_zn": None, "total_cu": None,
        "total_b": None, "total_si": None,
        "cn_ratio": None,
    }
    if not elements_jsonable:
        return res

    # Alias map for common names.
    alias_to_key = {
        "c": "total_c", "carbon": "total_c",
        "n": "total_n", "nitrogen": "total_n",
        "p": "total_p", "phosphorus": "total_p",
        "k": "total_k", "potassium": "total_k",
        "s": "total_s", "sulfur": "total_s",
        "ca": "total_ca", "calcium": "total_ca",
        "mg": "total_mg", "magnesium": "total_mg",
        "na": "total_na", "sodium": "total_na",
        "fe": "total_fe", "iron": "total_fe",
        "mn": "total_mn", "manganese": "total_mn",
        "zn": "total_zn", "zinc": "total_zn",
        "cu": "total_cu", "copper": "total_cu",
        "b": "total_b", "boron": "total_b",
        "si": "total_si", "silicon": "total_si",
    }

    for e in elements_jsonable:
        name = _norm(e.get("element"))
        val = _to_float_or_none(e.get("value"))

        # Capture C/N ratio variants.
        if name in {"c/n", "c:n", "cn", "cn ratio", "c/n ratio", "c:n ratio"}:
            if res["cn_ratio"] is None:
                res["cn_ratio"] = val
            continue

        # Capture "total X" forms.
        if name.startswith("total "):
            tail = name.replace("total ", "").strip()
            key = alias_to_key.get(tail)
            if key and res.get(key) is None:
                res[key] = val
            continue

    return res


async def upsert_tnd_analyses(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Counters.
    fields_tried = 0
    nodes = 0
    rels = 0

    # Inclusive day loop.
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
                analyses = await get_tnd_analyses(pg_pool, field_id=field_id, completed_at=day_dt)
            except Exception as e:
                print(f"[DBG] field_id={field_id}: get_tnd_analyses error {e!r}, skipping")
                continue

            if not analyses:
                print(f"[DBG] field_id={field_id}: no TND analyses for {day.isoformat()}")
                continue

            for a in analyses:
                crop_name        = getattr(a, "crop_name", None)
                sample_date      = getattr(a, "sample_date", None) or day
                lab_no           = getattr(a, "lab_no", None)
                beginning_depth  = _to_float_or_none(getattr(a, "beginning_depth", None))
                ending_depth     = _to_float_or_none(getattr(a, "ending_depth", None))
                elements_raw     = getattr(a, "elements", None)

                elements = _jsonable_elements(elements_raw)
                elements_json = json.dumps(elements) if elements else None

                sample_depth_cm: Optional[float] = None
                if beginning_depth is not None and ending_depth is not None:
                    sample_depth_cm = ending_depth - beginning_depth

                totals = _derive_totals_and_cn(elements or [])
                date_iso = _iso_day(sample_date)

                # Upsert TNDAnalysis node.
                await session.run(
                    """
                    MERGE (tn:TNDAnalysis {
                        field_id: $field_id,
                        date: $date,
                        lab_no: $lab_no
                    })
                    SET tn.field_name       = $field_name,
                        tn.crop_name        = $crop_name,
                        tn.sample_depth_cm  = $sample_depth_cm,
                        tn.beginning_depth  = $beginning_depth,
                        tn.ending_depth     = $ending_depth,
                        tn.elements_json    = $elements_json,
                        tn.total_c          = $total_c,
                        tn.total_n          = $total_n,
                        tn.total_p          = $total_p,
                        tn.total_k          = $total_k,
                        tn.total_s          = $total_s,
                        tn.total_ca         = $total_ca,
                        tn.total_mg         = $total_mg,
                        tn.total_na         = $total_na,
                        tn.total_fe         = $total_fe,
                        tn.total_mn         = $total_mn,
                        tn.total_zn         = $total_zn,
                        tn.total_cu         = $total_cu,
                        tn.total_b          = $total_b,
                        tn.total_si         = $total_si,
                        tn.cn_ratio         = $cn_ratio
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
                    **totals,
                )
                nodes += 1

                # Link Field → TNDAnalysis.
                await session.run(
                    """
                    MATCH (fld:Field { field_id: $field_id })
                    MATCH (tn:TNDAnalysis { field_id: $field_id, date: $date, lab_no: $lab_no })
                    MERGE (fld)-[:HAS_TND_ANALYSIS]->(tn)
                    """,
                    field_id=field_id,
                    date=date_iso,
                    lab_no=str(lab_no) if lab_no is not None else None,
                )
                rels += 1

        day += timedelta(days=1)

    # Final summary.
    print(f"[DBG] tnd_analyses summary → fields_tried:{fields_tried} nodes:{nodes} rels:{rels}")
