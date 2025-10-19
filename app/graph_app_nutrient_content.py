# app/graph_app_nutrient_content.py
# Extracts and writes AppNutrientContent nodes and relationships into Neo4j.

from __future__ import annotations

import re
from datetime import datetime, date, time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.agromatiq import get_activities
from app.utils import ensure_datetime_param

# Loose numeric parser (supports comma/period decimals and signs).
_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

def _as_date(v: Any) -> date:
    # Normalize datetime/str to date.
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()

def _to_datetime(v: Any, fallback_day: date) -> datetime:
    # Normalize to datetime; accept 'Z' suffix and fallback to midnight of given day.
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

def _pick_app_type(a: Any) -> str:
    # Pick the first available type code/name/category from activity object.
    for k in ("type_code", "type_name", "sub_type_code", "category_code"):
        val = getattr(a, k, None)
        if val:
            return str(val).strip()
    return "unknown"

def _make_app_ev_id(field_id: int, date_iso: str, crop_name: str, app_type: str, idx: int) -> str:
    # Stable ApplicationEvent identifier (field/date/crop/app_type/idx).
    return f"{field_id}:{date_iso}:{crop_name}:{app_type}:{idx}"

def _make_prod_app_id(field_id: int, date_iso: str, crop_name: str, app_type: str, ae_idx: int, prod_idx: int) -> str:
    # Stable ProductApplication identifier (extends AE id with product index).
    return f"{field_id}:{date_iso}:{crop_name}:{app_type}:{ae_idx}:{prod_idx}"

def _safe_get(obj: Any, *names: str):
    # Safely extract a named attribute/key from arbitrary objects/dicts/pydantic.
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

def _to_number(v: Any) -> Optional[float]:
    # Convert arbitrary nutrient value to float (drops '%' and handles commas).
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    s = s.replace("%", "").strip()
    m = _NUM_RE.search(s)
    if not m:
        return None
    raw = m.group(0).replace(",", ".")
    try:
        return float(raw)
    except Exception:
        return None

def _iter_nutrients(nut: Any) -> Iterable[Tuple[str, Optional[float]]]:
    # Yield (name, numeric_value) pairs from dict/list/objects with common keys.
    if not nut:
        return []
    if isinstance(nut, dict):
        for k, v in nut.items():
            name = _norm_str(str(k)) or ""
            if not name:
                continue
            yield name, _to_number(v)
        return
    if isinstance(nut, list):
        for e in nut:
            if isinstance(e, dict):
                name = _norm_str(e.get("nutrient") or e.get("name") or e.get("mineral") or e.get("element"))
                if not name:
                    continue
                val = e.get("value", None)
                if val is None:
                    val = e.get("pct") or e.get("g_L") or e.get("amount")
                yield name, _to_number(val)
            else:
                name = _norm_str(_safe_get(e, "nutrient", "name", "mineral", "element"))
                val = _safe_get(e, "value", "pct", "g_L", "amount")
                if name:
                    yield name, _to_number(val)
        return
    return []

# Neo4j write buffer size.
BATCH_SIZE = 1000

async def _write_rows_batched(session, rows: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    # Write AppNutrientContent nodes and relationships in a single Cypher UNWIND.
    if not rows:
        return 0, 0, 0
    await session.run(
        """
        UNWIND $rows AS r
        // Ensure parent ProductApplication exists.
        MATCH (pa:ProductApplication { application_event_id: r.app_ev_id, idx: r.pa_idx })
        // Upsert AppNutrientContent node keyed by PA id + nutrient name.
        MERGE (anc:AppNutrientContent { product_application_id: r.pa_id, nutrient: r.nutrient })
        SET anc.pct_or_g_L = r.val
        // Link PA → ANC.
        MERGE (pa)-[:HAS_NUTRIENT_CONTENT]->(anc)
        // Optionally link FertilizerProduct → ANC when name/brand exist.
        WITH anc, r
        CALL {
          WITH anc, r
          WITH anc, r WHERE r.name IS NOT NULL AND r.brand IS NOT NULL
          MATCH (fp:FertilizerProduct { name: r.name, brand: r.brand })
          MERGE (fp)-[:CONTAINS_NUTRIENT]->(anc)
          RETURN 0
        }
        """,
        rows=rows,
    )
    return len(rows), len(rows), 0

async def upsert_app_nutrient_contents(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
) -> None:
    # Accumulators for debug/telemetry.
    fields_tried = 0
    nodes = 0
    rels_pa = 0
    rels_fp = 0

    # Iterate daily within the inclusive date window.
    day = start_at.date()
    end_day = end_at.date()

    # Buffer for batched writes.
    buffer: List[Dict[str, Any]] = []

    while day <= end_day:
        # Use midnight of current day for activity fetch.
        created_at_dt = datetime.combine(day, datetime.min.time())

        for f in (fields or []):
            fields_tried += 1
            field_id = getattr(f, "id", None)
            if field_id is None:
                continue

            try:
                # Fetch activities for field/day from Postgres.
                acts = await get_activities(pg_pool, field_id=field_id, created_at=created_at_dt)
            except Exception as e:
                # Soft-fail on data source errors.
                print(f"[DBG] field_id={field_id}: get_activities error {e!r}, skipping")
                continue

            if not acts:
                continue

            for a in acts:
                # Basic event descriptors.
                crop_name   = _norm_str(getattr(a, "crop_name", None)) or ""
                app_type    = _pick_app_type(a)
                start_ts    = getattr(a, "start_at", None)
                ev_date     = _as_date(start_ts or day)

                # Param for Neo4j temporal matching (localized).
                date_params = ensure_datetime_param(ev_date, tz="Europe/Istanbul", default_offset="+03:00")

                # Find matching ApplicationEvent and take closest by start time.
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
                    field_id=field_id, date=date_params, crop_name=crop_name, app_type=app_type,
                )
                ae_list = await rec.data()
                if not ae_list:
                    # Skip nutrients if no parent AE is present.
                    print(f"[DBG] Missing ApplicationEvent for field_id={field_id} {ev_date} {crop_name} {app_type}; skip nutrients")
                    continue

                # Convert Neo4j temporal value to epoch seconds for diffing.
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

                # Pick AE idx closest to activity start time.
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

                # Build parent/child identifiers.
                app_ev_id = _make_app_ev_id(field_id, ev_date.isoformat(), crop_name, app_type, int(best_idx))
                inventories: List[Dict[str, Any]] = getattr(a, "inventories", None) or []

                # Iterate product inventories, collect nutrients or mark as missing.
                prod_idx = 0
                missing_pairs: set[Tuple[str, str, int]] = set()
                for it in inventories:
                    prod_idx += 1
                    prod_app_id = _make_prod_app_id(field_id, ev_date.isoformat(), crop_name, app_type, int(best_idx), prod_idx)

                    inv_nutrients = _safe_get(it, "nutrients", "composition", "elements")
                    name  = _norm_str(_safe_get(it, "fertilizer_name"))
                    brand = _norm_str(_safe_get(it, "fertilizer_brand"))

                    if inv_nutrients:
                        # Push parsed nutrient rows to buffer.
                        for nut_name, nut_val in _iter_nutrients(inv_nutrients):
                            if not nut_name:
                                continue
                            buffer.append({
                                "pa_id": prod_app_id,
                                "app_ev_id": app_ev_id,
                                "pa_idx": prod_idx,
                                "nutrient": nut_name,
                                "val": nut_val,
                                "name": name,
                                "brand": brand,
                            })
                    elif name and brand:
                        # No inline nutrients; will try lookup from FertilizerProduct node.
                        missing_pairs.add((name, brand, prod_idx))

                    # Flush when buffer reaches batch size.
                    if len(buffer) >= BATCH_SIZE:
                        wrote, relp, _ = await _write_rows_batched(session, buffer)
                        nodes += wrote
                        rels_pa += relp
                        buffer = []

                # Enrich missing inventories from FertilizerProduct.nutrients.
                if missing_pairs:
                    uniq_pairs = {(n, b) for (n, b, _i) in missing_pairs}
                    pairs = [{"name": n, "brand": b} for (n, b) in uniq_pairs]
                    rec_fp = await session.run(
                        """
                        UNWIND $pairs AS p
                        MATCH (fp:FertilizerProduct { name: p.name, brand: p.brand })
                        RETURN p.name AS name, p.brand AS brand, fp.nutrients AS nutrients
                        """,
                        pairs=pairs
                    )
                    rows = await rec_fp.data()
                    fp_map: Dict[Tuple[str, str], Any] = {
                        (r["name"], r["brand"]): r.get("nutrients") for r in rows
                    }
                    for (name, brand, pr_idx) in missing_pairs:
                        prod_app_id = _make_prod_app_id(field_id, ev_date.isoformat(), crop_name, app_type, int(best_idx), pr_idx)
                        nut_src = fp_map.get((name, brand))
                        if not nut_src:
                            continue
                        for nut_name, nut_val in _iter_nutrients(nut_src):
                            if not nut_name:
                                continue
                            buffer.append({
                                "pa_id": prod_app_id,
                                "app_ev_id": app_ev_id,
                                "pa_idx": pr_idx,
                                "nutrient": nut_name,
                                "val": nut_val,
                                "name": name,
                                "brand": brand,
                            })
                        if len(buffer) >= BATCH_SIZE:
                            wrote, relp, _ = await _write_rows_batched(session, buffer)
                            nodes += wrote
                            rels_pa += relp
                            buffer = []

        # Next day.
        day = day.fromordinal(day.toordinal() + 1)

    # Final flush.
    if buffer:
        wrote, relp, _ = await _write_rows_batched(session, buffer)
        nodes += wrote
        rels_pa += relp

    # Summary line (rels_fp counted in Cypher; approximated here).
    print(f"[DBG] app_nutrient_contents summary → fields:{fields_tried} anc_nodes:{nodes} rels_pa:{rels_pa} rels_fp:~(in query)")
