# app/graph_optimum_sap_range.py
# Upserts OptimumSAPRange headers per crop/date and links Crop→OptimumSAPRange.

from __future__ import annotations

from datetime import datetime, date
from typing import Any, List, Set, Tuple

from db.postgres import PostgresAsyncPool
from models.field import Field
from services.sap_analysis import get_sap_analyses


def _iso_day(v: Any) -> str:
    # ISO day string from datetime/date/str-like.
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


async def upsert_optimum_sap_ranges(
    session,
    fields: List[Field],
    pg_pool: PostgresAsyncPool,
    completed_at: datetime,
) -> None:
    # Track processed (crop_name, date) to avoid duplicates.
    seen: Set[Tuple[str, str]] = set()
    fields_tried = 0
    nodes_created = 0
    rels_created = 0

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

            key = (crop_name, date_iso)
            if key in seen:
                continue
            seen.add(key)

            # Create/ensure OptimumSAPRange header.
            await session.run(
                """
                MERGE (osr:OptimumSAPRange { crop_name: $crop_name, date: $date })
                """,
                crop_name=crop_name, date=date_iso,
            )
            nodes_created += 1

            # Link Crop → OptimumSAPRange (create Crop if missing).
            await session.run(
                """
                MERGE (c:Crop { name: $crop_name })
                WITH c
                MATCH (osr:OptimumSAPRange { crop_name: $crop_name, date: $date })
                MERGE (c)-[:HAS_OPTIMUM_RANGE]->(osr)
                """,
                crop_name=crop_name, date=date_iso,
            )
            rels_created += 1

    # Final summary.
    print(f"[DBG] optimum_sap_ranges summary → fields_tried:{fields_tried} headers:{nodes_created} rels:{rels_created}")
