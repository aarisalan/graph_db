# app/graph_irrigation_events.py
# Upserts IrrigationEvent nodes and links Station→IrrigationEvent with rich metadata.

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from models.device import Device
from db.postgres import PostgresAsyncPool
from services.enums import DataType, DataGroup, Measurement
from services.device import get_flow_data
from app.utils import ensure_datetime_param

# Direct SQL queries (kept local to avoid changing services).
from sqls.irrigation import (
    GET_IRRIGATIONS,
    GET_IRRIGATION_DEPTH_STATS,
    GET_IRRIGATION_SENSOR_STATS,
    GET_DEVICE_ROOTS_DEPTH,
)

# Models for typed DB rows.
from models.irrigation import (
    IrrigationData,
    DepthStat,
    SensorStat,
    RootDepth,
)

# --- helpers ---
def _dt_to_iso(x: Any) -> str:
    # ISO string for datetimes; passthrough for other types.
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)

def _to_json(obj: Any) -> str:
    # Serialize complex structures as JSON for Neo4j properties.
    return json.dumps(obj, ensure_ascii=False, default=_dt_to_iso)

async def _fetch_irrigations(
    db: PostgresAsyncPool,
    field_id: int,
    device_id: int,
    field_tz: str,
    start_at: datetime,
    end_at: datetime,
) -> List[Dict[str, Any]]:
    # Fetch irrigation intervals; widen the range slightly for safety.
    q_start = (start_at - timedelta(days=7)).isoformat()
    q_end   = (end_at   + timedelta(days=1)).isoformat()

    rows: List[IrrigationData] = await db.get_all(
        query=GET_IRRIGATIONS,
        params={
            "field_id": field_id,
            "device_id": device_id,
            "field_timezone": field_tz,
            "start_at": q_start,
            "end_at": q_end,
        },
        row_class=IrrigationData,
    )

    out: List[Dict[str, Any]] = []
    for r in rows or []:
        out.append({
            "id": r.id,
            "start_at": r.start_at,
            "end_at": r.end_at,
            "duration": r.duration,  # minutes
        })
    return out

async def _fetch_depth_stats(
    db: PostgresAsyncPool,
    device_id: int,
    start_at: datetime,
    end_at: datetime,
) -> List[Dict[str, Any]]:
    # Depth progression stats per event window.
    rows: List[DepthStat] = await db.get_all(
        query=GET_IRRIGATION_DEPTH_STATS,
        params={
            "device_id": device_id,
            "start_at": start_at.isoformat(),
            "end_at": end_at.isoformat(),
        },
        row_class=DepthStat,
    )
    return [{"d": r.d, "x": r.x, "y": r.y, "reached_at": r.reached_at} for r in (rows or [])]

async def _fetch_sensor_stats(
    db: PostgresAsyncPool,
    irrigation_id: int,
) -> List[Dict[str, Any]]:
    # VWC before/after per depth for a given irrigation.
    rows: List[SensorStat] = await db.get_all(
        query=GET_IRRIGATION_SENSOR_STATS,
        params={"irrigation_id": irrigation_id},
        row_class=SensorStat,
    )
    return [{"d": r.d, "vwc_start": r.vwc_start, "vwc_end": r.vwc_end} for r in (rows or [])]

async def _fetch_root_depths(
    db: PostgresAsyncPool,
    device_field_id: int,
) -> List[Dict[str, Any]]:
    # Static root depth list for the device/field pair.
    rows: List[RootDepth] = await db.get_all(
        query=GET_DEVICE_ROOTS_DEPTH,
        params={"device_field_id": device_field_id},
        row_class=RootDepth,
    )
    return [{"d": r.d, "x": r.x, "y": r.y} for r in (rows or [])]

async def _compute_avg_flow(
    db: PostgresAsyncPool,
    field_id: int,
    device_id: int,
    field_timezone: str,
    start_at: datetime,
    end_at: datetime,
) -> Optional[float]:
    # Average flow across Flow_1/Flow_2 for the event window.
    try:
        rows = await get_flow_data(
            db=db,
            field_id=field_id,
            device_id=device_id,
            field_timezone=field_timezone,
            start_at=start_at,
            end_at=end_at,
            type=DataType.Avg,
            group=DataGroup.Range,
        )
    except Exception:
        return None

    vals: List[float] = []
    for r in rows or []:
        for m in (getattr(r, "data", None) or []):
            fk = str(getattr(m, "fw_key", "") or "")
            if fk not in (str(int(Measurement.Flow_1)), str(int(Measurement.Flow_2))):
                continue
            v = m.avg if getattr(m, "avg", None) is not None else getattr(m, "data", None)
            if isinstance(v, (int, float)):
                vals.append(float(v))

    return (sum(vals) / len(vals)) if vals else None

def _pack_wetting_depths(depth_stats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Normalize wetting depth rows; keep reached_at as ISO string.
    out: List[Dict[str, Any]] = []
    for s in depth_stats:
        reached = s.get("reached_at")
        out.append({
            "d": s.get("d"),
            "x": s.get("x"),
            "y": s.get("y"),
            "reached_at": _dt_to_iso(reached) if reached else None,
        })
    return out

def _pack_sensor_wetting_times(depth_stats: List[Dict[str, Any]]) -> Dict[str, str]:
    # Earliest wetting time per depth: {"d_20": "...", ...}.
    earliest: Dict[int, datetime] = {}
    for s in depth_stats:
        d = s.get("d")
        t = s.get("reached_at")
        if not isinstance(d, int) or not isinstance(t, datetime):
            continue
        cur = earliest.get(d)
        if cur is None or t < cur:
            earliest[d] = t
    return {f"d_{k}": v.isoformat() for k, v in earliest.items()}

def _pack_vwc_before_after(sensor_stats: List[Dict[str, Any]]) -> Dict[str, Dict[str, Optional[float]]]:
    # {"d_20":{"before":X,"after":Y}, ...}
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for s in sensor_stats:
        d = s.get("d")
        if not isinstance(d, int):
            continue
        out[f"d_{d}"] = {
            "before": s.get("vwc_start"),
            "after": s.get("vwc_end"),
        }
    return out

def _pack_root_zone_list(root_depths: List[Dict[str, Any]]) -> List[int]:
    # Sorted unique integer depths.
    uniq = sorted({int(r.get("d")) for r in root_depths if isinstance(r.get("d"), int)})
    return uniq


async def upsert_irrigation_events(
    session,
    stations_by_field: Dict[int, List[Device]],
    pg_pool: PostgresAsyncPool,
    start_at: datetime,
    end_at: datetime,
    default_timezone: str = "UTC",
    timezone_by_field: Optional[Dict[int, str]] = None,
) -> None:
    # Total processed events (telemetry).
    total_events = 0

    for field_id, stations in stations_by_field.items():
        # Field timezone or fallback.
        field_tz = (timezone_by_field or {}).get(field_id, default_timezone) or default_timezone

        for st in stations:
            serial = getattr(st, "serial_number", None)
            if not serial:
                continue

            device_id = getattr(st, "id", None)
            device_field_id = getattr(st, "device_field_id", None)
            if device_id is None:
                continue

            # Fetch irrigation intervals for this device.
            try:
                events = await _fetch_irrigations(
                    db=pg_pool,
                    field_id=field_id,
                    device_id=device_id,
                    field_tz=field_tz,
                    start_at=start_at,
                    end_at=end_at,
                )
            except Exception as e:
                print(f"[DBG] {serial}: fetch irrigations error {e!r}, skipping")
                continue

            if not events:
                continue

            # Root zone depths (static list).
            try:
                root_depths = await _fetch_root_depths(pg_pool, device_field_id or 0)
                root_zone_list = _pack_root_zone_list(root_depths)
            except Exception:
                root_zone_list = []

            for ev in events:
                ev_id = ev.get("id")
                ev_start = ev.get("start_at")
                ev_end = ev.get("end_at")
                duration_min = ev.get("duration")
                duration_min = int(duration_min) if isinstance(duration_min, (int, float)) else None

                # Build datetime parameter maps with timezone info.
                start_p = ensure_datetime_param(ev_start, tz=field_tz)
                end_p   = ensure_datetime_param(ev_end,   tz=field_tz) if ev_end else None

                # Depth/sensor stats around the event.
                try:
                    depth_stats = await _fetch_depth_stats(pg_pool, device_id, ev_start, ev_end or ev_start)
                except Exception:
                    depth_stats = []

                try:
                    sensor_stats = await _fetch_sensor_stats(pg_pool, ev_id)
                except Exception:
                    sensor_stats = []

                wetting_depths = _pack_wetting_depths(depth_stats)
                sensor_wetting_times = _pack_sensor_wetting_times(depth_stats)
                vwc_before_after = _pack_vwc_before_after(sensor_stats)

                # JSON payloads for Neo4j properties.
                wetting_depths_json = _to_json(wetting_depths)
                sensor_wetting_times_json = _to_json(sensor_wetting_times)
                vwc_before_after_json = _to_json(vwc_before_after)

                # Average flow over the event window.
                try:
                    avg_flow = await _compute_avg_flow(
                        db=pg_pool,
                        field_id=field_id,
                        device_id=device_id,
                        field_timezone=field_tz,
                        start_at=ev_start,
                        end_at=ev_end or ev_start,
                    )
                except Exception:
                    avg_flow = None

                volume_per_emitter = None  # placeholder for future rule-based calc

                # Upsert IrrigationEvent node with datetime(start).
                await session.run(
                    """
                    MERGE (ie:IrrigationEvent {
                        station_serial: $serial,
                        start_datetime: datetime($start)
                    })
                    SET ie.duration_min = $duration_min,
                        ie.avg_flow = $avg_flow,
                        ie.volume_per_emitter = $vol_per_emitter,
                        ie.wetting_depths_json = $wetting_depths_json,
                        ie.sensor_wetting_times_json = $sensor_wetting_times_json,
                        ie.vwc_before_after_json = $vwc_before_after_json,
                        ie.root_zone_list = $root_zone_list
                    """,
                    serial=serial,
                    start=start_p,
                    duration_min=duration_min,
                    avg_flow=avg_flow,
                    vol_per_emitter=volume_per_emitter,
                    wetting_depths_json=wetting_depths_json,
                    sensor_wetting_times_json=sensor_wetting_times_json,
                    vwc_before_after_json=vwc_before_after_json,
                    root_zone_list=root_zone_list,
                )

                # Set end_datetime only when available.
                await session.run(
                    """
                    MATCH (ie:IrrigationEvent { station_serial: $serial, start_datetime: datetime($start) })
                    WITH ie, $end AS endp
                    WHERE endp IS NOT NULL
                    SET ie.end_datetime = datetime(endp)
                    """,
                    serial=serial,
                    start=start_p,
                    end=end_p,
                )

                # Link Station → IrrigationEvent.
                await session.run(
                    """
                    MATCH (s:Station { serial_number: $serial })
                    MATCH (ie:IrrigationEvent { station_serial: $serial, start_datetime: datetime($start) })
                    MERGE (s)-[:HAS_IRRIGATION_EVENT]->(ie)
                    """,
                    serial=serial,
                    start=start_p,
                )

                total_events += 1

    # Final summary.
    print(f"[DBG] irrigation_events summary → events:{total_events}")
