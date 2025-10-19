# app/graph_canopy_day.py
# Upserts daily canopy metrics (canopy temp / leaf wetness / fruit diameter) into Neo4j.

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, DefaultDict
from collections import defaultdict

from models.device import Device, DeviceData
from models.device_data import MeasurementData
from services.enums import Measurement, DataType, DataGroup
from services.device import get_device_data, _get_data_fields
from app.utils import ensure_datetime_param

# Target measurements to collect.
CANOPY_MEASUREMENTS: List[Measurement] = [
    Measurement.Canopy_Temperature,
    Measurement.Leaf_Wetness,
    Measurement.Fruit_Diameter,
]

# Canonical property names used on CanopyDay nodes.
MEAS_PROP: Dict[Measurement, str] = {
    Measurement.Canopy_Temperature: "canopy_temp",
    Measurement.Leaf_Wetness: "leaf_wetness",
    Measurement.Fruit_Diameter: "fruit_diameter",
}

def _sanitize(s: Optional[str]) -> Optional[str]:
    # Lowercase and replace non [a-z0-9_] with underscore.
    if not s:
        return s
    out = []
    for ch in s.lower():
        if ("a" <= ch <= "z") or ("0" <= ch <= "9") or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)

def _resolve_property_base(m: MeasurementData) -> str:
    # Resolve property base name: enum→MEAS_PROP, else sanitized label, else sanitized fw_key.
    base: Optional[str] = None
    if m.fw_key is not None:
        fk = str(m.fw_key)
        if fk.isdigit():
            try:
                enum_member = Measurement(int(fk))
                base = MEAS_PROP.get(enum_member)
            except ValueError:
                base = None
    if base is None and m.label:
        base = _sanitize(m.label)
    if base is None and m.fw_key is not None:
        base = _sanitize(str(m.fw_key))
    return base or "m"

async def _discover_df(pg_pool, field_id: int, device_id: int, tz: str,
                       start_at: datetime, end_at: datetime):
    # Discover data field definitions; some envs return empty even with daily data.
    try:
        df = await _get_data_fields(
            pg_pool,
            field_id=field_id,
            device_id=device_id,
            field_timezone=tz,
            start_at=start_at,
            end_at=end_at,
            measurements=[],
            named_measurements=[],
        )
    except Exception:
        df = []
    return df or []

def _aggregate_hourly_to_daily(hourly: List[DeviceData]) -> List[DeviceData]:
    # Aggregate hourly stats to daily stats (min/min, max/max, avg(mean of avgs), sum/sum).
    by_date: DefaultDict[str, List[DeviceData]] = defaultdict(list)
    for h in hourly:
        dkey = h.data_at.date().isoformat() if isinstance(h.data_at, datetime) else str(h.data_at)[:10]
        by_date[dkey].append(h)

    out: List[DeviceData] = []
    for dkey, rows in by_date.items():
        bucket: Dict[tuple, Dict[str, float]] = {}
        last_unit: Dict[tuple, Optional[str]] = {}

        # Accumulate per (fw_key, label, unit).
        for r in rows:
            for m in (r.data or []):
                k = (m.fw_key, m.label, m.unit)
                b = bucket.setdefault(k, {
                    "min": float("inf"),
                    "max": float("-inf"),
                    "avg_sum": 0.0,
                    "avg_cnt": 0,
                    "sum": 0.0,
                })
                if m.min is not None:
                    b["min"] = min(b["min"], m.min)
                if m.max is not None:
                    b["max"] = max(b["max"], m.max)
                if m.avg is not None:
                    b["avg_sum"] += m.avg
                    b["avg_cnt"] += 1
                if m.sum is not None:
                    b["sum"] += m.sum
                last_unit[k] = m.unit

        # Build daily MeasurementData rows.
        daily_md: List[MeasurementData] = []
        for k, agg in bucket.items():
            fw_key, label, _unit = k
            min_v = agg["min"] if agg["min"] != float("inf") else None
            max_v = agg["max"] if agg["max"] != float("-inf") else None
            avg_v = (agg["avg_sum"] / agg["avg_cnt"]) if agg["avg_cnt"] > 0 else None
            sum_v = agg["sum"] if agg["sum"] != 0.0 else None

            daily_md.append(MeasurementData(
                fw_key=str(fw_key) if fw_key is not None else None,
                label=label,
                unit=last_unit.get(k),
                data=None,
                min=min_v,
                max=max_v,
                avg=avg_v,
                sum=sum_v,
            ))
        out.append(DeviceData(
            # Interpret YYYY-MM-DD as date; keep original otherwise.
            data_at=datetime.fromisoformat(dkey) if len(dkey) == 10 else dkey,
            data=daily_md,
        ))
    # Ensure chronological order.
    out.sort(key=lambda x: x.data_at)
    return out

async def upsert_canopy_days(
    session,
    stations_by_field: Dict[int, List[Device]],
    pg_pool,
    start_at: datetime,
    end_at: datetime,
    default_timezone: str = "UTC",
    timezone_by_field: Optional[Dict[int, str]] = None,
) -> None:
    # Resolve wanted firmware keys from Measurement enum.
    wanted = {int(m) for m in CANOPY_MEASUREMENTS}

    for field_id, stations in stations_by_field.items():
        # Use field-specific timezone or global default.
        tz = (timezone_by_field or {}).get(field_id, default_timezone)

        for st in stations:
            serial = getattr(st, "serial_number", None)
            if not serial:
                continue

            dev_id = getattr(st, "id", None)
            fld_id = getattr(st, "field_id", None)
            if not dev_id or not fld_id:
                continue

            # Discover which fw_keys are present on this device.
            df = await _discover_df(pg_pool, fld_id, dev_id, tz, start_at, end_at)
            present_fw: List[int] = []
            for d in df:
                try:
                    fk = int(getattr(d, "fw_key"))
                except Exception:
                    continue
                if fk in wanted:
                    present_fw.append(fk)
            try_fw = present_fw if present_fw else list(wanted)

            # Prefer daily stats; fallback to hourly and aggregate.
            try:
                daily: List[DeviceData] = await get_device_data(
                    pg_pool, fld_id, dev_id, tz, start_at, end_at,
                    try_fw, DataType.Stats, DataGroup.Day,
                )
            except Exception as e:
                print(f"[DBG] {serial}: canopy get_device_data(Day) error {e!r}")
                daily = []

            if not daily:
                try:
                    hourly: List[DeviceData] = await get_device_data(
                        pg_pool, fld_id, dev_id, tz, start_at, end_at,
                        try_fw, DataType.Stats, DataGroup.Hour,
                    )
                except Exception as e:
                    print(f"[DBG] {serial}: canopy get_device_data(Hour) error {e!r}, skipping")
                    continue
                if not hourly:
                    print(f"[DBG] {serial}: no daily canopy data")
                    continue
                daily = _aggregate_hourly_to_daily(hourly)
                if not daily:
                    print(f"[DBG] {serial}: no daily canopy data (aggregated)")
                    continue

            # Upsert CanopyDay and attach per-measurement properties.
            for row in daily:
                dt_params = ensure_datetime_param(row.data_at, tz=tz)

                # Create/ensure CanopyDay node for station+date.
                await session.run(
                    "MERGE (cd:CanopyDay { station_serial: $serial, date: datetime($dt) })",
                    serial=serial, dt=dt_params,
                )

                # Write metric values and min/max/avg/sum under resolved property base.
                for m in (row.data or []):
                    base = _resolve_property_base(m)
                    await session.run(
                        f"""
                        MATCH (cd:CanopyDay {{ station_serial: $serial, date: datetime($dt) }})
                        SET cd.`{base}`      = $val,
                            cd.`{base}_min`  = $min,
                            cd.`{base}_max`  = $max,
                            cd.`{base}_avg`  = $avg,
                            cd.`{base}_sum`  = $sum
                        """,
                        serial=serial, dt=dt_params,
                        val=m.data, min=m.min, max=m.max, avg=m.avg, sum=m.sum,
                    )

                # Link Station → CanopyDay once per date.
                await session.run(
                    """
                    MATCH (s:Station { serial_number: $serial })
                    MATCH (cd:CanopyDay { station_serial: $serial, date: datetime($dt) })
                    MERGE (s)-[:HAS_CANOPY_DAY]->(cd)
                    """,
                    serial=serial, dt=dt_params,
                )
