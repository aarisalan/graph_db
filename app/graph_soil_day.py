# app/graph_soil_day.py
# Upserts daily soil stats (temp/moisture) and links Station→SoilDay.

from datetime import datetime
from typing import Dict, List, Optional

from models.device import Device, DeviceData
from models.device_data import MeasurementData
from services.enums import Measurement, DataType, DataGroup
from services.device import get_device_data
from app.utils import ensure_datetime_param

# Target daily soil measurements.
SOIL_MEASUREMENTS: list[Measurement] = [
    Measurement.Soil_Temperature,
    Measurement.Soil_Moisture,
]

# Canonical property names on SoilDay.
SOIL_PROP = {
    Measurement.Soil_Temperature: "soil_temp",
    Measurement.Soil_Moisture: "soil_moisture",
}


def _sanitize_prop_name(s: str) -> str:
    # Lowercase and replace non [a-z0-9_] with underscore.
    out = []
    for ch in s.lower():
        if ("a" <= ch <= "z") or ("0" <= ch <= "9") or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def _resolve_property_base(m: MeasurementData) -> str:
    # Property base: enum→SOIL_PROP, else sanitized label, else sanitized fw_key.
    base: Optional[str] = None

    if m.fw_key is not None:
        fk = str(m.fw_key)
        if fk.isdigit():
            try:
                enum_member = Measurement(int(fk))
                base = SOIL_PROP.get(enum_member)
            except ValueError:
                base = None

    if base is None and m.label:
        base = _sanitize_prop_name(m.label)

    if base is None and m.fw_key is not None:
        base = _sanitize_prop_name(str(m.fw_key))

    return base or "m"


async def upsert_soil_days(
    session,
    stations_by_field: Dict[int, List[Device]],
    pg_pool,
    start_at: datetime,
    end_at: datetime,
    default_timezone: str = "UTC",
    timezone_by_field: Optional[Dict[int, str]] = None,
) -> None:
    # Iterate stations by field and write daily soil stats.
    for field_id, stations in stations_by_field.items():
        tz = (timezone_by_field or {}).get(field_id, default_timezone)

        for st in stations:
            serial = getattr(st, "serial_number", None)
            if not serial:
                continue

            # Optional filter for irrigation devices:
            # if not getattr(st, "is_irrigation", False): continue

            # Fetch daily stats (min/max/avg/sum).
            try:
                daily: List[DeviceData] = await get_device_data(
                    pg_pool,
                    st.field_id,            # field_id
                    st.id,                  # device_id
                    tz,
                    start_at,
                    end_at,
                    SOIL_MEASUREMENTS,
                    DataType.Stats,
                    DataGroup.Day,
                )
            except Exception as e:
                print(f"[DBG] {serial}: soil get_device_data err {e!r}, skipping")
                continue

            if not daily:
                print(f"[DBG] {serial}: no daily soil data")
                continue

            for row in daily:
                if not getattr(row, "data_at", None):
                    continue

                dt_params = ensure_datetime_param(row.data_at, tz=tz)

                # Container node.
                await session.run(
                    "MERGE (sd:SoilDay { station_serial: $serial, date: datetime($dt) })",
                    serial=serial,
                    dt=dt_params,
                )

                # Per-measurement properties.
                for m in (row.data or []):
                    base = _resolve_property_base(m)
                    await session.run(
                        f"""
                        MATCH (sd:SoilDay {{ station_serial: $serial, date: datetime($dt) }})
                        SET sd.`{base}`      = $val,
                            sd.`{base}_min`  = $min,
                            sd.`{base}_max`  = $max,
                            sd.`{base}_avg`  = $avg,
                            sd.`{base}_sum`  = $sum
                        """,
                        serial=serial,
                        dt=dt_params,
                        val=m.data, min=m.min, max=m.max, avg=m.avg, sum=m.sum,
                    )

                # Link Station → SoilDay.
                await session.run(
                    """
                    MATCH (s:Station { serial_number: $serial })
                    MATCH (sd:SoilDay { station_serial: $serial, date: datetime($dt) })
                    MERGE (s)-[:HAS_SOIL_DAY]->(sd)
                    """,
                    serial=serial,
                    dt=dt_params,
                )
