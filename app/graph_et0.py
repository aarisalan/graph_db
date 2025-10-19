# app/graph_et0.py
# Upserts daily ET0 (reference evapotranspiration) into Neo4j per station and date.

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from models.device import Device, DeviceData
from services.device import get_weather_device, get_et0_data


async def upsert_et0_days(
    session,
    stations_by_field: Dict[int, List[Device]],
    pg_pool,
    start_at: datetime,
    end_at: datetime,
    default_timezone: str = "UTC",
    timezone_by_field: Optional[Dict[int, str]] = None,
) -> None:
    # Telemetry counters.
    stations_with_wx = 0
    total_rows = 0

    for field_id, stations in stations_by_field.items():
        # Resolve field timezone or default.
        tz = (timezone_by_field or {}).get(field_id, default_timezone)

        for st in stations:
            serial = getattr(st, "serial_number", None)
            if not serial:
                continue

            # Retrieve the weather device linked to this station (if any).
            try:
                wx = await get_weather_device(pg_pool, st.id)
            except Exception as e:
                print(f"[DBG] {serial}: get_weather_device error {e!r}, skipping")
                continue

            if not wx or not getattr(wx, "id", None):
                # No weather device found; skip ET0 for this station.
                continue

            stations_with_wx += 1

            # Fetch daily ET0 rows within the window.
            try:
                daily: List[DeviceData] = await get_et0_data(
                    pg_pool,
                    wx.field_id,
                    wx.id,
                    tz or default_timezone,
                    start_at,
                    end_at,
                )
            except Exception as e:
                print(f"[DBG] {serial}: get_et0_data error {e!r}, skipping")
                continue

            if not daily:
                # Quietly skip; not all stations produce ET0.
                continue

            for row in daily:
                # Normalize data_at to a YYYY-MM-DD HH:MM string.
                if isinstance(row.data_at, datetime):
                    date_str = row.data_at.strftime("%Y-%m-%d %H:%M")
                else:
                    date_str = str(row.data_at)

                # Extract ET0 value (service returns a single MeasurementData).
                et0_mm = row.data[0].data if row.data else None

                # Upsert ET0Day node for (station, date).
                await session.run(
                    """
                    MERGE (e:ET0Day { station_serial: $serial, date: $date })
                    SET e.et0_mm = $et0
                    """,
                    serial=serial, date=date_str, et0=et0_mm,
                )

                # Link Station → ET0Day.
                await session.run(
                    """
                    MATCH (s:Station { serial_number: $serial })
                    MATCH (e:ET0Day { station_serial: $serial, date: $date })
                    MERGE (s)-[:HAS_ET0]->(e)
                    """,
                    serial=serial, date=date_str,
                )

                total_rows += 1

    # Final summary.
    print(f"[DBG] et0_days summary → stations_with_wx:{stations_with_wx} rows:{total_rows}")
