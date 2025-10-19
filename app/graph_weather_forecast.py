# app/graph_weather_forecast.py
# Upserts WeatherForecast nodes and links Station→WeatherForecast using per-timestamp forecasts.

from typing import List, Dict, Any
from models.weather_forecast import WeatherForecast
from services.weather_forecast import get_weather_forecast
from app.utils import ensure_datetime_param

async def upsert_weather_forecast(session, stations_by_field, pg_pool):
    # Iterate fields → stations (must have a weather device).
    for _, stations in stations_by_field.items():
        for st in stations:
            serial = getattr(st, "serial_number", None)
            wd_id = getattr(st, "weather_device_id", None)
            if not serial or wd_id is None:
                print(f"[DBG] {serial}: no weather_device_id, skipping")
                continue

            # Pull forecasts (guard exceptions / empty results).
            forecasts: List[WeatherForecast] = []
            try:
                forecasts = await get_weather_forecast(
                    db=pg_pool,
                    customer_id=getattr(st, "customer_id", None),
                    field_id=getattr(st, "field_id", None),
                    device_id=wd_id,
                )
            except Exception as e:
                print(f"[DBG] {serial}: weather_forecast exception {e!r}, skipping")
                continue

            if not forecasts:
                print(f"[DBG] {serial}: no weather forecasts, skipping")
                continue

            # Upsert per-forecast timestamp.
            for wf in forecasts:
                if not getattr(wf, "at", None):
                    continue

                # Parameterize datetime for Neo4j (tz optional/safe).
                dt_params = ensure_datetime_param(wf.at)

                # Flatten measurement bundle into node props.
                props: Dict[str, Any] = {}
                for name, meas in (wf.measurements or {}).items():
                    if meas is None:
                        continue
                    props[f"{name}_min"]   = getattr(meas, "min", None)
                    props[f"{name}_max"]   = getattr(meas, "max", None)
                    props[f"{name}_avg"]   = getattr(meas, "avg", None)
                    props[f"{name}_total"] = getattr(meas, "total", None)

                # Node upsert (MERGE) + bulk property set.
                await session.run(
                    """
                    MERGE (wf:WeatherForecast { station_serial: $serial, date: datetime($dt) })
                    SET wf += $props
                    """,
                    serial=serial, dt=dt_params, props=props,
                )

                # Link Station → WeatherForecast.
                await session.run(
                    """
                    MATCH (s:Station {serial_number: $serial})
                    MATCH (wf:WeatherForecast {station_serial: $serial, date: datetime($dt)})
                    MERGE (s)-[:HAS_FORECAST]->(wf)
                    """,
                    serial=serial, dt=dt_params,
                )
