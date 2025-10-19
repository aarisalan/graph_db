# app/graph_weather_day.py
# Upserts daily weather stats per station: (WeatherDay) and Station→WeatherDay links.

from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from statistics import mean
from types import SimpleNamespace

from models.device import Device, DeviceData
from models.device_data import MeasurementData
from services.enums import Measurement, DataType, DataGroup
from services.device import get_device_data, _get_data_fields
from app.utils import ensure_datetime_param

# Target daily weather measurements.
WEATHER_MEASUREMENTS: List[Measurement] = [
    Measurement.Air_Temperature,
    Measurement.Relative_Humidity,
    Measurement.Air_Pressure,
    Measurement.Wind_Speed_1,
    Measurement.Wind_Direction_1,
    Measurement.Wind_Speed_2,
    Measurement.Wind_Direction_2,
    Measurement.Solar_Radiation,
    Measurement.Rainfall,
    Measurement.Leaf_Air_Temperature,
    Measurement.Leaf_Temperature,
]

# Property aliases for Neo4j nodes.
MEAS_PROP: Dict[Measurement, str] = {
    Measurement.Air_Temperature: "air_temp",
    Measurement.Relative_Humidity: "rel_hum",
    Measurement.Air_Pressure: "air_press",
    Measurement.Wind_Speed_1: "wind_speed_1",
    Measurement.Wind_Direction_1: "wind_dir_1",
    Measurement.Wind_Speed_2: "wind_speed_2",
    Measurement.Wind_Direction_2: "wind_dir_2",
    Measurement.Solar_Radiation: "solar_rad",
    Measurement.Rainfall: "rain",
    Measurement.Leaf_Air_Temperature: "leaf_air_temp",
    Measurement.Leaf_Temperature: "leaf_temp",
}

def _sanitize_prop_name(s: str) -> str:
    # Alphanumeric/underscore only.
    out = []
    for ch in s.lower():
        out.append(ch if ("a" <= ch <= "z") or ("0" <= ch <= "9") or ch == "_" else "_")
    return "".join(out)

def _resolve_property_base(m: MeasurementData) -> str:
    # Resolve property base: enum alias → sanitized label → sanitized fw_key.
    base: Optional[str] = None
    if m.fw_key is not None:
        fk = str(m.fw_key)
        if fk.isdigit():
            try:
                base = MEAS_PROP.get(Measurement(int(fk)))
            except ValueError:
                base = None
    if base is None and m.label:
        base = _sanitize_prop_name(m.label)
    if base is None and m.fw_key is not None:
        base = _sanitize_prop_name(str(m.fw_key))
    return base or "m"

def _mean_safe(vals: List[Optional[float]]) -> Optional[float]:
    # Mean over non-nulls.
    xs = [v for v in vals if v is not None]
    return mean(xs) if xs else None

def _aggregate_hourly_to_daily(hourly_rows: List[DeviceData]) -> List[DeviceData]:
    # Hourly stats → daily stats (avg over data/min/max/avg, sum over sum).
    by_date: Dict[date, List[DeviceData]] = defaultdict(list)
    for r in hourly_rows:
        d = r.data_at.date() if isinstance(r.data_at, datetime) else None
        if d:
            by_date[d].append(r)

    out: List[DeviceData] = []
    for d, rows in by_date.items():
        buckets: Dict[Tuple[Optional[str], Optional[str]], Dict[str, List[Optional[float]]]] = defaultdict(
            lambda: {"data": [], "min": [], "max": [], "avg": [], "sum": [], "unit": [], "fw_key": [], "label": []}
        )
        for r in rows:
            for m in (r.data or []):
                k = (m.fw_key, m.label)
                b = buckets[k]
                b["data"].append(m.data)
                b["min"].append(m.min)
                b["max"].append(m.max)
                b["avg"].append(m.avg)
                b["sum"].append(m.sum)
                b["unit"].append(m.unit)
                b["fw_key"].append(m.fw_key)
                b["label"].append(m.label)

        daily_measurements: List[MeasurementData] = []
        for (fw_key, label), b in buckets.items():
            daily_measurements.append(
                MeasurementData(
                    fw_key=fw_key or "",
                    label=label or "",
                    unit=(b["unit"][0] if b["unit"] else None),
                    data=_mean_safe(b["data"]),
                    min=(min([v for v in b["min"] if v is not None]) if any(v is not None for v in b["min"]) else None),
                    max=(max([v for v in b["max"] if v is not None]) if any(v is not None for v in b["max"]) else None),
                    avg=_mean_safe(b["avg"]),
                    sum=(sum([v for v in b["sum"] if v is not None]) if any(v is not None for v in b["sum"]) else None),
                )
            )
        out.append(DeviceData(data_at=datetime(d.year, d.month, d.day), data=daily_measurements))

    out.sort(key=lambda r: r.data_at)
    return out

async def _discover_weather_fwkeys(pg_pool, field_id: int, device_id: int, tz: str,
                                   start_at: datetime, end_at: datetime) -> List[int]:
    # Find present fw_keys on the weather device (filtered first, then full discover).
    wanted = [int(m) for m in WEATHER_MEASUREMENTS]
    try:
        df = await _get_data_fields(pg_pool, field_id, device_id, tz, start_at, end_at, wanted, [])
    except Exception:
        df = []
    if not df:
        try:
            df = await _get_data_fields(pg_pool, field_id, device_id, tz, start_at, end_at, [], [])
        except Exception:
            df = []
    present: set[int] = set()
    for d in (df or []):
        try:
            fk = int(getattr(d, "fw_key"))
            if fk in wanted:
                present.add(fk)
        except Exception:
            continue
    return sorted(present)

async def _discover_weather_df(pg_pool, field_id: int, device_id: int, tz: str,
                               start_at: datetime, end_at: datetime):
    # Raw data_fields discovery (unfiltered).
    try:
        df = await _get_data_fields(pg_pool, field_id, device_id, tz, start_at, end_at, [], [])
    except Exception:
        df = []
    return df or []

async def upsert_weather_days(
    session,
    stations_by_field: Dict[int, List[Device]],
    pg_pool,
    start_at: datetime,
    end_at: datetime,
    default_timezone: str = "UTC",
    timezone_by_field: Optional[Dict[int, str]] = None,
) -> None:
    # Iterate fields → stations (with weather device).
    for field_id, stations in stations_by_field.items():
        tz = (timezone_by_field or {}).get(field_id, default_timezone)

        for st in stations:
            serial = getattr(st, "serial_number", None)
            wx_id = getattr(st, "weather_device_id", None)
            if not serial or not wx_id:
                print(f"[DBG] {serial}: no weather_device_id, skipping")
                continue

            wx_field_id = getattr(st, "field_id", None)
            if not wx_field_id:
                print(f"[DBG] {serial}: missing field_id on device row, skipping")
                continue

            # Discover data_fields and intersect with target fw_keys.
            df = await _discover_weather_df(pg_pool, wx_field_id, wx_id, tz or default_timezone, start_at, end_at)
            wanted = {int(m) for m in WEATHER_MEASUREMENTS}
            present_fw: List[int] = []
            for d in df:
                try:
                    fk = int(getattr(d, "fw_key"))
                    if fk in wanted:
                        present_fw.append(fk)
                except Exception:
                    continue

            if not present_fw:
                preview = []
                for d in df[:5]:
                    try:
                        preview.append(f"{getattr(d, 'fw_key')}:{getattr(d, 'label', None)}")
                    except Exception:
                        continue
                print(f"[DBG] {serial}: no weather data_fields present in range (preview={preview or '[]'}), fallback try")

            try_fw = present_fw if present_fw else list(wanted)

            # Pull daily stats; fallback to hourly→daily aggregation.
            try:
                daily: List[DeviceData] = await get_device_data(
                    pg_pool, wx_field_id, wx_id, tz or default_timezone,
                    start_at, end_at, try_fw, DataType.Stats, DataGroup.Day,
                )
            except Exception as e:
                print(f"[DBG] {serial}: get_device_data(Day) error {e!r}")
                daily = []

            if not daily:
                try:
                    hourly: List[DeviceData] = await get_device_data(
                        pg_pool, wx_field_id, wx_id, tz or default_timezone,
                        start_at, end_at, try_fw, DataType.Stats, DataGroup.Hour,
                    )
                except Exception as e:
                    print(f"[DBG] {serial}: get_device_data(Hour) error {e!r}, skipping")
                    continue
                if not hourly:
                    print(f"[DBG] {serial}: no daily weather data")
                    continue
                daily = _aggregate_hourly_to_daily(hourly)
                if not daily:
                    print(f"[DBG] {serial}: no daily weather data (aggregated)")
                    continue

            # Upsert (WeatherDay) and link Station→WeatherDay; write metrics.
            for row in daily:
                dt_params = ensure_datetime_param(row.data_at, tz=tz)

                await session.run(
                    "MERGE (wd:WeatherDay { station_serial: $serial, date: datetime($dt) })",
                    serial=serial, dt=dt_params,
                )

                for m in (row.data or []):
                    base = _resolve_property_base(m)
                    await session.run(
                        f"""
                        MATCH (wd:WeatherDay {{ station_serial: $serial, date: datetime($dt) }})
                        SET wd.`{base}`      = $val,
                            wd.`{base}_min`  = $min,
                            wd.`{base}_max`  = $max,
                            wd.`{base}_avg`  = $avg,
                            wd.`{base}_sum`  = $sum
                        """,
                        serial=serial, dt=dt_params,
                        val=m.data, min=m.min, max=m.max, avg=m.avg, sum=m.sum,
                    )

                await session.run(
                    """
                    MATCH (s:Station { serial_number: $serial })
                    MATCH (wd:WeatherDay { station_serial: $serial, date: datetime($dt) })
                    MERGE (s)-[:HAS_WEATHER_DAY]->(wd)
                    """,
                    serial=serial, dt=dt_params,
                )
