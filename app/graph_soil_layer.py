# app/graph_soil_layer.py
# Upserts per-depth SoilLayerReading metrics under SoilDay and links SoilDay→SoilLayerReading.

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set

from models.device import Device
from services.enums import Measurement, DataType, DataGroup
from services.device import _get_data_fields, _get_data  # type: ignore
from app.utils import ensure_datetime_param

# Base measurements to pull (fw_key set).
BASE_MEASUREMENTS: List[Measurement] = [
    Measurement.Soil_Temperature,  # 166
    Measurement.Soil_Moisture,     # 167
]

# Canonical property names for base measurements.
MEAS_BASE_INT: Dict[int, str] = {
    int(Measurement.Soil_Temperature): "soil_temp",
    int(Measurement.Soil_Moisture): "vwc",
}

# First number inside label (supports "22", "22.5", "22,5", etc.).
_DEPTH_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[.,]\d+)?)", flags=re.IGNORECASE)


def _depth_from_label(lbl: Optional[str]) -> Optional[int]:
    # Extract depth (cm) from label; round halves (22.5→23).
    if not lbl:
        return None
    m = _DEPTH_RE.search(lbl)
    if not m:
        return None
    raw = m.group(1).replace(",", ".")
    try:
        return int(round(float(raw)))
    except Exception:
        return None


def _pick_label(current: Optional[str], new: Optional[str]) -> Optional[str]:
    # Prefer non-empty; if both present choose the longer (more descriptive).
    if not current:
        return new or current
    if new and len(new) > len(current):
        return new
    return current


async def upsert_soil_layer_readings(
    session,
    stations_by_field: Dict[int, List[Device]],
    pg_pool,
    start_at: datetime,
    end_at: datetime,
    default_timezone: str = "UTC",
    timezone_by_field: Optional[Dict[int, str]] = None,
    write_unknown_depth_as_minus1: bool = True,
) -> None:
    # Telemetry counters.
    measurement_ids: List[int] = [int(m) for m in BASE_MEASUREMENTS]
    total_tried = total_with_df = total_rows = total_nodes = 0
    unknown_label_samples: Set[str] = set()
    MAX_LABEL_SAMPLES = 20

    for field_id, stations in stations_by_field.items():
        tz = (timezone_by_field or {}).get(field_id, default_timezone)

        for st in stations:
            serial = getattr(st, "serial_number", None)
            if not serial:
                continue
            if not bool(getattr(st, "has_soil_moisture", False)):
                continue

            total_tried += 1

            # 1) Discover data_fields (prefer filtered, fallback to full discovery).
            try:
                df = await _get_data_fields(
                    pg_pool,
                    field_id=st.field_id,
                    device_id=st.id,
                    field_timezone=tz,
                    start_at=start_at,
                    end_at=end_at,
                    measurements=measurement_ids,
                    named_measurements=[],
                )
            except Exception as e:
                print(f"[DBG] {serial}: _get_data_fields err {e!r}, skip")
                continue

            if not df:
                print(f"[DBG] {serial}: no data_fields (filtered) → discover all")
                try:
                    df = await _get_data_fields(
                        pg_pool,
                        field_id=st.field_id,
                        device_id=st.id,
                        field_timezone=tz,
                        start_at=start_at,
                        end_at=end_at,
                        measurements=[],          # unfiltered discovery
                        named_measurements=[],
                    )
                except Exception as e:
                    print(f"[DBG] {serial}: _get_data_fields discover err {e!r}, skip")
                    continue

            if not df:
                print(f"[DBG] {serial}: still no data_fields after discover")
                continue

            total_with_df += 1

            # Build key→(fw_key, depth_cm, depth_label) map from data_fields.
            key_meta: Dict[str, Tuple[int, Optional[int], Optional[str]]] = {}
            for d in df:
                try:
                    fw_key_int = int(getattr(d, "fw_key"))
                except Exception:
                    continue
                if fw_key_int not in measurement_ids:
                    continue

                for item in (getattr(d, "data", None) or []):
                    # item is a mapping like {"c167_110": meta}
                    for key, meta in item.items():
                        # meta can be dict or model-like.
                        if isinstance(meta, dict):
                            label = meta.get("label")
                            sensor_detail = meta.get("sensor_detail")
                        else:
                            label = getattr(meta, "label", None)
                            sensor_detail = getattr(meta, "sensor_detail", None)

                        depth_cm = _depth_from_label(label)
                        if depth_cm is None and sensor_detail:
                            depth_cm = _depth_from_label(sensor_detail)

                        if depth_cm is None and write_unknown_depth_as_minus1:
                            depth_cm = -1
                        elif depth_cm is None:
                            if label and len(unknown_label_samples) < MAX_LABEL_SAMPLES:
                                unknown_label_samples.add(label)
                            continue

                        depth_label = label or sensor_detail
                        key_meta[str(key)] = (fw_key_int, depth_cm, depth_label)

            if not key_meta:
                print(f"[DBG] {serial}: key_meta empty (no soil keys or no depth)")
                continue

            # 2) Fetch daily stats rows for the period.
            try:
                rows = await _get_data(
                    pg_pool,
                    field_id=st.field_id,
                    device_id=st.id,
                    field_timezone=tz,
                    start_at=start_at,
                    end_at=end_at,
                    measurements=measurement_ids,
                    named_measurements=[],
                    type=DataType.Stats,
                    group=DataGroup.Day,
                )
            except Exception as e:
                print(f"[DBG] {serial}: _get_data err {e!r}, skip")
                continue

            if not rows:
                print(f"[DBG] {serial}: no daily soil layer data")
                continue

            total_rows += len(rows)

            # 3) Upsert SoilDay container, per-depth SoilLayerReading, and link.
            for row in rows:
                raw_dt = row.get("data_at_f") or row.get("data_at")  # some envs use 'data_at_f'
                if not raw_dt:
                    continue
                dt_params = ensure_datetime_param(raw_dt, tz=tz)

                # Ensure SoilDay exists.
                await session.run(
                    "MERGE (sd:SoilDay { station_serial: $serial, date: datetime($dt) })",
                    serial=serial, dt=dt_params,
                )

                # Group metrics by depth and base key.
                by_depth: Dict[int, Dict[str, Dict[str, Optional[float]]]] = {}
                depth_label_by_depth: Dict[int, Optional[str]] = {}

                for key, (fw_key, depth_cm, depth_label) in key_meta.items():
                    base = MEAS_BASE_INT.get(fw_key, f"m_{fw_key}")

                    dm = by_depth.setdefault(depth_cm, {}).setdefault(base, {})
                    dm["val"] = row.get(key)
                    dm["min"] = row.get(f"{key}_min")
                    dm["max"] = row.get(f"{key}_max")
                    dm["avg"] = row.get(f"{key}_avg")
                    dm["sum"] = row.get(f"{key}_sum")

                    depth_label_by_depth[depth_cm] = _pick_label(
                        depth_label_by_depth.get(depth_cm), depth_label
                    )

                # Upsert per-depth node and write metrics.
                for depth_cm, metrics in by_depth.items():
                    depth_label = depth_label_by_depth.get(depth_cm)

                    await session.run(
                        """
                        MERGE (sl:SoilLayerReading {
                            station_serial: $serial,
                            date: datetime($dt),
                            depth_cm: $depth
                        })
                        SET sl.depth_label = coalesce(sl.depth_label, $depth_label)
                        """,
                        serial=serial, dt=dt_params, depth=depth_cm, depth_label=depth_label,
                    )

                    for base, vals in metrics.items():
                        await session.run(
                            f"""
                            MATCH (sl:SoilLayerReading {{
                                station_serial: $serial,
                                date: datetime($dt),
                                depth_cm: $depth
                            }})
                            SET sl.`{base}`     = $val,
                                sl.`{base}_min` = $min,
                                sl.`{base}_max` = $max,
                                sl.`{base}_avg` = $avg,
                                sl.`{base}_sum` = $sum
                            """,
                            serial=serial, dt=dt_params, depth=depth_cm,
                            val=vals.get("val"),
                            min=vals.get("min"),
                            max=vals.get("max"),
                            avg=vals.get("avg"),
                            sum=vals.get("sum"),
                        )

                    # Link SoilDay → SoilLayerReading.
                    await session.run(
                        """
                        MATCH (sd:SoilDay { station_serial: $serial, date: datetime($dt) })
                        MATCH (sl:SoilLayerReading { station_serial: $serial, date: datetime($dt), depth_cm: $depth })
                        MERGE (sd)-[:HAS_LAYER_READING]->(sl)
                        """,
                        serial=serial, dt=dt_params, depth=depth_cm,
                    )
                    total_nodes += 1

    # Log a few unparsed labels (if any).
    if unknown_label_samples:
        print("[DBG] Depth parse edilemeyen label örnekleri (ilk 20):")
        for s in sorted(unknown_label_samples)[:20]:
            print(f"  - '{s}'")

    # Final summary.
    print(f"[DBG] soil_layers summary → tried:{total_tried} with_df:{total_with_df} rows:{total_rows} nodes:{total_nodes}")
