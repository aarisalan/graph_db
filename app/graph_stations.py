# app/graph_stations.py
# Upserts Station nodes, links Field→Station, and writes DepthPoint nodes per root depth.

from services.irrigation import _get_device_roots_depth

async def upsert_stations_and_depths(session, stations_by_field, pg_pool):
    # Iterate fields and their stations.
    for fid, stations in stations_by_field.items():
        for st in stations:
            # Fetch per-station root depth metadata.
            root_depths = await _get_device_roots_depth(pg_pool, device_field_id=st.device_field_id)
            if not root_depths:
                continue

            # Station identity and optional lat/lon.
            serial = st.serial_number
            lat = lon = None
            if hasattr(st, "location"):
                loc = st.location
                lat = loc.get("x") if isinstance(loc, dict) else getattr(loc, "x", None)
                lon = loc.get("y") if isinstance(loc, dict) else getattr(loc, "y", None)

            # Upsert Station node (core metadata).
            await session.run(
                """
                MERGE (s:Station {serial_number: $serial})
                SET s.label = $label, s.type = $type_id, s.lat = $lat, s.lon = $lon
                """,
                serial=serial, label=st.label, type_id=st.type_id, lat=lat, lon=lon
            )

            # Link Field → Station.
            await session.run(
                """
                MATCH (f:Field {field_id: $fid}), (s:Station {serial_number: $serial})
                MERGE (f)-[:HAS_STATION]->(s)
                """,
                fid=fid, serial=serial
            )

            # Upsert DepthPoint nodes and link Station → DepthPoint.
            for rd in root_depths:
                params = {"serial": serial, "d": rd.d, "y": rd.y}
                if rd.x is not None:
                    # With sensor_index dimension.
                    params["x"] = rd.x
                    merge_dp = """
                      MERGE (dp:DepthPoint {station_serial: $serial, depth: $d, sensor_index: $x})
                      SET dp.y = $y
                    """
                    match_dp = """
                      MATCH (s:Station {serial_number: $serial})
                      MATCH (dp:DepthPoint {station_serial: $serial, depth: $d, sensor_index: $x})
                      MERGE (s)-[:AT_DEPTH]->(dp)
                    """
                else:
                    # Without sensor_index dimension.
                    merge_dp = """
                      MERGE (dp:DepthPoint {station_serial: $serial, depth: $d})
                      SET dp.y = $y
                    """
                    match_dp = """
                      MATCH (s:Station {serial_number: $serial})
                      MATCH (dp:DepthPoint {station_serial: $serial, depth: $d})
                      MERGE (s)-[:AT_DEPTH]->(dp)
                    """
                await session.run(merge_dp, **params)
                await session.run(match_dp, **params)
