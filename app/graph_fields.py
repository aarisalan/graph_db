# app/graph_fields.py
# Upserts Field nodes with geo/metadata and links Field→Crop.

async def upsert_fields(session, fields):
    for field in fields or []:
        # Extract center coordinates if present (supports dict or object with x/y).
        lat = lon = None
        if hasattr(field, "center"):
            center = field.center
            lat = center.get("x") if isinstance(center, dict) else getattr(center, "x", None)
            lon = center.get("y") if isinstance(center, dict) else getattr(center, "y", None)

        # Upsert Field with identifiers, location, and administrative metadata.
        await session.run(
            """
            MERGE (f:Field {field_id: $id})
            SET f.name        = $name,
                f.customer_id = $cust,
                f.tz          = $tz,
                f.country     = $country,
                f.province    = $province,
                f.district    = $district,
                f.lat         = $lat,
                f.lon         = $lon
            """,
            id=field.id, name=field.name, cust=field.customer_id,
            tz=field.timezone, country=getattr(field, "country", None),
            province=getattr(field, "city", None), district=getattr(field, "county", None),
            lat=lat, lon=lon
        )

        # Optionally upsert Crop and link Field → Crop when crop info is available.
        crop_id = getattr(field, "crop_id", None)
        crop_name = getattr(field, "crop_name", None)
        if crop_id is not None and crop_name:
            await session.run(
                "MERGE (c:Crop {name: $cname}) SET c.crop_id = $cid",
                cname=crop_name, cid=crop_id
            )
            await session.run(
                """
                MATCH (f:Field {field_id: $fid}), (c:Crop {name: $cname})
                MERGE (f)-[:PLANTED_WITH]->(c)
                """,
                fid=field.id, cname=crop_name
            )
