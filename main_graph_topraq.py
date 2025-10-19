# main_graph_topraq.py
# Orchestrates ETL from PostgreSQL to Neo4j with selectable tasks and timing.

import os
import asyncio
import argparse
from datetime import datetime
from time import perf_counter

# Apply runtime patches before other imports.
from app.patches import *  # noqa: F401,F403

# Postgres connections and domain fetchers.
from db.postgres import PostgresAsyncPool
from services.field import get_fields
from services.device import get_devices

# Neo4j session and graph upsert tasks.
from app.neo4j_pool import Neo4jAsyncPool
from app.graph_constraints import create_constraints
from app.graph_fields import upsert_fields
from app.graph_stations import upsert_stations_and_depths
from app.graph_weather_day import upsert_weather_days
from app.graph_weather_forecast import upsert_weather_forecast
from app.graph_soil_day import upsert_soil_days
from app.graph_soil_layer import upsert_soil_layer_readings
from app.graph_irrigation_day import upsert_irrigation_days
from app.graph_canopy_day import upsert_canopy_days
from app.graph_sap_day import upsert_sap_days
from app.graph_irrigation_event import upsert_irrigation_events
from app.graph_et0 import upsert_et0_days
from app.graph_sap_analysis import upsert_sap_analyses
from app.graph_sap_element_result import upsert_sap_element_results
from app.graph_optimum_sap_range import upsert_optimum_sap_ranges
from app.graph_optimum_element_range import upsert_optimum_element_ranges
from app.graph_haney_analysis import upsert_haney_analyses
from app.graph_tnd_analysis import upsert_tnd_analyses
from app.graph_soil_analysis import upsert_soil_analyses
from app.graph_soil_param_result import upsert_soil_param_results
from app.graph_water_analysis import upsert_water_analyses
from app.graph_water_param_result import upsert_water_param_results
from app.graph_application_event import upsert_application_events
from app.graph_product_application import upsert_product_applications
from app.graph_fertilizer_product import upsert_fertilizer_products
from app.graph_app_nutrient_content import upsert_app_nutrient_contents


async def measure_async(label: str, coro):
    # Await the coroutine and print elapsed wall time.
    t0 = perf_counter()
    result = await coro
    dt = perf_counter() - t0
    print(f"[TIMER] {label}: {dt:.2f}s")
    return result


def _env_enabled(key: str, default: bool = True) -> bool:
    # Read ENABLE_<KEY> and map "0/false/''" to False; fallback to default.
    val = os.getenv(f"ENABLE_{key.upper()}")
    if val is None:
        return default
    return val.strip() not in ("0", "false", "False", "FALSE", "")


def _parse_args():
    # CLI: selection filters, Neo4j connection, and time window.
    p = argparse.ArgumentParser(description="Data Assistant graph integration")

    # Selection flags (comma-separated lists).
    p.add_argument("--only",  help="Run only these steps (comma-separated)", default="")
    p.add_argument("--skip",  help="Skip these steps (comma-separated)", default="")

    # Neo4j connection settings (override in prod via env/secrets).
    p.add_argument("--neo4j-uri", default="")
    p.add_argument("--neo4j-user", default="")
    p.add_argument("--neo4j-password", default="")

    # Processing window (ISO dates).
    p.add_argument("--start", help="YYYY-MM-DD", default="2025-06-01")
    p.add_argument("--end",   help="YYYY-MM-DD", default="2025-08-22")
    return p.parse_args()


async def run_integration(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    start_at: datetime,
    end_at: datetime,
    only: set[str],
    skip: set[str],
):
    # Open Postgres pool (shared across fetchers and projectors).
    pg_pool = PostgresAsyncPool()
    await measure_async("pg_pool.open", pg_pool.open())

    try:
        # Load fields and gather devices per field concurrently.
        fields = await measure_async("get_fields", get_fields(pg_pool))
        field_list = fields or []

        # Fetch devices for each field in parallel.
        stations_list = await measure_async(
            "get_devices (gather)",
            asyncio.gather(*[get_devices(pg_pool, f.customer_id, f.id) for f in field_list])
        )

        # Build quick-lookup structures for projectors.
        stations_by_field = {f.id: stations for f, stations in zip(field_list, stations_list)}
        timezone_by_field = {f.id: f.timezone for f in field_list}
        default_tz = field_list[0].timezone if field_list else "UTC"

        # Open Neo4j session (single session for deterministic ordering).
        neo4j_pool = Neo4jAsyncPool(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)
        async with neo4j_pool.session() as session:
            # Task registry: (env_key, label, thunk → awaitable).
            TASKS: list[tuple[str, str, callable]] = [
                # Schema/constraints first.
                ("constraints", "create_constraints", lambda: create_constraints(session)),

                # Static/domain nodes and station topology.
                ("fields", "upsert_fields", lambda: upsert_fields(session, fields)),
                ("stations", "upsert_stations_and_depths",
                    lambda: upsert_stations_and_depths(session, stations_by_field, pg_pool)),

                # Time-windowed sensor/day aggregations.
                ("soil_layers", "upsert_soil_layer_readings",
                    lambda: upsert_soil_layer_readings(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                        write_unknown_depth_as_minus1=True,
                    )),
                ("weather_days", "upsert_weather_days",
                    lambda: upsert_weather_days(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),
                ("soil_days", "upsert_soil_days",
                    lambda: upsert_soil_days(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),
                ("weather_forecast", "upsert_weather_forecast",
                    lambda: upsert_weather_forecast(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                    )),
                ("irrigation_days", "upsert_irrigation_days",
                    lambda: upsert_irrigation_days(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),
                ("canopy_days", "upsert_canopy_days",
                    lambda: upsert_canopy_days(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),
                ("sap_days", "upsert_sap_days",
                    lambda: upsert_sap_days(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),
                ("irrigation_events", "upsert_irrigation_events",
                    lambda: upsert_irrigation_events(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),
                ("et0_days", "upsert_et0_days",
                    lambda: upsert_et0_days(
                        session=session,
                        stations_by_field=stations_by_field,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                        default_timezone=default_tz,
                        timezone_by_field=timezone_by_field,
                    )),

                # Lab/analysis and reference ranges.
                ("sap_analyses", "upsert_sap_analyses",
                    lambda: upsert_sap_analyses(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        completed_at=end_at,
                    )),
                ("sap_element_results", "upsert_sap_element_results",
                    lambda: upsert_sap_element_results(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        completed_at=end_at,
                )),
                ("optimum_sap_ranges", "upsert_optimum_sap_ranges",
                    lambda: upsert_optimum_sap_ranges(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        completed_at=end_at,
                    )),
                ("optimum_element_ranges", "upsert_optimum_element_ranges",
                    lambda: upsert_optimum_element_ranges(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        completed_at=end_at,
                )),
                ("haney_analyses", "upsert_haney_analyses",
                    lambda: upsert_haney_analyses(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                    )),
                ("tnd_analyses", "upsert_tnd_analyses",
                    lambda: upsert_tnd_analyses(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("soil_analyses", "upsert_soil_analyses",
                    lambda: upsert_soil_analyses(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("soil_param_results", "upsert_soil_param_results",
                    lambda: upsert_soil_param_results(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("water_analyses", "upsert_water_analyses",
                    lambda: upsert_water_analyses(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("water_param_results", "upsert_water_param_results",
                    lambda: upsert_water_param_results(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("application_events", "upsert_application_events",
                    lambda: upsert_application_events(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("product_applications", "upsert_product_applications",
                    lambda: upsert_product_applications(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("fertilizer_products", "upsert_fertilizer_products",
                    lambda: upsert_fertilizer_products(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
                ("app_nutrient_contents", "upsert_app_nutrient_contents",
                    lambda: upsert_app_nutrient_contents(
                        session=session,
                        fields=field_list,
                        pg_pool=pg_pool,
                        start_at=start_at,
                        end_at=end_at,
                )),
            ]

            # Gate by env + --only/--skip and time each executed task.
            for key, label, fn in TASKS:
                env_ok = _env_enabled(key, default=True)
                only_ok = (not only) or (key in only)
                skip_ok = (key not in skip)
                if env_ok and only_ok and skip_ok:
                    await measure_async(label, fn())
                else:
                    print(f"[SKIP] {label} (key={key}, env={env_ok}, only_ok={only_ok}, skip_ok={skip_ok})")

        # Close Neo4j pool after session completes.
        await measure_async("neo4j_pool.close", neo4j_pool.close())
    finally:
        # Always close Postgres pool.
        await measure_async("pg_pool.close", pg_pool.close())


def main():
    # Parse args, build selection sets, convert date strings, run integration.
    args = _parse_args()
    t0 = perf_counter()

    # Normalize comma-separated lists to sets.
    only = set([x.strip() for x in args.only.split(",") if x.strip()]) if args.only else set()
    skip = set([x.strip() for x in args.skip.split(",") if x.strip()]) if args.skip else set()

    # Convert ISO strings to naive datetimes (timezone handled downstream).
    start_at = datetime.fromisoformat(args.start)
    end_at = datetime.fromisoformat(args.end)

    # Execute the async pipeline.
    asyncio.run(run_integration(
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        start_at=start_at,
        end_at=end_at,
        only=only,
        skip=skip,
    ))

    # Print total elapsed time and finish marker.
    total = perf_counter() - t0
    print(f"[TIMER] TOTAL: {total:.2f}s")
    print("Finished integration")


if __name__ == "__main__":
    # Script entry guard.
    main()
