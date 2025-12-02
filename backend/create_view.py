#!/usr/bin/env python3
"""
Recreate ALL materialized views and key indexes for NYC Taxi project:

Materialized Views:
1. trip_analytics_summary   – for Trip Analytics Dashboard (Feature 1)
2. trip_zone_density        – for Interactive Map / Zone flows (Feature 2)
3. peak_hours               – for Peak Hours analysis (Feature 3)
4. vendor_performance       – for Vendor stats (Feature 4)

Indexes:
- On base table "trips" to speed up the main query patterns
- On the materialized views to make dashboard queries fast

Safe to run AFTER data is loaded into nyc_taxi.<schema>.trips.
Does NOT reload data or drop base tables.
"""

import os
import getpass
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# --- DB config (aligned with setup_and_load.py) ---
PGUSER = os.environ.get("PGUSER") or getpass.getuser()
PGPASSWORD = os.environ.get("PGPASSWORD", "")
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "nyc_taxi")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "public")


def make_url(db_name: str) -> str:
    """Build a SQLAlchemy connection URL."""
    if PGPASSWORD:
        return f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{db_name}"
    return f"postgresql+psycopg2://{PGUSER}@{PGHOST}:{PGPORT}/{db_name}"


def recreate_materialized_views_and_indexes():
    engine = create_engine(make_url(DB_NAME))

    with engine.begin() as conn:
        # Make sure schema search_path is correct
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))

        # 0) Base table indexes on trips (now that data is loaded)

        print("🧱 Creating/ensuring core indexes on trips...")

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_weekday_hour
            ON "{SCHEMA_NAME}".trips (pickup_weekday, pickup_hour);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_pickup_zone
            ON "{SCHEMA_NAME}".trips (pickup_zone_id);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_dropoff_zone
            ON "{SCHEMA_NAME}".trips (dropoff_zone_id);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_pickup_time
            ON "{SCHEMA_NAME}".trips (pickup_time);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_payment
            ON "{SCHEMA_NAME}".trips (payment_id);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_vendor
            ON "{SCHEMA_NAME}".trips (vendor_id);
        """))

        print("✅ Base indexes on trips ready.\n")

        # 1) Trip Analytics Dashboard – trip_analytics_summary
        print("🔁 Recreating materialized view: trip_analytics_summary...")
        conn.execute(text(f"""
            DROP MATERIALIZED VIEW IF EXISTS "{SCHEMA_NAME}".trip_analytics_summary CASCADE;
        """))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW "{SCHEMA_NAME}".trip_analytics_summary AS
            SELECT
                z.borough,
                t.pickup_weekday AS weekday,
                t.pickup_hour    AS hour,
                ROUND(AVG(t.fare)::numeric, 2)              AS avg_fare,
                ROUND(AVG(t.distance)::numeric, 2)          AS avg_distance,
                ROUND(AVG(t.trip_duration_min)::numeric, 2) AS avg_duration_min
            FROM "{SCHEMA_NAME}".trips t
            LEFT JOIN "{SCHEMA_NAME}".zones z
                ON t.pickup_zone_id = z.zone_id
            GROUP BY z.borough, t.pickup_weekday, t.pickup_hour
            ORDER BY z.borough NULLS LAST, t.pickup_weekday, t.pickup_hour;
        """))

        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_analytics_summary
            ON "{SCHEMA_NAME}".trip_analytics_summary (borough, weekday, hour);
        """))
        print("✅ trip_analytics_summary ready.\n")

        # 2) Interactive Map View – trip_zone_density
        print("🔁 Recreating materialized view: trip_zone_density...")
        conn.execute(text(f"""
            DROP MATERIALIZED VIEW IF EXISTS "{SCHEMA_NAME}".trip_zone_density CASCADE;
        """))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW "{SCHEMA_NAME}".trip_zone_density AS
            SELECT
                pickup_zone_id,
                COUNT(*) AS pickup_count,
                dropoff_zone_id,
                COUNT(*) FILTER (WHERE dropoff_zone_id IS NOT NULL) AS dropoff_count
            FROM "{SCHEMA_NAME}".trips
            GROUP BY pickup_zone_id, dropoff_zone_id;
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trip_zone_density_pickup
            ON "{SCHEMA_NAME}".trip_zone_density (pickup_zone_id);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trip_zone_density_pair
            ON "{SCHEMA_NAME}".trip_zone_density (pickup_zone_id, dropoff_zone_id);
        """))
        print("✅ trip_zone_density ready.\n")

        # 3) Peak Hours – peak_hours
        print("🔁 Recreating materialized view: peak_hours...")
        conn.execute(text(f"""
            DROP MATERIALIZED VIEW IF EXISTS "{SCHEMA_NAME}".peak_hours CASCADE;
        """))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW "{SCHEMA_NAME}".peak_hours AS
            SELECT
                pickup_weekday AS weekday,
                pickup_hour    AS hour,
                COUNT(*)       AS trip_count,
                RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
            FROM "{SCHEMA_NAME}".trips
            GROUP BY pickup_weekday, pickup_hour;
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_peak_hours_rank
            ON "{SCHEMA_NAME}".peak_hours (rank, weekday, hour);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_peak_hours_wh
            ON "{SCHEMA_NAME}".peak_hours (weekday, hour);
        """))
        print("✅ peak_hours ready.\n")

        # 4) Vendor Performance – vendor_performance
        print("🔁 Recreating materialized view: vendor_performance...")
        conn.execute(text(f"""
            DROP MATERIALIZED VIEW IF EXISTS "{SCHEMA_NAME}".vendor_performance CASCADE;
        """))

        conn.execute(text(f"""
            CREATE MATERIALIZED VIEW "{SCHEMA_NAME}".vendor_performance AS
            SELECT
                v.name AS vendor,
                COUNT(*)                                AS total_trips,
                ROUND(AVG(t.fare)::numeric, 2)         AS avg_fare,
                ROUND(AVG(t.total_amount)::numeric, 2) AS avg_earning
            FROM "{SCHEMA_NAME}".trips t
            JOIN "{SCHEMA_NAME}".vendors v
              ON t.vendor_id = v.vendor_id
            GROUP BY v.name
            ORDER BY avg_earning DESC;
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_vendor_performance
            ON "{SCHEMA_NAME}".vendor_performance (vendor, total_trips);
        """))

        print("✅ vendor_performance ready.\n")

    engine.dispose()


if __name__ == "__main__":
    print(f"🔌 Connecting to DB '{DB_NAME}' as '{PGUSER}' to recreate materialized views and indexes...")
    recreate_materialized_views_and_indexes()
    print("🎉 All materialized views + indexes created successfully.")
