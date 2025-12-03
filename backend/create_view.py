#!/usr/bin/env python3
"""
Create only the materialized views required for the Analytics Dashboard.
"""

import os
import getpass
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PGUSER = os.environ.get("PGUSER") or getpass.getuser()
PGPASSWORD = os.environ.get("PGPASSWORD", "")
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "nyc_taxi")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "public")


def make_url(db):
    if PGPASSWORD:
        return f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{db}"
    return f"postgresql+psycopg2://{PGUSER}@{PGHOST}:{PGPORT}/{db}"


def recreate_analytics_mvs():
    engine = create_engine(make_url(DB_NAME))
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))

        # 1. Global KPIs
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics_kpis CASCADE;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW analytics_kpis AS
            SELECT
                COUNT(*)::bigint                             AS total_trips,
                ROUND(SUM(total_amount)::numeric, 2)         AS total_revenue,
                ROUND(AVG(fare)::numeric, 2)                 AS avg_fare,
                ROUND(AVG(distance)::numeric, 2)             AS avg_distance,
                ROUND(AVG(trip_duration_min)::numeric, 2)    AS avg_duration_min,
                MIN(pickup_time)                             AS min_pickup_time,
                MAX(pickup_time)                             AS max_pickup_time,
                COUNT(DISTINCT pickup_zone_id)               AS active_pickup_zones,
                COUNT(DISTINCT dropoff_zone_id)              AS active_dropoff_zones
            FROM trips;
        """))

        # 2. Payment mix
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics_payment_mix CASCADE;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW analytics_payment_mix AS
            SELECT
                p.payment_type,
                COUNT(*)::bigint AS trip_count
            FROM trips t
            LEFT JOIN payments p ON t.payment_id = p.payment_id
            GROUP BY p.payment_type
            ORDER BY trip_count DESC;
        """))

        # 3. Trips by borough
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics_trips_by_borough CASCADE;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW analytics_trips_by_borough AS
            SELECT
                z.borough,
                COUNT(*)::bigint AS trip_count
            FROM trips t
            LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
            GROUP BY z.borough
            ORDER BY trip_count DESC;
        """))

        # 4. Trips by weekday
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics_trips_by_weekday CASCADE;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW analytics_trips_by_weekday AS
            SELECT
                pickup_weekday AS weekday,
                COUNT(*)::bigint AS trip_count
            FROM trips
            GROUP BY pickup_weekday
            ORDER BY weekday;
        """))

        # 5. Trips by hour
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics_trips_by_hour CASCADE;"))
        conn.execute(text("""
            CREATE MATERIALIZED VIEW analytics_trips_by_hour AS
            SELECT
                pickup_hour AS hour,
                COUNT(*)::bigint AS trip_count
            FROM trips
            GROUP BY pickup_hour
            ORDER BY hour;
        """))

    engine.dispose()


if __name__ == "__main__":
    print("🔌 Creating Analytics Materialized Views...")
    recreate_analytics_mvs()
    print("✅ Analytics MVs created successfully.")

