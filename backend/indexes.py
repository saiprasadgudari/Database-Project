#!/usr/bin/env python3
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


def make_url(db_name: str) -> str:
    if PGPASSWORD:
        return f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{db_name}"
    return f"postgresql+psycopg2://{PGUSER}@{PGHOST}:{PGPORT}/{db_name}"


def create_indexes():
    engine = create_engine(make_url(DB_NAME))

    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))

        # =========================
        # Base table: trips
        # =========================
        print("🧱 Creating core indexes on trips...")

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_weekday_hour
            ON "{SCHEMA_NAME}".trips (pickup_weekday, pickup_hour);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_pickup_time
            ON "{SCHEMA_NAME}".trips (pickup_time);
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
            CREATE INDEX IF NOT EXISTS idx_trips_payment
            ON "{SCHEMA_NAME}".trips (payment_id);
        """))

        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_vendor
            ON "{SCHEMA_NAME}".trips (vendor_id);
        """))

        # Optional but nice for zone flows (pickup → dropoff combos)
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_trips_zone_pair
            ON "{SCHEMA_NAME}".trips (pickup_zone_id, dropoff_zone_id);
        """))

        print("✅ Base indexes on trips created.\n")

        # =========================
        # Analytics MVs
        # =========================
        print("📊 Creating indexes on analytics materialized views...")

        # analytics_payment_mix(payment_type)
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_analytics_payment_mix_payment_type
            ON "{SCHEMA_NAME}".analytics_payment_mix (payment_type);
        """))

        # analytics_trips_by_borough(borough)
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_analytics_trips_by_borough_borough
            ON "{SCHEMA_NAME}".analytics_trips_by_borough (borough);
        """))

        # analytics_trips_by_weekday(weekday)
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_analytics_trips_by_weekday_weekday
            ON "{SCHEMA_NAME}".analytics_trips_by_weekday (weekday);
        """))

        # analytics_trips_by_hour(hour)
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_analytics_trips_by_hour_hour
            ON "{SCHEMA_NAME}".analytics_trips_by_hour (hour);
        """))

        print("✅ Analytics MV indexes created.\n")

    engine.dispose()


if __name__ == "__main__":
    print(f"🔌 Connecting to DB '{DB_NAME}' as '{PGUSER}' to create indexes...")
    create_indexes()
    print("🎉 All indexes created successfully.")
