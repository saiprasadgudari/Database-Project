#!/usr/bin/env python3
import os
import sys
import getpass
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# =========================
# Config (override via env)
# =========================
PGUSER = os.environ.get("PGUSER") or getpass.getuser()
PGPASSWORD = os.environ.get("PGPASSWORD")  # optional on macOS local trust
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")

DB_NAME = os.environ.get("DB_NAME", "nyc_taxi")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "public")

# Path to your TLC dataset (CSV or Parquet)
# For your first-run schema: set DATA_PATH to that file.
DATA_PATH = os.environ.get("DATA_PATH", os.environ.get("CSV_PATH", "data/yellow_tripdata_2023-01.csv"))

# If True: drop any old 'trips' table before recreating
DROP_OLD_TRIPS = True


def make_url(db_name: str) -> str:
    """
    Build a SQLAlchemy connection URL:
    postgresql+psycopg2://user[:password]@host:port/dbname
    """
    if PGPASSWORD:
        return f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{db_name}"
    return f"postgresql+psycopg2://{PGUSER}@{PGHOST}:{PGPORT}/{db_name}"


# =========================
# DB / Schema management
# =========================
def ensure_database_exists():
    admin_engine = create_engine(make_url("postgres"), isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
            {"dbname": DB_NAME},
        ).scalar() is not None
        if not exists:
            print(f"🆕 Creating database '{DB_NAME}'...")
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
        else:
            print(f"✅ Database '{DB_NAME}' already exists.")
    admin_engine.dispose()


def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))
    print(f"✅ Schema '{SCHEMA_NAME}' is ready.")


def create_reference_tables(engine):
    """
    Create vendors, payments, zones lookup tables and seed them.
    """
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))

        # Vendors table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS vendors (
                vendor_id VARCHAR(10) PRIMARY KEY,
                name TEXT NOT NULL
            );
        """))
        conn.execute(text("""
            INSERT INTO vendors (vendor_id, name) VALUES
                ('CMT','Creative Mobile Technologies'),
                ('VTS','VeriFone Transportation Systems')
            ON CONFLICT DO NOTHING;
        """))

        # Payments table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id INT PRIMARY KEY,
                payment_type VARCHAR(20) UNIQUE NOT NULL,
                description TEXT
            );
        """))
        conn.execute(text("""
            INSERT INTO payments (payment_id, payment_type, description) VALUES
                (1, 'CRD', 'Credit Card'),
                (2, 'CSH', 'Cash'),
                (3, 'NOC', 'No Charge'),
                (4, 'DIS', 'Dispute'),
                (5, 'UNK', 'Unknown'),
                (6, 'VOD', 'Voided Trip')
            ON CONFLICT DO NOTHING;
        """))

        # Zones table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS zones (
                zone_id INT PRIMARY KEY,
                borough TEXT,
                zone_name TEXT,
                service_zone TEXT
            );
        """))

    print("✅ Reference tables (vendors, payments, zones) are ready and seeded.")


def load_zones_lookup(engine):
    """
    Load TLC taxi_zone_lookup.csv into zones table if file is available.
    """
    candidates = [
        os.environ.get("ZONES_CSV_PATH"),
        "taxi_zone_lookup.csv",
        "taxi_zone_lookup (1).csv",
    ]
    zones_path = next((p for p in candidates if p and os.path.exists(p)), None)

    if not zones_path:
        print("⚠️ No taxi_zone_lookup CSV found; zones table left empty.")
        return

    print(f"📂 Loading zones from: {zones_path}")
    df = pd.read_csv(zones_path)
    df = df.rename(columns={
        "LocationID": "zone_id",
        "Borough": "borough",
        "Zone": "zone_name",
        "service_zone": "service_zone",
    })[["zone_id", "borough", "zone_name", "service_zone"]]

    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))
        conn.execute(text('TRUNCATE TABLE "zones";'))

    df.to_sql(
        "zones",
        engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"✅ Loaded {len(df)} rows into zones table.")


def drop_old_trips_if_any(engine):
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))
        exists = conn.execute(
            text("SELECT to_regclass(:tbl) IS NOT NULL"),
            {"tbl": f'{SCHEMA_NAME}.trips'}
        ).scalar()
        if exists:
            print("♻️ Dropping old table 'trips' (clean start)...")
            conn.execute(text(f'DROP TABLE "{SCHEMA_NAME}"."trips" CASCADE'))
        else:
            print("ℹ️ No existing 'trips' table found; nothing to drop.")


def create_trips_table(engine):
    """
    Trips table tuned for modern TLC yellow schema (PULocationID/DOLocationID + surcharges),
    but still compatible with older lat/long-based files.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."trips" (
      trip_id SERIAL PRIMARY KEY,
      pickup_time TIMESTAMP NOT NULL,
      dropoff_time TIMESTAMP NOT NULL,
      distance FLOAT,
      fare FLOAT,
      tip_amount FLOAT,
      total_amount FLOAT,
      passenger_count INT,

      -- Zone-based IDs (from PULocationID/DOLocationID)
      pickup_zone_id INT REFERENCES "{SCHEMA_NAME}".zones(zone_id),
      dropoff_zone_id INT REFERENCES "{SCHEMA_NAME}".zones(zone_id),

      -- Normalized references
      vendor_id VARCHAR(10) REFERENCES "{SCHEMA_NAME}".vendors(vendor_id),
      payment_id INT REFERENCES "{SCHEMA_NAME}".payments(payment_id),

      -- Raw coordinates (if using older schema)
      pickup_long FLOAT,
      pickup_lat FLOAT,
      dropoff_long FLOAT,
      dropoff_lat FLOAT,

      -- Generated columns for speed
      pickup_weekday INT GENERATED ALWAYS AS (EXTRACT(DOW FROM pickup_time)) STORED,
      pickup_hour    INT GENERATED ALWAYS AS (EXTRACT(HOUR FROM pickup_time)) STORED,
      trip_duration_min FLOAT GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 60) STORED,

      -- TLC extras
      ratecodeid INT NULL,
      store_and_fwd_flag VARCHAR(1) NULL,
      extra FLOAT NULL,
      mta_tax FLOAT NULL,
      tolls_amount FLOAT NULL,
      improvement_surcharge FLOAT NULL,
      congestion_surcharge FLOAT NULL,
      airport_fee FLOAT NULL,
      cbd_congestion_fee FLOAT NULL
    );
    """

    idx = [
        f'CREATE INDEX IF NOT EXISTS "idx_trips_weekday_hour" '
        f'ON "{SCHEMA_NAME}"."trips"(pickup_weekday, pickup_hour);',

        f'CREATE INDEX IF NOT EXISTS "idx_trips_zones" '
        f'ON "{SCHEMA_NAME}"."trips"(pickup_zone_id, dropoff_zone_id);',

        f'CREATE INDEX IF NOT EXISTS "idx_trips_pickup_coords" '
        f'ON "{SCHEMA_NAME}"."trips"(pickup_long, pickup_lat);',

        f'CREATE INDEX IF NOT EXISTS "idx_trips_dropoff_coords" '
        f'ON "{SCHEMA_NAME}"."trips"(dropoff_long, dropoff_lat);',

        f'CREATE INDEX IF NOT EXISTS "idx_trips_payment" '
        f'ON "{SCHEMA_NAME}"."trips"(payment_id);',

        f'CREATE INDEX IF NOT EXISTS "idx_trips_pickup_time" '
        f'ON "{SCHEMA_NAME}"."trips"(pickup_time);',

        f'CREATE INDEX IF NOT EXISTS "idx_trips_vendor" '
        f'ON "{SCHEMA_NAME}"."trips"(vendor_id);',
    ]

    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))
        conn.execute(text(ddl))
        for stmt in idx:
            conn.execute(text(stmt))
    print(f"✅ Table '{SCHEMA_NAME}.trips' and feature-aligned indexes are ready.")


# =========================
# Input -> DB mapping
# =========================

CSV_TO_DB_RENAME = {
    # Modern yellow schema (PULocationID / DOLocationID)
    "tpep_pickup_datetime":  "pickup_time",
    "tpep_dropoff_datetime": "dropoff_time",
    "trip_distance":         "distance",
    "fare_amount":           "fare",
    "total_amount":          "total_amount",
    "passenger_count":       "passenger_count",
    "pulocationid":          "pickup_zone_id",
    "dolocationid":          "dropoff_zone_id",
    "payment_type":          "payment_type_raw",
    "vendorid":              "vendor_raw",
    "ratecodeid":            "ratecodeid",
    "store_and_fwd_flag":    "store_and_fwd_flag",
    "extra":                 "extra",
    "mta_tax":               "mta_tax",
    "tip_amount":            "tip_amount",
    "tolls_amount":          "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "congestion_surcharge":  "congestion_surcharge",
    "airport_fee":           "airport_fee",
    "cbd_congestion_fee":    "cbd_congestion_fee",

    # Older lat/long schema (still supported)
    "pickup_longitude":      "pickup_long",
    "pickup_latitude":       "pickup_lat",
    "dropoff_longitude":     "dropoff_long",
    "dropoff_latitude":      "dropoff_lat",
}

TARGET_COLS_FOR_INSERT = [
    "pickup_time", "dropoff_time", "distance", "fare", "tip_amount", "total_amount",
    "passenger_count",
    "pickup_zone_id", "dropoff_zone_id",
    "vendor_id", "payment_id",
    "pickup_long", "pickup_lat", "dropoff_long", "dropoff_lat",
    "ratecodeid", "store_and_fwd_flag", "extra", "mta_tax", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
]

VENDOR_MAP = {1: "CMT", 2: "VTS"}
PAYMENT_ID_VALID = {1, 2, 3, 4, 5, 6}


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # datetimes
    for dt in ("pickup_time", "dropoff_time"):
        if dt in df.columns:
            df[dt] = pd.to_datetime(df[dt], errors="coerce")

    # floats
    float_cols = [
        "distance", "fare", "tip_amount", "total_amount",
        "pickup_long", "pickup_lat", "dropoff_long", "dropoff_lat",
        "extra", "mta_tax", "tolls_amount", "improvement_surcharge",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
    ]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # ints
    int_cols = ["passenger_count", "ratecodeid", "pickup_zone_id", "dropoff_zone_id"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Vendor mapping
    if "vendor_raw" in df.columns:
        df["vendor_id"] = df["vendor_raw"].map(VENDOR_MAP).astype("string")

    # Payment mapping
    if "payment_type_raw" in df.columns:
        df["payment_id"] = (
            pd.to_numeric(df["payment_type_raw"], errors="coerce")
            .fillna(5)
            .astype(int)
        )
        df.loc[~df["payment_id"].isin(PAYMENT_ID_VALID), "payment_id"] = 5

    # Ensure zone cols exist
    if "pickup_zone_id" not in df.columns:
        df["pickup_zone_id"] = pd.NA
    if "dropoff_zone_id" not in df.columns:
        df["dropoff_zone_id"] = pd.NA

    return df


def insert_chunk(chunk: pd.DataFrame, engine, i: int, start_all: float, total_rows_so_far: int = 0) -> int:
    # normalize column names
    chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
    chunk.columns = [c.split("_m")[0] if "_m" in c else c for c in chunk.columns]
    chunk = chunk.loc[:, ~chunk.columns.duplicated()]

    # rename to canonical
    rename_pairs = {src: dst for src, dst in CSV_TO_DB_RENAME.items() if src in chunk.columns}
    chunk = chunk.rename(columns=rename_pairs)

    # keep only columns we know how to use
    have = [c for c in set(CSV_TO_DB_RENAME.values()) if c in chunk.columns]
    chunk = chunk[have].copy()

    # type + mapping
    chunk = coerce_types(chunk)

    # ensure all target columns exist
    for col in TARGET_COLS_FOR_INSERT:
        if col not in chunk.columns:
            chunk[col] = pd.NA

    out = chunk[TARGET_COLS_FOR_INSERT].dropna(
        subset=["pickup_time", "dropoff_time", "fare"],
        how="any",
    )

    if out.empty:
        print(f"⚠️ Chunk {i} produced 0 valid rows; skipping.")
        return total_rows_so_far

    out.to_sql(
        "trips",
        engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=50_000,
    )

    new_total = total_rows_so_far + len(out)
    print(f"✅ Inserted chunk {i:,} — cumulative rows: {new_total:,} (elapsed {time.time()-start_all:.1f}s)")
    return new_total


def load_data_in_chunks(engine):
    if not os.path.exists(DATA_PATH):
        print(f"❌ Data file not found: {DATA_PATH}")
        sys.exit(1)

    print(f"📂 Reading data from: {DATA_PATH}")
    _, ext = os.path.splitext(DATA_PATH.lower())

    chunksize = 250_000
    total_rows = 0
    start_all = time.time()

    if ext == ".parquet":
        # For your parquet dataset (first run)
        df = pd.read_parquet(DATA_PATH)
        total_rows = insert_chunk(df, engine, 1, start_all)
    else:
        # CSV streaming
        for i, chunk in enumerate(pd.read_csv(
            DATA_PATH,
            low_memory=False,
            chunksize=chunksize,
            on_bad_lines="skip",
            header=0,
        )):
            total_rows = insert_chunk(chunk, engine, i + 1, start_all, total_rows)

    print(f"🎉 Done. Inserted {total_rows:,} rows into {SCHEMA_NAME}.trips in {time.time()-start_all:.1f}s")


def create_materialized_views(engine):
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}", public'))

        # 1) Trip Analytics Dashboard
        conn.execute(text("""
            DROP MATERIALIZED VIEW IF EXISTS trip_analytics_summary CASCADE;
            CREATE MATERIALIZED VIEW trip_analytics_summary AS
            SELECT
                z.borough,
                t.pickup_weekday AS weekday,
                t.pickup_hour AS hour,
                AVG(t.fare) AS avg_fare,
                AVG(t.distance) AS avg_distance,
                AVG(t.trip_duration_min) AS avg_duration_min
            FROM trips t
            LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
            GROUP BY z.borough, t.pickup_weekday, t.pickup_hour
            ORDER BY z.borough NULLS LAST, t.pickup_weekday, t.pickup_hour;
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_analytics_summary
            ON trip_analytics_summary (borough, weekday, hour);
        """))

        # 2) Interactive Map View
        conn.execute(text("""
            DROP MATERIALIZED VIEW IF EXISTS trip_zone_density CASCADE;
            CREATE MATERIALIZED VIEW trip_zone_density AS
            SELECT
                pickup_zone_id,
                COUNT(*) AS pickup_count,
                dropoff_zone_id,
                COUNT(*) FILTER (WHERE dropoff_zone_id IS NOT NULL) AS dropoff_count
            FROM trips
            GROUP BY pickup_zone_id, dropoff_zone_id;
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_trip_zone_density_pickup
            ON trip_zone_density (pickup_zone_id);
        """))

        # 4) Peak Hour Detection
        conn.execute(text("""
            DROP MATERIALIZED VIEW IF EXISTS peak_hours CASCADE;
            CREATE MATERIALIZED VIEW peak_hours AS
            SELECT
                pickup_weekday AS weekday,
                pickup_hour AS hour,
                COUNT(*) AS trip_count,
                RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
            FROM trips
            GROUP BY pickup_weekday, pickup_hour;
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_peak_hours_rank
            ON peak_hours (rank, weekday, hour);
        """))

        # 5) Vendor Performance Comparison
        conn.execute(text("""
            DROP MATERIALIZED VIEW IF EXISTS vendor_performance CASCADE;
            CREATE MATERIALIZED VIEW vendor_performance AS
            SELECT
                v.name AS vendor,
                COUNT(*) AS total_trips,
                ROUND(AVG(fare),2) AS avg_fare,
                ROUND(AVG(total_amount),2) AS avg_earning
            FROM trips t
            JOIN vendors v ON t.vendor_id = v.vendor_id
            GROUP BY v.name
            ORDER BY avg_earning DESC;
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_vendor_performance
            ON vendor_performance (vendor, total_trips);
        """))

    print("✅ Materialized views created and indexed.")


def vacuum_analyze(engine):
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            text("VACUUM ANALYZE trips;")
        )
    print("🧹 VACUUM ANALYZE completed for trips.")


# =========================
# Main
# =========================
def main():
    print(f"🔌 Connecting as user='{PGUSER}' host='{PGHOST}' port='{PGPORT}'")
    ensure_database_exists()

    engine = create_engine(make_url(DB_NAME))

    ensure_schema(engine)
    create_reference_tables(engine)
    load_zones_lookup(engine)

    if DROP_OLD_TRIPS:
        drop_old_trips_if_any(engine)
    create_trips_table(engine)

    load_data_in_chunks(engine)
    vacuum_analyze(engine)
    create_materialized_views(engine)

    engine.dispose()
    print("✅ Database setup complete. Ready for FastAPI and your 5 features!")


if __name__ == "__main__":
    main()
