#!/usr/bin/env python3
import os
import sys
import getpass
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Config
PGUSER = os.environ.get("PGUSER") or getpass.getuser()
PGPASSWORD = os.environ.get("PGPASSWORD") 
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = os.environ.get("PGPORT", "5432")

DB_NAME = os.environ.get("DB_NAME", "nyc_taxi")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "public")

# NEW — ONLY use these Parquet files (no CSV fallback)
PARQUET_FILES = [
    "new_data/yellow_tripdata_2025-01.parquet",
    "new_data/yellow_tripdata_2025-02.parquet",
    "new_data/yellow_tripdata_2025-03.parquet",
    "new_data/yellow_tripdata_2025-04.parquet",
    "new_data/yellow_tripdata_2025-05.parquet",
    "new_data/yellow_tripdata_2025-06.parquet",
    "new_data/yellow_tripdata_2025-07.parquet",
    "new_data/yellow_tripdata_2025-08.parquet",



]

DROP_OLD_TRIPS = True


def make_url(db_name: str) -> str:
    """Build SQLAlchemy URL."""
    if PGPASSWORD:
        return f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{db_name}"
    return f"postgresql+psycopg2://{PGUSER}@{PGHOST}:{PGPORT}/{db_name}"


# DB & Schema Setup
def ensure_database_exists():
    admin_engine = create_engine(make_url("postgres"), isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
            {"dbname": DB_NAME},
        ).scalar()
        if not exists:
            print(f"🆕 Creating database '{DB_NAME}'...")
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
        else:
            print(f"✅ Database '{DB_NAME}' already exists.")
    admin_engine.dispose()


def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))
    print(f"✅ Schema '{SCHEMA_NAME}' ready.")


def create_reference_tables(engine):
    """Vendors, Payments, Zones lookup."""
    with engine.begin() as conn:
        conn.execute(text(f'SET search_path TO "{SCHEMA_NAME}"'))

        # Vendors
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

        # Payments
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

        # Zones
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS zones (
                zone_id INT PRIMARY KEY,
                borough TEXT,
                zone_name TEXT,
                service_zone TEXT
            );
        """))

    print("✅ Reference tables ready (vendors, payments, zones).")


def load_zones_lookup(engine):
    """Load taxi_zone_lookup.csv."""
    candidates = [
        os.environ.get("ZONES_CSV_PATH"),
        "taxi_zone_lookup.csv",
    ]
    zones_path = next((p for p in candidates if p and os.path.exists(p)), None)

    if not zones_path:
        print("⚠️ No taxi_zone_lookup.csv found. Skipping zone load.")
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
        conn.execute(text(f'TRUNCATE TABLE "{SCHEMA_NAME}".zones'))

    df.to_sql("zones", engine, schema=SCHEMA_NAME, if_exists="append", index=False)
    print(f"✅ Loaded {len(df)} zones.")


def drop_old_trips_if_any(engine):
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:tbl) IS NOT NULL"),
            {"tbl": f"{SCHEMA_NAME}.trips"}
        ).scalar()
        if exists:
            print("♻️ Dropping old trips table...")
            conn.execute(text(f'DROP TABLE "{SCHEMA_NAME}".trips CASCADE'))
        else:
            print("ℹ️ No existing trips table to drop.")


def create_trips_table(engine):
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
      pickup_zone_id INT REFERENCES "{SCHEMA_NAME}".zones(zone_id),
      dropoff_zone_id INT REFERENCES "{SCHEMA_NAME}".zones(zone_id),
      vendor_id VARCHAR(10) REFERENCES "{SCHEMA_NAME}".vendors(vendor_id),
      payment_id INT REFERENCES "{SCHEMA_NAME}".payments(payment_id),
      pickup_long FLOAT,
      pickup_lat FLOAT,
      dropoff_long FLOAT,
      dropoff_lat FLOAT,
      pickup_weekday INT GENERATED ALWAYS AS (EXTRACT(DOW FROM pickup_time)) STORED,
      pickup_hour INT GENERATED ALWAYS AS (EXTRACT(HOUR FROM pickup_time)) STORED,
      trip_duration_min FLOAT GENERATED ALWAYS AS (EXTRACT(EPOCH FROM dropoff_time - pickup_time)/60) STORED,
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

    with engine.begin() as conn:
        conn.execute(text(ddl))

    print("✅ trips table created (no indexes yet).")


# =========================
# Data Processing & Load
# =========================

CSV_TO_DB_RENAME = {
    "tpep_pickup_datetime": "pickup_time",
    "tpep_dropoff_datetime": "dropoff_time",
    "trip_distance": "distance",
    "fare_amount": "fare",
    "total_amount": "total_amount",
    "passenger_count": "passenger_count",
    "pulocationid": "pickup_zone_id",
    "dolocationid": "dropoff_zone_id",
    "payment_type": "payment_type_raw",
    "vendorid": "vendor_raw",
    "ratecodeid": "ratecodeid",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
    "pickup_longitude": "pickup_long",
    "pickup_latitude": "pickup_lat",
    "dropoff_longitude": "dropoff_long",
    "dropoff_latitude": "dropoff_lat",
}

TARGET_COLS = [
    "pickup_time", "dropoff_time", "distance", "fare", "tip_amount", "total_amount",
    "passenger_count",
    "pickup_zone_id", "dropoff_zone_id",
    "vendor_id", "payment_id",
    "pickup_long", "pickup_lat", "dropoff_long", "dropoff_lat",
    "ratecodeid", "store_and_fwd_flag", "extra", "mta_tax", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
]

VENDOR_MAP = {1: "CMT", 2: "VTS"}
VALID_PAYMENTS = {1, 2, 3, 4, 5, 6}


def coerce_types(df):
    # Datetimes
    for col in ("pickup_time", "dropoff_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Floats
    for col in [
        "distance", "fare", "tip_amount", "total_amount",
        "pickup_long", "pickup_lat", "dropoff_long", "dropoff_lat",
        "extra", "mta_tax", "tolls_amount", "improvement_surcharge",
        "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integers
    for col in ["passenger_count", "ratecodeid", "pickup_zone_id", "dropoff_zone_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Vendor
    if "vendor_raw" in df.columns:
        df["vendor_id"] = df["vendor_raw"].map(VENDOR_MAP).astype("string")

    # Payment
    if "payment_type_raw" in df.columns:
        df["payment_id"] = (
            pd.to_numeric(df["payment_type_raw"], errors="coerce")
            .fillna(5)
            .astype(int)
        )
        df.loc[~df["payment_id"].isin(VALID_PAYMENTS), "payment_id"] = 5

    return df


def insert_parquet(df, engine, file_index, start_time, total_rows):
    chunk_size = 200_000
    num_chunks = (len(df) // chunk_size) + 1

    print(f"   → Splitting into {num_chunks} chunks (size={chunk_size})")

    for chunk_idx in range(num_chunks):
        start_row = chunk_idx * chunk_size
        end_row = start_row + chunk_size
        chunk = df.iloc[start_row:end_row]

        if chunk.empty:
            continue

        # normalize column names
        chunk.columns = [c.lower().strip().replace(" ", "_") for c in chunk.columns]
        rename_map = {src: dst for src, dst in CSV_TO_DB_RENAME.items() if src in chunk.columns}
        chunk = chunk.rename(columns=rename_map)

        # type conversions
        chunk = coerce_types(chunk)

        # ensure all expected columns exist
        for col in TARGET_COLS:
            if col not in chunk.columns:
                chunk[col] = pd.NA

        # remove rows missing required fields
        out = chunk[TARGET_COLS].dropna(
            subset=["pickup_time", "dropoff_time", "fare"],
            how="any"
        )

        if out.empty:
            print(f"      ⚠️ Chunk {chunk_idx+1}/{num_chunks} → 0 valid rows (skipped)")
            continue

        out.to_sql(
            "trips",
            engine,
            schema=SCHEMA_NAME,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=50_000,
        )

        # update running total
        total_rows += len(out)

        print(
            f"      ✅ File {file_index} — chunk {chunk_idx+1}/{num_chunks} "
            f"inserted ({len(out):,} rows). Total={total_rows:,}. "
            f"Elapsed={time.time()-start_time:.1f}s"
        )

    return total_rows



def load_data(engine):
    total_rows = 0
    start = time.time()

    for i, path in enumerate(PARQUET_FILES, start=1):
        if not os.path.exists(path):
            print(f"❌ Missing file: {path}")
            continue

        print(f"📂 Reading: {path}")
        df = pd.read_parquet(path)
        total_rows = insert_parquet(df, engine, i, start, total_rows)

    print(f"🎉 Finished — inserted {total_rows:,} rows in {time.time()-start:.1f}s")


def vacuum_analyze(engine):
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            text("VACUUM ANALYZE trips;")
        )
    print("🧹 VACUUM ANALYZE done.")


# Main

def main():
    print(f"🔌 Connecting to PostgreSQL on {PGHOST}:{PGPORT} as {PGUSER}")
    ensure_database_exists()

    engine = create_engine(make_url(DB_NAME))

    ensure_schema(engine)
    create_reference_tables(engine)

    if DROP_OLD_TRIPS:
        drop_old_trips_if_any(engine)

    load_zones_lookup(engine)
    create_trips_table(engine)
    load_data(engine)
    vacuum_analyze(engine)

    engine.dispose()
    print("✅ Load Complete — Ready for FastAPI & Materialized Views!")


if __name__ == "__main__":
    main()
