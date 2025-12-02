# NYC Taxi Trip Analytics Backend

Backend for an NYC Yellow Taxi analytics dashboard using PostgreSQL + Flask.

This project:

- Loads TLC yellow taxi trip data (Parquet) into PostgreSQL
- Normalizes the schema into lookup tables + a `trips` fact table
- Adds indexes optimized for common query patterns
- Creates several materialized views for fast analytics
- Exposes REST API endpoints for:
  - Trip analytics
  - Map / zone density
  - Fare & tip analysis
  - Peak hour detection
  - Vendor performance

---

## 1. Database Schema

All tables live in the schema defined by `SCHEMA_NAME` (default: `public`).

### 1.1 Core Tables

#### `vendors`

Lookup table for taxi vendors.

```sql
vendors (
  vendor_id VARCHAR(10) PRIMARY KEY,  -- 'CMT', 'VTS'
  name      TEXT NOT NULL             -- full vendor name
)
Loaded by create_reference_tables() in setup_and_load.py.

payments
Lookup table for payment types.

sql
Copy code
payments (
  payment_id   INT PRIMARY KEY,       -- 1..6
  payment_type VARCHAR(20) UNIQUE NOT NULL,  -- 'CRD', 'CSH', ...
  description  TEXT                   -- human-readable description
)
Mapping used in ETL:

1 → CRD (Credit Card)

2 → CSH (Cash)

3 → NOC (No Charge)

4 → DIS (Dispute)

5 → UNK (Unknown / fallback)

6 → VOD (Voided Trip)

zones
TLC taxi zone lookup table, populated from taxi_zone_lookup.csv.

sql
Copy code
zones (
  zone_id      INT PRIMARY KEY,   -- TLC LocationID
  borough      TEXT,
  zone_name    TEXT,
  service_zone TEXT
)
1.2 Fact Table: trips
Main table storing each completed trip.

sql
Copy code
trips (
  trip_id           SERIAL PRIMARY KEY,

  -- Core timestamps
  pickup_time       TIMESTAMP NOT NULL,
  dropoff_time      TIMESTAMP NOT NULL,

  -- Trip metrics
  distance          FLOAT,
  fare              FLOAT,
  tip_amount        FLOAT,
  total_amount      FLOAT,
  passenger_count   INT,

  -- Foreign keys into lookup tables
  pickup_zone_id    INT REFERENCES zones(zone_id),
  dropoff_zone_id   INT REFERENCES zones(zone_id),
  vendor_id         VARCHAR(10) REFERENCES vendors(vendor_id),
  payment_id        INT REFERENCES payments(payment_id),

  -- Coordinates
  pickup_long       FLOAT,
  pickup_lat        FLOAT,
  dropoff_long      FLOAT,
  dropoff_lat       FLOAT,

  -- Generated / derived columns
  pickup_weekday    INT GENERATED ALWAYS AS (
                       EXTRACT(DOW FROM pickup_time)
                     ) STORED,   -- 0=Sunday .. 6=Saturday
  pickup_hour       INT GENERATED ALWAYS AS (
                       EXTRACT(HOUR FROM pickup_time)
                     ) STORED,   -- 0..23
  trip_duration_min FLOAT GENERATED ALWAYS AS (
                       EXTRACT(EPOCH FROM dropoff_time - pickup_time) / 60
                     ) STORED,

  -- Extra fare-related fields (nullable)
  ratecodeid             INT NULL,
  store_and_fwd_flag     VARCHAR(1) NULL,
  extra                  FLOAT NULL,
  mta_tax                FLOAT NULL,
  tolls_amount           FLOAT NULL,
  improvement_surcharge  FLOAT NULL,
  congestion_surcharge   FLOAT NULL,
  airport_fee            FLOAT NULL,
  cbd_congestion_fee     FLOAT NULL
);
Columns are populated from the TLC Parquet file via CSV_TO_DB_RENAME and coerce_types() in setup_and_load.py.

1.3 Materialized Views
Created by create_view.py (and/or your analytics MV script):

trip_analytics_summary

Aggregates average fare, distance, and duration by (borough, weekday, hour).

Used for the trip analytics dashboard.

trip_zone_density

Aggregates pickups and dropoffs per zone pair (pickup_zone_id, dropoff_zone_id).

peak_hours

Aggregates trip counts by (pickup_weekday, pickup_hour) and ranks busiest times.

vendor_performance

Aggregates per-vendor stats: trip count, average fare, average total_amount.

Additionally, separate analytics materialized views may be used for the landing dashboard (e.g., analytics_kpis, analytics_payment_mix, analytics_trips_by_borough, analytics_trips_by_weekday, analytics_trips_by_hour), depending on how you wired analytics.py.

1.4 Indexes
Created in two places:

On base table trips (in create_view.py and/or indexes.py):

(pickup_weekday, pickup_hour) – for time-slice queries

pickup_time – for date range filters

pickup_zone_id, dropoff_zone_id, (pickup_zone_id, dropoff_zone_id) – for zone density

payment_id – for payment-type filters

vendor_id – for vendor-based filters

On materialized views (for fast lookups):

trip_analytics_summary (borough, weekday, hour)

trip_zone_density (pickup_zone_id) and (pickup_zone_id, dropoff_zone_id)

peak_hours (rank, weekday, hour) and (weekday, hour)

vendor_performance (vendor, total_trips)

Plus extra single-column indexes on the analytics_* views if you use them.

2. File Overview & Run Order
2.1 Key Scripts
setup_and_load.py

Creates the database (if needed)

Ensures schema exists

Creates lookup tables (vendors, payments, zones)

Loads taxi_zone_lookup.csv into zones

Creates trips table

Loads TLC Parquet data from new_data/yellow_tripdata_2025-01.parquet

Runs VACUUM ANALYZE on trips

create_view.py

Creates materialized views:

trip_analytics_summary

trip_zone_density

peak_hours

vendor_performance

Also creates related indexes on those views and on trips.

indexes.py

Creates/ensures:

Base indexes on trips (time, zone, vendor, payment)

Indexes on analytics-oriented MVs (analytics_*) for the dashboard.

Flask routes (under app/routes/):

analytics.py – landing dashboard / trip analytics (MVs)

map_view.py – zone-based density endpoint

fare_trip.py – fare vs tip analysis

peak_hours.py – peak hour detection

vendor_performance.py – vendor comparison

3. Setup & Running Order
3.1 Prerequisites
Python 3.10+ (you are using 3.13)

PostgreSQL running locally on localhost:5432 (or adjust via env)

pip install for dependencies (typical stack):

psycopg2-binary

SQLAlchemy

pandas

python-dotenv

Flask

Flask-Cors

Flask-Caching

(If you have a requirements.txt, just run pip install -r requirements.txt.)

3.2 Environment Variables
Create a .env file in the project root:

env
Copy code
PGUSER=your_postgres_user
PGPASSWORD=your_postgres_password
PGHOST=localhost
PGPORT=5432

DB_NAME=nyc_taxi
SCHEMA_NAME=public

# Optional: custom path to TLC zone lookup
ZONES_CSV_PATH=taxi_zone_lookup.csv
3.3 Data Files
Place TLC taxi zone lookup CSV at:

./taxi_zone_lookup.csv or set ZONES_CSV_PATH.

Place the Parquet trip file at:

./new_data/yellow_tripdata_2025-01.parquet

You can later add more monthly Parquet files to PARQUET_FILES if you want to load more data.

3.4 Run Order (One-Time / After Data Updates)
Load data into PostgreSQL

bash
Copy code
python3 setup_and_load.py
This will:

Create DB nyc_taxi (if missing)

Create schema + tables

Load zones + trips

Run VACUUM ANALYZE

Create materialized views (feature-oriented)

bash
Copy code
python3 create_view.py
This builds:

trip_analytics_summary

trip_zone_density

peak_hours

vendor_performance

and key indexes for them.

Create / refresh schema indexes (and analytics MV indexes)

bash
Copy code
python3 indexes.py
This ensures:

Core indexes on trips

Indexes on analytics dashboard MVs (analytics_*) if you’re using them.

Run the Flask API

From the project root:

bash
Copy code
export FLASK_APP=app
export FLASK_ENV=development   # optional
flask run --port 5001
Your API will be available at:

http://127.0.0.1:5001/

4. API Overview (Quick Summary)
All endpoints use JSON and live under http://127.0.0.1:5001.

Feature	Method	Path
Analytics dashboard (overview)	GET	/api/trip-analytics
Refresh analytics materialized views	POST	/api/refresh-trip-analytics
Zone density (map view)	GET	/api/map-density
Fare & tip analysis	GET	/api/fare-tip-analysis
Peak hours (top 10)	GET	/api/peak-hours
Vendor performance (CMT vs VTS)	GET	/api/vendor-performance

Each endpoint accepts optional filters such as:

vendor_id (CMT, VTS)

payment_id (1–6)

weekday (0–6)

hour (0–23)

start, end (date strings YYYY-MM-DD)

Responses always contain:

metadata – row counts, filters used, execution time, timestamp

data – list of result objects

You can test, for example:

bash
Copy code
# Analytics dashboard
curl http://127.0.0.1:5001/api/trip-analytics

# Fare & tip analysis for credit card trips on Fridays
curl "http://127.0.0.1:5001/api/fare-tip-analysis?payment_id=1&weekday=5"

# Map density for pickups by zone
curl "http://127.0.0.1:5001/api/map-density?type=pickup&weekday=4&hour=18"

# Top 10 peak hours overall
curl http://127.0.0.1:5001/api/peak-hours

# Vendor performance, cash only
curl "http://127.0.0.1:5001/api/vendor-performance?payment_id=2" 