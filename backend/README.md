# NYC Taxi Trip Analytics – Backend

Backend for an NYC Yellow Taxi analytics dashboard using **PostgreSQL + Flask**.

This service:

- Loads TLC yellow taxi trip data (Parquet) into PostgreSQL  
- Normalizes the schema into lookup tables + a `trips` fact table  
- Builds materialized views + indexes for fast analytics  
- Exposes REST API endpoints for:
  - Trip analytics (KPIs, distributions)
  - Map / zone density
  - Fare & tip analysis
  - Peak hour detection
  - Vendor performance

---

## 1. Setup & Running (Backend Only)

### 1.1 Download data

Download **2025 January–August** Yellow Taxi Trip Record Parquet files from:

> https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

```text
backend/new_data/
    yellow_tripdata_2025-01.parquet
    yellow_tripdata_2025-02.parquet
    yellow_tripdata_2025-03.parquet
    yellow_tripdata_2025-04.parquet
    yellow_tripdata_2025-05.parquet
    yellow_tripdata_2025-06.parquet
    yellow_tripdata_2025-07.parquet
    yellow_tripdata_2025-08.parquet
````

Also place the TLC zone lookup file (e.g. `taxi_zone_lookup.csv`) in `backend/`
or point to it via `ZONES_CSV_PATH`.

---

### 1.2 Create `.env`

In the **backend** folder, create a file named `.env`:

```env
PGUSER=your_postgres_user
PGPASSWORD=your_postgres_password
PGHOST=localhost
PGPORT=5432

DB_NAME=nyc_taxi
SCHEMA_NAME=public

# Optional: custom path to TLC zone lookup
# ZONES_CSV_PATH=/absolute/path/to/taxi_zone_lookup.csv
```

> ⚠️ **Important**
> The scripts target the database named `nyc_taxi`.
> `setup_and_load.py` will **drop and recreate** the `trips` table if `DROP_OLD_TRIPS = True`.
> If you already have data in `nyc_taxi.trips`, it will be deleted and replaced with this new load.

---

### 1.3 Install Python dependencies

From the **backend** folder:

```bash
pip install -r requirements.txt
```

`requirements.txt` includes:

* Flask, flask-cors, flask-caching
* SQLAlchemy, psycopg2-binary
* pandas, pyarrow
* python-dotenv

---

### 1.4 Load data and build analytics layer

From the **backend** folder, run in this order:

1. **Load data into PostgreSQL**

   ```bash
   python3 setup_and_load.py
   ```

   This will:

   * Create database `nyc_taxi` (if it doesn’t exist)
   * Ensure the schema exists (`SCHEMA_NAME`, default `public`)
   * Create lookup tables: `vendors`, `payments`, `zones`
   * Load `taxi_zone_lookup.csv` into `zones`
   * Create the `trips` fact table
   * Load all Parquet files from `new_data/` into `trips`
   * Run `VACUUM ANALYZE` on `trips`

2. **Create materialized views**

   ```bash
   python3 create_views.py
   ```

   This script creates materialized views such as:

   * `trip_analytics_summary`
   * `trip_zone_density`
   * `peak_hours`
   * `vendor_performance`

3. **Create indexes**

   ```bash
   python3 indexes.py
   ```

   This script ensures:

   * Core indexes on `trips` (time, zone, vendor, payment, etc.)
   * Indexes on analytics materialized views (including `analytics_*` views used by the dashboard).

---

### 1.5 Run the backend server

From the **backend** folder:

```bash
python3 -m app.main
```

By default the server runs on:

```text
http://127.0.0.1:5001/
```

Health check:

```text
GET /api/health
```

---

## 2. Database Schema

All tables live in the schema given by `SCHEMA_NAME` (default: `public`).

### 2.1 Lookup Tables

#### `vendors`

```sql
vendors (
  vendor_id VARCHAR(10) PRIMARY KEY,  -- 'CMT', 'VTS'
  name      TEXT NOT NULL
);
```

Populated by `create_reference_tables()` in `setup_and_load.py`.

#### `payments`

```sql
payments (
  payment_id   INT PRIMARY KEY,            -- 1..6
  payment_type VARCHAR(20) UNIQUE NOT NULL,
  description  TEXT
);
```

Mapping used in ETL:

* 1 → CRD (Credit Card)
* 2 → CSH (Cash)
* 3 → NOC (No Charge)
* 4 → DIS (Dispute)
* 5 → UNK (Unknown / fallback)
* 6 → VOD (Voided Trip)

#### `zones`

```sql
zones (
  zone_id      INT PRIMARY KEY,   -- TLC LocationID
  borough      TEXT,
  zone_name    TEXT,
  service_zone TEXT
);
```

Loaded from `taxi_zone_lookup.csv` (or `ZONES_CSV_PATH`).

---

### 2.2 Fact Table: `trips`

Main table storing each completed trip.

```sql
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

  -- Foreign keys
  pickup_zone_id    INT REFERENCES zones(zone_id),
  dropoff_zone_id   INT REFERENCES zones(zone_id),
  vendor_id         VARCHAR(10) REFERENCES vendors(vendor_id),
  payment_id        INT REFERENCES payments(payment_id),

  -- Coordinates
  pickup_long       FLOAT,
  pickup_lat        FLOAT,
  dropoff_long      FLOAT,
  dropoff_lat       FLOAT,

  -- Derived columns
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
  ratecodeid             INT,
  store_and_fwd_flag     VARCHAR(1),
  extra                  FLOAT,
  mta_tax                FLOAT,
  tolls_amount           FLOAT,
  improvement_surcharge  FLOAT,
  congestion_surcharge   FLOAT,
  airport_fee            FLOAT,
  cbd_congestion_fee     FLOAT
);
```

Columns are populated from TLC Parquet via `CSV_TO_DB_RENAME` + `coerce_types()` in `setup_and_load.py`.

---

### 2.3 Materialized Views (summary)

Created by `create_views.py` (and/or related scripts):

* **`trip_analytics_summary`**
  Average fare, distance, and duration grouped by `(borough, weekday, hour)`.

* **`trip_zone_density`**
  Trip counts per `(pickup_zone_id, dropoff_zone_id)` pair.

* **`peak_hours`**
  Trip counts by `(pickup_weekday, pickup_hour)` with a rank for busiest times.

* **`vendor_performance`**
  Per-vendor stats: trip count, average fare, average total, total revenue.

Additional `analytics_*` materialized views (e.g. `analytics_kpis`, `analytics_payment_mix`,
`analytics_trips_by_borough`, `analytics_trips_by_weekday`, `analytics_trips_by_hour`) may also be created for the dashboard.

---

### 2.4 Indexes (summary)

On **`trips`**:

* `(pickup_weekday, pickup_hour)` – time-slice queries
* `pickup_time` – date range filters
* `pickup_zone_id`, `dropoff_zone_id`, `(pickup_zone_id, dropoff_zone_id)` – zone density
* `payment_id` – payment filters
* `vendor_id` – vendor comparison

On **materialized views** (examples):

* `trip_analytics_summary (borough, weekday, hour)`
* `trip_zone_density (pickup_zone_id)` and `(pickup_zone_id, dropoff_zone_id)`
* `peak_hours (rank, weekday, hour)` and `(weekday, hour)`
* `vendor_performance (vendor, total_trips)`

Plus additional single-column indexes on `analytics_*` views.

---

## 3. API Endpoints

All endpoints are served from the backend (default: `http://127.0.0.1:5001`).

Common query parameters (where supported):

* `vendor_id` – `'CMT'` or `'VTS'`
* `payment_id` – integer `1–6`
* `weekday` – `0–6` (0 = Sunday)
* `hour` – `0–23`
* `start`, `end` – date or timestamp strings (`YYYY-MM-DD` or ISO)

All responses are JSON and include a `metadata` field with row counts, filters, execution time, and timestamp.

---

### 3.1 Health

**GET** `/api/health`
Simple health check + DB connectivity.

---

### 3.2 Trip Analytics Dashboard

**GET** `/api/trip-analytics`

Uses analytics materialized views to return:

* Global KPIs (`analytics_kpis`)
* Payment mix (`analytics_payment_mix`)
* Trips by borough (`analytics_trips_by_borough`)
* Trips by weekday (`analytics_trips_by_weekday`)
* Trips by hour (`analytics_trips_by_hour`)

Example:

```bash
curl http://127.0.0.1:5001/api/trip-analytics
```

---

### 3.3 Refresh Analytics Views

**POST** `/api/refresh-trip-analytics`

Refreshes the analytics MVs after new data is loaded:

* `analytics_kpis`
* `analytics_payment_mix`
* `analytics_trips_by_borough`
* `analytics_trips_by_weekday`
* `analytics_trips_by_hour`

---

### 3.4 Map / Zone Density

**GET** `/api/map-density`

Query parameters:

* `type` – `"pickup"` (default) or `"dropoff"`
* `limit` – max number of zones (default `150`)
* Optional filters: `weekday`, `hour`, `vendor_id`, `payment_id`, `start`, `end`

Example:

```bash
curl "http://127.0.0.1:5001/api/map-density?type=pickup&weekday=4&hour=18"
```

---

### 3.5 Fare & Tip Analysis

**GET** `/api/fare-tip-analysis`

Filters (all optional):

* `vendor_id`
* `payment_id`
* `weekday`
* `hour`
* `start`
* `end`

Returns avg fare, avg tip, tip-to-fare ratio and trip count grouped by weekday, hour, and payment type.

Example:

```bash
curl "http://127.0.0.1:5001/api/fare-tip-analysis?payment_id=1&weekday=5"
```

---

### 3.6 Peak Hours

**GET** `/api/peak-hours`

Filters (optional):

* `vendor_id`
* `payment_id`
* `start`
* `end`

Returns top 10 busiest `(weekday, hour)` combinations with:

* `trip_count`
* `avg_fare`
* `avg_distance`
* `avg_duration_min`

Example:

```bash
curl http://127.0.0.1:5001/api/peak-hours
```

---

### 3.7 Vendor Performance

**GET** `/api/vendor-performance`

Filters (optional):

* `payment_id`
* `weekday`
* `hour`
* `start`
* `end`

Returns per-vendor metrics:

* `avg_fare`
* `avg_tip`
* `avg_distance`
* `total_revenue`
* `trip_count`

Example:

```bash
curl "http://127.0.0.1:5001/api/vendor-performance?payment_id=2"
```

---

