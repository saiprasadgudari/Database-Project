from flask import Blueprint, jsonify
from sqlalchemy import create_engine, text
from app.config import Config
import time
import datetime

# Optional caching
try:
    from flask_caching import Cache
    cache = Cache(config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 600})
except ImportError:
    cache = None

analytics_bp = Blueprint("analytics", __name__)
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)


@analytics_bp.record_once
def on_load(state):
    app = state.app
    if cache:
        cache.init_app(app)



# 1st Feature — Trip Analytics Dashboard (Materialized Views)
@analytics_bp.route("/api/trip-analytics", methods=["GET"])
def trip_analytics():
    start_time = time.time()

    with engine.connect() as conn:
        # 1) Global KPIs (single row)
        kpis_row = conn.execute(
            text("SELECT * FROM analytics_kpis LIMIT 1;")
        ).mappings().first()

        # 2) Payment mix
        payment_rows = conn.execute(
            text("""
                SELECT payment_type, trip_count
                FROM analytics_payment_mix
                ORDER BY trip_count DESC;
            """)
        ).mappings().all()

        # 3) Trips by borough
        borough_rows = conn.execute(
            text("""
                SELECT borough, trip_count
                FROM analytics_trips_by_borough
                ORDER BY trip_count DESC;
            """)
        ).mappings().all()

        # 4) Trips by weekday
        weekday_rows = conn.execute(
            text("""
                SELECT weekday, trip_count
                FROM analytics_trips_by_weekday
                ORDER BY weekday;
            """)
        ).mappings().all()

        # 5) Trips by hour
        hour_rows = conn.execute(
            text("""
                SELECT hour, trip_count
                FROM analytics_trips_by_hour
                ORDER BY hour;
            """)
        ).mappings().all()

    elapsed = time.time() - start_time

    # Convert rows dict/list for JSON
    kpis = dict(kpis_row) if kpis_row is not None else {}

    payment_mix = [dict(r) for r in payment_rows]
    trips_by_borough = [dict(r) for r in borough_rows]
    trips_by_weekday = [dict(r) for r in weekday_rows]
    trips_by_hour = [dict(r) for r in hour_rows]

    return jsonify({
        "metadata": {
            "execution_time_sec": round(elapsed, 3),
            "data_source": "materialized_views",
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "kpis": kpis,
        "payment_mix": payment_mix,
        "trips_by_borough": trips_by_borough,
        "trips_by_weekday": trips_by_weekday,
        "trips_by_hour": trips_by_hour,
    })


# Manual refresh of Analytics MVs

@analytics_bp.route("/api/refresh-trip-analytics", methods=["POST"])
def refresh_trip_analytics():
    start_time = time.time()

    with engine.begin() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW analytics_kpis;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW analytics_payment_mix;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW analytics_trips_by_borough;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW analytics_trips_by_weekday;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW analytics_trips_by_hour;"))

    elapsed = time.time() - start_time

    return jsonify({
        "message": "✅ Analytics materialized views refreshed successfully.",
        "execution_time_sec": round(elapsed, 3),
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
