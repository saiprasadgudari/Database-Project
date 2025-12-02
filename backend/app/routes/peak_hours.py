from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text
from app.config import Config
import time
import datetime

peak_bp = Blueprint("peak_hours", __name__)

# Global DB engine
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)


@peak_bp.route("/api/peak-hours", methods=["GET"])
def peak_hours():

    vendor_id = request.args.get("vendor_id")
    payment_id = request.args.get("payment_id", type=int)
    start = request.args.get("start")
    end = request.args.get("end")

    base_query = """
        SELECT
            pickup_weekday AS weekday,
            pickup_hour AS hour,
            COUNT(*) AS trip_count,
            ROUND(AVG(fare)::numeric, 2) AS avg_fare,
            ROUND(AVG(distance)::numeric, 2) AS avg_distance,
            ROUND(AVG(EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 60)::numeric, 2)
                AS avg_duration_min
        FROM public.trips
        WHERE pickup_time IS NOT NULL
    """

    filters = []
    params = {}

    if vendor_id:
        filters.append("vendor_id = :vendor_id")
        params["vendor_id"] = vendor_id
    if payment_id:
        filters.append("payment_id = :payment_id")
        params["payment_id"] = payment_id
    if start:
        filters.append("pickup_time >= :start")
        params["start"] = start
    if end:
        filters.append("pickup_time < :end")
        params["end"] = end

    if filters:
        base_query += " AND " + " AND ".join(filters)

    # Group, order, and limit
    base_query += """
        GROUP BY pickup_weekday, pickup_hour
        ORDER BY trip_count DESC
        LIMIT 10;
    """

    query = text(base_query)

    start_time = time.time()
    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    elapsed = time.time() - start_time

    data = [dict(row._mapping) for row in rows]

    return jsonify({
        "metadata": {
            "row_count": len(data),
            "filters": {k: v for k, v in params.items()},
            "execution_time_sec": round(elapsed, 3),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        "data": data
    })
