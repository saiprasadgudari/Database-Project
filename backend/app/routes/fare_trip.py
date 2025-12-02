from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text
from app.config import Config
import time
import datetime

fare_tip_bp = Blueprint("fare_tip", __name__)

# Global DB connection
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)


@fare_tip_bp.route("/api/fare-tip-analysis", methods=["GET"])
def fare_tip_analysis():
    vendor_id = request.args.get("vendor_id")
    payment_id = request.args.get("payment_id", type=int)
    weekday = request.args.get("weekday", type=int)
    hour = request.args.get("hour", type=int)
    start = request.args.get("start")
    end = request.args.get("end")

    # Build base query dynamically
    base_query = """
        SELECT
            pickup_weekday AS weekday,
            pickup_hour AS hour,
            p.payment_type,
            ROUND(AVG(t.fare)::numeric, 2) AS avg_fare,
            ROUND(AVG(t.tip_amount)::numeric, 2) AS avg_tip,
            ROUND((AVG(t.tip_amount) / NULLIF(AVG(t.fare), 0))::numeric, 3) AS tip_to_fare_ratio,
            COUNT(*) AS trip_count
        FROM public.trips t
        LEFT JOIN public.payments p ON t.payment_id = p.payment_id
        WHERE t.pickup_time IS NOT NULL
    """

    filters = []
    params = {}

    if vendor_id:
        filters.append("t.vendor_id = :vendor_id")
        params["vendor_id"] = vendor_id
    if payment_id:
        filters.append("t.payment_id = :payment_id")
        params["payment_id"] = payment_id
    if weekday is not None:
        filters.append("t.pickup_weekday = :weekday")
        params["weekday"] = weekday
    if hour is not None:
        filters.append("t.pickup_hour = :hour")
        params["hour"] = hour
    if start:
        filters.append("t.pickup_time >= :start")
        params["start"] = start
    if end:
        filters.append("t.pickup_time < :end")
        params["end"] = end

    if filters:
        base_query += " AND " + " AND ".join(filters)

    # Group and order by time/payment
    base_query += """
        GROUP BY pickup_weekday, pickup_hour, p.payment_type
        ORDER BY pickup_weekday, pickup_hour, p.payment_type;
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
