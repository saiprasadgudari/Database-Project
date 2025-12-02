from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text
from app.config import Config
import time
import datetime

vendor_bp = Blueprint("vendor_performance", __name__)

# Global DB engine
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)


@vendor_bp.route("/api/vendor-performance", methods=["GET"])
def vendor_performance():
    payment_id = request.args.get("payment_id", type=int)
    weekday = request.args.get("weekday", type=int)
    hour = request.args.get("hour", type=int)
    start = request.args.get("start")
    end = request.args.get("end")

    base_query = """
        SELECT
            v.vendor_id,
            v.name AS vendor_name,
            ROUND(AVG(t.fare)::numeric, 2) AS avg_fare,
            ROUND(AVG(t.tip_amount)::numeric, 2) AS avg_tip,
            ROUND(AVG(t.distance)::numeric, 2) AS avg_distance,
            ROUND(SUM(t.total_amount)::numeric, 2) AS total_revenue,
            COUNT(*) AS trip_count
        FROM public.trips t
        JOIN public.vendors v ON t.vendor_id = v.vendor_id
        WHERE t.pickup_time IS NOT NULL
    """

    filters = []
    params = {}

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

    base_query += """
        GROUP BY v.vendor_id, v.name
        ORDER BY total_revenue DESC;
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
