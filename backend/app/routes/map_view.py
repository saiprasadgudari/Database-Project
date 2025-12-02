from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, text
from app.config import Config
import time, datetime

map_bp = Blueprint("map_view", __name__)
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)


@map_bp.route("/api/map-density", methods=["GET"])
def map_density():
    qtype = request.args.get("type", "pickup").lower()
    if qtype not in ("pickup", "dropoff"):
        return jsonify({"error": "type must be 'pickup' or 'dropoff'"}), 400

    # Which zone column to use
    zone_col = "pickup_zone_id" if qtype == "pickup" else "dropoff_zone_id"

    # Limit for top-N busiest zones
    try:
        limit = int(request.args.get("limit", 150))
        limit = max(10, min(1000, limit))  # clamp
    except ValueError:
        return jsonify({"error": "invalid limit"}), 400

    where, params = [], {}

    # Standard filters (same pattern as other endpoints)
    for key, col in [
        ("weekday", "t.pickup_weekday"),
        ("hour", "t.pickup_hour"),
        ("vendor_id", "t.vendor_id"),
        ("payment_id", "t.payment_id"),
    ]:
        val = request.args.get(key)
        if val is not None and val != "":
            where.append(f"{col} = :{key}")
            params[key] = val

    # Date range filters on pickup_time
    start = request.args.get("start")
    end = request.args.get("end")
    if start:
        where.append("t.pickup_time >= :start")
        params["start"] = start
    if end:
        where.append("t.pickup_time < :end")
        params["end"] = end

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    # Zone-based density query
    query = text(f"""
        SELECT
            t.{zone_col} AS zone_id,
            z.borough,
            z.zone_name,
            COUNT(*)::bigint AS trip_count
        FROM public.trips t
        LEFT JOIN public.zones z
            ON t.{zone_col} = z.zone_id
        {where_sql}
        GROUP BY t.{zone_col}, z.borough, z.zone_name
        ORDER BY trip_count DESC
        LIMIT :limit;
    """)

    params["limit"] = limit

    start_time = time.time()
    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    elapsed = time.time() - start_time

    data = [dict(row._mapping) for row in rows]

    return jsonify({
        "metadata": {
            "rows": len(data),
            "type": qtype,
            "limit": limit,
            "filters": {
                k: v
                for k, v in params.items()
                if k in ("weekday", "hour", "vendor_id", "payment_id", "start", "end")
            },
            "execution_time_sec": round(elapsed, 3),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "data": data,
    })
