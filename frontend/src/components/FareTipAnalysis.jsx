import { useEffect, useState } from "react";
import Plot from "react-plotly.js";
import { taxiApi } from "../api/client";

const PAYMENT_LABELS = {
  CRD: "Credit Card",
  CSH: "Cash",
  NOC: "No Charge",
  DIS: "Dispute",
  UNK: "Unknown",
  VOD: "Voided Trip",
};

const BASE_LAYOUT = {
  margin: { t: 10, r: 20, b: 55, l: 65 },
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: {
    color: "#e5e7eb",
    family: "system-ui",
    size: 15,
  },
  xaxis: {
    tickfont: { color: "#9ca3af", size: 14 },
    titlefont: { color: "#e5e7eb", size: 16 },
    gridcolor: "rgba(255,255,255,0.02)",
  },
  yaxis: {
    tickfont: { color: "#9ca3af", size: 14 },
    titlefont: { color: "#e5e7eb", size: 16 },
    gridcolor: "rgba(255,255,255,0.08)",
    zeroline: false,
  },
  hovermode: "closest",
};

export default function FareTipAnalysis({ filters }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    const { start, end, weekday, hour, vendor_id, payment_id } = filters;

    taxiApi
      .fareTipAnalysis({
        start: start || undefined,
        end: end || undefined,
        weekday: weekday !== "" ? weekday : undefined,
        hour: hour !== "" ? hour : undefined,
        vendor_id: vendor_id || undefined,
        payment_id: payment_id || undefined,
      })
      .then((res) => {
        if (cancelled) return;
        const result = res?.data || res?.rows || res;
        setRows(Array.isArray(result) ? result : []);
        setError("");
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || "Failed to load fare/tip data");
          setRows([]);
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [filters]);

  if (loading) {
    return (
      <>
        <section>
          <h2 className="section-title">Fare &amp; Tip Analysis – Volume</h2>
          <p className="text-muted">Loading…</p>
        </section>
        <section>
          <h2 className="section-title">Fare &amp; Tip Analysis – Tipping Behavior</h2>
        </section>
      </>
    );
  }

  if (error) {
    return (
      <>
        <section>
          <h2 className="section-title">Fare &amp; Tip Analysis – Volume</h2>
          <p className="text-danger">{error}</p>
        </section>
        <section>
          <h2 className="section-title">Fare &amp; Tip Analysis – Tipping Behavior</h2>
        </section>
      </>
    );
  }

  if (!rows.length) {
    return (
      <>
        <section>
          <h2 className="section-title">Fare &amp; Tip Analysis – Volume</h2>
          <p className="text-muted">No data for selected filters.</p>
        </section>
        <section>
          <h2 className="section-title">Fare &amp; Tip Analysis – Tipping Behavior</h2>
        </section>
      </>
    );
  }

  // ---- Normalize incoming rows ----
  const normalized = rows.map((r) => ({
    hour: Number(r.hour),
    avg_fare: Number(r.avg_fare),
    avg_tip: Number(r.avg_tip),
    ratio: Number(r.tip_to_fare_ratio), // already 0–0.3
    trip_count: Number(r.trip_count),
    payment_type: r.payment_type,
  }));

  // ---- Group by hour ----
  // For each hour, we:
  //   - sum trip_count
  //   - compute weighted average of ratio: sum(ratio * trips)/sum(trips)
  const grouped = new Map();
  normalized.forEach((r) => {
    const h = r.hour;
    let g = grouped.get(h);
    if (!g) {
      g = { hour: h, tripCount: 0, ratioWeightedSum: 0 };
      grouped.set(h, g);
    }
    g.tripCount += r.trip_count;
    g.ratioWeightedSum += r.ratio * r.trip_count;
  });

  const groupedSorted = [...grouped.values()].sort((a, b) => a.hour - b.hour);

  const hours = groupedSorted.map((g) => `${g.hour}:00`);
  const tripCounts = groupedSorted.map((g) => g.tripCount);
  const ratios = groupedSorted.map((g) =>
    g.tripCount > 0 ? g.ratioWeightedSum / g.tripCount : 0
  );

  const maxRatio = Math.max(...ratios);
  const paymentTypeLabel =
    filters.payment_id && normalized[0]?.payment_type
      ? PAYMENT_LABELS[normalized[0].payment_type] ?? normalized[0].payment_type
      : "all payment types";

  // --------- CHARTS ----------

  return (
    <>
      {/* 1. Trip volume by hour */}
      <section>
        <h2 className="section-title">Fare &amp; Tip Analysis – Volume</h2>
        <p className="section-subtitle">
          Trip count distribution by hour for {paymentTypeLabel}.
        </p>

        <div className="card plot-card">
          <div className="card-header">
            <h3 className="card-title">Trip Count by Hour</h3>
            <span className="card-meta">
              Bar height shows how many trips start in each hour of the day.
            </span>
          </div>

          <div className="plot-wrapper">
            <Plot
              data={[
                {
                  type: "bar",
                  x: hours,
                  y: tripCounts,
                  marker: {
                    color: "#fbbf24",
                    line: { color: "#facc15", width: 1.2 },
                  },
                  hovertemplate:
                    "Hour: %{x}<br>Trips: %{y:,}<extra></extra>",
                },
              ]}
              layout={{
                ...BASE_LAYOUT,
                xaxis: { ...BASE_LAYOUT.xaxis, title: "Hour" },
                yaxis: {
                  ...BASE_LAYOUT.yaxis,
                  title: "Trip count",
                  tickformat: "~s",
                },
              }}
              style={{ width: "100%", height: 320 }}
              useResizeHandler
            />
          </div>
        </div>
      </section>

      {/* 2. Ratio vs hour – THIS IS WHAT YOU ASKED FOR */}
      <section>
        <h2 className="section-title">Fare &amp; Tip Analysis – Tipping Behavior</h2>
        <p className="section-subtitle">
          Tip-to-fare ratio by hour (y-axis = ratio, x-axis = hour).
        </p>

        <div className="card plot-card">
          <div className="card-header">
            <h3 className="card-title">Tip-to-Fare Ratio by Hour</h3>
            <span className="card-meta">
              Ratio is computed as tip divided by fare, aggregated across{" "}
              {paymentTypeLabel}.
            </span>
          </div>

          <div className="plot-wrapper">
            <Plot
              data={[
                {
                  type: "bar",
                  x: hours,
                  y: ratios,
                  marker: {
                    color: "#22c55e",
                    line: { color: "#16a34a", width: 1.3 },
                  },
                  hovertemplate:
                    "Hour: %{x}<br>Ratio: %{y:.2%}<extra></extra>",
                  name: "Tip-to-fare ratio",
                },
              ]}
              layout={{
                ...BASE_LAYOUT,
                xaxis: { ...BASE_LAYOUT.xaxis, title: "Hour" },
                yaxis: {
                  ...BASE_LAYOUT.yaxis,
                  title: "Tip-to-fare ratio",
                  tickformat: ".0%",
                  rangemode: "tozero",
                  range: [0, Math.max(0.35, maxRatio * 1.1)], // 0–35% or a bit above max
                },
                showlegend: false,
              }}
              style={{ width: "100%", height: 340 }}
              useResizeHandler
            />
          </div>
        </div>
      </section>
    </>
  );
}
