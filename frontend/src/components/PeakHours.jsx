import { useEffect, useState } from "react";
import Plot from "react-plotly.js";
import { taxiApi } from "../api/client";

const WEEKDAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

const DARK_BAR_LAYOUT = {
  margin: { t: 10, r: 20, b: 70, l: 65 },
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: {
    color: "#e5e7eb",
    family: "system-ui",
    size: 15,
  },
  xaxis: {
    title: "",
    tickfont: { color: "#9ca3af", size: 13 },
    titlefont: { color: "#e5e7eb", size: 14 },
    gridcolor: "rgba(255,255,255,0.02)",
    tickangle: -35,
  },
  yaxis: {
    title: "Trip Count",
    tickfont: { color: "#9ca3af", size: 14 },
    titlefont: { color: "#e5e7eb", size: 16 },
    gridcolor: "rgba(255,255,255,0.08)",
    zeroline: false,
  },
  hovermode: "closest",
};

export default function PeakHours({ filters }) {
  const [rows, setRows] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    const { start, end, vendor_id, payment_id } = filters;

    taxiApi
      .peakHours({
        start: start || undefined,
        end: end || undefined,
        vendor_id: vendor_id || undefined,
        payment_id: payment_id || undefined,
      })
      .then((res) => {
        if (cancelled) return;
        setRows(res?.rows || res?.data || res || []);
        setError("");
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || "Failed to load peak hours");
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
      <section>
        <div className="section-header">
          <div>
            <h2 className="section-title">Top 10 Peak Hours</h2>
            <p className="section-subtitle">
              Identifying the busiest weekday–hour combinations by trip volume…
            </p>
          </div>
        </div>
        <p className="text-muted">Loading peak hours…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section>
        <div className="section-header">
          <div>
            <h2 className="section-title">Top 10 Peak Hours</h2>
          </div>
          <div className="alert text-danger">{error}</div>
        </div>
      </section>
    );
  }

  if (!rows.length) {
    return (
      <section>
        <div className="section-header">
          <div>
            <h2 className="section-title">Top 10 Peak Hours</h2>
          </div>
        </div>
        <p className="text-muted">No data for current filters.</p>
      </section>
    );
  }

  // Labels like "Thu 15:00"
  const labels = rows.map((r) => {
    const weekdayIndex = r.weekday;
    const weekdayName =
      r.weekday_name || WEEKDAY_NAMES[weekdayIndex] || "Unknown";
    const hour = r.hour ?? "";
    return `${weekdayName} ${hour}:00`;
  });

  const counts = rows.map((r) => r.trip_count);
  const maxCount = Math.max(...counts);
  const yMax = maxCount * 1.05; // little breathing room at the top

  const trace = {
    type: "bar",
    x: labels,
    y: counts,
    marker: {
      color: "#fbbf24",
      line: { color: "#facc15", width: 1 },
    },
    hovertemplate:
      "<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
  };

  return (
    <section>
      <div className="section-header">
        <div>
          <h2 className="section-title">Top 10 Peak Hours</h2>
          <p className="section-subtitle">
            Busiest weekday–hour combinations, sorted by trip volume.
          </p>
        </div>
      </div>

      <div className="card plot-card">
        <div className="card-header">
          <h3 className="card-title">Busiest Times of the Week</h3>
          <span className="card-meta">
            Bars are ordered from most to least busy; hover to see exact counts.
          </span>
        </div>

        <div className="plot-wrapper">
          <Plot
            data={[trace]}
            layout={{ ...DARK_BAR_LAYOUT, yaxis: { ...DARK_BAR_LAYOUT.yaxis, range: [0, yMax] } }}
            style={{ width: "100%", height: 360 }}
            useResizeHandler
          />
        </div>
      </div>
    </section>
  );
}
