import { useEffect, useState } from "react";
import Plot from "react-plotly.js";
import { taxiApi } from "../api/client";

/* ------------------ Helpers ------------------ */

function formatLargeNumber(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return "-";
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + "M";
  if (num >= 1_000) return (num / 1_000).toFixed(1) + "K";
  return num.toString();
}

function formatCurrency(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return "-";
  if (num >= 1_000_000) return "$" + (num / 1_000_000).toFixed(2) + "M";
  if (num >= 1_000) return "$" + (num / 1_000).toFixed(1) + "K";
  return "$" + num.toFixed(2);
}

function formatHourLabel(h) {
  return h.toString().padStart(2, "0") + ":00";
}

function weekdayName(i) {
  return [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
  ][i];
}

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/* ------------------ Plot Layout ------------------ */

const DARK_PLOT_LAYOUT = {
  margin: { t: 10, r: 20, b: 50, l: 60 },
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: { color: "#e5e7eb", family: "system-ui", size: 13 },

  xaxis: {
    tickfont: { color: "#9ca3af", size: 12 },
    titlefont: { color: "#e5e7eb", size: 14 },
    gridcolor: "rgba(255,255,255,0.06)",
    zeroline: false,
  },

  yaxis: {
    tickfont: { color: "#9ca3af", size: 12 },
    titlefont: { color: "#e5e7eb", size: 14 },
    gridcolor: "rgba(255,255,255,0.08)",
    zerolinecolor: "rgba(255,255,255,0.15)",
  },

  hovermode: "closest",
};

/* ------------------ Component ------------------ */

export default function TripAnalyticsDashboard() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let mounted = true;
    setLoading(true);

    taxiApi
      .tripAnalytics()
      .then((res) => {
        if (!mounted) return;

        if (
          res &&
          res.kpis &&
          res.payment_mix &&
          res.trips_by_borough &&
          res.trips_by_weekday &&
          res.trips_by_hour
        ) {
          setData(res);
        } else {
          setError("Invalid analytics data from API.");
        }
      })
      .catch(() => {
        if (mounted) setError("Failed to load analytics.");
      })
      .finally(() => mounted && setLoading(false));

    return () => {
      mounted = false;
    };
  }, []);

  if (loading) return <p>Loading analytics…</p>;
  if (!data) return null;

  const {
    kpis,
    payment_mix,
    trips_by_borough,
    trips_by_weekday,
    trips_by_hour,
  } = data;

  // Coerce KPI fields to numbers so .toFixed works reliably
  const totalTrips = safeNumber(kpis.total_trips);
  const avgFare = safeNumber(kpis.avg_fare);
  const avgDistance = safeNumber(kpis.avg_distance);
  const avgDuration = safeNumber(kpis.avg_duration_min);
  const totalRevenue = safeNumber(kpis.total_revenue);
  const activePickupZones = safeNumber(kpis.active_pickup_zones);
  const activeDropoffZones = safeNumber(kpis.active_dropoff_zones);

  return (
    <section>
      <div className="section-header">
        <div>
          <h2 className="section-title">Trip Analytics Overview</h2>
          <p className="section-subtitle">
            High-level KPIs and temporal patterns across all loaded trips. Time
            range: {kpis.min_pickup_time} → {kpis.max_pickup_time}
          </p>
        </div>
        {error && <div className="alert text-danger">{error}</div>}
      </div>

      {/* KPI Row */}
      <div className="kpi-grid">
        <KpiCard label="Total Trips" value={formatLargeNumber(totalTrips)} />
        <KpiCard label="Average Fare" value={formatCurrency(avgFare)} />
        <KpiCard
          label="Average Distance"
          value={avgDistance.toFixed(2) + " mi"}
        />
        <KpiCard
          label="Average Duration"
          value={avgDuration.toFixed(1) + " min"}
        />
        <KpiCard label="Total Revenue" value={formatCurrency(totalRevenue)} />
        {/* <KpiCard
          label="Active Zones"
          value={`Pickup: ${activePickupZones}\nDropoff: ${activeDropoffZones}`}
        /> */}
      </div>

      {/* Payment Mix Donut + Trips by Borough */}
      <div className="grid-2-even" style={{ marginTop: "1.2rem" }}>
        <div className="card plot-card" style={{ padding: "1.1rem 1.3rem" }}>
          <div className="card-header">
            <h3 className="card-title">Payment Mix</h3>
            <span className="card-meta">
              Share of trips by payment type across the current date range.
            </span>
          </div>

          <Plot
            data={[
              {
                type: "pie",
                labels: payment_mix.map((p) => p.payment_type),
                values: payment_mix.map((p) => p.trip_count),
                textinfo: "label+percent",
                hole: 0.48,
                marker: {
                  colors: [
                    "#facc15", // CRD
                    "#fbbf24", // UNK
                    "#fcd34d", // CSH
                    "#fde68a", // DIS
                    "#fffbeb", // NOC
                  ],
                },
              },
            ]}
            layout={{
              ...DARK_PLOT_LAYOUT,
              showlegend: true,
              legend: {
                orientation: "h",
                y: -0.2,
                x: 0.5,
                xanchor: "center",
                font: { size: 12 },
              },
            }}
            style={{ width: "100%", height: 360 }}
            useResizeHandler
          />
        </div>

        <div className="card plot-card" style={{ padding: "1.1rem 1.3rem" }}>
          <div className="card-header">
            <h3 className="card-title">Trips by Borough</h3>
            <span className="card-meta">
              Distribution of total trip counts across NYC boroughs.
            </span>
          </div>

          <Plot
            data={[
              {
                type: "bar",
                x: trips_by_borough.map((r) => r.borough ?? "Unknown"),
                y: trips_by_borough.map((r) => r.trip_count),
                marker: {
                  color: "#fbbf24",
                  line: { color: "#fcd34d", width: 1 },
                },
                hovertemplate: "<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
              },
            ]}
            layout={{
              ...DARK_PLOT_LAYOUT,
              xaxis: { ...DARK_PLOT_LAYOUT.xaxis, title: "Borough" },
              yaxis: { ...DARK_PLOT_LAYOUT.yaxis, title: "Trip Count" },
            }}
            style={{ width: "100%", height: 360 }}
            useResizeHandler
          />
        </div>
      </div>

      {/* Trips by Weekday + Hour */}
      <div className="grid-2-even" style={{ marginTop: "1.4rem" }}>
        <div className="card plot-card" style={{ padding: "1.1rem 1.3rem" }}>
          <div className="card-header">
            <h3 className="card-title">Trips by Weekday</h3>
            <span className="card-meta">
              How demand varies across days of the week.
            </span>
          </div>

          <Plot
            data={[
              {
                type: "bar",
                x: trips_by_weekday.map((r) => weekdayName(r.weekday)),
                y: trips_by_weekday.map((r) => r.trip_count),
                marker: {
                  color: "#facc15",
                  line: { color: "#fde047", width: 1 },
                },
                hovertemplate: "<b>%{x}</b><br>%{y:,} trips<extra></extra>",
              },
            ]}
            layout={{
              ...DARK_PLOT_LAYOUT,
              xaxis: { ...DARK_PLOT_LAYOUT.xaxis, title: "Weekday" },
              yaxis: { ...DARK_PLOT_LAYOUT.yaxis, title: "Trip Count" },
            }}
            style={{ width: "100%", height: 310 }}
            useResizeHandler
          />
        </div>

        <div className="card plot-card" style={{ padding: "1.1rem 1.3rem" }}>
          <div className="card-header">
            <h3 className="card-title">Trips by Hour of Day</h3>
            <span className="card-meta">
              Intra-day demand, aggregated over all days.
            </span>
          </div>

          <Plot
            data={[
              {
                type: "bar",
                x: trips_by_hour.map((r) => formatHourLabel(r.hour)),
                y: trips_by_hour.map((r) => r.trip_count),
                marker: {
                  color: "#fbbf24",
                  line: { color: "#fcd34d", width: 1 },
                },
                hovertemplate: "Hour %{x}<br>%{y:,} trips<extra></extra>",
              },
            ]}
            layout={{
              ...DARK_PLOT_LAYOUT,
              xaxis: {
                ...DARK_PLOT_LAYOUT.xaxis,
                title: "Hour (24-hour)",
              },
              yaxis: { ...DARK_PLOT_LAYOUT.yaxis, title: "Trip Count" },
            }}
            style={{ width: "100%", height: 310 }}
            useResizeHandler
          />
        </div>
      </div>
    </section>
  );
}

/* ------------------ KPI Card ------------------ */

function KpiCard({ label, value }) {
  return (
    <div className="kpi-card">
      <div className="kpi-label">{label}</div>
      <div className="kpi-value" style={{ whiteSpace: "pre-line" }}>
        {value}
      </div>
    </div>
  );
}
