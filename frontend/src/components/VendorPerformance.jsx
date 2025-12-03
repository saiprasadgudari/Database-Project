import { useEffect, useState } from "react";
import Plot from "react-plotly.js";
import { taxiApi } from "../api/client";

const BASE_BAR_LAYOUT = {
  margin: { t: 10, r: 20, b: 55, l: 70 },
  paper_bgcolor: "rgba(0,0,0,0)",
  plot_bgcolor: "rgba(0,0,0,0)",
  font: {
    color: "#e5e7eb",
    family: "system-ui",
    size: 15,
  },
  xaxis: {
    title: "Vendor",
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
  bargap: 0.45,
  hovermode: "closest",
};

export default function VendorPerformance({ filters }) {
  const [rows, setRows] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    const { start, end, weekday, hour, payment_id } = filters;

    taxiApi
      .vendorPerformance({
        start: start || undefined,
        end: end || undefined,
        weekday: weekday !== "" ? weekday : undefined,
        hour: hour !== "" ? hour : undefined,
        payment_id: payment_id || undefined,
      })
      .then((res) => {
        if (cancelled) return;
        setRows(res?.rows || res?.data || res || []);
        setError("");
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || "Failed to load vendor stats");
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
            <h2 className="section-title">Vendor Performance</h2>
            <p className="section-subtitle">
              Comparing demand and revenue across taxi vendors…
            </p>
          </div>
        </div>
        <p className="text-muted">Loading vendor performance…</p>
      </section>
    );
  }

  if (error) {
    return (
      <section>
        <div className="section-header">
          <div>
            <h2 className="section-title">Vendor Performance</h2>
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
            <h2 className="section-title">Vendor Performance</h2>
          </div>
        </div>
        <p className="text-muted">No data for current filters.</p>
      </section>
    );
  }

  const vendors = rows.map((r) => r.vendor_id);
  const trips = rows.map((r) => Number(r.trip_count) || 0);
  const revenue = rows.map((r) => Number(r.total_revenue) || 0);

  const tripsTrace = {
    type: "bar",
    x: vendors,
    y: trips,
    marker: {
      color: "#fbbf24",
      line: { color: "#facc15", width: 1.5 },
    },
    text: trips.map((v) => `${v.toLocaleString()} trips`),
    textposition: "outside",
    cliponaxis: false,
    hovertemplate: "<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
  };

  const revenueTrace = {
    type: "bar",
    x: vendors,
    y: revenue,
    marker: {
      color: "#34d399",
      line: { color: "#6ee7b7", width: 1.5 },
    },
    text: revenue.map((v) => `$${(v / 1000).toFixed(1)}k`),
    textposition: "outside",
    cliponaxis: false,
    hovertemplate:
      "<b>%{x}</b><br>Total revenue: $%{y:,.0f}<extra></extra>",
  };

  const tripsLayout = {
    ...BASE_BAR_LAYOUT,
    xaxis: {
      ...BASE_BAR_LAYOUT.xaxis,
      categoryorder: "array",
      categoryarray: vendors,
    },
    yaxis: {
      ...BASE_BAR_LAYOUT.yaxis,
      title: "Trip Count",
      tickformat: "~s", // 10k, 30k, …
    },
  };

  const revenueLayout = {
    ...BASE_BAR_LAYOUT,
    xaxis: {
      ...BASE_BAR_LAYOUT.xaxis,
      categoryorder: "array",
      categoryarray: vendors,
    },
    yaxis: {
      ...BASE_BAR_LAYOUT.yaxis,
      title: "Total Revenue ($)",
      tickprefix: "$",
      tickformat: "~s", // 200k, 800k, …
    },
  };

  return (
    <section>
      <div className="section-header">
        <div>
          <h2 className="section-title">Vendor Performance</h2>
          <p className="section-subtitle">
            Side-by-side comparison of total trips and total revenue by vendor.
          </p>
        </div>
      </div>

      <div className="grid-2-even">
        {/* Trips per vendor */}
        <div className="card plot-card">
          <div className="card-header">
            <h3 className="card-title">Trips per Vendor</h3>
            <span className="card-meta">
              Total completed trips in the selected time window.
            </span>
          </div>
          <div className="plot-wrapper">
            <Plot
              data={[tripsTrace]}
              layout={tripsLayout}
              style={{ width: "100%", height: 320 }}
              useResizeHandler
            />
          </div>
        </div>

        {/* Revenue per vendor */}
        <div className="card plot-card">
          <div className="card-header">
            <h3 className="card-title">Total Revenue per Vendor</h3>
            <span className="card-meta">
              Sum of total_amount across all trips for each vendor.
            </span>
          </div>
          <div className="plot-wrapper">
            <Plot
              data={[revenueTrace]}
              layout={revenueLayout}
              style={{ width: "100%", height: 320 }}
              useResizeHandler
            />
          </div>
        </div>
      </div>
    </section>
  );
}
