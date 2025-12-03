import { useEffect, useMemo, useState } from "react";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import { taxiApi } from "../api/client";

const BOROUGHS = [
  "Manhattan",
  "Brooklyn",
  "Queens",
  "Bronx",
  "Staten Island",
  "Unknown",
];

export default function MapDensityView({ filters }) {
  const [geoJson, setGeoJson] = useState(null);
  const [densityRows, setDensityRows] = useState([]);
  const [type, setType] = useState("pickup");
  const [boroughFilter, setBoroughFilter] = useState("");
  const [loadingGeo, setLoadingGeo] = useState(true);
  const [loadingDensity, setLoadingDensity] = useState(true);
  const [error, setError] = useState("");

  // ---- 1) Load static taxi_zones.geojson once ----
  useEffect(() => {
    setLoadingGeo(true);
    fetch("/taxi_zones.geojson")
      .then((res) => {
        if (!res.ok) throw new Error(`GeoJSON HTTP ${res.status}`);
        return res.json();
      })
      .then((json) => {
        console.log("[MapDensity] GeoJSON loaded. Top-level keys:", Object.keys(json));
        if (json.features && json.features.length > 0) {
          console.log("[MapDensity] Sample feature:", json.features[0]);
        }
        setGeoJson(json);
      })
      .catch((err) => {
        console.error("[MapDensity] Error loading GeoJSON:", err);
        setError("Failed to load taxi zone shapes.");
      })
      .finally(() => setLoadingGeo(false));
  }, []);

  // ---- 2) Load density from /api/map-density on filter/type change ----
  useEffect(() => {
    let cancelled = false;
    setLoadingDensity(true);

    const { start, end, weekday, hour, vendor_id, payment_id } = filters;

    taxiApi
      .mapDensity({
        type,
        limit: 150,
        start: start || undefined,
        end: end || undefined,
        weekday: weekday !== "" ? weekday : undefined,
        hour: hour !== "" ? hour : undefined,
        vendor_id: vendor_id || undefined,
        payment_id: payment_id || undefined,
      })
      .then((res) => {
        if (cancelled) return;
        const arr = res?.data || res?.rows || res || [];
        setDensityRows(Array.isArray(arr) ? arr : []);
        setError("");
        console.log("[MapDensity] Loaded density rows:", arr.length);
      })
      .catch((err) => {
        if (cancelled) return;
        console.error("[MapDensity] API error:", err);
        setError(err.message || "Failed to load map density.");
        setDensityRows([]);
      })
      .finally(() => {
        if (!cancelled) setLoadingDensity(false);
      });

    return () => {
      cancelled = true;
    };
  }, [filters, type]);

  // ---- 3) Build zone_id -> density map ----
  const densityByZone = useMemo(() => {
    const m = new Map();
    densityRows.forEach((r) => {
      const key = Number(r.zone_id);
      if (!Number.isNaN(key)) m.set(key, r);
    });
    return m;
  }, [densityRows]);

  const maxCount = useMemo(() => {
    if (!densityRows.length) return 1;
    return Math.max(...densityRows.map((r) => Number(r.trip_count) || 0));
  }, [densityRows]);

  const { start, end, weekday, hour, vendor_id, payment_id } = filters;

  // ---- 4) Style function: MAKE ZONES VERY VISIBLE ----
  const styleFeature = (feature) => {
    const locId = Number(feature.properties.LocationID);
    const row = densityByZone.get(locId);
    const count = row ? Number(row.trip_count) || 0 : 0;

    const borough =
      row?.borough ||
      feature.properties.boro_name ||
      feature.properties.borough ||
      "Unknown";

    // If user selected a borough, dim others
    const dimmed = boroughFilter && borough !== boroughFilter;

    // If we have density data, use it; else show a strong grey outline
    if (!row) {
      return {
        weight: dimmed ? 0.5 : 1.2,
        color: dimmed ? "rgba(148,163,184,0.4)" : "#e5e7eb",
        fillColor: "rgba(15,23,42,0.0)",
        fillOpacity: dimmed ? 0.02 : 0.06,
      };
    }

    const intensity = maxCount > 0 ? count / maxCount : 0;
    const clamped = Math.min(1, Math.max(0, intensity));

    // Yellow → green heat scale
    const hue = 55 - clamped * 45; // 55 ≈ taxi yellow, 10 ≈ green
    const lightness = 45 + clamped * 10;
    const fill = `hsl(${hue}, 96%, ${lightness}%)`;

    return {
      weight: dimmed ? 0.7 : 1.4,
      color: dimmed ? "rgba(234,179,8,0.35)" : "rgba(250,204,21,0.95)",
      fillColor: dimmed ? "rgba(15,23,42,0.9)" : fill,
      fillOpacity: dimmed ? 0.12 : 0.55 + 0.25 * clamped,
    };
  };

  // ---- 5) Tooltip for each polygon ----
  const onEachFeature = (feature, layer) => {
    const locId = Number(feature.properties.LocationID);
    const row = densityByZone.get(locId);

    const zoneName =
      row?.zone_name ||
      feature.properties.zone ||
      feature.properties.Zone ||
      "Unknown zone";

    const borough =
      row?.borough ||
      feature.properties.boro_name ||
      feature.properties.borough ||
      "Unknown";

    const count = row ? Number(row.trip_count) || 0 : 0;
    const tripsLabel = count
      ? `${count.toLocaleString()} trips`
      : "No trips for current filters";

    const html = `
      <div style="font-size: 0.8rem;">
        <strong>${zoneName}</strong><br/>
        ${borough}<br/>
        ${tripsLabel}
      </div>
    `;

    layer.bindTooltip(html, {
      direction: "top",
      opacity: 1,
      sticky: true,
      className: "leaflet-tooltip",
    });
  };

  const isLoadingInitial = loadingGeo && !geoJson;
  const isUpdatingDensity = loadingDensity && !!geoJson;

  return (
    <section>
      <div className="section-header">
        <div>
          <h2 className="section-title">Zone Density Map</h2>
          <p className="section-subtitle">
            Taxi {type === "pickup" ? "pickup" : "dropoff"} density by TLC taxi zone.
          </p>
        </div>

        <div className="row">
          <div className="filter-group" style={{ maxWidth: 180 }}>
            <span className="filter-label">View</span>
            <select value={type} onChange={(e) => setType(e.target.value)}>
              <option value="pickup">Pickup density</option>
              <option value="dropoff">Dropoff density</option>
            </select>
          </div>

          <div className="filter-group" style={{ maxWidth: 200 }}>
            <span className="filter-label">Borough</span>
            <select
              value={boroughFilter}
              onChange={(e) => setBoroughFilter(e.target.value)}
            >
              <option value="">All boroughs</option>
              {BOROUGHS.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {error && <p className="text-danger">{error}</p>}

      {isLoadingInitial && (
        <p className="text-muted">Loading taxi zones and density data…</p>
      )}

      {geoJson && (
        <div className="card" style={{ padding: "0.7rem" }}>
          <div style={{ height: 430 }}>
            <MapContainer
              center={[40.73, -73.97]} // NYC-ish
              zoom={10.7}
              style={{ height: "100%", width: "100%" }}
            >
              <TileLayer
                attribution="&copy; OpenStreetMap contributors, &copy; CARTO"
                url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              />

              <GeoJSON
                key={`${type}-${boroughFilter}-${start}-${end}-${weekday}-${hour}-${vendor_id}-${payment_id}`}
                data={geoJson}
                style={styleFeature}
                onEachFeature={onEachFeature}
              />
            </MapContainer>
          </div>

          {isUpdatingDensity && (
            <p className="text-muted" style={{ marginTop: "0.5rem" }}>
              Updating density for current filters…
            </p>
          )}
        </div>
      )}
    </section>
  );
}
