// src/components/MapDensityView.jsx
import { useEffect, useState } from "react";
import {
  MapContainer,
  TileLayer,
  GeoJSON,
} from "react-leaflet";
import { taxiApi } from "../api/client";

export default function MapDensityView({ filters }) {
  const [zoneData, setZoneData] = useState(null);
  const [densityRows, setDensityRows] = useState([]);
  const [loadingZones, setLoadingZones] = useState(true);
  const [loadingDensity, setLoadingDensity] = useState(true);
  const [type, setType] = useState("pickup");
  const [borough, setBorough] = useState("all");

  // Load GeoJSON from public folder once
  useEffect(() => {
    fetch("/taxi_zones.geojson")
      .then((res) => res.json())
      .then((data) => {
        setZoneData(data);
        setLoadingZones(false);
      })
      .catch((e) => {
        console.error("Failed to load GeoJSON:", e);
        setLoadingZones(false);
      });
  }, []);

  // Load density data whenever filters or type change
  useEffect(() => {
    setLoadingDensity(true);
    const { start, end, weekday, hour, vendor_id, payment_id } = filters;

    taxiApi
      .mapDensity({
        type,
        limit: 150,
        weekday: weekday !== "" ? weekday : undefined,
        hour: hour !== "" ? hour : undefined,
        vendor_id: vendor_id || undefined,
        payment_id: payment_id || undefined,
        start: start || undefined,
        end: end || undefined,
      })
      .then((res) => {
        setDensityRows(res.data || res.rows || res);
      })
      .catch((err) => {
        console.error("Density API error:", err);
        setDensityRows([]);
      })
      .finally(() => setLoadingDensity(false));
  }, [filters, type]);

  // Create a lookup map of LocationID → tripCount
  const densityMap = {};
  densityRows.forEach(({ zone_id, trip_count }) => {
    densityMap[Number(zone_id)] = trip_count;
  });

  // Compute maximum count for color scaling
  const maxCount = Math.max(...Object.values(densityMap), 0);

  // Style function for GeoJSON polygons
  const getZoneStyle = (feature) => {
    const locID = feature?.properties?.LocationID;
    const zoneBorough = feature?.properties?.borough;
    const count = densityMap[locID] || 0;
    const intensity = maxCount > 0 ? count / maxCount : 0;
    const fillOpacity = 0.25 + intensity * 0.45; // range 0.25–0.70

    // If a borough filter is active, hide other boroughs
    if (borough !== "all" && zoneBorough !== borough) {
      return { fillOpacity: 0, opacity: 0 };
    }

    return {
      color: "#facc15",        // outline color
      weight: 1,
      fillColor: "#fbbf24",    // fill color (yellow)
      fillOpacity,
    };
  };

  const onEachZone = (feature, layer) => {
    const locID = feature?.properties?.LocationID;
    const zoneName = feature?.properties?.zone || feature?.properties?.zone_name;
    const tripCount = densityMap[locID] || 0;
    layer.bindTooltip(
      `<strong>${zoneName ?? "Unknown"}</strong><br/>${tripCount.toLocaleString()} trips`,
      { sticky: true }
    );
  };

  return (
    <section>
      <div className="section-header" style={{ marginBottom: "0.7rem" }}>
      <div>
        <h2 className="section-title">Zone Density Map</h2>
        <p className="section-subtitle">
          Taxi {type} density by TLC taxi zone.
        </p>
      </div>
      <div className="row" style={{ gap: "0.75rem" }}>
        <div className="filter-group">
          <label className="filter-label">View</label>
          <select value={type} onChange={(e) => setType(e.target.value)}>
            <option value="pickup">Pickup density</option>
            <option value="dropoff">Dropoff density</option>
          </select>
        </div>
        <div className="filter-group">
          <label className="filter-label">Borough</label>
          <select value={borough} onChange={(e) => setBorough(e.target.value)}>
            <option value="all">All boroughs</option>
            <option value="Manhattan">Manhattan</option>
            <option value="Brooklyn">Brooklyn</option>
            <option value="Queens">Queens</option>
            <option value="Bronx">Bronx</option>
            <option value="Staten Island">Staten Island</option>
            <option value="EWR">EWR</option>
            <option value="Unknown">Unknown</option>
          </select>
        </div>
      </div>
      </div>

      {(loadingZones || loadingDensity) && (
        <p className="text-muted">Loading map data…</p>
      )}

      {!loadingZones && !loadingDensity && zoneData && (
        <div className="card" style={{ padding: "0.75rem" }}>
          <div style={{ height: 460 }}>
            <MapContainer
              center={[40.73, -73.97]}
              zoom={11}
              style={{ height: "100%", width: "100%" }}
              scrollWheelZoom={true}
            >
              <TileLayer
                url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
                attribution='&copy; OpenStreetMap contributors, &copy; CARTO'
              />
              <GeoJSON
                data={zoneData}
                style={getZoneStyle}
                onEachFeature={onEachZone}
              />
            </MapContainer>
          </div>
        </div>
      )}
    </section>
  );
}
