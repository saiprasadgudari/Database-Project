import axios from "axios";

const api = axios.create({
  baseURL: "http://127.0.0.1:5001",
});

// helper to unwrap data, and centralize error logging
async function get(path, params = {}) {
  const res = await api.get(path, { params });
  return res.data;
}

export const taxiApi = {
  health: () => get("/api/health"),

  tripAnalytics: () => get("/api/trip-analytics"),

  mapDensity: (params) =>
    get("/api/map-density", params),

  fareTipAnalysis: (params) =>
    get("/api/fare-tip-analysis", params),

  peakHours: (params) =>
    get("/api/peak-hours", params),

  vendorPerformance: (params) =>
    get("/api/vendor-performance", params),
};
