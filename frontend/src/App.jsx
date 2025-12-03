import { useState } from "react";
import AppLayout from "./components/layout/AppLayout";
import FiltersBar from "./components/FiltersBar";
import TripAnalyticsDashboard from "./components/TripAnalyticsDashboard";
import MapDensityView from "./components/MapDensityView";
import FareTipAnalysis from "./components/FareTipAnalysis";
import PeakHours from "./components/PeakHours";
import VendorPerformance from "./components/VendorPerformance";

function App() {
  const [filters, setFilters] = useState({
    start: "2025-01-01",
    end: "2025-08-31",
    weekday: "",
    hour: "",
    vendor_id: "",
    payment_id: "",
  });

  const handleFilterChange = (patch) =>
    setFilters((prev) => ({ ...prev, ...patch }));

  return (
    <AppLayout>
      <FiltersBar filters={filters} onChange={handleFilterChange} />

      <TripAnalyticsDashboard />

      <MapDensityView filters={filters} />

      <FareTipAnalysis filters={filters} />

      <PeakHours filters={filters} />

      <VendorPerformance filters={filters} />
    </AppLayout>
  );
}

export default App;
