const weekdays = [
  { value: "", label: "All days" },
  { value: 0, label: "Sunday" },
  { value: 1, label: "Monday" },
  { value: 2, label: "Tuesday" },
  { value: 3, label: "Wednesday" },
  { value: 4, label: "Thursday" },
  { value: 5, label: "Friday" },
  { value: 6, label: "Saturday" },
];

const hours = [{ value: "", label: "All hours" }].concat(
  Array.from({ length: 24 }, (_, h) => ({ value: h, label: `${h}:00` }))
);

export default function FiltersBar({ filters, onChange }) {
  const handleChange = (field) => (e) => {
    const value = e.target.value;
    onChange({ [field]: value === "" ? "" : value });
  };

  return (
    <section className="filters-bar">
      <div className="filter-group">
        <span className="filter-label">Start date</span>
        <input
          type="date"
          value={filters.start}
          onChange={handleChange("start")}
        />
      </div>

      <div className="filter-group">
        <span className="filter-label">End date</span>
        <input
          type="date"
          value={filters.end}
          onChange={handleChange("end")}
        />
      </div>

      <div className="filter-group">
        <span className="filter-label">Weekday</span>
        <select value={filters.weekday} onChange={handleChange("weekday")}>
          {weekdays.map((w) => (
            <option key={w.label} value={w.value}>
              {w.label}
            </option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <span className="filter-label">Hour</span>
        <select value={filters.hour} onChange={handleChange("hour")}>
          {hours.map((h) => (
            <option key={h.label} value={h.value}>
              {h.label}
            </option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <span className="filter-label">Vendor</span>
        <select value={filters.vendor_id} onChange={handleChange("vendor_id")}>
          <option value="">All</option>
          <option value="CMT">CMT</option>
          <option value="VTS">VTS</option>
        </select>
      </div>

      <div className="filter-group">
        <span className="filter-label">Payment</span>
        <select
          value={filters.payment_id}
          onChange={handleChange("payment_id")}
        >
          <option value="">All</option>
          <option value={1}>Credit Card</option>
          <option value={2}>Cash</option>
          <option value={3}>No Charge</option>
          <option value={4}>Dispute</option>
          <option value={5}>Unknown</option>
          <option value={6}>Voided Trip</option>
        </select>
      </div>
    </section>
  );
}
