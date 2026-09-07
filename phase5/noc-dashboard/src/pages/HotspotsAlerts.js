import { useEffect, useState, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { apiGet } from "../api/client";
import { useMilanGrid } from "../context/MilanGridContext";
import { buildGridLookup, getPolygonPoints } from "../hooks/gridLookup";

// Milan's real-world bounding box, used to project lon/lat -> SVG pixels
const LON_MIN = 9.0, LON_MAX = 9.3;
const LAT_MIN = 45.35, LAT_MAX = 45.55;
const SVG_WIDTH = 600, SVG_HEIGHT = 600;

function project(lon, lat) {
  const x = ((lon - LON_MIN) / (LON_MAX - LON_MIN)) * SVG_WIDTH;
  const y = SVG_HEIGHT - ((lat - LAT_MIN) / (LAT_MAX - LAT_MIN)) * SVG_HEIGHT;
  return [x, y];
}

// Maps API severity strings to the badge classes already defined in App.css
function severityBadgeClass(severity) {
  if (severity === "high") return "badge badge-high";
  if (severity === "medium") return "badge badge-medium";
  return "badge badge-normal";
}

function severityLabel(severity) {
  if (severity === "high") return "HIGH";
  if (severity === "medium") return "ATTENTION";
  return "NORMAL";
}

// SVG polygon styling still needs raw fill/stroke (SVG doesn't take
// CSS classes for this the same way), kept separate from badge styling
function severityMapStyle(severity) {
  if (severity === "high") return { fill: "#e74c3c", stroke: "#922b21", strokeWidth: 3 };
  if (severity === "medium") return { fill: "#f39c12", stroke: "#9c640c", strokeWidth: 2 };
  return { fill: "#2ecc71", stroke: "#1e8449", strokeWidth: 1 };
}

function HotspotsAlerts() {
  const navigate = useNavigate();
  const { geoJson, loading: geoLoading, error: geoError } = useMilanGrid();

  const [hotspots, setHotspots] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [severityFilter, setSeverityFilter] = useState("");
  const [limit, setLimit] = useState(10);
  const [error, setError] = useState(null);

  useEffect(() => {
    setError(null);
    const params = new URLSearchParams({ limit: String(limit) });
    if (severityFilter) params.set("severity", severityFilter);

    Promise.all([
      apiGet(`/network/hotspots?${params.toString()}`),
      apiGet(`/network/alerts?${params.toString()}`),
    ])
      .then(([hotspotRes, alertRes]) => {
        setHotspots(hotspotRes.items);
        setAlerts(alertRes.items);
      })
      .catch((err) => setError(err.message));
  }, [limit, severityFilter]);

  const gridLookup = useMemo(() => buildGridLookup(geoJson), [geoJson]);

  const flaggedGrids = useMemo(() => {
    const map = new Map();
    hotspots.forEach((h) => map.set(h.grid_id, { severity: h.severity, source: "hotspot" }));
    alerts.forEach((a) => {
      const sev = a.alert_type === "HIGH_ACTIVITY" ? "high" : "medium";
      if (!map.has(a.grid_id)) map.set(a.grid_id, { severity: sev, source: "alert" });
    });
    return map;
  }, [hotspots, alerts]);

  if (error) return <ApiUnavailableBannerFallback message={error} />;
  if (geoError) return <ApiUnavailableBannerFallback message={`Error loading grid geometry: ${geoError}`} />;

  return (
    <div>
      <h2 className="page-title">Hotspots &amp; Alerts</h2>
      <p className="page-subtitle">Grids ranked by current activity, filtered by severity</p>

      <div className="form-row">
        <label className="form-label">
          Limit:
          <input
            type="number"
            value={limit}
            min={1}
            max={100}
            onChange={(e) => setLimit(Number(e.target.value))}
          />
        </label>
        <label className="form-label">
          Severity:
          <select value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value)}>
            <option value="">All</option>
            <option value="high">High</option>
            <option value="medium">Medium</option>
          </select>
        </label>
      </div>

      <div style={{ display: "flex", gap: "2rem", flexWrap: "wrap" }}>
        {/* Ranked table */}
        <div style={{ flex: "1", minWidth: "320px" }}>
          <h3 className="panel-title">Ranked Hotspots</h3>
          <table className="data-table">
            <thead>
              <tr>
                <th>Grid</th>
                <th>Timestamp</th>
                <th>Total Activity</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {hotspots.map((h) => (
                <tr
                  key={h.grid_id}
                  className="clickable"
                  onClick={() => navigate(`/grid?grid_id=${h.grid_id}`)}
                >
                  <td>{h.grid_id}</td>
                  <td>{h.timestamp}</td>
                  <td>{h.total_activity.toFixed(1)}</td>
                  <td>
                    <span className={severityBadgeClass(h.severity)}>
                      {severityLabel(h.severity)}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Map */}
        <div className="card">
          <h3 className="panel-title">Milan Grid Map</h3>
          {geoLoading ? (
            <p>Loading grid geometry...</p>
          ) : (
            <svg width={SVG_WIDTH} height={SVG_HEIGHT} style={{ border: "1px solid var(--color-border)", background: "#f7f7f7", borderRadius: "6px" }}>
              {Array.from(flaggedGrids.entries()).map(([gridId, info]) => {
                const feature = gridLookup.get(gridId);
                if (!feature) return null;
                const points = getPolygonPoints(feature)
                  .map(([lon, lat]) => project(lon, lat).join(","))
                  .join(" ");
                const style = severityMapStyle(info.severity);
                return (
                  <polygon
                    key={gridId}
                    points={points}
                    fill={style.fill}
                    stroke={style.stroke}
                    strokeWidth={style.strokeWidth}
                    style={{ cursor: "pointer" }}
                    onClick={() => navigate(`/grid?grid_id=${gridId}`)}
                  >
                    <title>{`Grid ${gridId} — ${severityLabel(info.severity)}`}</title>
                  </polygon>
                );
              })}
            </svg>
          )}
        </div>
      </div>
    </div>
  );
}

// Small inline fallback so this file doesn't need a new import if you'd
// rather keep using ApiUnavailableBanner — swap this for your existing
// component import if you already have one wired in this file's siblings.
function ApiUnavailableBannerFallback({ message }) {
  return <div className="api-unavailable-banner">Error: {message}</div>;
}

export default HotspotsAlerts;