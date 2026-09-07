import { useEffect, useState } from "react";
import { apiGet } from "../api/client";
import MetricCard from "../components/MetricCard";
import ApiUnavailableBanner from "../components/ApiUnavailableBanner";

function NetworkOverview() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    setError(null);
    apiGet("/network/summary")
      .then((result) => setData(result))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p>Loading network summary...</p>;

  return (
    <div>
      <h2 className="page-title">Network Overview</h2>

      {error && <ApiUnavailableBanner message={error} />}

      {data && (
        <>
          <p className="page-subtitle">Reporting as of: {data.as_of}</p>
          <div className="metric-card-grid">
            <MetricCard label="Total Activity" value={data.total_activity.toFixed(1)} />
            <MetricCard label="Active Grids" value={data.active_grids} />
            <MetricCard label="Peak Hour" value={`${data.peak_hour}:00`} />
            <MetricCard label="Top Grid" value={data.grid_id ?? data.top_grid} />
          </div>
        </>
      )}
    </div>
  );
}

export default NetworkOverview;