import { useState } from "react";
import { apiGet } from "../api/client";

function GridExplorer() {
  const [gridIdInput, setGridIdInput] = useState("");
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [searchedGridId, setSearchedGridId] = useState(null);

  const handleSearch = (e) => {
    e.preventDefault();
    const gridId = parseInt(gridIdInput, 10);
    if (isNaN(gridId)) {
      setError("Please enter a valid numeric grid ID.");
      setData(null);
      return;
    }

    setLoading(true);
    setError(null);
    setData(null);
    setSearchedGridId(gridId);

    apiGet(`/network/grid/${gridId}`)
      .then((result) => setData(result))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  };

  return (
    <div>
      <h2 className="page-title">Grid Explorer</h2>
      <p className="page-subtitle">Search a grid ID to view its hourly activity history</p>

      <form onSubmit={handleSearch} className="form-row">
        <label className="form-label">
          Grid ID:
          <input
            type="text"
            value={gridIdInput}
            onChange={(e) => setGridIdInput(e.target.value)}
            placeholder="e.g. 4821"
          />
        </label>
        <button type="submit">Search</button>
      </form>

      {loading && <p>Loading activity for grid {searchedGridId}...</p>}

      {error && (
        <div className="api-unavailable-banner">
          {error.includes("not found")
            ? `Grid ${searchedGridId} was not found. Valid range is 1-10000.`
            : `Error: ${error}`}
        </div>
      )}

      {data && (
        <>
          <p className="page-subtitle">
            Grid {data.grid_id} — as of {data.as_of} — {data.points.length} hourly points
          </p>
          <table className="data-table">
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>SMS (in+out)</th>
                <th>Calls (in+out)</th>
                <th>Internet Activity</th>
                <th>Total Activity</th>
              </tr>
            </thead>
            <tbody>
              {data.points.map((p) => (
                <tr key={p.timestamp}>
                  <td>{p.timestamp}</td>
                  <td>{(p.sms_in + p.sms_out).toFixed(2)}</td>
                  <td>{(p.call_in + p.call_out).toFixed(2)}</td>
                  <td>{p.internet_activity.toFixed(2)}</td>
                  <td>{p.total_activity.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}

export default GridExplorer;