import { useState } from "react";
import { apiPost } from "../api/client";

function PredictiveRisk() {
  const [gridIdInput, setGridIdInput] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = (e) => {
    e.preventDefault();
    const gridId = parseInt(gridIdInput, 10);
    if (isNaN(gridId)) {
      setError("Please enter a valid numeric grid ID.");
      setResult(null);
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    apiPost("/network/predict-risk", { grid_id: gridId })
      .then((res) => setResult(res))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  };

  return (
    <div>
      <h2 className="page-title">Predictive Risk</h2>
      <p className="page-subtitle">Enter a grid ID to get the model's risk assessment</p>

      <form onSubmit={handleSubmit} className="form-row">
        <label className="form-label">
          Grid ID:
          <input
            type="text"
            value={gridIdInput}
            onChange={(e) => setGridIdInput(e.target.value)}
            placeholder="e.g. 4821"
          />
        </label>
        <button type="submit">Assess Risk</button>
      </form>

      {loading && <p>Assessing grid {gridIdInput}...</p>}
      {error && <div className="api-unavailable-banner">Error: {error}</div>}

      {result && (
        <div style={{ display: "flex", gap: "1.5rem", flexWrap: "wrap" }}>
          {/* MODEL OUTPUT REGION — visually distinct box, clearly
              labeled as coming from the model, not from an LLM */}
          <div className="model-output-panel">
            <h4 className="panel-title">Model Output</h4>
            <p className="panel-row">
              <strong>Risk Score:</strong>{" "}
              <span className={`badge ${
                result.risk_level === "high" ? "badge-high" :
                result.risk_level === "medium" ? "badge-medium" : "badge-normal"
              }`}>
                {result.risk_score.toFixed(3)}
              </span>
            </p>
            <p className="panel-row">
              <strong>Risk Level:</strong> {result.risk_level}
            </p>
            <p className="panel-row">
              <strong>As of:</strong> {result.feature_timestamp}
            </p>
            {result.top_features && result.top_features.length > 0 && (
              <p className="panel-row">
                <strong>Top Contributing Features:</strong> {result.top_features.join(", ")}
              </p>
            )}
            <p className="panel-note">Model version: {result.model_version}</p>
            <p className="panel-note">
              This is a model-generated risk signal, not a confirmed network fault.
            </p>
          </div>

          {/* NARRATIVE REGION — visually separate panel, placeholder for
              the future Claude explanation phase. Never merged into
              the model output panel above. */}
          <div className="narrative-panel">
            <h4 className="panel-title">Narrative Explanation</h4>
            <p style={{ color: "var(--color-text-muted)" }}>{result.explanation_note}</p>
            <button disabled>Explain with AI (coming soon)</button>
          </div>
        </div>
      )}
    </div>
  );
}

export default PredictiveRisk;