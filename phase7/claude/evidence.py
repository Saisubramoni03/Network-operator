"""
evidence.py — Assembles a curated grid-evidence object from the
existing FastAPI endpoints. Never touches raw CSVs or the warehouse
directly — this deliberately mirrors what a real operator/tool would
see, nothing more.
"""

import os
import requests

API_BASE_URL = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000")


def get_grid_evidence(grid_id: int) -> dict:
    """Gathers everything Claude is allowed to see for one grid:
    current features, risk score (if available), and any currently
    firing alerts. Returns a plain dict — the exact shape Claude's
    prompt will describe."""

    features_resp = requests.get(f"{API_BASE_URL}/network/grid/{grid_id}/features")
    features = features_resp.json() if features_resp.status_code == 200 else None

    alerts_resp = requests.get(f"{API_BASE_URL}/network/alerts", params={"limit": 100})
    alerts_data = alerts_resp.json() if alerts_resp.status_code == 200 else {"items": []}
    grid_alerts = [a for a in alerts_data.get("items", []) if a["grid_id"] == grid_id]

    risk_resp = requests.post(f"{API_BASE_URL}/network/predict-risk", json={"grid_id": grid_id})
    risk = risk_resp.json() if risk_resp.status_code == 200 else None

    return {
        "grid_id": grid_id,
        "timestamp": features["feature_timestamp"] if features else None,
        "current_total_activity": features["avg_activity"] if features else None,
        "activity_growth": features["activity_growth"] if features else None,
        "peak_ratio": features["peak_ratio"] if features else None,
        "variability": features["variability"] if features else None,
        "internet_share": features["internet_share"] if features else None,
        "anomaly_score": risk["risk_score"] if risk else None,
        "anomaly_direction": risk["risk_level"] if risk else None,
        "rule_alerts_firing": [a["alert_type"] for a in grid_alerts],
    }