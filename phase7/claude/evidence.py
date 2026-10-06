"""
C3 - Curated evidence package for long-context incident investigation.

The evidence layer gathers summarized information from the existing
FastAPI endpoints. It does not read raw CSV files or the warehouse
directly.
"""

import os
import requests

API_BASE_URL = os.environ.get(
    "API_BASE_URL",
    "http://127.0.0.1:8000"
)

GRID_ID = 4821


def api_get(path, params=None):
    response = requests.get(
        f"{API_BASE_URL}{path}",
        params=params,
        timeout=10
    )
    response.raise_for_status()
    return response.json()


def api_post(path, json_body=None):
    response = requests.post(
        f"{API_BASE_URL}{path}",
        json=json_body,
        timeout=10
    )
    response.raise_for_status()
    return response.json()


def summarize_history(points):
    """Convert hourly rows into a compact historical summary."""

    if not points:
        return {
            "hours_available": 0,
            "min_activity": None,
            "max_activity": None,
            "average_activity": None,
            "peak_timestamp": None,
            "peak_activity": None,
        }

    activities = [
        float(point["total_activity"])
        for point in points
    ]

    peak_point = max(
        points,
        key=lambda point: float(point["total_activity"])
    )

    return {
        "hours_available": len(points),
        "min_activity": round(min(activities), 2),
        "max_activity": round(max(activities), 2),
        "average_activity": round(
            sum(activities) / len(activities), 2
        ),
        "peak_timestamp": peak_point["timestamp"],
        "peak_activity": round(
            float(peak_point["total_activity"]),
            2
        ),
    }


def get_grid_evidence(grid_id=GRID_ID):
    """
    Build the curated evidence package used by C3.

    The package intentionally contains summaries instead of raw
    hourly rows so Claude receives relevant context without
    unnecessary data.
    """

    # Current feature evidence
    features = api_get(
        f"/network/grid/{grid_id}/features"
    )

    # Recent history
    activity = api_get(
        f"/network/grid/{grid_id}"
    )

    history_summary = summarize_history(
        activity.get("points", [])
    )

    # Model risk
    risk = api_post(
        "/network/predict-risk",
        {"grid_id": grid_id}
    )

    # Alerts
    alerts_data = api_get(
        "/network/alerts",
        {"limit": 100}
    )

    grid_alerts = [
        alert
        for alert in alerts_data.get("items", [])
        if alert.get("grid_id") == grid_id
    ]

    # Pipeline quality
    pipeline = api_get(
        "/pipeline/status"
    )

    return {
        "grid_id": grid_id,

        "current_evidence": {
            "feature_timestamp": features.get(
                "feature_timestamp"
            ),
            "avg_activity": features.get(
                "avg_activity"
            ),
            "activity_growth": features.get(
                "activity_growth"
            ),
            "active_hours": features.get(
                "active_hours"
            ),
            "peak_ratio": features.get(
                "peak_ratio"
            ),
            "variability": features.get(
                "variability"
            ),
            "internet_share": features.get(
                "internet_share"
            ),
            "data_quality": features.get(
                "data_quality"
            ),
            "freshness": features.get(
                "freshness"
            ),
        },

        "historical_evidence": history_summary,

        "prior_alerts": {
            "count": len(grid_alerts),
            "alert_types": sorted(
                {
                    alert.get("alert_type")
                    for alert in grid_alerts
                }
            ),
        },

        "model_evidence": {
            "risk_score": risk.get(
                "risk_score"
            ),
            "risk_level": risk.get(
                "risk_level"
            ),
            "model_version": risk.get(
                "model_version"
            ),
            "top_features": risk.get(
                "top_features"
            ),
        },

        "pipeline_evidence": {
            "run_id": pipeline.get(
                "run_id"
            ),
            "run_timestamp": pipeline.get(
                "run_timestamp"
            ),
            "task_status": pipeline.get(
                "task_status"
            ),
            "rows_in": pipeline.get(
                "rows_in"
            ),
            "rows_rejected": pipeline.get(
                "rows_rejected"
            ),
            "nulls_handled": pipeline.get(
                "nulls_handled"
            ),
            "rows_published": pipeline.get(
                "rows_published"
            ),
            "as_of": pipeline.get(
                "as_of"
            ),
            "freshness": pipeline.get(
                "freshness"
            ),
            "healthy": pipeline.get(
                "healthy"
            ),
            "reasons": pipeline.get(
                "reasons"
            ),
        },
    }


if __name__ == "__main__":
    import json

    evidence = get_grid_evidence()

    print(
        json.dumps(
            evidence,
            indent=2
        )
    )