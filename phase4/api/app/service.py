import sqlite3
from .db import get_connection

import json
import os
from datetime import datetime, timezone
from .model_loader import load_model
from functools import lru_cache

STATUS_RECORD_PATH = os.environ.get(
    "PIPELINE_STATUS_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "analytics", "pipeline_status.json")
)


def get_effective_as_of(conn: sqlite3.Connection, as_of: str | None) -> str:
    """Returns the given as_of if provided, otherwise MAX(timestamp)
    from dim_time. Never hardcodes a date."""
    if as_of:
        return as_of
    row = conn.execute("SELECT MAX(timestamp) as max_ts FROM dim_time").fetchone()
    if row is None or row["max_ts"] is None:
        raise ValueError("No timestamps found in dim_time — warehouse appears empty.")
    return row["max_ts"]


@lru_cache(maxsize=32)
def _compute_network_summary_cached(effective_as_of: str) -> dict:
    """The actual expensive aggregation, cached per as_of value. Since
    this dataset is static historical data, the same as_of always
    produces the same answer — no reason to recompute it every request."""
    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT SUM(f.total_activity) as total_activity,
                   COUNT(DISTINCT f.grid_id) as active_grids
            FROM fact_network_activity f
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE t.timestamp <= ?
        """, (effective_as_of,)).fetchone()

        if row is None or row["total_activity"] is None:
            raise ValueError(f"No activity data found at or before as_of={effective_as_of}")

        peak_row = conn.execute("""
            SELECT t.hour, SUM(f.total_activity) as hour_total
            FROM fact_network_activity f
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE t.timestamp <= ?
            GROUP BY t.hour
            ORDER BY hour_total DESC
            LIMIT 1
        """, (effective_as_of,)).fetchone()

        top_grid_row = conn.execute("""
            SELECT f.grid_id, SUM(f.total_activity) as grid_total
            FROM fact_network_activity f
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE t.timestamp <= ?
            GROUP BY f.grid_id
            ORDER BY grid_total DESC
            LIMIT 1
        """, (effective_as_of,)).fetchone()

        return {
            "total_activity": row["total_activity"],
            "active_grids": row["active_grids"],
            "peak_hour": peak_row["hour"],
            "top_grid": top_grid_row["grid_id"],
        }
    finally:
        conn.close()


def get_network_summary(as_of: str | None = None) -> dict:
    conn = get_connection()
    try:
        effective_as_of = get_effective_as_of(conn, as_of)
    finally:
        conn.close()

    result = _compute_network_summary_cached(effective_as_of)
    result["as_of"] = effective_as_of
    return result

def get_grid_activity(grid_id: int, as_of: str | None = None, date: str | None = None, hour: int | None = None):
    """Returns grid-level hourly activity time series, or None if the
    grid_id is out of range or doesn't exist (route layer converts
    None to a 404)."""
    if grid_id < 1 or grid_id > 10000:
        return None

    conn = get_connection()
    try:
        effective_as_of = get_effective_as_of(conn, as_of)

        grid_row = conn.execute("SELECT grid_id FROM dim_grid WHERE grid_id = ?", (grid_id,)).fetchone()
        if grid_row is None:
            return None

        if date:
            query = """
                SELECT t.timestamp, f.sms_in, f.sms_out, f.call_in, f.call_out,
                       f.internet_activity, f.total_activity
                FROM fact_network_activity f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.grid_id = ? AND t.date = ?
            """
            params = [grid_id, date]
            if hour is not None:
                query += " AND t.hour = ?"
                params.append(hour)
            query += " ORDER BY t.timestamp"
            rows = conn.execute(query, params).fetchall()
        else:
            rows = conn.execute("""
                SELECT t.timestamp, f.sms_in, f.sms_out, f.call_in, f.call_out,
                       f.internet_activity, f.total_activity
                FROM fact_network_activity f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.grid_id = ? AND t.timestamp <= ?
                ORDER BY t.timestamp DESC
                LIMIT 24
            """, (grid_id, effective_as_of)).fetchall()
            rows = list(reversed(rows))

        points = [
            {
                "timestamp": r["timestamp"],
                "sms_in": r["sms_in"],
                "sms_out": r["sms_out"],
                "call_in": r["call_in"],
                "call_out": r["call_out"],
                "internet_activity": r["internet_activity"],
                "total_activity": r["total_activity"],
            }
            for r in rows
        ]

        return {"grid_id": grid_id, "as_of": effective_as_of, "points": points}
    finally:
        conn.close()
def get_hotspots(as_of: str | None = None, limit: int = 10, severity: str | None = None) -> dict:
    conn = get_connection()
    try:
        effective_as_of = get_effective_as_of(conn, as_of)

        rows = conn.execute("""
            SELECT f.grid_id, t.timestamp, f.total_activity,
                   rs.risk_score, rs.risk_level, rs.model_version
            FROM fact_network_activity f
            JOIN dim_time t ON f.time_id = t.time_id
            LEFT JOIN network_risk_scores rs
                ON rs.grid_id = f.grid_id AND rs.timestamp = t.timestamp
            WHERE t.timestamp = ?
            ORDER BY f.total_activity DESC
        """, (effective_as_of,)).fetchall()

        if not rows:
            return {"as_of": effective_as_of, "count": 0, "items": []}

        activities = [r["total_activity"] for r in rows]
        threshold_high = sorted(activities, reverse=True)[max(0, len(activities) // 20 - 1)]

        items = []
        for r in rows:
            sev = "high" if r["total_activity"] >= threshold_high else "medium"
            if severity and sev != severity:
                continue
            items.append({
                "grid_id": r["grid_id"],
                "timestamp": r["timestamp"],
                "total_activity": r["total_activity"],
                "severity": sev,
                "reason": f"Total activity {r['total_activity']:.1f} ranks in the "
                          f"{'top 5%' if sev == 'high' else 'elevated range'} of all grids at this hour",
                "risk_score": r["risk_score"],
                "risk_level": r["risk_level"],
                "model_version": r["model_version"],
            })
            if len(items) >= limit:
                break

        return {"as_of": effective_as_of, "count": len(items), "items": items}
    finally:
        conn.close()


@lru_cache(maxsize=1)
def _get_grid_baselines() -> dict:
    """Per-grid average activity across the whole dataset — computed
    once per server process instead of on every /network/alerts call."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT grid_id, AVG(total_activity) as baseline
            FROM fact_network_activity
            GROUP BY grid_id
        """).fetchall()
        return {r["grid_id"]: r["baseline"] for r in rows}
    finally:
        conn.close()


def get_alerts(as_of: str | None = None, limit: int = 10, severity: str | None = None) -> dict:
    conn = get_connection()
    try:
        effective_as_of = get_effective_as_of(conn, as_of)

        baselines = _get_grid_baselines()  # cached — no recompute per request

        rows = conn.execute("""
            SELECT f.grid_id, t.timestamp, f.total_activity as current_activity,
                   rs.risk_score, rs.risk_level, rs.model_version
            FROM fact_network_activity f
            JOIN dim_time t ON f.time_id = t.time_id
            LEFT JOIN network_risk_scores rs
                ON rs.grid_id = f.grid_id AND rs.timestamp = t.timestamp
            WHERE t.timestamp = ?
            ORDER BY f.grid_id
        """, (effective_as_of,)).fetchall()

        items = []
        for r in rows:
            baseline = baselines.get(r["grid_id"])
            if not baseline:
                continue
            current = r["current_activity"]
            ratio = current / baseline

            alert_type = None
            sev = None
            if ratio >= 1.5:
                alert_type, sev = "HIGH_ACTIVITY", "high"
            elif ratio <= 0.5:
                alert_type, sev = "ACTIVITY_DROP", "medium"

            if alert_type is None:
                continue
            if severity and sev != severity:
                continue

            items.append({
                "grid_id": r["grid_id"],
                "timestamp": r["timestamp"],
                "alert_type": alert_type,
                "current_activity": current,
                "baseline_activity": baseline,
                "reason": f"Current activity {current:.1f} is {ratio:.1f}x the grid's overall baseline of {baseline:.1f}",
                "risk_score": r["risk_score"],
                "risk_level": r["risk_level"],
                "model_version": r["model_version"],
            })
            if len(items) >= limit:
                break

        return {"as_of": effective_as_of, "count": len(items), "items": items}
    finally:
        conn.close()

def get_grid_features(grid_id: int):
    """Reads the stored feature table only — NEVER computes features
    here. If no row exists for this grid, returns None so the route
    layer can return a clear, explicit error rather than zeros."""
    if grid_id < 1 or grid_id > 10000:
        return None

    conn = get_connection()
    try:
        row = conn.execute("""
            SELECT grid_id, feature_timestamp, avg_activity, activity_growth,
                   active_hours, peak_ratio, variability, internet_share
            FROM grid_features
            WHERE grid_id = ?
            ORDER BY feature_timestamp DESC
            LIMIT 1
        """, (grid_id,)).fetchone()

        if row is None:
            return None

        return {
            "grid_id": row["grid_id"],
            "feature_timestamp": row["feature_timestamp"],
            "avg_activity": row["avg_activity"],
            "activity_growth": row["activity_growth"],
            "active_hours": row["active_hours"],
            "peak_ratio": row["peak_ratio"],
            "variability": row["variability"],
            "internet_share": row["internet_share"],
            "data_quality": "OK",
            "freshness": f"as of {row['feature_timestamp']}",
        }
    finally:
        conn.close()

def predict_risk(grid_id: int, as_of: str | None = None) -> dict:
    """ML5: real implementation. Same inputs as the stub — contract
    unchanged. Loads the trained model, reads the grid's latest
    features, and returns a real risk_score/risk_level."""
    if grid_id < 1 or grid_id > 10000:
        return None

    artifact = load_model()
    model = artifact["model"]
    feature_cols = artifact["feature_cols"]
    model_version = artifact["model_version"]
    thresholds = artifact["risk_level_thresholds"]

    conn = get_connection()
    try:
        grid_row = conn.execute("SELECT grid_id FROM dim_grid WHERE grid_id = ?", (grid_id,)).fetchone()
        if grid_row is None:
            return None

        effective_as_of = get_effective_as_of(conn, as_of)

        feature_row = conn.execute("""
            SELECT feature_timestamp, avg_activity, activity_growth,
                   active_hours, peak_ratio, variability, internet_share
            FROM grid_features
            WHERE grid_id = ?
            ORDER BY feature_timestamp DESC
            LIMIT 1
        """, (grid_id,)).fetchone()

        if feature_row is None:
            raise ValueError(
                f"No stored features for grid {grid_id}. Feature table is populated by "
                f"ml/features.py (ML2), which has not yet been run for this grid."
            )

        feature_values = {}
        for col in feature_cols:
            val = feature_row[col]
            if val is None:
                raise ValueError(f"Feature '{col}' is null for grid {grid_id} — cannot score.")
            feature_values[col] = val

        X = [[feature_values[col] for col in feature_cols]]
        risk_score = float(model.predict_proba(X)[0][1])

        if risk_score >= thresholds["high"]:
            risk_level = "high"
        elif risk_score >= thresholds["medium"]:
            risk_level = "medium"
        else:
            risk_level = "low"

        top_features = None
        if hasattr(model, "coef_"):
            contributions = [
                (col, model.coef_[0][i] * feature_values[col])
                for i, col in enumerate(feature_cols)
            ]
            contributions.sort(key=lambda c: abs(c[1]), reverse=True)
            top_features = [c[0] for c in contributions[:3]]

        return {
            "grid_id": grid_id,
            "as_of": effective_as_of,
            "feature_timestamp": feature_row["feature_timestamp"],
            "risk_score": risk_score,
            "risk_level": risk_level,
            "model_version": model_version,
            "top_features": top_features,
            "explanation_note": (
                f"Prediction from trained model {model_version}, using features as of "
                f"{feature_row['feature_timestamp']}. risk_score is the model's predicted "
                f"probability that this grid's activity at t+1 exceeds its per-grid "
                f"90th-percentile training threshold — treat it as an investigation "
                f"signal, not a confirmed fault."
            ),
        }
    finally:
        conn.close()


def get_pipeline_status() -> dict:
    """Reads DE7's own status record — NEVER recomputes health
    independently. Two sources of truth for pipeline health is worse
    than none."""
    if not os.path.exists(STATUS_RECORD_PATH):
        raise FileNotFoundError(f"Pipeline status record not found at {STATUS_RECORD_PATH}. Has DE7 ever run?")

    with open(STATUS_RECORD_PATH, "r") as f:
        record = json.load(f)

    task_status = record.get("task_status", {})
    required_tasks = ["ingest_validate_route", "spark_process", "load_warehouse"]

    reasons = []
    for task in required_tasks:
        status = task_status.get(task)
        if status != "success":
            reasons.append(f"Task '{task}' status is '{status or 'missing'}', expected 'success'.")

    healthy = len(reasons) == 0

    # Freshness: how old is the run itself (wall-clock), independent of
    # whether it succeeded — a stale-but-successful run is still
    # trustworthy, just worth flagging as not current
    freshness = "unknown"
    try:
        run_time = datetime.fromisoformat(record["run_timestamp"])
        age = datetime.now(timezone.utc) - run_time
        hours = age.total_seconds() / 3600
        freshness = f"{hours:.1f} hours since last run"
    except (KeyError, ValueError):
        pass

    return {
        "run_id": record.get("run_id", "unknown"),
        "run_timestamp": record.get("run_timestamp", "unknown"),
        "task_status": task_status,
        "rows_in": record.get("rows_in"),
        "rows_rejected": record.get("rows_rejected"),
        "nulls_handled": record.get("nulls_handled"),
        "rows_published": record.get("rows_published"),
        "as_of": record.get("AS_OF"),
        "freshness": freshness,
        "healthy": healthy,
        "reasons": reasons,
    }


def get_grid_location(grid_id: int):
    """Reads dim_grid only — never returns full polygon geometry from
    this endpoint, only a reference to it."""
    if grid_id < 1 or grid_id > 10000:
        return None

    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT grid_id, centroid_lon, centroid_lat, geometry_ref FROM dim_grid WHERE grid_id = ?",
            (grid_id,)
        ).fetchone()

        if row is None:
            return None

        return {
            "grid_id": row["grid_id"],
            "centroid_lat": row["centroid_lat"],
            "centroid_lon": row["centroid_lon"],
            "polygon_ref": row["geometry_ref"],
        }
    finally:
        conn.close()


