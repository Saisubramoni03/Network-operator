"""
batch_score.py — ML6: Batch Score All Grids

Reads the full grid_features table (every grid, every feature_timestamp
— not just the latest), scores all of it in one vectorized batch using
the ML5 model artifact, and publishes network_risk_scores for the API
(hotspots/alerts) to surface. Also prints a top-20 operational
attention report.
"""

import sqlite3
import os
import numpy as np
import pandas as pd
import joblib

DB_PATH = os.environ.get(
    "WAREHOUSE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "phase3", "warehouse", "network_warehouse.db")
)

MODEL_ARTIFACT_PATH = os.path.join(os.path.dirname(__file__), "model_artifacts", "risk_model.joblib")

RISK_SCORES_DDL = """
CREATE TABLE IF NOT EXISTS network_risk_scores (
    grid_id        INTEGER NOT NULL,
    timestamp      TEXT NOT NULL,
    risk_score     REAL,
    risk_level     TEXT,
    model_version  TEXT,
    PRIMARY KEY (grid_id, timestamp)
);
"""


def get_connection(db_path=None):
    path = db_path or DB_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"Warehouse database not found at {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def load_model_artifact(path=None):
    path = path or MODEL_ARTIFACT_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Model artifact not found at {path}. Run phase6/ml/train_risk_model.py first."
        )
    return joblib.load(path)


def load_feature_table(conn) -> pd.DataFrame:
    """Reads the ENTIRE grid_features table — every grid, every
    feature_timestamp — not just the latest row per grid. ML6 scores
    all grid/time windows, matching the batch-inference goal."""
    return pd.read_sql_query(
        "SELECT * FROM grid_features ORDER BY grid_id, feature_timestamp",
        conn
    )


def score_all(features_df: pd.DataFrame, artifact: dict) -> pd.DataFrame:
    """Vectorized batch scoring — one predict_proba call for every
    row, not a per-grid loop."""
    model = artifact["model"]
    feature_cols = artifact["feature_cols"]
    thresholds = artifact["risk_level_thresholds"]
    model_version = artifact["model_version"]

    missing = features_df[feature_cols].isnull().any(axis=1)
    if missing.any():
        print(f"WARNING: dropping {missing.sum()} rows with null features before scoring")
    clean = features_df[~missing].copy()

    X = clean[feature_cols]
    risk_scores = model.predict_proba(X)[:, 1]

    result = clean[["grid_id", "feature_timestamp"]].rename(columns={"feature_timestamp": "timestamp"})
    result["risk_score"] = risk_scores
    result["risk_level"] = np.select(
        [risk_scores >= thresholds["high"], risk_scores >= thresholds["medium"]],
        ["high", "medium"],
        default="low"
    )
    result["model_version"] = model_version
    return result


def persist_risk_scores(scored_df: pd.DataFrame, db_path: str = None):
    conn = sqlite3.connect(db_path or DB_PATH)
    try:
        conn.executescript(RISK_SCORES_DDL)  # safety net if DE6 hasn't been rerun since this table was added
        conn.execute("DELETE FROM network_risk_scores")  # rebuild fresh each run, same pattern as grid_features
        scored_df.to_sql("network_risk_scores", conn, if_exists="append", index=False)
        conn.commit()
        print(f"Persisted {len(scored_df)} rows to network_risk_scores")
    finally:
        conn.close()


def top_twenty_report(scored_df: pd.DataFrame) -> pd.DataFrame:
    """Top-20 operational attention report: latest score per grid,
    ranked by risk_score descending."""
    latest = (
        scored_df.sort_values("timestamp")
        .groupby("grid_id", as_index=False)
        .tail(1)
    )
    top20 = latest.sort_values("risk_score", ascending=False).head(20)
    print("\n=== Top 20 Grids by Current Risk Score ===")
    print(top20[["grid_id", "timestamp", "risk_score", "risk_level"]].to_string(index=False))
    return top20


def run_batch_scoring(db_path: str = None) -> dict:
    """Callable entry point — used both by `python batch_score.py`
    directly and by the Airflow DAG task."""
    artifact = load_model_artifact()
    conn = get_connection(db_path)
    try:
        features_df = load_feature_table(conn)
    finally:
        conn.close()

    print(f"Scoring {len(features_df)} feature rows across {features_df['grid_id'].nunique()} grids...")
    scored_df = score_all(features_df, artifact)

    persist_risk_scores(scored_df, db_path)
    top20 = top_twenty_report(scored_df)

    return {
        "rows_scored": len(scored_df),
        "grids_scored": scored_df["grid_id"].nunique(),
        "top20_grid_ids": top20["grid_id"].tolist(),
    }


if __name__ == "__main__":
    run_batch_scoring()