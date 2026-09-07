"""
features.py — Engineer network activity features from the warehouse's
fact_network_activity table.

WINDOW RULE (applies to every feature, no exceptions): features for a
prediction about interval t+1 are computed ONLY from the trailing
24-hour window ending at t (inclusive of t, nothing after it).
feature_timestamp records t.
"""

import sqlite3
import os
import pandas as pd
import numpy as np

DB_PATH = os.environ.get(
    "WAREHOUSE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "phase3", "warehouse", "network_warehouse.db")
)



def get_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Warehouse database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_hourly_activity(conn):
    """Load all grid/hour activity, ordered by grid then time — the
    only data source features are ever computed from."""
    df = pd.read_sql_query("""
        SELECT f.grid_id, t.timestamp, f.total_activity, f.internet_activity
        FROM fact_network_activity f
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY f.grid_id, t.timestamp
    """, conn, parse_dates=["timestamp"])
    return df


def compute_features_for_grid(grid_df: pd.DataFrame) -> pd.DataFrame:
    """Computes all 6 features for ONE grid's full timeline, using
    pandas' rolling() — vectorized, not a manual per-row loop. Window
    is 24 hours ending at each row's own timestamp (t); nothing after
    t is ever read (rolling windows are backward-looking by
    definition)."""
    grid_df = grid_df.sort_values("timestamp").reset_index(drop=True)
    grid_df = grid_df.set_index("timestamp")

    activity = grid_df["total_activity"]
    internet = grid_df["internet_activity"]

    # Rolling 24h window ending at t (inclusive) — pandas rolling()
    # only ever looks BACKWARD from each row, so this is leakage-safe
    # by construction, same guarantee as the manual version, just fast
    roll = activity.rolling("24h")

    avg_activity = roll.mean()
    active_hours = (activity > 0).rolling("24h").sum().astype(int)
    peak = roll.max()
    std = roll.std(ddof=0).fillna(0.0)

    internet_sum = internet.rolling("24h").sum()
    total_sum = activity.rolling("24h").sum()

    # Prior window: same rolling mean, shifted back 24 hours
    prior_avg = avg_activity.shift(freq="24h").reindex(avg_activity.index)

    # Division-by-zero guards — explicit, vectorized
    peak_ratio = np.where(avg_activity > 0, peak / avg_activity, 0.0)
    variability = np.where(avg_activity > 0, std / avg_activity, 0.0)
    internet_share = np.where(total_sum > 0, internet_sum / total_sum, 0.0)
    activity_growth = np.where(
        (prior_avg.notna()) & (prior_avg > 0),
        (avg_activity / prior_avg) - 1,
        0.0
    )

    result = pd.DataFrame({
        "feature_timestamp": grid_df.index,
        "avg_activity": avg_activity.values,
        "activity_growth": activity_growth,
        "active_hours": active_hours.values,
        "peak_ratio": peak_ratio,
        "variability": variability,
        "internet_share": internet_share,
    })
    return result

def build_feature_table() -> pd.DataFrame:
    conn = get_connection()
    try:
        hourly_df = load_hourly_activity(conn)
    finally:
        conn.close()

    all_features = []
    for grid_id, grid_df in hourly_df.groupby("grid_id"):
        feat_df = compute_features_for_grid(grid_df[["timestamp", "total_activity", "internet_activity"]])
        feat_df["grid_id"] = grid_id
        all_features.append(feat_df)

    result = pd.concat(all_features, ignore_index=True)
    result["feature_timestamp"] = result["feature_timestamp"].astype(str)
    return result[["grid_id", "feature_timestamp", "avg_activity", "activity_growth",
                    "active_hours", "peak_ratio", "variability", "internet_share"]]


def persist_feature_table(df: pd.DataFrame, db_path: str = None):
    """Writes into the grid_features table already created (empty) in
    DE6_build_warehouse.py — API4 reads from this exact table."""
    conn = sqlite3.connect(db_path or DB_PATH)
    try:
        conn.execute("DELETE FROM grid_features")  # rebuild fresh each run
        df.to_sql("grid_features", conn, if_exists="append", index=False)
        conn.commit()
        print(f"Persisted {len(df)} feature rows to grid_features")
    finally:
        conn.close()


if __name__ == "__main__":
    features_df = build_feature_table()
    print(f"Computed features for {features_df['grid_id'].nunique()} grids, "
          f"{len(features_df)} total rows")
    persist_feature_table(features_df)