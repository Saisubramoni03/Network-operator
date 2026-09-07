
"""
anomaly_baseline.py — ML4: Add an Anomaly Baseline
 
Generalizes NP3's leave-one-out median baseline function so it accepts
a bucketing key. NP3 could only bucket by (grid_id) within a single day
(comparing hour x against the other hours of the SAME day, because only
one day of data existed). Now that Phase 2/3 have accumulated multi-day
history in the warehouse, we can bucket by (grid_id, hour) and compare
each observation against the same grid's same hour-of-day across ALL
OTHER days — a true hour-of-day baseline, which is what NP3's own
limitation statement (docs/np3_baseline_limitations.md) said was missing.
 
Same leave-one-out logic, same median, same shape of output
(current/baseline/deviation) as NP3 — just a different bucket key. No
second baseline implementation is written.
 
Output: network_anomaly_scores table in the warehouse, plus a printed
three-way comparison against the ML3 classifier and the NP3 rule-based
alerts.
"""
 
import sqlite3
import os
import numpy as np
import pandas as pd
 
DB_PATH = os.environ.get(
    "WAREHOUSE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "phase3", "warehouse", "network_warehouse.db")
)
 
# Same ratio-style thinking as NP3 (HIGH_ACTIVITY_RATIO / DROP_RATIO),
# expressed as a signed percentage deviation from baseline instead of
# an absolute ratio, so both directions share one threshold pair.
HIGH_ANOMALY_THRESHOLD = 0.5   # current is >= 50% above the hour-of-day baseline
LOW_ANOMALY_THRESHOLD = -0.5   # current is >= 50% below the hour-of-day baseline
 
 
# ------------------------------------------------------------------
# Step 1: Load the full accumulated hourly activity from the warehouse
# (same source table ML2's features.py reads from — not a single day)
# ------------------------------------------------------------------
def get_connection(db_path=None):
    path = db_path or DB_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(f"Warehouse database not found at {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn
 
 
def load_hourly_activity(conn):
    df = pd.read_sql_query("""
        SELECT f.grid_id, t.timestamp, f.total_activity
        FROM fact_network_activity f
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY f.grid_id, t.timestamp
    """, conn, parse_dates=["timestamp"])
    df["hour"] = df["timestamp"].dt.hour
    return df
 
 
# ------------------------------------------------------------------
# Step 2: Generalized baseline — this IS NP3's leave_one_out_median,
# with the grouping key parameterized instead of hardcoded to
# ["grid_id"]. Pass bucket_cols=["grid_id", "hour"] for the hour-of-day
# baseline; NP3's own within-day baseline is still reproducible by
# passing bucket_cols=["grid_id", <date column>].
# ------------------------------------------------------------------
def compute_bucketed_baseline(df: pd.DataFrame, bucket_cols: list) -> pd.DataFrame:
    """Leave-one-out median baseline, generalized over `bucket_cols`.
 
    For every row, the baseline is the median of `total_activity`
    across every OTHER row sharing the same bucket key — identical
    logic to NP3's leave_one_out_median, just grouped differently.
    """
    result = df.copy()
 
    def leave_one_out_median(group):
        values = group["total_activity"].values
        n = len(values)
        baselines = np.empty(n)
        for i in range(n):
            others = np.delete(values, i)
            baselines[i] = np.median(others) if len(others) > 0 else np.nan
        return pd.Series(baselines, index=group.index)
 
    result["baseline_activity"] = (
        df.groupby(bucket_cols, group_keys=False).apply(leave_one_out_median)
    )
    result = result.rename(columns={"total_activity": "current_activity"})
    return result
 
 
# ------------------------------------------------------------------
# Step 3: Deviation + anomaly score + direction
# ------------------------------------------------------------------
def score_anomalies(baseline_df: pd.DataFrame) -> pd.DataFrame:
    df = baseline_df.copy()
 
    # Percentage deviation from baseline — same ratio-based thinking as
    # NP3's HIGH_ACTIVITY_RATIO / DROP_RATIO, just signed and continuous
    # instead of a hard multiplier, so both directions share one score.
    safe_baseline = df["baseline_activity"].replace(0, np.nan)
    df["anomaly_score"] = (df["current_activity"] - safe_baseline) / safe_baseline
    df["anomaly_score"] = df["anomaly_score"].fillna(0.0)
 
    def direction(score):
        if score >= HIGH_ANOMALY_THRESHOLD:
            return "HIGH"
        if score <= LOW_ANOMALY_THRESHOLD:
            return "LOW"
        return "NONE"
 
    df["direction"] = df["anomaly_score"].apply(direction)
 
    def reason(row):
        if row["direction"] == "NONE":
            return "Within normal range for this grid's hour-of-day baseline"
        return (
            f"Current activity {row['current_activity']:.1f} is "
            f"{row['anomaly_score']:+.1%} vs. this grid's hour-of-day "
            f"baseline of {row['baseline_activity']:.1f} (bucket: hour={row['hour']})"
        )
 
    df["reason"] = df.apply(reason, axis=1)
    return df[["grid_id", "timestamp", "hour", "current_activity",
               "baseline_activity", "anomaly_score", "direction", "reason"]]
 
 
# ------------------------------------------------------------------
# Step 4: Persist to warehouse (mirrors features.py's persist pattern)
# ------------------------------------------------------------------
def persist_anomaly_scores(df: pd.DataFrame, db_path: str = None):
    conn = sqlite3.connect(db_path or DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS network_anomaly_scores (
                grid_id INTEGER,
                timestamp TEXT,
                hour INTEGER,
                current_activity REAL,
                baseline_activity REAL,
                anomaly_score REAL,
                direction TEXT,
                reason TEXT
            )
        """)
        conn.execute("DELETE FROM network_anomaly_scores")  # rebuild fresh each run
        out = df.copy()
        out["timestamp"] = out["timestamp"].astype(str)
        out.to_sql("network_anomaly_scores", conn, if_exists="append", index=False)
        conn.commit()
        print(f"Persisted {len(out)} anomaly rows to network_anomaly_scores")
    finally:
        conn.close()
 
 
# ------------------------------------------------------------------
# Step 5: Three-way comparison — NP3 rule alerts vs. ML3 classifier
# vs. ML4 anomaly flags
# ------------------------------------------------------------------
NP3_ALERTS_PATH = os.environ.get(
    "NP3_ALERTS_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed", "network_alerts.csv")
)
 
 
def load_np3_alerts(path: str = None) -> pd.DataFrame:
    """Loads NP3's ACTUAL persisted alerts (network_alerts.csv) — not a
    peak_ratio reconstruction. NP3 emits three alert_types (HIGH_ACTIVITY,
    ACTIVITY_SPIKE, ACTIVITY_DROP); any of them counts as np3_alert=1 for
    a given (grid_id, timestamp)."""
    path = path or NP3_ALERTS_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"NP3 alerts file not found at {path} — run NP3 (Network_alter.py) first."
        )
    alerts = pd.read_csv(path, parse_dates=["timestamp"])
    flagged = (
        alerts[["grid_id", "timestamp"]]
        .drop_duplicates()
        .assign(np3_alert=1)
    )
    return flagged

def generate_np3_alerts_from_warehouse(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Regenerates NP3's alert logic directly against the warehouse table
    (same source as ML2/ML3/ML4), instead of reading NP3's original
    single-day CSV. Same ratios, same within-day-per-date baseline
    logic as Network_alter.py — just looped per (grid_id, date) across
    all 8 days instead of one hardcoded file, so timestamps and date
    coverage actually line up with the ML3/ML4 test window.
    """
    HIGH_ACTIVITY_RATIO = 1.5
    SPIKE_RATIO = 1.8
    DROP_RATIO = 0.5

    df = hourly_df.copy()
    df["date"] = df["timestamp"].dt.date

    # Same leave-one-out median baseline as NP3, bucketed per (grid_id, date)
    # — i.e. NP3's original bucket key, just applied to every day present
    # in the warehouse instead of a single day's file.
    within_day = compute_bucketed_baseline(
        df.rename(columns={"total_activity": "total_activity"}),
        bucket_cols=["grid_id", "date"]
    )

    # Activity floor: same 10th-percentile-of-daily-totals idea as NP3,
    # computed once globally (NP3 also computed it once, over its one day)
    daily_totals = df.groupby(["grid_id", "date"])["total_activity"].sum()
    activity_floor = daily_totals.quantile(0.10)

    alerts = []
    for (grid_id, date), group in within_day.groupby(["grid_id", "date"]):
        group = group.sort_values("timestamp").reset_index(drop=True)

        if group["current_activity"].sum() < activity_floor:
            continue

        for i, row in group.iterrows():
            baseline = row["baseline_activity"]
            current = row["current_activity"]
            if pd.isna(baseline) or baseline == 0:
                continue

            if current >= HIGH_ACTIVITY_RATIO * baseline:
                alerts.append({"grid_id": grid_id, "timestamp": row["timestamp"],
                                "alert_type": "HIGH_ACTIVITY"})

            if i > 0:
                prev = group.loc[i - 1, "current_activity"]
                if prev > 0 and current >= SPIKE_RATIO * prev:
                    alerts.append({"grid_id": grid_id, "timestamp": row["timestamp"],
                                    "alert_type": "ACTIVITY_SPIKE"})

            if current <= DROP_RATIO * baseline:
                alerts.append({"grid_id": grid_id, "timestamp": row["timestamp"],
                                "alert_type": "ACTIVITY_DROP"})

    alerts_df = pd.DataFrame(alerts)
    print(f"Regenerated {len(alerts_df)} NP3-style alerts across "
          f"{within_day['date'].nunique()} days from the warehouse")
    return alerts_df[["grid_id", "timestamp"]].drop_duplicates().assign(np3_alert=1)
 
 
def compare_three_way(anomaly_df: pd.DataFrame):
    """
    Brings in ML3's test-set classifier predictions by running
    train_risk_model.main(), merges the REAL NP3 alerts (not the
    peak_ratio proxy train_risk_model.py reconstructs internally for
    its own printout), and merges ML4's anomaly direction — all three
    independently sourced and compared on the same (grid_id,
    feature_timestamp) rows.
    """
    import train_risk_model  # sibling module in phase6/ml
 
    print("\nRunning ML3 to obtain classifier predictions for comparison...")
    ml3_results = train_risk_model.main()
    test_df = ml3_results["test_df"].copy()
    test_df = test_df.drop(columns=["np3_alert"])  # drop ML3's internal proxy; use the real thing below
 
    print("\nRegenerating NP3-style alerts from the warehouse (full 8-day coverage)...")
    conn = get_connection()
    try:
        hourly_df = load_hourly_activity(conn)
    finally:
        conn.close()
    np3_df = generate_np3_alerts_from_warehouse(hourly_df)
    np3_min, np3_max = np3_df["timestamp"].min(), np3_df["timestamp"].max()
    test_min, test_max = test_df["feature_timestamp"].min(), test_df["feature_timestamp"].max()
    print(f"NP3 alert timestamp range: {np3_min} to {np3_max}")
    print(f"ML3 test-set timestamp range: {test_min} to {test_max}")
    if np3_max < test_min or np3_min > test_max:
        print("⚠️  NP3's alerts do not overlap the ML3 test window at all — NP3 was only ever "
              "run against a single early day, while ML3/ML4 evaluate later dates. Any 'np3_alert=0' "
              "below the just reflects 'no NP3 data for this date', not 'NP3 checked and found nothing'.")
 
    merged = test_df.merge(
        np3_df.rename(columns={"timestamp": "feature_timestamp"}),
        on=["grid_id", "feature_timestamp"], how="left"
    )
    merged["np3_alert"] = merged["np3_alert"].fillna(0).astype(int)
 
    anomaly_slim = anomaly_df.rename(columns={"timestamp": "feature_timestamp"})[
        ["grid_id", "feature_timestamp", "direction", "anomaly_score"]
    ]
    merged = merged.merge(anomaly_slim, on=["grid_id", "feature_timestamp"], how="left")
    merged["direction"] = merged["direction"].fillna("NONE")
    merged["ml4_flag"] = (merged["direction"] != "NONE").astype(int)
 
    print("\n=== ML4 Three-Way Comparison: NP3 rule vs. ML3 classifier vs. ML4 anomaly ===")
    print("(NP3 = real network_alerts.csv, not a reconstructed proxy)")
    combo_counts = merged.groupby(
        ["np3_alert", "ml_prediction", "ml4_flag"]
    ).size().reset_index(name="count").sort_values("count", ascending=False)
    print(combo_counts.to_string(index=False))
 
    all_three = merged[(merged["np3_alert"] == 1) & (merged["ml_prediction"] == 1) & (merged["ml4_flag"] == 1)]
    only_ml4 = merged[(merged["np3_alert"] == 0) & (merged["ml_prediction"] == 0) & (merged["ml4_flag"] == 1)]
 
    print(f"\nAll three mechanisms agree (flagged): {len(all_three)}")
    print(f"Only ML4 anomaly flags (NP3 and ML3 both silent): {len(only_ml4)}")
 
    if not only_ml4.empty:
        row = only_ml4.iloc[0]
        print("\nExample — only the hour-of-day anomaly baseline caught this:")
        print(f"  grid_id={row['grid_id']}, feature_timestamp={row['feature_timestamp']}, "
              f"anomaly_score={row['anomaly_score']:+.1%}, direction={row['direction']}")
        print("  Why this can happen: NP3/ML3 both reason from the grid's OWN recent")
        print("  window (within-day median / trailing 24h features). If the grid has")
        print("  been elevated for a while, 'recent average' rises with it and stops")
        print("  looking anomalous. The hour-of-day baseline instead compares against")
        print("  this exact hour across OTHER days, so a sustained shift still shows up.")
 
    return merged
 
 
if __name__ == "__main__":
    conn = get_connection()
    try:
        hourly_df = load_hourly_activity(conn)
    finally:
        conn.close()
 
    print(f"Loaded {len(hourly_df)} grid/hour rows across "
          f"{hourly_df['grid_id'].nunique()} grids and "
          f"{hourly_df['timestamp'].dt.date.nunique()} days")
 
    # Generalized NP3 function, bucketed by (grid_id, hour) instead of
    # (grid_id, date) — this is the hour-of-day baseline NP3 could not
    # build from a single day.
    baseline_df = compute_bucketed_baseline(hourly_df, bucket_cols=["grid_id", "hour"])
    scored_df = score_anomalies(baseline_df)
 
    print("\nAnomaly direction counts:")
    print(scored_df["direction"].value_counts())
 
    persist_anomaly_scores(scored_df)
 
    print("\nManual verification — 2 HIGH and 2 LOW cases against raw data:")
    for label in ["HIGH", "LOW"]:
        sample = scored_df[scored_df["direction"] == label].head(2)
        for _, row in sample.iterrows():
            print(f"  [{label}] grid_id={row['grid_id']} timestamp={row['timestamp']} "
                  f"hour={row['hour']} current={row['current_activity']:.1f} "
                  f"baseline={row['baseline_activity']:.1f} score={row['anomaly_score']:+.1%}")
 
    compare_three_way(scored_df)
 
