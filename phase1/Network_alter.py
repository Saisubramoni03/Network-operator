import pandas as pd
import numpy as np
import os

# ------------------------------------------------------------------
# Step 1: Load the grid/hour analytics table exported from NP2
# ------------------------------------------------------------------
df = pd.read_csv("../data/processed/grid_hour_activity.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

# ------------------------------------------------------------------
# Step 2: Within-day baseline (vectorized, no double loop)
# ------------------------------------------------------------------
def compute_within_day_baseline(df):
    result = df.copy()

    def leave_one_out_median(group):
        values = group["total_activity"].values
        baselines = np.empty(len(values))
        for i in range(len(values)):
            baselines[i] = np.median(np.delete(values, i))
        return pd.Series(baselines, index=group.index)

    result["baseline_activity"] = (
        df.groupby("grid_id", group_keys=False)
          .apply(leave_one_out_median)
    )
    result = result.rename(columns={"total_activity": "current_activity"})
    return result[["grid_id", "timestamp", "hour", "current_activity", "baseline_activity"]]

baseline_df = compute_within_day_baseline(df)

# ------------------------------------------------------------------
# Step 3: Activity floor
# ------------------------------------------------------------------
daily_totals = df.groupby("grid_id")["total_activity"].sum()
activity_floor = daily_totals.quantile(0.10)
print(f"Activity floor (10th percentile of daily grid totals): {activity_floor:.2f}")

# ------------------------------------------------------------------
# Step 4: Thresholds
# ------------------------------------------------------------------
HIGH_ACTIVITY_RATIO = 1.5
SPIKE_RATIO = 1.8
DROP_RATIO = 0.5

# ------------------------------------------------------------------
# Step 5: Classify alerts
# ------------------------------------------------------------------
def classify_alerts(baseline_df, df, floor):
    alerts = []
    df_sorted = df.sort_values(["grid_id", "hour"])

    for grid_id, group in df_sorted.groupby("grid_id"):
        group = group.reset_index(drop=True)

        if group["total_activity"].sum() < floor:
            continue

        for i, row in group.iterrows():
            baseline_row = baseline_df[
                (baseline_df["grid_id"] == grid_id) & (baseline_df["hour"] == row["hour"])
            ]
            if baseline_row.empty:
                continue

            baseline = baseline_row["baseline_activity"].values[0]
            current = row["total_activity"]

            if baseline == 0:
                continue

            if current >= HIGH_ACTIVITY_RATIO * baseline:
                alerts.append({
                    "grid_id": grid_id, "timestamp": row["timestamp"], "alert_type": "HIGH_ACTIVITY",
                    "current_activity": current, "baseline_activity": baseline,
                    "reason": f"Current activity {current:.1f} is {(current/baseline):.1f}x the grid's within-day baseline of {baseline:.1f}"
                })

            if i > 0:
                prev_activity = group.loc[i - 1, "total_activity"]
                if prev_activity > 0 and current >= SPIKE_RATIO * prev_activity:
                    alerts.append({
                        "grid_id": grid_id, "timestamp": row["timestamp"], "alert_type": "ACTIVITY_SPIKE",
                        "current_activity": current, "baseline_activity": prev_activity,
                        "reason": f"Activity jumped {(current/prev_activity):.1f}x versus the immediately preceding hour ({prev_activity:.1f})"
                    })

            if current <= DROP_RATIO * baseline:
                alerts.append({
                    "grid_id": grid_id, "timestamp": row["timestamp"], "alert_type": "ACTIVITY_DROP",
                    "current_activity": current, "baseline_activity": baseline,
                    "reason": f"Current activity {current:.1f} is only {(current/baseline):.1f}x the grid's within-day baseline of {baseline:.1f}"
                })

    return pd.DataFrame(alerts)

alerts_df = classify_alerts(baseline_df, df, activity_floor)

# ------------------------------------------------------------------
# Step 6: Alert rate check
# ------------------------------------------------------------------
total_grid_hours = len(df)
alert_rate = len(alerts_df) / total_grid_hours

print(f"\nProportion of grid/hours that alerted: {alert_rate:.2%}")
if alert_rate > 0.15:
    print("⚠️ Alert rate looks too high — thresholds likely need tightening before sign-off.")

# ------------------------------------------------------------------
# Step 7: Operational summary
# ------------------------------------------------------------------
print("\nAlerts by type:")
print(alerts_df["alert_type"].value_counts())

print("\nTop 10 grids by alert count:")
print(alerts_df["grid_id"].value_counts().head(10))

# ------------------------------------------------------------------
# Step 8: Export
# ------------------------------------------------------------------
os.makedirs("../data/processed", exist_ok=True)
alerts_df.to_csv("../data/processed/network_alerts.csv", index=False)
print("\nSaved to ../data/processed/network_alerts.csv")

# ------------------------------------------------------------------
# Step 9: Manual verification
# ------------------------------------------------------------------
if not alerts_df.empty:
    check_grid = alerts_df.iloc[0]["grid_id"]
    check_rows = df[df["grid_id"] == check_grid][["hour", "total_activity"]]
    print(f"\nManual check for grid {check_grid}:")
    print(check_rows)
else:
    print("\nNo alerts generated — review thresholds if this is unexpected.")

# ------------------------------------------------------------------
# Step 10: Written limitation statement
# ------------------------------------------------------------------
limitation_statement = """
LIMITATION STATEMENT — Within-Day Baseline

This baseline compares each hour only against the same grid's own median
activity across the rest of the same day. It has no concept of what is
"normal" for a specific hour of day — it cannot distinguish "this grid is
always quiet at 03:00" from "this grid's activity just dropped."

Building a true hour-of-day baseline requires multiple days of history so
each (grid, hour-of-day) bucket has more than one observation. That
capability arrives at ML4, once Phase 2 (Spark) and Phase 3 (Data
Engineering) have accumulated multi-day history.
"""
print(limitation_statement)

os.makedirs("../docs", exist_ok=True)
with open("../docs/np3_baseline_limitations.md", "w") as f:
    f.write(limitation_statement)