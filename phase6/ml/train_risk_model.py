"""
train_risk_model.py — Train a simple Logistic Regression risk
classifier on grid_features, using a chronological train/test split
and a leakage-safe, training-period-only threshold for labels.
"""

import sqlite3
import os
import pandas as pd
import numpy as np
import joblib  # >>> ML5: needed to persist the trained model

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "phase3", "warehouse", "network_warehouse.db")

FEATURE_COLS = ["avg_activity", "activity_growth", "active_hours",
                "peak_ratio", "variability", "internet_share"]

# >>> ML5: where the trained model gets persisted for the API to load
MODEL_ARTIFACT_PATH = os.path.join(os.path.dirname(__file__), "model_artifacts", "risk_model.joblib")
MODEL_VERSION = "logreg-balanced-v1"
RISK_LEVEL_THRESHOLDS = {"medium": 0.3, "high": 0.6}  # risk_score cutoffs used by the API


# >>> ML5: new function — saves model + everything the API needs to use it safely
def save_model_artifact(model, feature_cols, path=None):
    path = path or MODEL_ARTIFACT_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    artifact = {
        "model": model,
        "feature_cols": feature_cols,
        "model_version": MODEL_VERSION,
        "risk_level_thresholds": RISK_LEVEL_THRESHOLDS,
        "trained_at": pd.Timestamp.now("UTC").isoformat(),    }
    joblib.dump(artifact, path)
    print(f"Saved model artifact to {path} (version={MODEL_VERSION})")


def load_features_and_raw_activity(conn):
    features = pd.read_sql_query(
        "SELECT * FROM grid_features ORDER BY grid_id, feature_timestamp",
        conn, parse_dates=["feature_timestamp"]
    )
    raw = pd.read_sql_query("""
        SELECT f.grid_id, t.timestamp, f.total_activity
        FROM fact_network_activity f
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY f.grid_id, t.timestamp
    """, conn, parse_dates=["timestamp"])
    return features, raw


def build_labels(features: pd.DataFrame, raw: pd.DataFrame, train_cutoff: pd.Timestamp) -> pd.DataFrame:
    """For each feature row (grid_id, t), find total_activity at t+1
    and threshold-label it. Threshold is computed per-grid using ONLY
    raw activity strictly before train_cutoff — leakage-safe."""
    raw_indexed = raw.set_index(["grid_id", "timestamp"])["total_activity"]

    # Per-grid threshold from TRAINING PERIOD ONLY
    train_raw = raw[raw["timestamp"] < train_cutoff]
    thresholds = train_raw.groupby("grid_id")["total_activity"].quantile(0.90)

    rows = []
    for _, row in features.iterrows():
        grid_id = row["grid_id"]
        t = row["feature_timestamp"]
        t_plus_1 = t + pd.Timedelta(hours=1)

        try:
            future_activity = raw_indexed.loc[(grid_id, t_plus_1)]
        except KeyError:
            continue  # no t+1 data available (end of dataset) — can't label, skip

        threshold = thresholds.get(grid_id, np.inf)
        label = int(future_activity > threshold)

        row_dict = row.to_dict()
        row_dict["label"] = label
        rows.append(row_dict)

    return pd.DataFrame(rows)


def chronological_split(labeled_df: pd.DataFrame, train_frac: float = 0.7):
    labeled_df = labeled_df.sort_values("feature_timestamp")
    all_dates = sorted(labeled_df["feature_timestamp"].unique())
    cutoff_idx = int(len(all_dates) * train_frac)
    cutoff_timestamp = all_dates[cutoff_idx]

    train_df = labeled_df[labeled_df["feature_timestamp"] < cutoff_timestamp]
    test_df = labeled_df[labeled_df["feature_timestamp"] >= cutoff_timestamp]
    return train_df, test_df, cutoff_timestamp


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        features, raw = load_features_and_raw_activity(conn)
    finally:
        conn.close()

    # Determine chronological cutoff FIRST (needed to compute leakage-safe threshold)
    all_dates = sorted(features["feature_timestamp"].unique())
    cutoff_idx = int(len(all_dates) * 0.7)
    train_cutoff = all_dates[cutoff_idx]

    print(f"Building labels with threshold from training period only (before {train_cutoff})...")
    labeled = build_labels(features, raw, train_cutoff)

    train_df, test_df, actual_cutoff = chronological_split(labeled, train_frac=0.7)

    print("\n=== ML3 Chronological Split Report ===")
    print(f"Train: {train_df['feature_timestamp'].min()} to {train_df['feature_timestamp'].max()} "
          f"({len(train_df)} rows)")
    print(f"Test:  {test_df['feature_timestamp'].min()} to {test_df['feature_timestamp'].max()} "
          f"({len(test_df)} rows)")
    assert train_df["feature_timestamp"].max() < test_df["feature_timestamp"].min(), \
        "LEAKAGE: train and test date ranges overlap!"
    print("Confirmed: train and test date ranges do NOT overlap.")

    X_train, y_train = train_df[FEATURE_COLS], train_df["label"]
    X_test, y_test = test_df[FEATURE_COLS], test_df["label"]

    base_rate_train = y_train.mean()
    base_rate_test = y_test.mean()
    print(f"\nBase rate (positive class) — train: {base_rate_train:.3f}, test: {base_rate_test:.3f}")

    print("\n=== Model A: class_weight='balanced' ===")
    model_balanced = LogisticRegression(max_iter=1000, class_weight="balanced")
    model_balanced.fit(X_train, y_train)
    preds_balanced = model_balanced.predict(X_test)

    print("\n=== Model B: unweighted (default) ===")
    model_unweighted = LogisticRegression(max_iter=1000)
    model_unweighted.fit(X_train, y_train)
    preds_unweighted = model_unweighted.predict(X_test)

    def report(name, y_test, preds):
        acc = accuracy_score(y_test, preds)
        prec = precision_score(y_test, preds, zero_division=0)
        rec = recall_score(y_test, preds, zero_division=0)
        print(f"\n--- {name} ---")
        print(f"Accuracy:  {acc:.3f}")
        print(f"Precision: {prec:.3f}")
        print(f"Recall:    {rec:.3f}")
        return acc, prec, rec

    acc_b, prec_b, rec_b = report("Balanced", y_test, preds_balanced)
    acc_u, prec_u, rec_u = report("Unweighted", y_test, preds_unweighted)

    model = model_balanced  # keep balanced as the primary model going forward
    preds = preds_balanced
    acc, prec, rec = acc_b, prec_b, rec_b
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    prec = precision_score(y_test, preds, zero_division=0)
    rec = recall_score(y_test, preds, zero_division=0)

    print("\n=== ML3 Evaluation Report ===")
    print(f"Base rate (test):  {base_rate_test:.3f}")
    print(f"Accuracy:          {acc:.3f}")
    print(f"Precision:         {prec:.3f}")
    print(f"Recall:            {rec:.3f}")

    if acc > 0.95:
        print("\n⚠️  WARNING: accuracy exceeds 0.95 — per ML1's known trap, this "
              "indicates leakage or a circular label, NOT a good model. Investigate before signing off.")

    print("\n=== Feature Coefficients (Logistic Regression) ===")
    for feat, coef in zip(FEATURE_COLS, model.coef_[0]):
        print(f"  {feat}: {coef:+.4f}")
        # ------------------------------------------------------------------
    # Compare ML predictions against NP3-style rule-based alerts
    # ------------------------------------------------------------------
    print("\n=== Comparison: ML Model vs. NP3 Rule-Based Alert ===")

    test_df = test_df.copy()
    test_df["ml_prediction"] = preds

    # Reconstruct NP3's rule directly: HIGH_ACTIVITY if current avg_activity
    # is already >= 1.5x this grid's own average (same ratio NP3 used)
    test_df["np3_alert"] = (test_df["peak_ratio"] >= 1.5).astype(int)

    both_flag = ((test_df["ml_prediction"] == 1) & (test_df["np3_alert"] == 1)).sum()
    only_ml = ((test_df["ml_prediction"] == 1) & (test_df["np3_alert"] == 0)).sum()
    only_np3 = ((test_df["ml_prediction"] == 0) & (test_df["np3_alert"] == 1)).sum()
    neither = ((test_df["ml_prediction"] == 0) & (test_df["np3_alert"] == 0)).sum()

    print(f"Both flag:        {both_flag}")
    print(f"Only ML flags:    {only_ml}")
    print(f"Only NP3 flags:   {only_np3}")
    print(f"Neither flags:    {neither}")

    # Show one concrete disagreement case
    disagreement = test_df[(test_df["ml_prediction"] == 1) & (test_df["np3_alert"] == 0)].head(1)
    if not disagreement.empty:
        row = disagreement.iloc[0]
        print(f"\nExample disagreement (ML flags, NP3 doesn't):")
        print(f"  grid_id={row['grid_id']}, timestamp={row['feature_timestamp']}, "
              f"variability={row['variability']:.2f}, activity_growth={row['activity_growth']:.2f}, "
              f"peak_ratio={row['peak_ratio']:.2f}")

    # >>> ML5: persist the trained model + metadata for the API to load
    save_model_artifact(model, FEATURE_COLS)

    return {
        "model": model,
        "train_df": train_df,
        "test_df": test_df,
        "test_preds": preds,
        "base_rate_test": base_rate_test,
        "accuracy": acc,
        "precision": prec,
        "recall": rec,
    }


if __name__ == "__main__":
    main()


    