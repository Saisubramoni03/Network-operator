"""
test_features.py — Required leakage test: must PASS on the real
implementation, and must FAIL when a feature is deliberately allowed
to see data from after feature_timestamp (t+1).
"""

import pandas as pd
import numpy as np
from features import compute_features_for_grid


def make_test_series():
    """24 hours of low activity (0-1), followed by ONE hour (t+1) of a
    huge spike (1000) — if a feature leaks t+1, avg_activity/peak_ratio
    at the row for 't' would be contaminated by that spike."""
    timestamps = pd.date_range("2013-11-01 00:00:00", periods=25, freq="h")
    activity = [0.5] * 24 + [1000.0]  # last value is t+1, the future spike
    df = pd.DataFrame({
        "timestamp": timestamps,
        "total_activity": activity,
        "internet_activity": [a * 0.5 for a in activity],
    })
    return df


def test_no_leakage_correct_implementation():
    """This MUST pass: features at row 't' (the 24th row, index 23)
    must NOT reflect the t+1 spike."""
    df = make_test_series()
    features = compute_features_for_grid(df)

    t_row = features.iloc[23]  # the row for 't' — the last "normal" hour
    assert t_row["avg_activity"] < 10, (
        f"LEAKAGE DETECTED: avg_activity={t_row['avg_activity']} is contaminated "
        f"by the t+1 spike — features must not see beyond feature_timestamp."
    )
    print("PASS: no leakage in the real implementation.")


def test_leakage_detected_when_deliberately_broken():
    """This test PROVES the leakage test itself works, by deliberately
    breaking the window to include t+1, and confirming the assertion
    then correctly FAILS."""
    df = make_test_series()

    # Deliberately broken version: window includes one extra hour (t+1)
    def broken_compute(grid_df):
        grid_df = grid_df.sort_values("timestamp").reset_index(drop=True).set_index("timestamp")
        results = []
        for t in grid_df.index:
            window = grid_df.loc[t - pd.Timedelta(hours=23):t + pd.Timedelta(hours=1)]  # LEAK: +1 hour
            results.append({"feature_timestamp": t, "avg_activity": window["total_activity"].mean()})
        return pd.DataFrame(results)

    broken_features = broken_compute(df)
    t_row = broken_features.iloc[23]

    leaked = t_row["avg_activity"] >= 10
    assert leaked, "Expected the deliberately-broken version to show contamination, but it didn't."
    print(f"CONFIRMED: broken implementation leaks t+1 (avg_activity={t_row['avg_activity']:.2f}), "
          f"proving this test correctly detects leakage when it occurs.")


if __name__ == "__main__":
    test_no_leakage_correct_implementation()
    test_leakage_detected_when_deliberately_broken()
    print("\nAll leakage tests passed — real implementation is clean, "
          "and the test correctly catches a broken one.")