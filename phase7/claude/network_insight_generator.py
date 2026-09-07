"""
network_insight_generator.py — C1: Claude API — Network Insight Generator

Pulls a curated grid evidence object from the warehouse (grid_features,
network_anomaly_scores from ML4, network_risk_scores from ML6) and
calls the Claude API to turn it into an operations-friendly
explanation: SEVERITY, EVIDENCE, INTERPRETATION, INVESTIGATION STEPS.

Hard rule enforced in the prompt: every number in EVIDENCE must appear
in the input. If a field is missing, Claude must say so explicitly —
never invent a value.
"""

import os
import sqlite3
import time
import json
from anthropic import Anthropic

DB_PATH = os.environ.get(
    "WAREHOUSE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "phase3", "warehouse", "network_warehouse.db")
)

client = Anthropic()  # reads ANTHROPIC_API_KEY from environment

SYSTEM_PROMPT = """You are a network operations analyst assistant. You will be given a
structured JSON evidence object describing a single grid cell's recent activity.

Respond in exactly this structure, using these four headers verbatim:

SEVERITY: one word — low, medium, or high.

EVIDENCE:
List only the fields present in the input JSON, with their exact values. Do not
calculate, round differently, or introduce any number that is not present in the
input. If a field is null or missing, write "not available" for that field — do
not guess or infer what it might be.

INTERPRETATION:
Your plain-language read of what the evidence suggests is happening. This is
your judgment, not raw data — keep it clearly separate from EVIDENCE. If key
fields are missing, explicitly state that your interpretation is limited by
the missing data, and say which fields are missing.

INVESTIGATION STEPS:
2-4 concrete next checks a NOC engineer should do, grounded in what's actually
missing or anomalous — not generic advice.

Critical rule: if the evidence object is missing fields needed to support a
claim, say so explicitly in INTERPRETATION rather than filling the gap with
an assumption. Never invent a number that isn't in the input."""


# ------------------------------------------------------------------
# Step 1: Build the curated evidence object from the warehouse
# ------------------------------------------------------------------
def get_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Warehouse database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def build_grid_evidence(grid_id: int, drop_fields: list = None) -> dict:
    """Assembles the curated grid evidence object: current activity,
    baseline, growth, peak_ratio, variability, anomaly score — pulled
    from the ML2 feature table, ML4's anomaly scores, and ML6's risk
    scores. `drop_fields` lets you simulate missing evidence for the
    insufficient-evidence test."""
    drop_fields = drop_fields or []
    conn = get_connection()
    try:
        feature_row = conn.execute("""
            SELECT feature_timestamp, avg_activity, activity_growth,
                   active_hours, peak_ratio, variability, internet_share
            FROM grid_features
            WHERE grid_id = ?
            ORDER BY feature_timestamp DESC
            LIMIT 1
        """, (grid_id,)).fetchone()

        anomaly_row = conn.execute("""
            SELECT timestamp, current_activity, baseline_activity, anomaly_score, direction, reason
            FROM network_anomaly_scores
            WHERE grid_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (grid_id,)).fetchone()

        risk_row = conn.execute("""
            SELECT timestamp, risk_score, risk_level, model_version
            FROM network_risk_scores
            WHERE grid_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (grid_id,)).fetchone()
    finally:
        conn.close()

    evidence = {"grid_id": grid_id}

    if feature_row:
        evidence["feature_timestamp"] = feature_row["feature_timestamp"]
        evidence["avg_activity"] = feature_row["avg_activity"]
        evidence["activity_growth"] = feature_row["activity_growth"]
        evidence["peak_ratio"] = feature_row["peak_ratio"]
        evidence["variability"] = feature_row["variability"]

    if anomaly_row:
        evidence["current_activity"] = anomaly_row["current_activity"]
        evidence["baseline_activity"] = anomaly_row["baseline_activity"]
        evidence["anomaly_score"] = anomaly_row["anomaly_score"]
        evidence["anomaly_direction"] = anomaly_row["direction"]

    if risk_row:
        evidence["risk_score"] = risk_row["risk_score"]
        evidence["risk_level"] = risk_row["risk_level"]
        evidence["model_version"] = risk_row["model_version"]

    for field in drop_fields:
        evidence.pop(field, None)

    return evidence


# ------------------------------------------------------------------
# Step 2: Call the Claude API with the evidence object
# ------------------------------------------------------------------
def generate_insight(evidence: dict, model: str = "claude-sonnet-5") -> dict:
    """Calls the Claude API with the evidence object. Returns the
    response text plus latency and token usage, so callers can compare
    models on cost/latency/reasoning depth (per the lab's activities)."""
    user_message = (
        "Here is the grid evidence object:\n\n"
        f"{json.dumps(evidence, indent=2, default=str)}"
    )

    start = time.time()
    response = client.messages.create(
        model=model,
        max_tokens=600,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )
    elapsed = time.time() - start

    text = "".join(block.text for block in response.content if block.type == "text")

    return {
        "model": model,
        "text": text,
        "elapsed_seconds": round(elapsed, 2),
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }


# ------------------------------------------------------------------
# Step 3: Model comparison — cost, latency, reasoning depth
# ------------------------------------------------------------------
MODELS_TO_COMPARE = ["claude-haiku-4-5-20251001", "claude-sonnet-5", "claude-opus-5"]


def compare_models(evidence: dict):
    print(f"\n=== Model Comparison for grid {evidence.get('grid_id')} ===")
    results = []
    for model in MODELS_TO_COMPARE:
        print(f"\n--- {model} ---")
        result = generate_insight(evidence, model=model)
        print(f"Latency: {result['elapsed_seconds']}s | "
              f"Input tokens: {result['input_tokens']} | Output tokens: {result['output_tokens']}")
        print(result["text"])
        results.append(result)
    return results


# ------------------------------------------------------------------
# Step 4: Short vs. rich context comparison
# ------------------------------------------------------------------
def compare_context_richness(grid_id: int, model: str = "claude-sonnet-5"):
    print(f"\n=== Short vs. Rich Context Comparison for grid {grid_id} ===")

    full_evidence = build_grid_evidence(grid_id)
    short_evidence = {
        "grid_id": grid_id,
        "current_activity": full_evidence.get("current_activity"),
    }

    print("\n--- SHORT context ---")
    print(json.dumps(short_evidence, indent=2, default=str))
    short_result = generate_insight(short_evidence, model=model)
    print(short_result["text"])

    print("\n--- RICH context (full evidence object) ---")
    print(json.dumps(full_evidence, indent=2, default=str))
    rich_result = generate_insight(full_evidence, model=model)
    print(rich_result["text"])

    return short_result, rich_result


# ------------------------------------------------------------------
# Step 5: Insufficient-evidence test — remove a field, confirm Claude
# says so rather than filling the gap
# ------------------------------------------------------------------
def test_insufficient_evidence(grid_id: int, model: str = "claude-sonnet-5"):
    print(f"\n=== Insufficient-Evidence Test for grid {grid_id} (risk_score removed) ===")
    evidence = build_grid_evidence(grid_id, drop_fields=["risk_score", "risk_level", "model_version"])
    print(json.dumps(evidence, indent=2, default=str))
    result = generate_insight(evidence, model=model)
    print(result["text"])
    return result


if __name__ == "__main__":
    TEST_GRID_ID = 4821  # change this to whichever grid you want to inspect

    evidence = build_grid_evidence(TEST_GRID_ID)
    print(f"=== Evidence object for grid {TEST_GRID_ID} ===")
    print(json.dumps(evidence, indent=2, default=str))

    print("\n=== Standard Insight (claude-sonnet-5) ===")
    result = generate_insight(evidence, model="claude-sonnet-5")
    print(f"Latency: {result['elapsed_seconds']}s | Tokens in/out: "
          f"{result['input_tokens']}/{result['output_tokens']}")
    print(result["text"])

    compare_models(evidence)
    compare_context_richness(TEST_GRID_ID)
    test_insufficient_evidence(TEST_GRID_ID)