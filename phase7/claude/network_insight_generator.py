"""
network_insight_generator.py — C1: Claude API — Network Insight Generator

Builds a curated grid evidence object from the warehouse and sends it
to Claude to generate an operations-friendly explanation.

Output:
    SEVERITY
    EVIDENCE
    INTERPRETATION
    NEXT CHECKS

Hard rule:
Every number mentioned under EVIDENCE must exist exactly in the
input evidence object. Claude must never invent missing values.
"""

import os
import sqlite3
import time
import json

from anthropic import Anthropic


# ================================================================
# CONFIGURATION
# ================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.environ.get(
    "WAREHOUSE_DB_PATH",
    os.path.abspath(
        os.path.join(
            BASE_DIR,
            "..",
            "..",
            "phase3",
            "warehouse",
            "network_warehouse.db",
        )
    ),
)

# Anthropic SDK automatically reads ANTHROPIC_API_KEY
# from the environment.
client = Anthropic()


# Current model used for the main C1 test
DEFAULT_MODEL = "claude-sonnet-5"


# ================================================================
# SYSTEM PROMPT
# ================================================================

SYSTEM_PROMPT = """
You are a network operations analyst assistant.

You will receive a structured JSON evidence object describing
one network grid cell.

Your job is to explain the evidence for a NOC engineer.

Respond using EXACTLY these four headers:

SEVERITY:
One word only:
low
medium
high

EVIDENCE:
List only values that are actually present in the input JSON.

Rules:
- Do not invent numbers.
- Do not calculate new numbers.
- Do not round numbers differently.
- Do not estimate missing values.
- Every numeric value must already appear in the input JSON.
- If an expected field is missing, write:
  "not available"

INTERPRETATION:
Explain in simple operational language what the evidence might
suggest.

Important:
- Clearly separate interpretation from raw evidence.
- Do not present an inference as a fact.
- If important fields are missing, explicitly say that the
  interpretation is limited by the missing fields.
- Do not claim network congestion because the evidence does not
  contain capacity or utilization data.

NEXT CHECKS:
Give 2-4 concrete checks that a NOC engineer should perform.

The checks must be related to the actual evidence or missing
evidence. Avoid generic advice.

Critical rule:
Never invent a number or missing field.
"""


# ================================================================
# DATABASE CONNECTION
# ================================================================

def get_connection():
    """Open a connection to the network warehouse."""

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"Warehouse database not found at:\n{DB_PATH}"
        )

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    return conn


# ================================================================
# BUILD CURATED EVIDENCE
# ================================================================

def build_grid_evidence(grid_id: int, drop_fields=None) -> dict:
    """
    Build the curated evidence object for one grid.

    Data comes from:
        grid_features
        network_anomaly_scores
        network_risk_scores

    drop_fields can be used to test Claude's behaviour when
    evidence is missing.
    """

    if drop_fields is None:
        drop_fields = []

    conn = get_connection()

    try:

        # --------------------------------------------------------
        # ML2 / Feature table
        # --------------------------------------------------------

        feature_row = conn.execute(
            """
            SELECT
                feature_timestamp,
                avg_activity,
                activity_growth,
                active_hours,
                peak_ratio,
                variability,
                internet_share
            FROM grid_features
            WHERE grid_id = ?
            ORDER BY feature_timestamp DESC
            LIMIT 1
            """,
            (grid_id,),
        ).fetchone()

        # --------------------------------------------------------
        # ML4 / Anomaly table
        # --------------------------------------------------------

        anomaly_row = conn.execute(
            """
            SELECT
                timestamp,
                current_activity,
                baseline_activity,
                anomaly_score,
                direction,
                reason
            FROM network_anomaly_scores
            WHERE grid_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (grid_id,),
        ).fetchone()

        # --------------------------------------------------------
        # ML6 / Risk table
        # --------------------------------------------------------

        risk_row = conn.execute(
            """
            SELECT
                timestamp,
                risk_score,
                risk_level,
                model_version
            FROM network_risk_scores
            WHERE grid_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (grid_id,),
        ).fetchone()

    finally:
        conn.close()

    # ------------------------------------------------------------
    # Start curated evidence object
    # ------------------------------------------------------------

    evidence = {
        "grid_id": grid_id
    }

    # ------------------------------------------------------------
    # Add feature evidence
    # ------------------------------------------------------------

    if feature_row:

        evidence["feature_timestamp"] = feature_row[
            "feature_timestamp"
        ]

        evidence["avg_activity"] = feature_row[
            "avg_activity"
        ]

        evidence["activity_growth"] = feature_row[
            "activity_growth"
        ]

        evidence["peak_ratio"] = feature_row[
            "peak_ratio"
        ]

        evidence["variability"] = feature_row[
            "variability"
        ]

    # ------------------------------------------------------------
    # Add anomaly evidence
    # ------------------------------------------------------------

    if anomaly_row:

        evidence["current_activity"] = anomaly_row[
            "current_activity"
        ]

        evidence["baseline_activity"] = anomaly_row[
            "baseline_activity"
        ]

        evidence["anomaly_score"] = anomaly_row[
            "anomaly_score"
        ]

        evidence["anomaly_direction"] = anomaly_row[
            "direction"
        ]

    # ------------------------------------------------------------
    # Add risk evidence
    # ------------------------------------------------------------

    if risk_row:

        evidence["risk_score"] = risk_row[
            "risk_score"
        ]

        evidence["risk_level"] = risk_row[
            "risk_level"
        ]

        evidence["model_version"] = risk_row[
            "model_version"
        ]

    # ------------------------------------------------------------
    # Remove fields for insufficient-evidence testing
    # ------------------------------------------------------------

    for field in drop_fields:
        evidence.pop(field, None)

    return evidence


# ================================================================
# CALL CLAUDE API
# ================================================================

def generate_insight(
    evidence: dict,
    model: str = DEFAULT_MODEL
) -> dict:
    """
    Send curated evidence to Claude.

    Returns:
        model
        text
        latency
        input tokens
        output tokens
    """

    user_message = (
        "Here is the curated grid evidence object.\n\n"
        + json.dumps(
            evidence,
            indent=2,
            default=str
        )
    )

    start = time.time()

    try:

        response = client.messages.create(
            model=model,
            max_tokens=1200,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": user_message
                }
            ],
        )

    except Exception as exc:

        print("\nClaude API request failed.")
        print(f"Model: {model}")
        print(f"Error: {exc}")

        raise

    elapsed = time.time() - start

    # ------------------------------------------------------------
    # Extract text response
    # ------------------------------------------------------------

    text_parts = []

    for block in response.content:

        if block.type == "text":
            text_parts.append(block.text)

    text = "\n".join(text_parts)

    return {
        "model": model,
        "text": text,
        "elapsed_seconds": round(elapsed, 2),
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }


# ================================================================
# MODEL COMPARISON
# ================================================================

MODELS_TO_COMPARE = [
    "claude-haiku-4-5-20251001",
    "claude-sonnet-5",
    "claude-opus-5",
]


def compare_models(evidence: dict):
    """
    Compare Haiku, Sonnet and Opus using the same evidence.
    """

    print(
        f"\n=== Model Comparison for grid "
        f"{evidence.get('grid_id')} ==="
    )

    results = []

    for model in MODELS_TO_COMPARE:

        print(f"\n--- {model} ---")

        result = generate_insight(
            evidence,
            model=model
        )

        print(
            f"Latency: {result['elapsed_seconds']}s | "
            f"Input tokens: {result['input_tokens']} | "
            f"Output tokens: {result['output_tokens']}"
        )

        print("\n" + result["text"])

        results.append(result)

    return results


# ================================================================
# SHORT VS RICH CONTEXT
# ================================================================

def compare_context_richness(
    grid_id: int,
    model: str = DEFAULT_MODEL
):
    """
    Compare Claude's response when it receives:

    1. Only current activity
    2. Full curated evidence
    """

    print(
        f"\n=== Short vs Rich Context Comparison "
        f"for grid {grid_id} ==="
    )

    full_evidence = build_grid_evidence(grid_id)

    # ------------------------------------------------------------
    # Short context
    # ------------------------------------------------------------

    short_evidence = {
        "grid_id": grid_id,
        "current_activity": full_evidence.get(
            "current_activity"
        ),
    }

    print("\n--- SHORT CONTEXT ---")

    print(
        json.dumps(
            short_evidence,
            indent=2,
            default=str
        )
    )

    short_result = generate_insight(
        short_evidence,
        model=model
    )

    print("\n" + short_result["text"])

    # ------------------------------------------------------------
    # Rich context
    # ------------------------------------------------------------

    print("\n--- RICH CONTEXT ---")

    print(
        json.dumps(
            full_evidence,
            indent=2,
            default=str
        )
    )

    rich_result = generate_insight(
        full_evidence,
        model=model
    )

    print("\n" + rich_result["text"])

    return short_result, rich_result


# ================================================================
# INSUFFICIENT EVIDENCE TEST
# ================================================================

def test_insufficient_evidence(
    grid_id: int,
    model: str = DEFAULT_MODEL
):
    """
    Remove risk-related fields and verify that Claude does not
    invent them.
    """

    print(
        f"\n=== Insufficient-Evidence Test "
        f"for grid {grid_id} ==="
    )

    evidence = build_grid_evidence(
        grid_id,
        drop_fields=[
            "risk_score",
            "risk_level",
            "model_version",
        ],
    )

    print("\nEvidence with risk fields removed:")

    print(
        json.dumps(
            evidence,
            indent=2,
            default=str
        )
    )

    result = generate_insight(
        evidence,
        model=model
    )

    print("\nClaude response:")

    print(result["text"])

    return result


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":

    TEST_GRID_ID = 4821

    print("=" * 70)
    print("C1 — CLAUDE API NETWORK INSIGHT GENERATOR")
    print("=" * 70)

    # ------------------------------------------------------------
    # 1. Build evidence
    # ------------------------------------------------------------

    evidence = build_grid_evidence(
        TEST_GRID_ID
    )

    print(
        f"\n=== Evidence object for grid "
        f"{TEST_GRID_ID} ==="
    )

    print(
        json.dumps(
            evidence,
            indent=2,
            default=str
        )
    )

    # ------------------------------------------------------------
    # 2. Standard Claude Sonnet test
    # ------------------------------------------------------------

    print(
        "\n=== Standard Insight "
        f"({DEFAULT_MODEL}) ==="
    )

    result = generate_insight(
        evidence,
        model=DEFAULT_MODEL
    )

    print(
        f"Latency: {result['elapsed_seconds']}s | "
        f"Tokens in/out: "
        f"{result['input_tokens']}/"
        f"{result['output_tokens']}"
    )

    print("\n" + result["text"])

    # ------------------------------------------------------------
    # 3. Model comparison
    # ------------------------------------------------------------

    compare_models(evidence)

    # ------------------------------------------------------------
    # 4. Short vs rich context
    # ------------------------------------------------------------

    compare_context_richness(
        TEST_GRID_ID,
        model=DEFAULT_MODEL
    )

    # ------------------------------------------------------------
    # 5. Missing evidence test
    # ------------------------------------------------------------

    test_insufficient_evidence(
        TEST_GRID_ID,
        model=DEFAULT_MODEL
    )
