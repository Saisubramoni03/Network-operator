"""
C3 - Long-Context Incident Investigation

Compares a large "dump everything" context with a curated
evidence package and checks whether pipeline health changes
the uncertainty section.
"""

import json
import os
from dotenv import load_dotenv
from anthropic import Anthropic

load_dotenv()

from evidence import get_grid_evidence, API_BASE_URL
import requests


MODEL = "claude-sonnet-5"
GRID_ID = 4821


client = Anthropic(
    api_key=os.environ["ANTHROPIC_API_KEY"]
)

INVESTIGATION_PROMPT = """
Investigate an unusual activity pattern at Grid 4821.

I am providing an evidence package containing:
- current interval metrics
- summarized recent history
- prior alerts for this grid
- the current model risk score
- the pipeline status record for the run that produced this data

Answer in exactly three sections:

CURRENT EVIDENCE
What is true right now, with figures.

HISTORICAL EVIDENCE
Whether this has happened before, and how often.
Only make claims supported by the historical evidence provided.

UNCERTAINTY
What you do NOT know, including anything the pipeline
status makes doubtful.

If the pipeline status indicates rejected rows, handled nulls,
or a stale analytics layer, treat that as material and explain
how it limits the conclusion.

Do not restate raw rows back to me.
Do not claim congestion simply from high activity.
Separate observed evidence from interpretation.
"""


def ask_claude(context, label):
    """Send one investigation context to Claude."""

    prompt = f"""
{INVESTIGATION_PROMPT}

CONTEXT TYPE:
{label}

EVIDENCE:
{json.dumps(context, indent=2)}
"""

    response = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        system=(
            "You are a telecom network operations analyst. "
            "Use only the supplied evidence. "
            "Do not invent historical facts. "
            "Be explicit when evidence is limited."
        ),
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    text_parts = [
    block.text
    for block in response.content
    if hasattr(block, "text") and block.text
]

    if not text_parts:
        raise RuntimeError(
            f"Claude returned no text content for {label}. "
            f"Response blocks: {[type(block).__name__ for block in response.content]}"
        )

    return "\n".join(text_parts)


def get_dump_context(grid_id):
    """
    Build the intentionally large context.

    This includes the full recent hourly activity response
    instead of summarizing it.
    """

    features = requests.get(
        f"{API_BASE_URL}/network/grid/{grid_id}/features",
        timeout=10
    ).json()

    activity = requests.get(
        f"{API_BASE_URL}/network/grid/{grid_id}",
        timeout=10
    ).json()

    risk = requests.post(
        f"{API_BASE_URL}/network/predict-risk",
        json={"grid_id": grid_id},
        timeout=10
    ).json()

    alerts = requests.get(
        f"{API_BASE_URL}/network/alerts",
        params={"limit": 100},
        timeout=10
    ).json()

    pipeline = requests.get(
        f"{API_BASE_URL}/pipeline/status",
        timeout=10
    ).json()

    return {
        "grid_id": grid_id,
        "features": features,
        "full_hourly_activity": activity,
        "risk": risk,
        "alerts": alerts,
        "pipeline": pipeline
    }


def create_unhealthy_copy(curated):
    """
    Simulate an unhealthy pipeline without changing the real
    pipeline status file or database.
    """

    unhealthy = json.loads(json.dumps(curated))

    pipeline = unhealthy["pipeline_evidence"]

    pipeline["healthy"] = False
    pipeline["reasons"] = [
        "SIMULATION: analytics pipeline unhealthy",
        "SIMULATION: analytics layer cannot be considered fully trustworthy"
    ]
    pipeline["freshness"] = "SIMULATION: stale and unhealthy"

    return unhealthy


def save_report(
    dump_answer,
    curated_answer,
    unhealthy_answer
):
    """Write the C3 investigation results to disk."""

    with open(
        "c3_incident_investigation_report.md",
        "w",
        encoding="utf-8"
    ) as file:

        file.write("# C3 - Long-Context Incident Investigation\n\n")

        file.write("## 1. Dump-Everything Context\n\n")
        file.write(dump_answer)
        file.write("\n\n")

        file.write("## 2. Curated Context\n\n")
        file.write(curated_answer)
        file.write("\n\n")

        file.write(
            "## 3. Unhealthy Pipeline Simulation\n\n"
        )
        file.write(unhealthy_answer)
        file.write("\n\n")

        file.write("## 4. Comparison\n\n")
        file.write(
            "- Both dump-everything and curated runs were executed.\n"
            "- The curated package summarizes recent history instead "
            "of passing all hourly rows.\n"
            "- The unhealthy-pipeline simulation was performed "
            "without modifying the real pipeline status.\n"
            "- The UNCERTAINTY section should materially increase "
            "its concern when pipeline health is changed to false.\n"
        )


def main():
    print("Building curated evidence...")
    curated = get_grid_evidence(GRID_ID)

    print("Building dump-everything context...")
    dump_context = get_dump_context(GRID_ID)

    print("Running dump-everything investigation...")
    dump_answer = ask_claude(
        dump_context,
        "DUMP-EVERYTHING"
    )

    print("Running curated investigation...")
    curated_answer = ask_claude(
        curated,
        "CURATED"
    )

    print("Simulating unhealthy pipeline...")
    unhealthy = create_unhealthy_copy(curated)

    print("Running unhealthy-pipeline investigation...")
    unhealthy_answer = ask_claude(
        unhealthy,
        "CURATED WITH UNHEALTHY PIPELINE"
    )

    save_report(
        dump_answer,
        curated_answer,
        unhealthy_answer
    )

    print()
    print("=" * 60)
    print("C3 COMPLETE")
    print("=" * 60)
    print()
    print("Report created:")
    print("c3_incident_investigation_report.md")
    print()
    print("The report contains:")
    print("1. Dump-everything result")
    print("2. Curated-context result")
    print("3. Unhealthy-pipeline result")
    print("4. Comparison notes")


if __name__ == "__main__":
    main()
    