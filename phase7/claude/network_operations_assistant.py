import os
import json
import requests
from anthropic import Anthropic


# ============================================================
# CONFIGURATION
# ============================================================

API_BASE_URL = "http://127.0.0.1:8000"

client = Anthropic(
    api_key=os.environ["ANTHROPIC_API_KEY"]
)


# ============================================================
# API HELPER
# ============================================================

def call_api(method, endpoint, params=None, json_body=None):
    """
    Calls the existing Phase 4 FastAPI API.

    If the API fails, return the failure information.
    Never invent replacement data.
    """

    url = f"{API_BASE_URL}{endpoint}"

    try:
        response = requests.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            timeout=10
        )

        response.raise_for_status()

        return {
            "success": True,
            "data": response.json()
        }

    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": str(e),
            "endpoint": endpoint
        }

    except ValueError as e:
        return {
            "success": False,
            "error": f"Invalid JSON response: {str(e)}",
            "endpoint": endpoint
        }


# ============================================================
# NETWORK TOOLS
# ============================================================

def get_network_summary(as_of=None):
    """
    API1:
    GET /network/summary
    """

    params = {}

    if as_of:
        params["as_of"] = as_of

    return call_api(
        "GET",
        "/network/summary",
        params=params
    )


def get_grid_activity(grid_id, as_of=None):
    """
    API2:
    GET /network/grid/{grid_id}

    Returns the hourly activity points for the grid.
    """

    params = {}

    if as_of:
        params["as_of"] = as_of

    return call_api(
        "GET",
        f"/network/grid/{grid_id}",
        params=params
    )


def get_hotspots(limit=10, severity=None, as_of=None):
    """
    API3:
    GET /network/hotspots
    """

    params = {
        "limit": limit
    }

    if severity:
        params["severity"] = severity

    if as_of:
        params["as_of"] = as_of

    return call_api(
        "GET",
        "/network/hotspots",
        params=params
    )


def get_grid_features(grid_id):
    """
    ML feature endpoint:
    GET /network/grid/{grid_id}/features
    """

    return call_api(
        "GET",
        f"/network/grid/{grid_id}/features"
    )


def get_anomaly_score(grid_id, as_of=None):
    """
    ML risk endpoint:
    POST /network/predict-risk
    """

    body = {
        "grid_id": grid_id
    }

    if as_of:
        body["as_of"] = as_of

    return call_api(
        "POST",
        "/network/predict-risk",
        json_body=body
    )


def get_grid_location(grid_id):
    """
    API6:
    GET /network/grid/{grid_id}/location

    Returns centroid and polygon reference.
    Does not request full polygon geometry.
    """

    return call_api(
        "GET",
        f"/network/grid/{grid_id}/location"
    )


def get_pipeline_status():
    """
    API6:
    GET /pipeline/status

    Returns DE7 pipeline health and freshness.
    """

    return call_api(
        "GET",
        "/pipeline/status"
    )


# ============================================================
# CLAUDE TOOL DEFINITIONS
# ============================================================

TOOLS = [

    {
        "name": "get_network_summary",
        "description": (
            "Get the current overall Milan network summary. "
            "Use this for questions about the overall network."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "as_of": {
                    "type": "string",
                    "description": (
                        "Optional reporting timestamp, for example "
                        "'2013-11-07 23:00:00'."
                    )
                }
            },
            "required": []
        }
    },

    {
        "name": "get_grid_activity",
        "description": (
            "Get recent hourly activity for a specific grid. "
            "Use this when investigating a particular grid. "
            "Activity values are not counts or MB."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "grid_id": {
                    "type": "integer",
                    "description": "Grid identifier."
                },
                "as_of": {
                    "type": "string",
                    "description": (
                        "Optional reporting timestamp, for example "
                        "'2013-11-07 23:00:00'."
                    )
                }
            },
            "required": ["grid_id"]
        }
    },

    {
        "name": "get_hotspots",
        "description": (
            "Get the current network hotspots ranked by activity. "
            "Use this to identify areas that may need attention. "
            "Do not automatically describe a hotspot as congestion."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of hotspots to return."
                },
                "severity": {
                    "type": "string",
                    "description": (
                        "Optional severity filter: 'high' or 'medium'."
                    )
                },
                "as_of": {
                    "type": "string",
                    "description": "Optional reporting timestamp."
                }
            },
            "required": []
        }
    },

    {
        "name": "get_grid_features",
        "description": (
            "Get stored ML features for a specific grid. "
            "Use this when explaining the evidence behind a grid's "
            "behavior."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "grid_id": {
                    "type": "integer",
                    "description": "Grid identifier."
                }
            },
            "required": ["grid_id"]
        }
    },

    {
        "name": "get_anomaly_score",
        "description": (
            "Get the trained ML risk score for a specific grid. "
            "The result includes risk score, risk level, model version, "
            "feature timestamp and top features."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "grid_id": {
                    "type": "integer",
                    "description": "Grid identifier."
                },
                "as_of": {
                    "type": "string",
                    "description": "Optional reporting timestamp."
                }
            },
            "required": ["grid_id"]
        }
    },

    {
        "name": "get_grid_location",
        "description": (
            "Get the geographic location of a grid. "
            "Returns centroid latitude, longitude and polygon reference. "
            "Does not return full polygon geometry."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "grid_id": {
                    "type": "integer",
                    "description": "Grid identifier."
                }
            },
            "required": ["grid_id"]
        }
    },

    {
        "name": "get_pipeline_status",
        "description": (
            "Get the current DE7 pipeline status, including task status, "
            "rows processed, AS_OF, freshness and overall health. "
            "MUST be called before making claims about the current "
            "network situation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]


# ============================================================
# TOOL DISPATCHER
# ============================================================

def execute_tool(tool_name, tool_input):

    print("\n" + "-" * 60)
    print(f"[TOOL CALL] {tool_name}")
    print(f"[TOOL INPUT] {json.dumps(tool_input)}")

    if tool_name == "get_network_summary":

        result = get_network_summary(
            tool_input.get("as_of")
        )

    elif tool_name == "get_grid_activity":

        result = get_grid_activity(
            tool_input["grid_id"],
            tool_input.get("as_of")
        )

    elif tool_name == "get_hotspots":

        result = get_hotspots(
            limit=tool_input.get("limit", 10),
            severity=tool_input.get("severity"),
            as_of=tool_input.get("as_of")
        )

    elif tool_name == "get_grid_features":

        result = get_grid_features(
            tool_input["grid_id"]
        )

    elif tool_name == "get_anomaly_score":

        result = get_anomaly_score(
            tool_input["grid_id"],
            tool_input.get("as_of")
        )

    elif tool_name == "get_grid_location":

        result = get_grid_location(
            tool_input["grid_id"]
        )

    elif tool_name == "get_pipeline_status":

        result = get_pipeline_status()

    else:

        result = {
            "success": False,
            "error": f"Unknown tool: {tool_name}"
        }

    print(f"[TOOL RESULT] {json.dumps(result)}")

    return result


# ============================================================
# SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT = """
You are the Network Operations Assistant for the Milan grid.

Your job is to help a NOC engineer investigate network conditions
using LIVE evidence from the available tools.

IMPORTANT RULES:

1. ALWAYS call a tool for factual network claims.

2. NEVER answer a current network question from memory.

3. Before reporting the current network situation, ALWAYS call
   get_pipeline_status().

4. Report whether the underlying pipeline data is healthy and
   trustworthy.

5. Every important factual figure must identify its source tool.

   Example:
   "Total activity is X, according to get_network_summary()."

6. If a tool fails, explicitly report:
   - which tool failed
   - what information is unavailable
   - what conclusion therefore cannot be made

   NEVER invent replacement data.

7. Activity values are activity measures.
   They are NOT automatically counts, users, calls or MB.

8. Do NOT claim congestion simply because a grid has high activity.

9. Separate your answer into:

   OBSERVED EVIDENCE
   What the tools actually returned.

   INTERPRETATION
   What the evidence may indicate.
   Clearly label this as interpretation.

   NEXT CHECKS
   What the NOC engineer should investigate next.

10. When investigating a specific grid, use:
    - get_grid_activity()
    - get_grid_features()
    - get_anomaly_score()
    - get_grid_location()

    when those tools are relevant.

11. Never claim a location from memory.
    Use get_grid_location().

12. Never claim an ML risk score from memory.
    Use get_anomaly_score().

13. Never claim pipeline health from memory.
    Use get_pipeline_status().

14. Be cautious. Evidence first, interpretation second.

For the question:

"Which areas need attention right now?"

First check pipeline status.
Then retrieve the relevant hotspot/network evidence.

For:

"Explain Grid 4821."

Retrieve:
- pipeline status
- grid activity
- grid features
- anomaly/risk score
- grid location

Then explain:
- where Grid 4821 is
- what the observed evidence shows
- what the ML signal shows
- what the evidence might mean
- what the NOC should check next

Do not turn activity measures into claims about congestion
unless the evidence explicitly supports it.
"""


# ============================================================
# CLAUDE TOOL-USE LOOP
# ============================================================

def ask_claude(messages):

    while True:

        response = client.messages.create(
            model="claude-sonnet-5",
            max_tokens=1500,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages
        )

        # Save Claude's response in conversation history.
        messages.append({
            "role": "assistant",
            "content": response.content
        })

        # Claude has finished its answer.
        if response.stop_reason == "end_turn":
            return response

        # Claude wants to call tools.
        tool_results = []

        for block in response.content:

            if block.type == "tool_use":

                result = execute_tool(
                    block.name,
                    block.input
                )

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result)
                })

        # Send the tool results back to Claude.
        if tool_results:

            messages.append({
                "role": "user",
                "content": tool_results
            })

        else:

            return response


# ============================================================
# PRINT RESPONSE
# ============================================================

def print_response(response):

    print("\n")
    print("=" * 70)
    print("NETWORK OPERATIONS ASSISTANT")
    print("=" * 70)

    for block in response.content:

        if block.type == "text":
            print(block.text)

    print("=" * 70)


# ============================================================
# MAIN
# ============================================================

def main():

    messages = []

    print("=" * 70)
    print("NETWORK OPERATIONS ASSISTANT - C2")
    print("=" * 70)
    print("Connected API:", API_BASE_URL)
    print("Type 'exit' to quit.")

    while True:

        question = input("\nNOC Engineer: ").strip()

        if not question:
            continue

        if question.lower() == "exit":
            print("Assistant stopped.")
            break

        messages.append({
            "role": "user",
            "content": question
        })

        try:

            response = ask_claude(messages)

            print_response(response)

        except Exception as e:

            print("\n[ASSISTANT ERROR]")
            print(str(e))


if __name__ == "__main__":
    main()