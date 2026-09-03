"""
DE3_telecom_pipeline_dag.py — Airflow DAG orchestrating the network operations pipeline.

Task graph: task_ingest -> task_spark -> task_publish

Rules enforced by design:
- This file contains ZERO cleaning/aggregation/validation logic — it only
  calls existing functions/scripts (ingestion/ingest.py, spark/telecom_pipeline.py).
- If task_ingest fails, task_spark never runs.
- If task_spark fails, task_publish never runs, and the analytics layer
  is left untouched (telecom_pipeline.py only overwrites its outputs on
  its own successful completion — a failed Spark run raises before any
  write_outputs() call completes).
- The GeoJSON reference path is passed as configuration, not hardcoded
  inside task logic.

NOTE ON PATHS: ingestion (small files) reads/writes via the Windows-mounted
path (/mnt/c/...) since that's fine for small file operations. Spark
(large CSV reads/writes) uses a separate, native-WSL-filesystem copy of
the data (~/network_data) because reading large files across the
Windows<->WSL boundary via /mnt/c/ is dramatically slower than reading
native Linux files.
"""

import os
import sys
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
PROJECT_ROOT = "/mnt/c/Network-operator"   # scripts + ingestion data stay here (small, fast enough)
FAST_DATA_ROOT = os.path.expanduser("~/network_data")  # Spark reads/writes here (native WSL filesystem, much faster)

INGESTION_SCRIPT_DIR = f"{PROJECT_ROOT}/phase3/ingestion"
SPARK_SCRIPT = f"{PROJECT_ROOT}/phase2/spark/telecom_pipeline.py"

LANDING_DIR = f"{PROJECT_ROOT}/data/landing"
RAW_DIR = f"{PROJECT_ROOT}/data/raw"
REJECTED_DIR = f"{PROJECT_ROOT}/data/rejected"
LOG_PATH = f"{PROJECT_ROOT}/logs/ingestion_log.jsonl"

# Spark specifically uses the fast native-filesystem copies
SPARK_INPUT_DIR = f"{FAST_DATA_ROOT}/raw"
REFERENCE_DIR = f"{FAST_DATA_ROOT}/reference"     # config, not hardcoded inside tasks
OUTPUT_DIR = FAST_DATA_ROOT


def run_ingest_task(**context):
    """Calls the existing ingest.run_ingestion() — no logic duplicated here."""
    sys.path.insert(0, INGESTION_SCRIPT_DIR)
    from ingest import run_ingestion  # reuse existing, tested function

    results = run_ingestion(LANDING_DIR, RAW_DIR, REJECTED_DIR, LOG_PATH)

    rejected = [r for r in results if r["status"] == "REJECTED"]
    accepted = [r for r in results if r["status"] == "ACCEPTED"]

    print(f"Ingestion complete: {len(accepted)} accepted, {len(rejected)} rejected")

    if not accepted and not any(r["status"] == "SKIPPED" for r in results):
        # No usable files at all this run — fail loudly, don't proceed to Spark
        raise RuntimeError("INGESTION FAILURE: no files were accepted or already processed.")

    return {"accepted": len(accepted), "rejected": len(rejected)}


def run_spark_task(**context):
    """Launches telecom_pipeline.py as a subprocess, reading from the fast
    native-filesystem copy of the data. Raises on any non-zero exit code,
    which stops task_publish from running (Airflow's default failure
    propagation via task dependencies)."""
    result = subprocess.run(
        [
            sys.executable, SPARK_SCRIPT,
            "--input-dir", SPARK_INPUT_DIR,
            "--reference-dir", REFERENCE_DIR,
            "--output-dir", OUTPUT_DIR,
        ],
        capture_output=True, text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"SPARK JOB FAILED with exit code {result.returncode}")

    return {"status": "SUCCESS"}


def run_publish_task(**context):
    """Marks the analytics output as published. Only runs if task_spark
    succeeded (Airflow will not call this function at all otherwise)."""
    print("Analytics outputs published and ready for downstream consumers "
          "(API, dashboard, Claude assistant).")


default_args = {
    "owner": "sai",
    "retries": 0,   # explicit: no silent retries masking real failures during learning
}

with DAG(
    dag_id="telecom_network_pipeline",
    default_args=default_args,
    schedule=None,       # manual trigger only, for this learning project
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["network-operations", "phase3", "DE3"],
) as dag:

    task_ingest = PythonOperator(
        task_id="task_ingest",
        python_callable=run_ingest_task,
    )

    task_spark = PythonOperator(
        task_id="task_spark",
        python_callable=run_spark_task,
    )

    task_publish = PythonOperator(
        task_id="task_publish",
        python_callable=run_publish_task,
    )

    # Explicit dependency chain — enforces the required failure propagation
    task_ingest >> task_spark >> task_publish
