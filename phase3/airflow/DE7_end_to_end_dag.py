"""
DE7_end_to_end_dag.py — Full end-to-end orchestration:
ingest -> validate -> spark_process -> load_warehouse -> quality_check -> notify

Reuses existing, already-tested components — no cleaning/aggregation/
warehouse logic reimplemented here. quality_check writes a
machine-readable pipeline status record consumed by later labs
(API6, Claude C3/C14).
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = "/mnt/c/Network-operator"
FAST_DATA_ROOT = os.path.expanduser("~/network_data")

INGESTION_SCRIPT_DIR = f"{PROJECT_ROOT}/phase3/ingestion"
WAREHOUSE_SCRIPT_DIR = f"{PROJECT_ROOT}/phase3/warehouse"
SPARK_SCRIPT = f"{PROJECT_ROOT}/phase2/spark/telecom_pipeline.py"

LANDING_DIR = f"{PROJECT_ROOT}/data/landing"
RAW_DIR = f"{PROJECT_ROOT}/data/raw"
REJECTED_DIR = f"{PROJECT_ROOT}/data/rejected"
LOG_PATH = f"{PROJECT_ROOT}/logs/ingestion_log.jsonl"

SPARK_INPUT_DIR = f"{FAST_DATA_ROOT}/raw"
REFERENCE_DIR = f"{FAST_DATA_ROOT}/reference"
SPARK_OUTPUT_DIR = FAST_DATA_ROOT
HOURLY_PARQUET_DIR = f"{FAST_DATA_ROOT}/analytics/hourly_grid_summary"
GEOJSON_PATH = f"{FAST_DATA_ROOT}/reference/milano-grid.geojson"
WAREHOUSE_DB_PATH = f"{PROJECT_ROOT}/phase3/warehouse/network_warehouse.db"

STATUS_RECORD_PATH = f"{PROJECT_ROOT}/data/analytics/pipeline_status.json"
NOTIFY_LOG_PATH = f"{PROJECT_ROOT}/logs/notify_log.jsonl"

ML_SCRIPT_DIR = f"{PROJECT_ROOT}/phase6/ml"
sys.path.insert(0, ML_SCRIPT_DIR)

sys.path.insert(0, INGESTION_SCRIPT_DIR)
sys.path.insert(0, WAREHOUSE_SCRIPT_DIR)


def ingest_validate_route_task(**context):
    from ingest import run_ingestion
    results = run_ingestion(LANDING_DIR, RAW_DIR, REJECTED_DIR, LOG_PATH)

    accepted = [r for r in results if r["status"] in ("ACCEPTED", "ACCEPTED_WITH_WARNINGS")]
    rejected = [r for r in results if r["status"] == "REJECTED"]

    metrics = {"rows_in": len(results), "rows_rejected": len(rejected), "rows_accepted": len(accepted)}
    print(f"Ingestion metrics: {metrics}")

    # CONTROL #1: Missing daily file is NOT a failure — it's normal
    # operation (no new file arrived this run).
    if len(results) == 0:
        print("WARN: No files detected in landing/ this run. "
              "This is treated as CONTINUE (missing daily file is not a failure).")

    return metrics


def spark_process_task(**context):
    result = subprocess.run(
        [sys.executable, SPARK_SCRIPT,
         "--input-dir", SPARK_INPUT_DIR,
         "--reference-dir", REFERENCE_DIR,
         "--output-dir", SPARK_OUTPUT_DIR],
        capture_output=True, text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"SPARK JOB FAILED with exit code {result.returncode}")

    combined_output = result.stdout + "\n" + result.stderr
    metrics = {}
    for line in combined_output.splitlines():
        for key in ["input_rows", "rejected_missing_keys", "rejected_negative", "nulls_handled", "output_rows"]:
            if f"{key}=" in line:
                try:
                    metrics[key] = int(line.split(f"{key}=")[1].strip())
                except ValueError:
                    pass

    print(f"Spark metrics parsed: {metrics}")
    return metrics


def load_warehouse_task(**context):
    from DE6_build_warehouse import build_warehouse_from_paths
    result = build_warehouse_from_paths(HOURLY_PARQUET_DIR, GEOJSON_PATH, WAREHOUSE_DB_PATH)
    print(f"Warehouse load result: {result}")
    return result


def quality_check_task(**context):
    ti = context["ti"]
    run_id = context["run_id"]

    spark_metrics = ti.xcom_pull(task_ids="spark_process") or {}
    warehouse_result = ti.xcom_pull(task_ids="load_warehouse") or {}

    task_states = {}
    for task_id in ["ingest_validate_route", "spark_process", "load_warehouse", "generate_features", "score_risk"]:
        ti_state = context["dag_run"].get_task_instance(task_id)
        task_states[task_id] = ti_state.state if ti_state else "unknown"

    status_record = {
        "run_id": run_id,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "task_status": task_states,
        "rows_in": spark_metrics.get("input_rows"),
        "rows_rejected": (spark_metrics.get("rejected_missing_keys", 0)
                           + spark_metrics.get("rejected_negative", 0)),
        "nulls_handled": spark_metrics.get("nulls_handled"),
        "rows_published": spark_metrics.get("output_rows"),
        "AS_OF": warehouse_result.get("as_of"),
    }

    os.makedirs(os.path.dirname(STATUS_RECORD_PATH), exist_ok=True)
    with open(STATUS_RECORD_PATH, "w") as f:
        json.dump(status_record, f, indent=2)

    print(f"Pipeline status record written: {status_record}")
    return status_record


def notify_task(**context):
    ti = context["ti"]
    status_record = ti.xcom_pull(task_ids="quality_check")

    if status_record is None:
        outcome = {"run_id": context["run_id"], "outcome": "FAILURE", "reason": "Pipeline failed before quality_check completed"}
    else:
        outcome = {"run_id": context["run_id"], "outcome": "SUCCESS", "summary": status_record}

    os.makedirs(os.path.dirname(NOTIFY_LOG_PATH), exist_ok=True)
    with open(NOTIFY_LOG_PATH, "a") as f:
        f.write(json.dumps(outcome) + "\n")

    print(f"NOTIFY: {outcome}")

def generate_features_task(**context):
    from features import build_feature_table, persist_feature_table
    features_df = build_feature_table()
    persist_feature_table(features_df)
    metrics = {"grids_featured": int(features_df["grid_id"].nunique()), "feature_rows": len(features_df)}
    print(f"Feature generation metrics: {metrics}")
    return metrics


def score_risk_task(**context):
    from batch_score import run_batch_scoring
    result = run_batch_scoring()
    print(f"Batch scoring metrics: {result}")
    return result


default_args = {"owner": "sai", "retries": 0}

with DAG(
    dag_id="de7_end_to_end_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["network-operations", "phase3", "DE7"],
) as dag:

    ingest_validate_route = PythonOperator(task_id="ingest_validate_route", python_callable=ingest_validate_route_task)
    spark_process = PythonOperator(task_id="spark_process", python_callable=spark_process_task)
    load_warehouse = PythonOperator(task_id="load_warehouse", python_callable=load_warehouse_task)
    generate_features = PythonOperator(task_id="generate_features", python_callable=generate_features_task)
    score_risk = PythonOperator(task_id="score_risk", python_callable=score_risk_task)

    quality_check = PythonOperator(task_id="quality_check", python_callable=quality_check_task)
    notify = PythonOperator(task_id="notify", python_callable=notify_task, trigger_rule=TriggerRule.ALL_DONE)

    ingest_validate_route >> spark_process >> load_warehouse >> generate_features >> score_risk >> quality_check >> notify

