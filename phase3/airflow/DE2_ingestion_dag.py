"""
DE2_ingestion_dag.py — Airflow DAG for the ingestion flow specifically:
detect -> validate -> route -> log, as four distinct, visible tasks.

Reuses the existing tested functions from ingest.py — no logic
duplicated here. This DAG is separate from DE3's larger pipeline DAG,
per DE2's own requirement for a dedicated detect/validate/route/log DAG.
"""

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/mnt/c/Network-operator"
INGESTION_SCRIPT_DIR = f"{PROJECT_ROOT}/phase3/ingestion"

LANDING_DIR = f"{PROJECT_ROOT}/data/landing"
RAW_DIR = f"{PROJECT_ROOT}/data/raw"
REJECTED_DIR = f"{PROJECT_ROOT}/data/rejected"
LOG_PATH = f"{PROJECT_ROOT}/logs/ingestion_log.jsonl"

sys.path.insert(0, INGESTION_SCRIPT_DIR)


def detect_task(**context):
    """Task 1: find candidate files. Pushes the filename list to XCom
    so downstream tasks can use it without re-scanning the folder."""
    from ingest import detect_files
    filenames = detect_files(LANDING_DIR)
    print(f"Detected {len(filenames)} candidate file(s): {filenames}")
    return filenames  # stored in XCom automatically


def validate_task(**context):
    """Task 2: run schema + quality checks on each detected file.
    Pushes a dict of {filename: (is_valid, reason, row_count)}."""
    from ingest import validate_schema, validate_minimum_quality

    filenames = context["ti"].xcom_pull(task_ids="detect")
    results = {}

    for filename in filenames:
        filepath = os.path.join(LANDING_DIR, filename)

        schema_ok, schema_reason = validate_schema(filepath)
        if not schema_ok:
            results[filename] = (False, schema_reason, None)
            continue

        quality_ok, quality_reason, row_count = validate_minimum_quality(filepath)
        results[filename] = (quality_ok, quality_reason, row_count)

    print(f"Validation results: {results}")
    return results


def route_task(**context):
    """Task 3: move each file to raw/ or rejected/ based on validation results."""
    from ingest import route_file

    validation_results = context["ti"].xcom_pull(task_ids="validate")
    routed = {}

    for filename, (is_valid, reason, row_count) in validation_results.items():
        filepath = os.path.join(LANDING_DIR, filename)
        dest = route_file(filepath, filename, is_valid, RAW_DIR, REJECTED_DIR)
        routed[filename] = {"is_valid": is_valid, "reason": reason, "row_count": row_count, "dest": dest}

    print(f"Routed: {routed}")
    return routed


def log_task(**context):
    """Task 4: write the audit record for every file processed."""
    from ingest import write_audit_record

    routed = context["ti"].xcom_pull(task_ids="route")

    for filename, info in routed.items():
        status = "ACCEPTED" if info["is_valid"] else "REJECTED"
        write_audit_record(LOG_PATH, filename, status, info["row_count"], info["reason"])
        print(f"Logged: {filename} -> {status} ({info['reason'] or 'OK'})")


default_args = {"owner": "sai", "retries": 0}

with DAG(
    dag_id="de2_ingestion_flow",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["network-operations", "phase3", "DE2"],
) as dag:

    detect = PythonOperator(task_id="detect", python_callable=detect_task)
    validate = PythonOperator(task_id="validate", python_callable=validate_task)
    route = PythonOperator(task_id="route", python_callable=route_task)
    log = PythonOperator(task_id="log", python_callable=log_task)

    detect >> validate >> route >> log