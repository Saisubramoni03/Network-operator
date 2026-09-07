"""
telecom_pipeline.py — Reusable Spark ETL job for Network Operations Predictive Intelligence

JOB CONTRACT
============
Inputs (all paths configurable via command-line args):
  --input-dir      Folder containing daily files matching sms-call-internet-mi-*.csv
  --reference-dir  Folder containing milano-grid.geojson (static reference data)
  --output-dir     Folder to write processed/analytics outputs into

Outputs:
  {output-dir}/processed/activity/           Parquet, partitioned by date
  {output-dir}/analytics/hourly_grid_summary/ Parquet, one row per grid/hour, no geometry
  {output-dir}/processed/dashboard_summary.csv CSV, daily total activity

Failure conditions:
  - No files matching sms-call-internet-mi-*.csv in --input-dir -> RuntimeError, exit code 1
  - milano-grid.geojson missing from --reference-dir           -> RuntimeError, exit code 1
  - Grain violation (duplicates on grid_id+timestamp after aggregation) -> RuntimeError, exit code 1
  - Join violation (unmatched grids after geo enrichment)      -> RuntimeError, exit code 1

Logged per run: input_rows, rejected_missing_keys, rejected_negative,
nulls_handled, output_rows, start_time, end_time, status.
"""

import os
import sys
import glob
import argparse
import logging
import time
from datetime import datetime

import platform

# Only override JAVA_HOME if it's not already set correctly by the environment
if not os.environ.get("JAVA_HOME"):
    if platform.system() == "Windows":
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
        os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
    # On Linux/WSL, rely on JAVA_HOME already being set in the environment
    # (e.g. via ~/.bashrc or the shell that launched this script)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, dayofweek, sum as spark_sum, broadcast
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("telecom_pipeline")

ACTIVITY_COLS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

RAW_SCHEMA = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])


def read_raw(spark, input_dir):
    """Read all daily activity files. Fails loudly if none exist."""
    pattern = os.path.join(input_dir, "sms-call-internet-mi-*.csv")
    matching_files = glob.glob(pattern)

    if not matching_files:
        raise RuntimeError(
            f"NO INPUT FILES FOUND: no files matching 'sms-call-internet-mi-*.csv' "
            f"in {input_dir}. Cannot proceed."
        )

    logger.info(f"Found {len(matching_files)} input file(s) in {input_dir}")

    raw_df = spark.read.csv(pattern, schema=RAW_SCHEMA, header=True)
    raw_df = raw_df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))

    canonical_df = (
        raw_df
        .withColumnRenamed("datetime", "timestamp")
        .withColumnRenamed("CellID", "grid_id")
        .withColumnRenamed("countrycode", "country_code")
        .withColumnRenamed("smsin", "sms_in")
        .withColumnRenamed("smsout", "sms_out")
        .withColumnRenamed("callin", "call_in")
        .withColumnRenamed("callout", "call_out")
        .withColumnRenamed("internet", "internet_activity")
    )
    return canonical_df, len(matching_files)


def clean(canonical_df):
    """Reject bad rows, apply null-to-zero rule, derive time features.
    Returns (clean_df, metrics_dict)."""
    input_rows = canonical_df.count()

    after_keys = canonical_df.dropna(subset=["grid_id", "timestamp"])
    rejected_missing_keys = input_rows - after_keys.count()

    condition = None
    for c in ACTIVITY_COLS:
        cond = (col(c) >= 0) | col(c).isNull()
        condition = cond if condition is None else (condition & cond)

    before_negative = after_keys.count()
    after_negative = after_keys.filter(condition)
    rejected_negative = before_negative - after_negative.count()

    null_counts = {c: after_negative.filter(col(c).isNull()).count() for c in ACTIVITY_COLS}
    nulls_handled = sum(null_counts.values())

    clean_df = (
        after_negative
        .fillna(0, subset=ACTIVITY_COLS)
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
    )

    metrics = {
        "input_rows": input_rows,
        "rejected_missing_keys": rejected_missing_keys,
        "rejected_negative": rejected_negative,
        "nulls_handled": nulls_handled,
    }
    return clean_df, metrics


def aggregate(clean_df):
    """THE grain transition: country_code rows -> one row per grid/hour.
    Country-code aggregation happens here, BEFORE any operational analytics."""
    hourly_grid_summary = (
        clean_df.groupBy("grid_id", "timestamp", "date", "hour", "day_of_week")
        .agg(*[spark_sum(c).alias(c) for c in ACTIVITY_COLS])
        .withColumn("total_activity",
                    col("sms_in") + col("sms_out") + col("call_in") + col("call_out") + col("internet_activity"))
    )

    duplicate_count = (
        hourly_grid_summary.groupBy("grid_id", "timestamp").count()
        .filter(col("count") > 1).count()
    )
    if duplicate_count != 0:
        raise RuntimeError(f"GRAIN VIOLATION: {duplicate_count} duplicate (grid_id, timestamp) pairs!")
    if "country_code" in hourly_grid_summary.columns:
        raise RuntimeError("GRAIN VIOLATION: country_code leaked into hourly_grid_summary!")

    return hourly_grid_summary


def enrich(spark, hourly_grid_summary, reference_dir):
    """Join grid/hour analytics with static geometry from milano-grid.geojson.
    Reads reference data from reference_dir — fails loudly if missing."""
    import json

    geojson_path = os.path.join(reference_dir, "milano-grid.geojson")
    if not os.path.exists(geojson_path):
        raise RuntimeError(f"REFERENCE DATA MISSING: {geojson_path} not found.")

    with open(geojson_path, "r") as f:
        grid_geojson = json.load(f)

    def polygon_centroid(coords):
        lons = [pt[0] for pt in coords]
        lats = [pt[1] for pt in coords]
        return sum(lons) / len(lons), sum(lats) / len(lats)

    lookup_rows = []
    for feature in grid_geojson["features"]:
        cell_id = feature["properties"]["cellId"]  # confirmed identifier — NOT top-level "id"
        ring = feature["geometry"]["coordinates"][0]
        lon, lat = polygon_centroid(ring)
        lookup_rows.append({"grid_id": cell_id, "centroid_lon": lon, "centroid_lat": lat})

    lookup_schema = StructType([
        StructField("grid_id", IntegerType(), True),
        StructField("centroid_lon", DoubleType(), True),
        StructField("centroid_lat", DoubleType(), True),
    ])
    grid_lookup_df = spark.createDataFrame(lookup_rows, schema=lookup_schema)

    rows_before = hourly_grid_summary.count()
    enriched_df = hourly_grid_summary.join(grid_lookup_df, on="grid_id", how="left")
    rows_after = enriched_df.count()

    if rows_after != rows_before:
        raise RuntimeError("JOIN VIOLATION: left join changed row count — duplicate keys in lookup!")

    unmatched = enriched_df.filter(col("centroid_lon").isNull()).select("grid_id").distinct().count()
    if unmatched != 0:
        raise RuntimeError(f"JOIN VIOLATION: {unmatched} grid(s) unmatched to geometry!")

    return enriched_df


def write_outputs(clean_df, hourly_grid_summary, output_dir):
    """Write processed + analytics outputs. hourly_grid_summary written
    WITHOUT geometry (geometry belongs only in the static reference layer)."""
    clean_output_path = os.path.join(output_dir, "processed", "activity")
    hourly_output_path = os.path.join(output_dir, "analytics", "hourly_grid_summary")
    dashboard_output_path = os.path.join(output_dir, "processed", "dashboard_summary.csv")

    clean_df.write.mode("overwrite").partitionBy("date").parquet(clean_output_path)
    logger.info(f"Wrote cleaned activity to {clean_output_path}")

    # Write only fact-shaped columns — no geometry duplicated into every row
    fact_columns = ["grid_id", "timestamp", "date", "hour", "day_of_week"] + ACTIVITY_COLS + ["total_activity"]
    hourly_grid_summary.select(*fact_columns).write.mode("overwrite").parquet(hourly_output_path)
    logger.info(f"Wrote hourly_grid_summary to {hourly_output_path}")

    dashboard_summary = (
        hourly_grid_summary.groupBy("date")
        .agg(spark_sum("total_activity").alias("daily_total_activity"))
        .orderBy("date")
    )
    dashboard_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(dashboard_output_path)
    logger.info(f"Wrote dashboard summary to {dashboard_output_path}")

    output_rows = hourly_grid_summary.count()
    return output_rows


def main():
    parser = argparse.ArgumentParser(description="Telecom network activity ETL pipeline")
    parser.add_argument("--input-dir", required=True, help="Folder with daily sms-call-internet-mi-*.csv files")
    parser.add_argument("--reference-dir", required=True, help="Folder with milano-grid.geojson")
    parser.add_argument("--output-dir", required=True, help="Folder to write processed/analytics outputs")
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"=== Pipeline started at {start_time} ===")
    logger.info(f"input_dir={args.input_dir}, reference_dir={args.reference_dir}, output_dir={args.output_dir}")

    spark = SparkSession.builder \
        .appName("TelecomPipelineSP7") \
        .master("local[2]") \
        .config("spark.python.worker.timeout", "600") \
        .getOrCreate()

    status = "FAILED"
    try:
        canonical_df, file_count = read_raw(spark, args.input_dir)
        clean_df, clean_metrics = clean(canonical_df)
        hourly_grid_summary = aggregate(clean_df)
        enriched_df = enrich(spark, hourly_grid_summary, args.reference_dir)  # validated, not written directly
        output_rows = write_outputs(clean_df, hourly_grid_summary, args.output_dir)

        status = "SUCCESS"

        logger.info("=== Pipeline Metrics ===")
        logger.info(f"input_rows={clean_metrics['input_rows']}")
        logger.info(f"rejected_missing_keys={clean_metrics['rejected_missing_keys']}")
        logger.info(f"rejected_negative={clean_metrics['rejected_negative']}")
        logger.info(f"nulls_handled={clean_metrics['nulls_handled']}")
        logger.info(f"output_rows={output_rows}")

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        status = "FAILED"
        end_time = datetime.now()
        logger.info(f"=== Pipeline ended at {end_time} — status: {status} ===")
        spark.stop()
        sys.exit(1)  # non-zero exit code — required for "fail cleanly" acceptance criterion

    end_time = datetime.now()
    logger.info(f"=== Pipeline ended at {end_time} — status: {status} ===")
    spark.stop()


if __name__ == "__main__":
    main()
