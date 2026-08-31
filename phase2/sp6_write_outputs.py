import os
import sys
import glob

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, dayofweek, sum as spark_sum
)

spark = SparkSession.builder \
    .appName("WriteOutputsSP6") \
    .master("local[2]") \
    .config("spark.python.worker.timeout", "600") \
    .getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------------
# Rebuild clean_network_df + hourly_grid_summary (SP2/SP3 logic)
# ------------------------------------------------------------------
network_schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])

DATA_PATH = os.path.join(BASE_DIR, "..", "data", "raw", "sms-call-internet-mi-*.csv")
raw_df = spark.read.csv(DATA_PATH, schema=network_schema, header=True)
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

activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]
condition = None
for c in activity_cols:
    cond = (col(c) >= 0) | col(c).isNull()
    condition = cond if condition is None else (condition & cond)

clean_network_df = (
    canonical_df.dropna(subset=["grid_id", "timestamp"])
    .filter(condition)
    .fillna(0, subset=activity_cols)
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))
)

hourly_grid_summary = (
    clean_network_df.groupBy("grid_id", "timestamp", "date", "hour", "day_of_week")
    .agg(*[spark_sum(c).alias(c) for c in activity_cols])
    .withColumn("total_activity",
                col("sms_in") + col("sms_out") + col("call_in") + col("call_out") + col("internet_activity"))
)

# ==================================================================
# Step 1: Write cleaned activity data as Parquet, partitioned by date
# ==================================================================
CLEAN_OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data", "processed", "activity")

print("=== Step 1: Writing cleaned activity as Parquet (partitioned by date) ===")
(
    clean_network_df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(CLEAN_OUTPUT_PATH)
)
print(f"Written to: {CLEAN_OUTPUT_PATH}")

# ==================================================================
# Step 2: Write hourly_grid_summary as Parquet — NO geometry here
# ==================================================================
HOURLY_OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data", "analytics", "hourly_grid_summary")

print("\n=== Step 2: Writing hourly_grid_summary as Parquet ===")
assert "geometry" not in hourly_grid_summary.columns, "Geometry must NOT be in the analytics fact table!"

(
    hourly_grid_summary
    .write
    .mode("overwrite")
    .parquet(HOURLY_OUTPUT_PATH)
)
print(f"Written to: {HOURLY_OUTPUT_PATH}")

# ==================================================================
# Step 3: Write a small dashboard summary as CSV
# ==================================================================
DASHBOARD_OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data", "processed", "dashboard_summary.csv")

dashboard_summary = (
    hourly_grid_summary
    .groupBy("date")
    .agg(spark_sum("total_activity").alias("daily_total_activity"))
    .orderBy("date")
)

print("\n=== Step 3: Writing dashboard summary as CSV ===")
# coalesce(1) -> a single readable CSV file, since this is a small summary table
(
    dashboard_summary
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(DASHBOARD_OUTPUT_PATH)
)
print(f"Written to: {DASHBOARD_OUTPUT_PATH}")

# NOTE: milano-grid.geojson intentionally stays in data/raw (or should be
# moved to data/reference/) as static reference data — it is NOT rewritten
# here, since it doesn't change per run.

# ==================================================================
# Step 4: Round-trip validation — read Parquet back, check everything
# ==================================================================
print("\n=== Step 4: Round-trip validation ===")

original_clean_count = clean_network_df.count()
original_hourly_count = hourly_grid_summary.count()

reread_clean_df = spark.read.parquet(CLEAN_OUTPUT_PATH)
reread_hourly_df = spark.read.parquet(HOURLY_OUTPUT_PATH)

reread_clean_count = reread_clean_df.count()
reread_hourly_count = reread_hourly_df.count()

print(f"Original clean_network_df rows: {original_clean_count}")
print(f"Re-read clean activity rows:    {reread_clean_count}")
assert original_clean_count == reread_clean_count, "ROUND-TRIP MISMATCH: clean activity row count differs!"

print(f"Original hourly_grid_summary rows: {original_hourly_count}")
print(f"Re-read hourly_grid_summary rows:  {reread_hourly_count}")
assert original_hourly_count == reread_hourly_count, "ROUND-TRIP MISMATCH: hourly summary row count differs!"

# Schema check
print("\nRe-read clean activity schema:")
reread_clean_df.printSchema()

print("Re-read hourly_grid_summary schema:")
reread_hourly_df.printSchema()

# Duplicate check STILL holds after round-trip
duplicate_count_after = (
    reread_hourly_df.groupBy("grid_id", "timestamp").count()
    .filter(col("count") > 1)
    .count()
)
assert duplicate_count_after == 0, f"ROUND-TRIP FAILURE: {duplicate_count_after} duplicates found after re-read!"
print(f"\n✅ Zero duplicates confirmed after round-trip: {duplicate_count_after}")

assert "geometry" not in reread_hourly_df.columns, "Geometry leaked into the re-read analytics table!"
print("✅ Confirmed: no geometry column in hourly_grid_summary")

# ==================================================================
# Step 5: Verify partition layout on disk
# ==================================================================
print("\n=== Step 5: Partition folder layout on disk ===")
partition_folders = sorted(glob.glob(os.path.join(CLEAN_OUTPUT_PATH, "date=*")))
for folder in partition_folders:
    print(" -", os.path.basename(folder))
print(f"Total date partitions found: {len(partition_folders)} (expect 7)")

# ==================================================================
# Step 6: File size comparison — CSV vs Parquet
# ==================================================================
print("\n=== Step 6: File size comparison (CSV vs Parquet) ===")

def get_folder_size(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    return total

raw_csv_size = sum(
    os.path.getsize(f) for f in glob.glob(os.path.join(BASE_DIR, "..", "data", "raw", "sms-call-internet-mi-*.csv"))
)
parquet_clean_size = get_folder_size(CLEAN_OUTPUT_PATH)

print(f"Raw CSV total size (7 files):        {raw_csv_size / (1024**2):.1f} MB")
print(f"Parquet clean activity total size:   {parquet_clean_size / (1024**2):.1f} MB")
print(f"Size ratio (Parquet/CSV):            {parquet_clean_size / raw_csv_size:.2%}")

print("\n✅ SP6 complete.")