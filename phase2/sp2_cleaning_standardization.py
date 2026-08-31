import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    col, to_timestamp, input_file_name, to_date, hour, dayofweek
)

# ------------------------------------------------------------------
# Step 0: Spark session
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("NetworkCleaningSP2") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session started:", spark.version)

# ------------------------------------------------------------------
# Step 1: Load raw data (same as SP1, with the datetime fix applied)
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "..", "data", "raw", "sms-call-internet-mi-*.csv")
print("Looking for files at:", DATA_PATH)

raw_network_df = spark.read.csv(DATA_PATH, schema=network_schema, header=True)

raw_network_df = raw_network_df.withColumn(
    "datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")
)
raw_network_df = raw_network_df.withColumn("input_file_name", input_file_name())

# ------------------------------------------------------------------
# Step 2: Rename raw columns to canonical names
# ------------------------------------------------------------------
canonical_df = (
    raw_network_df
    .withColumnRenamed("datetime", "timestamp")
    .withColumnRenamed("CellID", "grid_id")
    .withColumnRenamed("countrycode", "country_code")
    .withColumnRenamed("smsin", "sms_in")
    .withColumnRenamed("smsout", "sms_out")
    .withColumnRenamed("callin", "call_in")
    .withColumnRenamed("callout", "call_out")
    .withColumnRenamed("internet", "internet_activity")
)

# ------------------------------------------------------------------
# Step 3: Verify hourly cadence still holds across ALL files
# ------------------------------------------------------------------
distinct_hours = canonical_df.select("timestamp").distinct().count()
file_count = canonical_df.select("input_file_name").distinct().count()
expected_hours = file_count * 24
print(f"Distinct hours: {distinct_hours}, Expected: {expected_hours}")
assert distinct_hours == expected_hours, "Cadence check failed — hours don't match file count × 24"

# ------------------------------------------------------------------
# Step 4: Quarantine bad rows — reject vs handle, same rule as NP2
# ------------------------------------------------------------------
activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

input_rows = canonical_df.count()

# Reject: missing grid_id / timestamp
after_key_check = canonical_df.dropna(subset=["grid_id", "timestamp"])
rejected_missing_keys = input_rows - after_key_check.count()

# Reject: negative activity values (null allowed through here — handled next step)
condition = None
for c in activity_cols:
    cond = (col(c) >= 0) | col(c).isNull()
    condition = cond if condition is None else (condition & cond)

before_negative_check = after_key_check.count()
after_negative_check = after_key_check.filter(condition)
rejected_negative = before_negative_check - after_negative_check.count()

# ------------------------------------------------------------------
# Step 5: Curated-layer null-to-zero rule (counted separately)
# ------------------------------------------------------------------
null_counts_before = {
    c: after_negative_check.filter(col(c).isNull()).count() for c in activity_cols
}
nulls_handled = sum(null_counts_before.values())
print(f"Nulls handled per column: {null_counts_before}")

clean_df = after_negative_check.fillna(0, subset=activity_cols)

# ------------------------------------------------------------------
# Step 6: Derive date, hour, day_of_week
# ------------------------------------------------------------------
clean_df = (
    clean_df
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))  # Spark: 1=Sunday...7=Saturday
)

# ------------------------------------------------------------------
# Step 7: Derived measures — keep originals too
# ------------------------------------------------------------------
clean_network_df = (
    clean_df
    .withColumn("total_sms", col("sms_in") + col("sms_out"))
    .withColumn("total_calls", col("call_in") + col("call_out"))
    .withColumn("total_activity", col("total_sms") + col("total_calls") + col("internet_activity"))
)

# ------------------------------------------------------------------
# Step 8: Final report — three separate numbers, never merged
# ------------------------------------------------------------------
output_rows = clean_network_df.count()

print("\n=== SP2 Cleaning Report ===")
print(f"Input rows:              {input_rows}")
print(f"Rejected (missing keys): {rejected_missing_keys}")
print(f"Rejected (negative):     {rejected_negative}")
print(f"Nulls handled:           {nulls_handled}")
print(f"Output rows:             {output_rows}")

clean_network_df.printSchema()
clean_network_df.show(5)