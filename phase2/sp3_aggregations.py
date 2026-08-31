import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    col, to_timestamp, input_file_name, to_date, hour, dayofweek, sum as spark_sum
)

spark = SparkSession.builder.appName("NetworkAggregationsSP3").master("local[*]").getOrCreate()

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

raw_network_df = spark.read.csv(DATA_PATH, schema=network_schema, header=True)
raw_network_df = raw_network_df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
raw_network_df = raw_network_df.withColumn("input_file_name", input_file_name())

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

activity_cols = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

condition = None
for c in activity_cols:
    cond = (col(c) >= 0) | col(c).isNull()
    condition = cond if condition is None else (condition & cond)

clean_network_df = (
    canonical_df
    .dropna(subset=["grid_id", "timestamp"])
    .filter(condition)
    .fillna(0, subset=activity_cols)
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))
)

clean_input_rows = clean_network_df.count()

# ------------------------------------------------------------------
# Step 1: THE grain transition — country_code rows -> one row per grid/hour
# ------------------------------------------------------------------
hourly_grid_summary = (
    clean_network_df
    .groupBy("grid_id", "timestamp", "date", "hour", "day_of_week")
    .agg(
        spark_sum("sms_in").alias("sms_in"),
        spark_sum("sms_out").alias("sms_out"),
        spark_sum("call_in").alias("call_in"),
        spark_sum("call_out").alias("call_out"),
        spark_sum("internet_activity").alias("internet_activity"),
    )
)

duplicate_count = (
    hourly_grid_summary.groupBy("grid_id", "timestamp").count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count != 0:
    raise RuntimeError(f"GRAIN VIOLATION: {duplicate_count} duplicate (grid_id, timestamp) pairs found!")

print(f"✅ Zero duplicates confirmed on (grid_id, timestamp): {duplicate_count}")

if "country_code" in hourly_grid_summary.columns:
    raise RuntimeError("GRAIN VIOLATION: country_code leaked into hourly_grid_summary!")

hourly_grid_summary = (
    hourly_grid_summary
    .withColumn("total_sms", col("sms_in") + col("sms_out"))
    .withColumn("total_calls", col("call_in") + col("call_out"))
    .withColumn("total_activity", col("total_sms") + col("total_calls") + col("internet_activity"))
    .withColumn(
        "internet_share",
        col("internet_activity") / col("total_activity")
    )
)

daily_traffic_summary = (
    hourly_grid_summary
    .groupBy("grid_id", "date")
    .agg(spark_sum("total_activity").alias("daily_total_activity"))
)

top_10_grids = (
    hourly_grid_summary
    .groupBy("grid_id")
    .agg(spark_sum("total_activity").alias("grid_total_activity"))
    .orderBy(col("grid_total_activity").desc())
    .limit(10)
)

peak_hour_row = (
    hourly_grid_summary
    .groupBy("hour")
    .agg(spark_sum("total_activity").alias("hour_total_activity"))
    .orderBy(col("hour_total_activity").desc())
    .limit(1)
    .collect()[0]
)
print(f"Peak activity hour: {peak_hour_row['hour']} (total_activity={peak_hour_row['hour_total_activity']:.2f})")

hourly_row_count = hourly_grid_summary.count()
file_count = canonical_df.select("input_file_name").distinct().count()

print("\n=== SP3 Acceptance Checks ===")
print(f"clean_network_df rows:   {clean_input_rows}")
print(f"hourly_grid_summary rows: {hourly_row_count}")
print(f"Max allowed (D×24×10000): {file_count * 24 * 10000}")

assert hourly_row_count < clean_input_rows, "hourly_grid_summary is not smaller than clean_network_df!"
assert hourly_row_count <= file_count * 24 * 10000, "hourly_grid_summary exceeds max possible grid/hour combinations!"
assert "country_code" not in hourly_grid_summary.columns, "country_code leaked into analytics grain!"

print("✅ All SP3 acceptance checks passed.")


check_row = hourly_grid_summary.limit(1).collect()[0]
check_grid, check_ts = check_row["grid_id"], check_row["timestamp"]

print(f"\nManual check for grid_id={check_grid}, timestamp={check_ts}")
canonical_df.filter(
    (col("grid_id") == check_grid) & (col("timestamp") == check_ts)
).select("grid_id", "timestamp", "country_code", *activity_cols).show()

print(f"Aggregated row (should equal the sum of the above rows):")
hourly_grid_summary.filter(
    (col("grid_id") == check_grid) & (col("timestamp") == check_ts)
).select("grid_id", "timestamp", *activity_cols).show()


