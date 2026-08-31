import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, to_timestamp, to_date, hour, dayofweek

# ------------------------------------------------------------------
# Config — test against ONE single day only (as required by the lab)
# ------------------------------------------------------------------
TEST_DATE = "2013-11-01"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_FILE = os.path.join(BASE_DIR, "..", "data", "raw", f"sms-call-internet-mi-{TEST_DATE}.csv")

# ------------------------------------------------------------------
# PANDAS PATH — reuse the exact NP2 UsageProcessor logic
# ------------------------------------------------------------------
RAW_TO_CANONICAL = {
    "datetime": "timestamp", "CellID": "grid_id", "countrycode": "country_code",
    "smsin": "sms_in", "smsout": "sms_out", "callin": "call_in",
    "callout": "call_out", "internet": "internet_activity",
}
ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

pdf = pd.read_csv(RAW_FILE)
pdf = pdf.rename(columns=RAW_TO_CANONICAL)
pdf = pdf.dropna(subset=["grid_id", "timestamp"])
for c in ACTIVITY_COLUMNS:
    pdf = pdf[~(pdf[c] < 0)]
pdf[ACTIVITY_COLUMNS] = pdf[ACTIVITY_COLUMNS].fillna(0)
pdf["timestamp"] = pd.to_datetime(pdf["timestamp"])
pdf["total_sms"] = pdf["sms_in"] + pdf["sms_out"]
pdf["total_calls"] = pdf["call_in"] + pdf["call_out"]
pdf["total_activity"] = pdf["total_sms"] + pdf["total_calls"] + pdf["internet_activity"]

pandas_row_count = len(pdf)
pandas_total_activity_sum = round(pdf["total_activity"].sum(), 2)

# ------------------------------------------------------------------
# SPARK PATH — same rules, same single file
# ------------------------------------------------------------------
spark = SparkSession.builder.appName("SP2ComparisonTest").master("local[*]").getOrCreate()

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

sdf = spark.read.csv(RAW_FILE, schema=network_schema, header=True)
sdf = sdf.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))

sdf = (
    sdf.withColumnRenamed("datetime", "timestamp")
       .withColumnRenamed("CellID", "grid_id")
       .withColumnRenamed("countrycode", "country_code")
       .withColumnRenamed("smsin", "sms_in")
       .withColumnRenamed("smsout", "sms_out")
       .withColumnRenamed("callin", "call_in")
       .withColumnRenamed("callout", "call_out")
       .withColumnRenamed("internet", "internet_activity")
)

sdf = sdf.dropna(subset=["grid_id", "timestamp"])
condition = None
for c in ACTIVITY_COLUMNS:
    cond = (col(c) >= 0) | col(c).isNull()
    condition = cond if condition is None else (condition & cond)
sdf = sdf.filter(condition)
sdf = sdf.fillna(0, subset=ACTIVITY_COLUMNS)

sdf = (
    sdf.withColumn("total_sms", col("sms_in") + col("sms_out"))
       .withColumn("total_calls", col("call_in") + col("call_out"))
       .withColumn("total_activity", col("total_sms") + col("total_calls") + col("internet_activity"))
)

spark_row_count = sdf.count()
spark_total_activity_sum = round(sdf.selectExpr("sum(total_activity) as s").collect()[0]["s"], 2)

# ------------------------------------------------------------------
# COMPARISON — must match
# ------------------------------------------------------------------
print("=== SP2 Pandas vs Spark Comparison ===")
print(f"Pandas row count:  {pandas_row_count}")
print(f"Spark row count:   {spark_row_count}")
print(f"Pandas total_activity sum: {pandas_total_activity_sum}")
print(f"Spark total_activity sum:  {spark_total_activity_sum}")

assert pandas_row_count == spark_row_count, "MISMATCH: row counts differ between pandas and Spark"
assert abs(pandas_total_activity_sum - spark_total_activity_sum) < 0.01, "MISMATCH: total_activity sums differ"

print("\n✅ Pandas and Spark outputs match for", TEST_DATE)