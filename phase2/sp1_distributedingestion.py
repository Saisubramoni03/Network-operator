import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, to_timestamp, input_file_name

spark = SparkSession.builder \
    .appName("NetworkIngestionSP1") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session started:", spark.version)

network_schema = StructType([
    StructField("datetime", StringType(), True),   # raw file: human-readable string, not epoch ms
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

raw_network_df = spark.read.csv(
    DATA_PATH,
    schema=network_schema,
    header=True
)

# Fix: parse the human-readable datetime string properly
raw_network_df = raw_network_df.withColumn(
    "datetime",
    to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")
)

raw_network_df = raw_network_df.withColumn("input_file_name", input_file_name())

raw_network_df.select("datetime", "input_file_name").show(10, truncate=False)
raw_network_df.printSchema()

row_count = raw_network_df.count()
file_count = raw_network_df.select("input_file_name").distinct().count()
unique_grids = raw_network_df.select("CellID").distinct().count()
country_categories = raw_network_df.select("countrycode").distinct().count()
distinct_hours = raw_network_df.select("datetime").distinct().count()

print(f"Row count: {row_count}")
print(f"File count: {file_count}")
print(f"Unique grids: {unique_grids}")
print(f"Country-code categories: {country_categories}")
print(f"Distinct hourly timestamps: {distinct_hours}")

print("Number of partitions:", raw_network_df.rdd.getNumPartitions())