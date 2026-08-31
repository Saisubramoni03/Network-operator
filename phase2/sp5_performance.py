import os
import sys
import time

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import (
    col, to_timestamp, input_file_name, to_date, hour, dayofweek,
    sum as spark_sum
)

spark = SparkSession.builder \
    .appName("PerformanceSP5") \
    .master("local[2]") \
    .config("spark.python.worker.timeout", "600") \
    .getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------------
# Rebuild clean_network_df (SP2/SP3 logic) — needed as our test subject
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

# ==================================================================
# EXPERIMENT 1: explain() on the hotspot aggregation — read the plan first
# ==================================================================
hourly_grid_summary = (
    clean_network_df.groupBy("grid_id", "timestamp", "date", "hour", "day_of_week")
    .agg(*[spark_sum(c).alias(c) for c in activity_cols])
)

print("=" * 70)
print("EXPERIMENT 1: Physical plan for the grid/hour aggregation")
print("=" * 70)
hourly_grid_summary.explain()

# ==================================================================
# EXPERIMENT 2: cache() — before/after timing on a REUSED DataFrame
# ==================================================================
print("\n" + "=" * 70)
print("EXPERIMENT 2: Cache timing — same DataFrame, action run 3 times")
print("=" * 70)

# --- WITHOUT cache ---
start = time.time()
count1 = clean_network_df.count()
t1 = time.time() - start

start = time.time()
count2 = clean_network_df.count()
t2 = time.time() - start

start = time.time()
count3 = clean_network_df.count()
t3 = time.time() - start

print(f"WITHOUT cache — run1: {t1:.2f}s, run2: {t2:.2f}s, run3: {t3:.2f}s")

# --- WITH cache ---
cached_df = clean_network_df.cache()

start = time.time()
count1c = cached_df.count()   # first action actually materializes the cache
t1c = time.time() - start

start = time.time()
count2c = cached_df.count()
t2c = time.time() - start

start = time.time()
count3c = cached_df.count()
t3c = time.time() - start

print(f"WITH cache    — run1: {t1c:.2f}s (builds cache), run2: {t2c:.2f}s, run3: {t3c:.2f}s")

# ==================================================================
# EXPERIMENT 3: repartition — observe partition counts, don't assume "more=better"
# ==================================================================
print("\n" + "=" * 70)
print("EXPERIMENT 3: Partition counts before/after repartition")
print("=" * 70)

print("Default partitions:", clean_network_df.rdd.getNumPartitions())

repartitioned_by_date = clean_network_df.repartition("date")
print("Repartitioned by 'date':", repartitioned_by_date.rdd.getNumPartitions())

over_partitioned = clean_network_df.repartition(200)
print("Over-partitioned to 200:", over_partitioned.rdd.getNumPartitions())

start = time.time()
clean_network_df.repartition("date").count()
t_repart = time.time() - start

start = time.time()
over_partitioned.count()
t_over = time.time() - start

print(f"Timing — repartition by date: {t_repart:.2f}s | over-partitioned(200): {t_over:.2f}s")

# ==================================================================
# EXPERIMENT 4: column pruning — select only what's needed BEFORE aggregating
# ==================================================================
print("\n" + "=" * 70)
print("EXPERIMENT 4: Column pruning — full columns vs pruned columns")
print("=" * 70)

start = time.time()
full_result = (
    clean_network_df
    .groupBy("grid_id")
    .agg(spark_sum("internet_activity").alias("total_internet"))
    .count()
)
t_full = time.time() - start

start = time.time()
pruned_result = (
    clean_network_df
    .select("grid_id", "internet_activity")   # prune BEFORE aggregating
    .groupBy("grid_id")
    .agg(spark_sum("internet_activity").alias("total_internet"))
    .count()
)
t_pruned = time.time() - start

print(f"Full columns: {t_full:.2f}s | Pruned columns: {t_pruned:.2f}s")

print("\n✅ SP5 experiments complete — see written observations below.")




"""
=== SP5 Performance Observations ===

OBSERVATION 1: Caching a reused DataFrame produces a dramatic speedup on 
repeated actions — but with an important caveat about memory
Evidence: WITHOUT cache, three .count() calls took 13.15s, 11.57s, 11.41s — 
roughly equal, since Spark recomputes the full read+parse+filter+fillna chain 
from scratch every time (lazy evaluation). WITH .cache(), the first call took 
22.38s (slower than uncached — this is the cost of actually materializing and 
storing the data), but the second and third calls dropped to 0.34s and 0.33s — 
over 30x faster than the uncached runs. 
Important caveat: the logs show repeated "Not enough space to cache in 
memory! ... Persisting to disk instead" warnings — this machine doesn't have 
enough RAM to hold the full cached dataset in memory, so Spark automatically 
spilled part of it to disk. Even with disk spill, cache is still dramatically 
faster than full recomputation from raw CSV, because reading from local disk 
cache skips re-parsing 15M rows of text every time.
Decision: ACCEPTED — but noted that on memory-constrained machines, cache's 
benefit comes with a real cost (disk I/O), so it should be reserved for 
DataFrames genuinely reused many times, not applied reflexively.

OBSERVATION 2: Repartitioning to 200 is measurably WORSE than repartitioning 
by "date" on this machine — over-partitioning has a real, measured cost, not 
just a theoretical one
Evidence: Default partition count is 7 (matching the 7 input files). 
Repartitioning by "date" kept it at 7 partitions and took 2.97s. Forcing 200 
partitions took 5.30s — nearly double, even though "200 partitions" sounds 
like more parallelism. On this local[2] machine (only 2 usable workers, due to 
an earlier environment issue), 200 tiny partitions creates far more 
scheduling overhead — managing 200 small tasks across just 2 workers costs 
more than it saves, since each partition ends up too small to be worth its 
own coordination overhead.
Decision: REJECTED (the over-partitioning suggestion) — this is the required 
"at least one rejected suggestion." Interesting side note: the physical plan 
in Experiment 1 shows Spark's *default* shuffle step already uses 200 
partitions internally (Spark's global default, unrelated to our manual test) 
— on a machine this size, even that default is arguably too high, but that's 
a Spark-wide tuning question beyond this lab's scope.

OBSERVATION 3: Column pruning before aggregating provides a real, if modest, 
improvement
Evidence: aggregating with all columns present took 2.51s; selecting only 
grid_id and internet_activity before aggregating took 2.19s — about 13% 
faster. The difference reflects less data being carried through the shuffle 
stage, even though both produce the identical final result.
Decision: ACCEPTED as a low-risk default habit — prune columns before 
expensive groupBy/join operations, especially as this project scales to more 
columns or more days of data in later phases where the saving would compound.
"""