import os
import sys
import json

# Fix: make Spark use the exact same Python interpreter, avoid mismatch issues
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import (
    col, to_timestamp, input_file_name, to_date, hour, dayofweek,
    sum as spark_sum
)

# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("GeoEnrichmentSP4") \
    .master("local[2]") \
    .config("spark.python.worker.timeout", "600") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------------
# Step 1: Load and inspect milano-grid.geojson (plain Python, not Spark)
# ------------------------------------------------------------------
GEOJSON_PATH = os.path.join(BASE_DIR, "..", "data", "raw", "milano-grid.geojson")

with open(GEOJSON_PATH, "r") as f:
    grid_geojson = json.load(f)

print("Top-level type:", grid_geojson["type"])
print("Number of features:", len(grid_geojson["features"]))

sample_feature = grid_geojson["features"][0]
print("\nFull sample feature keys:", list(sample_feature.keys()))
print("Top-level 'id' present:", "id" in sample_feature, "-> value:", sample_feature.get("id"))
print("'properties' content:", sample_feature.get("properties"))

# ------------------------------------------------------------------
# Step 2 & 3: Flatten features[] into grid_id + geometry
# properties.cellId -> grid_id  (confirmed identifier mapping)
# ------------------------------------------------------------------
grid_lookup_rows = []
for feature in grid_geojson["features"]:
    cell_id = feature["properties"]["cellId"]   # confirmed source identifier
    geometry_type = feature["geometry"]["type"]
    coordinates = feature["geometry"]["coordinates"]

    ring = coordinates[0] if geometry_type == "Polygon" else coordinates

    grid_lookup_rows.append({
        "grid_id": cell_id,
        "geometry_type": geometry_type,
        "coordinates": ring,
    })

print(f"\nFlattened grid lookup size: {len(grid_lookup_rows)} (expect 10,000)")

# ------------------------------------------------------------------
# Step 4: Compute centroid for each grid in plain Python
# ------------------------------------------------------------------
def polygon_centroid(coords):
    lons = [pt[0] for pt in coords]
    lats = [pt[1] for pt in coords]
    return sum(lons) / len(lons), sum(lats) / len(lats)

for row in grid_lookup_rows:
    lon, lat = polygon_centroid(row["coordinates"])
    row["centroid_lon"] = lon
    row["centroid_lat"] = lat

# ------------------------------------------------------------------
# Step 5: Build the Spark grid lookup DataFrame (small: 10,000 rows)
# ------------------------------------------------------------------
grid_lookup_schema = StructType([
    StructField("grid_id", IntegerType(), True),
    StructField("centroid_lon", DoubleType(), True),
    StructField("centroid_lat", DoubleType(), True),
])

grid_lookup_df = spark.createDataFrame(
    [{"grid_id": r["grid_id"], "centroid_lon": r["centroid_lon"], "centroid_lat": r["centroid_lat"]}
     for r in grid_lookup_rows],
    schema=grid_lookup_schema
)

print(f"\nGrid lookup rows: {grid_lookup_df.count()} (expect 10,000, one per grid)")

# ------------------------------------------------------------------
# Step 6: Rebuild hourly_grid_summary (SP3 logic)
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
raw_df = raw_df.withColumn("input_file_name", input_file_name())

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

clean_df = (
    canonical_df.dropna(subset=["grid_id", "timestamp"])
    .filter(condition)
    .fillna(0, subset=activity_cols)
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))
)

hourly_grid_summary = (
    clean_df.groupBy("grid_id", "timestamp", "date", "hour", "day_of_week")
    .agg(*[spark_sum(c).alias(c) for c in activity_cols])
    .withColumn("total_activity",
                col("sms_in") + col("sms_out") + col("call_in") + col("call_out") + col("internet_activity"))
)

# ------------------------------------------------------------------
# Step 7: The join — LEFT join, NO broadcast hint (removed due to
# a local Windows PySpark issue where the broadcast mechanism's
# background Python worker fails to start; a normal shuffle join
# is slightly slower but functionally identical and more reliable here)
# ------------------------------------------------------------------
rows_before_join = hourly_grid_summary.count()
distinct_grids_before = hourly_grid_summary.select("grid_id").distinct().count()

grid_activity_geo_df = hourly_grid_summary.join(
    grid_lookup_df,
    on="grid_id",
    how="left"
)

rows_after_join = grid_activity_geo_df.count()

# ------------------------------------------------------------------
# Step 8: Validate the join — numerically
# ------------------------------------------------------------------
unmatched_grids = (
    grid_activity_geo_df.filter(col("centroid_lon").isNull())
    .select("grid_id").distinct()
)
unmatched_count = unmatched_grids.count()
coverage_pct = 100 * (distinct_grids_before - unmatched_count) / distinct_grids_before

print("\n=== SP4 Join Validation (numeric) ===")
print(f"Rows before join:  {rows_before_join}")
print(f"Rows after join:   {rows_after_join}")
print(f"Distinct grids before join: {distinct_grids_before}")
print(f"Unmatched grid count: {unmatched_count}")
print(f"Enrichment coverage: {coverage_pct:.2f}%")

assert rows_after_join == rows_before_join, "LEFT JOIN MULTIPLIED ROWS — grid lookup has duplicate keys!"
assert unmatched_count == 0, "Unmatched grids found — coverage is not 100%!"

# ------------------------------------------------------------------
# Step 9: Validate the join — geographically
# ------------------------------------------------------------------
def get_centroid(grid_id):
    row = grid_lookup_df.filter(col("grid_id") == grid_id).collect()[0]
    return row["centroid_lon"], row["centroid_lat"]

lon1, lat1 = get_centroid(1)
lon2, lat2 = get_centroid(2)

print("\n=== SP4 Geographic Spot-Check ===")
print(f"Grid 1 centroid:  lon={lon1:.5f}, lat={lat1:.5f}")
print(f"Grid 2 centroid:  lon={lon2:.5f}, lat={lat2:.5f}")
print("(Expect these to be close/adjacent to each other, NOT identical, NOT far apart)")
print("(Expect both to fall within Milan's bounding box: lon ~9.0–9.3, lat ~45.35–45.55)")

# ------------------------------------------------------------------
# Step 10: Top high-activity grids, geometry retained
# ------------------------------------------------------------------
top_10_grids_geo = (
    grid_activity_geo_df.groupBy("grid_id", "centroid_lon", "centroid_lat")
    .agg(spark_sum("total_activity").alias("grid_total_activity"))
    .orderBy(col("grid_total_activity").desc())
    .limit(10)
)

print("\n=== Top 10 High-Activity Grids (with geometry) ===")
top_10_grids_geo.show()

print("\n✅ SP4 complete — grid_activity_geo_df is ready.")