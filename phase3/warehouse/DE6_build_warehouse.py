"""
DE6_build_warehouse.py — Build a star-schema SQLite warehouse from the
Spark-produced hourly_grid_summary Parquet output and the static Milan
grid reference.
"""

import os
import json
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..", "..")

HOURLY_PARQUET_DIR = os.path.join(PROJECT_ROOT, "data", "analytics", "hourly_grid_summary")
GEOJSON_PATH = os.path.join(PROJECT_ROOT, "data", "reference", "milano-grid.geojson")
DB_PATH = os.path.join(BASE_DIR, "network_warehouse.db")

# ------------------------------------------------------------------
# Step 1: DDL — tables only, NO indexes yet (indexes added after load)
# ------------------------------------------------------------------
TABLE_DDL = """
DROP TABLE IF EXISTS fact_network_activity;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_grid;

CREATE TABLE dim_grid (
    grid_id      INTEGER PRIMARY KEY,
    centroid_lon REAL,
    centroid_lat REAL,
    geometry_ref TEXT
);

CREATE TABLE dim_time (
    time_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp    TEXT UNIQUE NOT NULL,
    date         TEXT,
    hour         INTEGER,
    day_of_week  INTEGER
);

CREATE TABLE fact_network_activity (
    fact_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    grid_id           INTEGER NOT NULL REFERENCES dim_grid(grid_id),
    time_id           INTEGER NOT NULL REFERENCES dim_time(time_id),
    sms_in            REAL,
    sms_out           REAL,
    call_in           REAL,
    call_out          REAL,
    internet_activity REAL,
    total_activity    REAL
);
"""

INDEX_DDL = """
CREATE INDEX idx_fact_grid ON fact_network_activity(grid_id);
CREATE INDEX idx_fact_time ON fact_network_activity(time_id);
CREATE UNIQUE INDEX idx_dim_time_timestamp ON dim_time(timestamp);
"""


def build_tables(conn):
    conn.executescript(TABLE_DDL)
    print("Tables created: dim_grid, dim_time, fact_network_activity (indexes come after load)")


def load_dim_grid(conn):
    with open(GEOJSON_PATH, "r") as f:
        grid_geojson = json.load(f)

    def polygon_centroid(coords):
        lons = [pt[0] for pt in coords]
        lats = [pt[1] for pt in coords]
        return sum(lons) / len(lons), sum(lats) / len(lats)

    rows = []
    for feature in grid_geojson["features"]:
        cell_id = feature["properties"]["cellId"]
        ring = feature["geometry"]["coordinates"][0]
        lon, lat = polygon_centroid(ring)
        rows.append((cell_id, lon, lat, GEOJSON_PATH))

    conn.executemany(
        "INSERT INTO dim_grid (grid_id, centroid_lon, centroid_lat, geometry_ref) VALUES (?, ?, ?, ?)",
        rows
    )
    conn.commit()
    print(f"dim_grid populated: {len(rows)} rows (expect 10000)")


def load_fact_and_dim_time(conn):
    df = pd.read_parquet(HOURLY_PARQUET_DIR)
    print(f"Loaded hourly_grid_summary: {len(df)} rows from Parquet")

    time_df = (
        df[["timestamp", "date", "hour", "day_of_week"]]
        .drop_duplicates(subset="timestamp")
        .copy()
    )
    time_df["timestamp"] = time_df["timestamp"].astype(str)
    time_df["date"] = time_df["date"].astype(str)

    time_rows = list(time_df[["timestamp", "date", "hour", "day_of_week"]].itertuples(index=False, name=None))
    conn.executemany(
        "INSERT INTO dim_time (timestamp, date, hour, day_of_week) VALUES (?, ?, ?, ?)",
        time_rows
    )
    conn.commit()
    print(f"dim_time populated: {len(time_rows)} distinct timestamps")

    time_id_lookup = dict(conn.execute("SELECT timestamp, time_id FROM dim_time").fetchall())

    df["timestamp_str"] = df["timestamp"].astype(str)
    df["time_id"] = df["timestamp_str"].map(time_id_lookup)

    fact_rows = list(
        df[["grid_id", "time_id", "sms_in", "sms_out", "call_in", "call_out",
            "internet_activity", "total_activity"]]
        .itertuples(index=False, name=None)
    )

    # Speed tuning: single transaction, relaxed durability (fine for a
    # rebuildable analytics warehouse — see storage contract: this DB is
    # always regenerated from Spark output, never hand-edited)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")

    print(f"Inserting {len(fact_rows)} fact rows (this may take a moment)...")
    conn.executemany(
        """INSERT INTO fact_network_activity
           (grid_id, time_id, sms_in, sms_out, call_in, call_out, internet_activity, total_activity)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        fact_rows
    )
    conn.commit()
    print(f"fact_network_activity populated: {len(fact_rows)} rows")

    return df


def build_indexes(conn):
    conn.executescript(INDEX_DDL)
    conn.commit()
    print("Indexes created: idx_fact_grid, idx_fact_time, idx_dim_time_timestamp")


def run_validations(conn, source_df):
    print("\n=== DE6 Validation Checks ===")

    fact_cols = [row[1] for row in conn.execute("PRAGMA table_info(fact_network_activity)").fetchall()]
    assert "geometry" not in fact_cols and "geometry_ref" not in fact_cols, \
        "GEOMETRY LEAKED into fact_network_activity!"
    print(f"✅ fact_network_activity columns (no geometry): {fact_cols}")

    dim_grid_count = conn.execute("SELECT COUNT(*) FROM dim_grid").fetchone()[0]
    dim_grid_distinct = conn.execute("SELECT COUNT(DISTINCT grid_id) FROM dim_grid").fetchone()[0]
    assert dim_grid_count == dim_grid_distinct, "DUPLICATE grid_id values found in dim_grid!"
    print(f"✅ dim_grid: {dim_grid_count} rows, all distinct (expect 10000)")

    fact_count = conn.execute("SELECT COUNT(*) FROM fact_network_activity").fetchone()[0]
    source_count = len(source_df)
    assert fact_count == source_count, \
        f"FACT ROW COUNT MISMATCH: {fact_count} in DB vs {source_count} in source!"
    print(f"✅ Fact row count matches source: {fact_count} == {source_count}")

    sql_total = conn.execute("SELECT SUM(total_activity) FROM fact_network_activity").fetchone()[0]
    pandas_total = source_df["total_activity"].sum()
    print(f"SQL total_activity sum:    {sql_total}")
    print(f"Source total_activity sum: {pandas_total}")
    assert abs(sql_total - pandas_total) < 0.01, "AGGREGATE MISMATCH between SQL and source!"
    print("✅ SQL aggregate matches source aggregate exactly")

    indexes = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
    index_names = [i[0] for i in indexes]
    assert "idx_fact_grid" in index_names and "idx_fact_time" in index_names, \
        "Required indexes missing!"
    print(f"✅ Indexes present: {index_names}")

    print("\n✅ ALL DE6 VALIDATION CHECKS PASSED")


def run_sample_queries(conn):
    print("\n=== Sample Query 1: Top 10 grids by total activity ===")
    rows = conn.execute("""
        SELECT f.grid_id, g.centroid_lon, g.centroid_lat, SUM(f.total_activity) AS total
        FROM fact_network_activity f
        JOIN dim_grid g ON f.grid_id = g.grid_id
        GROUP BY f.grid_id
        ORDER BY total DESC
        LIMIT 10
    """).fetchall()
    for r in rows:
        print(r)

    print("\n=== Sample Query 2: Hourly trend (total activity by hour of day) ===")
    rows = conn.execute("""
        SELECT t.hour, SUM(f.total_activity) AS total
        FROM fact_network_activity f
        JOIN dim_time t ON f.time_id = t.time_id
        GROUP BY t.hour
        ORDER BY t.hour
    """).fetchall()
    for r in rows:
        print(r)

    print("\n=== Sample Query 3: Internet-heavy windows (top 10 grid/hours by internet_activity) ===")
    rows = conn.execute("""
        SELECT f.grid_id, t.timestamp, f.internet_activity
        FROM fact_network_activity f
        JOIN dim_time t ON f.time_id = t.time_id
        ORDER BY f.internet_activity DESC
        LIMIT 10
    """).fetchall()
    for r in rows:
        print(r)


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)

    build_tables(conn)
    load_dim_grid(conn)
    source_df = load_fact_and_dim_time(conn)
    build_indexes(conn)
    run_validations(conn, source_df)
    run_sample_queries(conn)

    conn.close()
    print(f"\nWarehouse built at: {DB_PATH}")

def build_warehouse_from_paths(hourly_parquet_dir, geojson_path, db_path):
    """Callable entry point for reuse from the DE7 DAG — same logic as
    main(), just parameterized instead of hardcoded to module-level paths."""
    global HOURLY_PARQUET_DIR, GEOJSON_PATH, DB_PATH
    HOURLY_PARQUET_DIR = hourly_parquet_dir
    GEOJSON_PATH = geojson_path
    DB_PATH = db_path

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    build_tables(conn)
    load_dim_grid(conn)
    source_df = load_fact_and_dim_time(conn)
    build_indexes(conn)
    run_validations(conn, source_df)

    # AS_OF: the latest timestamp actually present in the analytics layer
    as_of = conn.execute("SELECT MAX(timestamp) FROM dim_time").fetchone()[0]

    conn.close()
    return {
        "fact_rows": len(source_df),
        "as_of": as_of,
    }


if __name__ == "__main__":
    main()

