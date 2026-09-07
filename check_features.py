import sqlite3

conn = sqlite3.connect('C:/Users/sai.subramoni/Downloads/Networkproject/phase3/warehouse/network_warehouse.db')

# Stored feature row for grid 4821
feature_row = conn.execute("""
    SELECT feature_timestamp, avg_activity, peak_ratio
    FROM grid_features
    WHERE grid_id = 4821
    ORDER BY feature_timestamp DESC
    LIMIT 1
""").fetchone()
print("Stored feature row:", feature_row)

feature_timestamp = feature_row[0]

# Raw hourly data for the 24-hour window ending at that same timestamp
rows = conn.execute("""
    SELECT t.timestamp, f.total_activity
    FROM fact_network_activity f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.grid_id = 4821 AND t.timestamp <= ?
    ORDER BY t.timestamp DESC
    LIMIT 24
""", (feature_timestamp,)).fetchall()

vals = [r[1] for r in rows]
avg = sum(vals) / len(vals)
peak = max(vals)
print("Hand-computed avg_activity:", avg)
print("Hand-computed peak_ratio:", peak / avg)