import sqlite3
import os

db_path = 'C:/Users/sai.subramoni/Downloads/Networkproject/phase3/warehouse/network_warehouse.db'
print("File exists:", os.path.exists(db_path))

conn = sqlite3.connect(db_path)

count = conn.execute("SELECT COUNT(*) FROM fact_network_activity WHERE grid_id = 4821").fetchone()
print("Rows for grid 4821:", count)

total_rows = conn.execute("SELECT COUNT(*) FROM fact_network_activity").fetchone()
print("Total rows in fact table:", total_rows)

rows = conn.execute("""
    SELECT t.timestamp, f.total_activity
    FROM fact_network_activity f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.grid_id = 4821
    ORDER BY t.timestamp DESC
    LIMIT 3
""").fetchall()
print("Query result:", rows)