import sqlite3
import os

DB_PATH = os.environ.get(
    "WAREHOUSE_DB_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "phase3", "warehouse", "network_warehouse.db")
)

def get_connection():
    """Returns a new SQLite connection. Raises a clear error if the
    warehouse file doesn't exist, rather than a cryptic sqlite3 error."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Warehouse database not found at {DB_PATH}. Has DE6/DE7 been run?")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn