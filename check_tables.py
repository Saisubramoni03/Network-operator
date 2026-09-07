import sqlite3
conn = sqlite3.connect(r'C:\Network-operator\phase3\warehouse\network_warehouse.db')
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print(tables)
